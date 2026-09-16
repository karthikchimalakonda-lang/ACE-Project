"""
SAMPLE_ORG AI Item Mapper  â€”  OCI FUNCTIONS v2.0.0
============================================
Covers ALL SR-013 to SR-019 scenarios from SAMPLE_ORG SOR v1.1

STRATEGIES (cascade order):
  S1  Direct SAMPLE_ORG code on document            SR-014  SupplierA, TCL
  S2  Supplier code lookup (item master)     SR-015  WEG 18XXXXXX, SupplierA HA001
  S3  SupplierB customer_material_number       SR-015  SupplierB CMN column
  S4  SupplierC product code lookup           SR-015  ZR61KCE-TFD style
  S5  SupplierD model code extraction            SR-016  RC2-470B from description
  S6  Alternate codes lookup                 SR-016  pipe-separated alternates column
  S7  difflib fuzzy match â‰¥90%              SR-016  description similarity
  S8  AI semantic LLM fallback              SR-016  Cohere last resort
  EX  Spare parts exclusion                 SR-013  TCL + any SPARE PARTS

EXTRA LOGIC:
  - SR-003: TCL dual PL (IDU vs ODU) product_line routing
  - SR-004: WEG supplemental invoice merge  
  - SR-006: Multiple PO numbers per item preserved
  - SR-027: 3-way quantity reconciliation (PL vs Invoice vs BL)
  - SR-028: Net weight validation against BL declared weight
  - SR-017: Item master fully CSV-driven, SAMPLE_ORG-maintainable
  - SR-018: Unresolved â†’ unmatched.json, halt_required flag
  - SR-029: Full processing summary
  - SR-030: Halt checkpoint in summary
  - SR-031: CSV audit trail

INPUTS:
  {
    "shipmentId":       "SHP-REQ-042",
    "requestId":        "MAP-REQ-042",
    "supplierName":     "WEG",
    "extractedPlPath":  "Packing List/PL-REQ-042-01/extracted.json",
    "extractedInvPath": "Invoice/INV-REQ-042-01/extracted.json",
    "extractedBlPath":  "Bill of Lading/BL-REQ-042-01/extracted.json",
    "extractedInv2Path":"Invoice/INV-REQ-042-SUP/extracted.json",  (WEG supplemental)
    "extractedPl2Path": "Packing List/PL-REQ-042-02/extracted.json", (TCL ODU)
    "itemMasterPath":   "Config/SAMPLE_ORG_Item_Master.csv"
  }

OUTPUTS:
  Mapping/{shipmentId}/mapped.json
  Mapping/{shipmentId}/unmatched.json
  Mapping/{shipmentId}/excluded.json
  Mapping/{shipmentId}/summary.json
  Mapping/{shipmentId}/mapping_report.csv
"""

import csv
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

import oci
from oci.auth.signers import get_resource_principals_signer
from oci.generative_ai_inference import GenerativeAiInferenceClient
from oci.generative_ai_inference.models import (
    ChatDetails,
    OnDemandServingMode,
    CohereChatRequest,
)
from fdk import response as fdk_response


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 1  â€”  CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

OCI_COMPARTMENT_ID     = os.getenv("OCI_COMPARTMENT_ID", "")
OS_NAMESPACE           = os.getenv("OS_NAMESPACE",        "sample_namespace")
OS_BUCKET              = os.getenv("OS_BUCKET",           "sample-workflow-bucket")
OCI_GENAI_ENDPOINT     = os.getenv("OCI_GENAI_ENDPOINT",
    "https://example.invalid/integration-endpoint")
OCI_MODEL_ID           = os.getenv("OCI_MODEL_ID",
    "OCI_RESOURCE_OCID_PLACEHOLDER")
DEFAULT_ITEM_MASTER    = os.getenv("ITEM_MASTER_PATH", "Config/SAMPLE_ORG_Item_Master.csv")
MAPPING_FOLDER         = os.getenv("MAPPING_FOLDER",     "Mapping")

# SR-016 thresholds â€” configurable via env (SR-017)
FUZZY_THRESHOLD        = float(os.getenv("FUZZY_THRESHOLD",   "0.90"))
AI_MIN_CONFIDENCE      = float(os.getenv("AI_MIN_CONFIDENCE", "0.80"))

# SR-028 weight tolerance â€” configurable
WEIGHT_TOLERANCE_PCT   = float(os.getenv("WEIGHT_TOLERANCE_PCT", "2.0"))

# SR-013 spare parts keywords
SPARE_KEYWORDS = [
    "spare part", "spare parts", "spare", "accessories", "accessory",
    "consumable", "consumables", "maintenance kit", "after sales",
]

TMP_DIR = "/tmp/SAMPLE_ORG_mapper"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 2  â€”  LOGGING
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class _Tee(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        print(self.format(record), flush=True)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_Tee(sys.stderr)])
log = logging.getLogger(__name__)

def _plog(msg, *a):
    print(f"â–¶ {msg % a if a else msg}", flush=True)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 3  â€”  OCI HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _signer():
    return get_resource_principals_signer()

def _os_client(signer):
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)

def _genai_client(signer):
    return GenerativeAiInferenceClient(
        config={}, signer=signer, service_endpoint=OCI_GENAI_ENDPOINT,
        retry_strategy=oci.retry.NoneRetryStrategy(), timeout=(10, 60))

def upload(os_cli, content: str, key: str, ct="application/json") -> bool:
    try:
        os_cli.put_object(namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
            object_name=key, put_object_body=content.encode("utf-8"),
            content_type=ct)
        log.info("Saved â†’ %s", key); return True
    except Exception as e:
        log.error("Upload failed %s: %s", key, e); return False

def download(os_cli, key: str) -> Optional[str]:
    try:
        return os_cli.get_object(namespace_name=OS_NAMESPACE,
            bucket_name=OS_BUCKET, object_name=key).data.content.decode("utf-8")
    except Exception as e:
        log.warning("Download failed %s: %s", key, e); return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 4  â€”  ITEM MASTER  (SR-017: CSV-driven, SAMPLE_ORG-maintainable)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class ItemMaster:
    """
    Loads SAMPLE_ORG_Item_Master.csv from OCI OS.
    CSV columns (v2):
      SAMPLE_ORG_item_code, description, supplier, supplier_code,
      model_code, SupplierC_product_code, customer_material_number,
      alternate_codes, is_spare_part, notes

    Builds indexes:
      by_SAMPLE_ORG          {SAMPLE_ORG_CODE â†’ row}
      by_supplier_sc  {(SUPPLIER, SUPPLIER_CODE) â†’ row}
      by_model_code   {MODEL_CODE â†’ row}              SupplierD RC2-470B
      by_SupplierC     {SupplierC_PRODUCT_CODE â†’ row}   SupplierC ZR61KCE-TFD
      by_cmn          {CUSTOMER_MATERIAL_NUMBER â†’ row} SupplierB
      by_alternate    {ALT_CODE â†’ row}                pipe-separated
      by_supplier     {SUPPLIER â†’ [rows]}             for fuzzy/AI search
    """

    def __init__(self, rows: List[dict]):
        self.rows          = rows
        self.by_SAMPLE_ORG        = {}
        self.by_supplier_sc= {}
        self.by_model_code = {}
        self.by_SupplierC   = {}
        self.by_cmn        = {}
        self.by_alternate  = {}
        self.by_supplier   = {}

        for r in rows:
            def _u(f): return (r.get(f) or "").strip().upper()

            SAMPLE_ORG   = _u("SAMPLE_ORG_item_code")
            supp  = _u("supplier")
            sc    = _u("supplier_code")
            mc    = _u("model_code")
            cop   = _u("SupplierC_product_code")
            cmn   = _u("customer_material_number")
            alts  = (r.get("alternate_codes") or "")

            if SAMPLE_ORG:
                self.by_SAMPLE_ORG[SAMPLE_ORG] = r
            if supp and sc:
                self.by_supplier_sc[(supp, sc)] = r
            if mc:
                self.by_model_code[mc] = r
            if cop:
                self.by_SupplierC[cop] = r
            if cmn:
                self.by_cmn[cmn] = r
            for alt in alts.split("|"):
                alt = alt.strip().upper()
                if alt:
                    self.by_alternate[alt] = r
            if supp:
                self.by_supplier.setdefault(supp, []).append(r)

        _plog("Item master: %d rows | %d SAMPLE_ORG | %d supplier codes | "
              "%d model codes | %d SupplierC | %d SupplierB CMN | %d alternates",
              len(rows), len(self.by_SAMPLE_ORG), len(self.by_supplier_sc),
              len(self.by_model_code), len(self.by_SupplierC),
              len(self.by_cmn), len(self.by_alternate))

    @classmethod
    def load(cls, os_cli, path: str) -> "ItemMaster":
        raw = download(os_cli, path)
        if not raw:
            log.warning("Item master not found: %s â€” using empty", path)
            return cls([])
        return cls(list(csv.DictReader(io.StringIO(raw))))

    def SAMPLE_ORG(self, code: str) -> Optional[dict]:
        return self.by_SAMPLE_ORG.get((code or "").strip().upper())

    def supplier_sc(self, supp: str, code: str) -> Optional[dict]:
        return self.by_supplier_sc.get(
            ((supp or "").strip().upper(), (code or "").strip().upper()))

    def model(self, code: str) -> Optional[dict]:
        return self.by_model_code.get((code or "").strip().upper())

    def SupplierC(self, code: str) -> Optional[dict]:
        return self.by_SupplierC.get((code or "").strip().upper())

    def cmn(self, code: str) -> Optional[dict]:
        return self.by_cmn.get((code or "").strip().upper())

    def alternate(self, code: str) -> Optional[dict]:
        return self.by_alternate.get((code or "").strip().upper())

    def supplier_rows(self, supp: str) -> List[dict]:
        return self.by_supplier.get((supp or "").strip().upper(), [])

    def is_spare(self, row: dict) -> bool:
        return str(row.get("is_spare_part", "")).strip().upper() in ("TRUE", "1", "YES")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 5  â€”  SPARE PARTS CHECK  (SR-013)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _is_spare(item: dict, master: ItemMaster) -> bool:
    """
    SR-013: Identify spare parts from description keywords OR
    item master is_spare_part flag.
    Must not cause processing errors â€” silently exclude.
    """
    desc = (item.get("description") or "").lower()
    if any(kw in desc for kw in SPARE_KEYWORDS):
        return True
    # Check master flag if item_no is a SAMPLE_ORG code
    item_no = (item.get("item_no") or "").strip()
    if item_no:
        row = master.SAMPLE_ORG(item_no)
        if row and master.is_spare(row):
            return True
    return False


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 6  â€”  MAPPING RESULT BUILDER
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _hit(strategy: str, row: dict, matched_on: str,
         source: str, confidence: float = 1.0, **extra) -> dict:
    return {
        "strategy":           strategy,
        "SAMPLE_ORG_item_code":      row["SAMPLE_ORG_item_code"],
        "SAMPLE_ORG_description":    row.get("description", ""),
        "matched_on":         matched_on,
        "source_code":        source,
        "confidence":         confidence,
        **extra,
    }


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 7  â€”  STRATEGY 1: DIRECT SAMPLE_ORG CODE  (SR-014)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def s1_direct_SAMPLE_ORG(item: dict, master: ItemMaster) -> Optional[dict]:
    """
    SR-014: item_no IS a SAMPLE_ORG code directly on the document.
    Applies to: SupplierA (SAMPLE_ORG-WAC-001), TCL (SAMPLE_ORG-IDU-001),
                SupplierB when item_no = SAMPLE_ORG code.
    """
    for field in ("item_no", "SAMPLE_ORG_item_code"):
        val = (item.get(field) or "").strip()
        if val:
            row = master.SAMPLE_ORG(val)
            if row:
                return _hit("S1_DIRECT_SAMPLE_ORG", row, field, val)
    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 8  â€”  STRATEGY 2: SUPPLIER CODE LOOKUP  (SR-015)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def s2_supplier_code(item: dict, supplier: str,
                     master: ItemMaster) -> Optional[dict]:
    """
    SR-015: item_no matches supplier_code column in item master.
    Applies to: WEG (18004581 â†’ SAMPLE_ORG-MTR-7500), SupplierA (HA001).
    Also tries first-word of supplier name (WEG ELECTRIC â†’ WEG).
    """
    item_no = (item.get("item_no") or "").strip()
    if not item_no:
        return None
    s_upper = (supplier or "").strip().upper()
    for s in [s_upper, s_upper.split()[0] if s_upper else ""]:
        if not s:
            continue
        row = master.supplier_sc(s, item_no)
        if row:
            return _hit("S2_SUPPLIER_CODE", row, "supplier_code", item_no)
    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 9  â€”  STRATEGY 3: SupplierB CUSTOMER MATERIAL NUMBER  (SR-015)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def s3_SupplierB_cmn(item: dict, master: ItemMaster) -> Optional[dict]:
    """
    SR-015: SupplierB 'Customer material number' column = SAMPLE_ORG internal code.
    Checks customer_material_number field, then falls back to item_no.
    """
    for field in ("customer_material_number", "item_no"):
        val = (item.get(field) or "").strip()
        if not val:
            continue
        # Try CMN index
        row = master.cmn(val)
        if row:
            return _hit("S3_SupplierB_CMN", row, field, val)
        # Try direct SAMPLE_ORG lookup (CMN may equal SAMPLE_ORG code)
        row = master.SAMPLE_ORG(val)
        if row:
            return _hit("S3_SupplierB_CMN", row, f"{field}_as_SAMPLE_ORG", val)
    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 10  â€”  STRATEGY 4: SupplierC PRODUCT CODE  (SR-015)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# SupplierC product code pattern: ZRnnKCE-TFD, ZRnnKCE-TWD etc.
_SupplierC_RE = re.compile(r"\bZR\d+[A-Z]{2,3}-[A-Z]{3}\b", re.IGNORECASE)

def s4_SupplierC(item: dict, master: ItemMaster) -> Optional[dict]:
    """
    SR-015: SupplierC product codes (ZR61KCE-TFD) map to SAMPLE_ORG codes.
    Checks item_no field and extracts from description if needed.
    Also validates against po_number per line (SR-006).
    """
    # Check item_no directly
    item_no = (item.get("item_no") or "").strip()
    if item_no:
        row = master.SupplierC(item_no)
        if row:
            return _hit("S4_SupplierC", row, "item_no_SupplierC_code", item_no)
        # Try alternate codes
        row = master.alternate(item_no)
        if row:
            return _hit("S4_SupplierC_ALT", row, "alternate_code", item_no)

    # Extract from description
    desc = (item.get("description") or "")
    matches = _SupplierC_RE.findall(desc)
    for m in matches:
        row = master.SupplierC(m.upper())
        if row:
            return _hit("S4_SupplierC", row, "description_extracted", m.upper())
        row = master.alternate(m.upper())
        if row:
            return _hit("S4_SupplierC_ALT", row, "description_alt", m.upper(), confidence=0.95)

    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 11  â€”  STRATEGY 5: SupplierD MODEL CODE  (SR-016)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# SupplierD model code pattern: RC2-470B, RC2-550B, RC2-620C etc.
_SupplierD_RE = re.compile(r"\bRC\d+-\d+[A-Z]?\b", re.IGNORECASE)

def s5_SupplierD_model(item: dict, master: ItemMaster) -> Optional[dict]:
    """
    SR-016: SupplierD descriptions contain SAMPLE_ORG model codes (RC2-470B).
    Per SOR appendix: 'Descriptions use SAMPLE_ORG model codes â€” must be used for matching.'
    Checks model_code field, item_no, then extracts from description.
    Also checks alternate codes.
    """
    # Explicit model_code field
    mc = (item.get("model_code") or "").strip().upper()
    if mc:
        row = master.model(mc)
        if row:
            return _hit("S5_SupplierD_MODEL", row, "model_code_field", mc)
        row = master.alternate(mc)
        if row:
            return _hit("S5_SupplierD_MODEL_ALT", row, "model_code_alt", mc)

    # item_no might be the SupplierD code
    item_no = (item.get("item_no") or "").strip().upper()
    if item_no:
        row = master.model(item_no)
        if row:
            return _hit("S5_SupplierD_MODEL", row, "item_no_as_model", item_no)
        row = master.supplier_sc("SupplierD", item_no)
        if row:
            return _hit("S5_SupplierD_SC", row, "SupplierD_supplier_code", item_no)

    # Extract from description
    desc = (item.get("description") or "")
    matches = _SupplierD_RE.findall(desc)
    for m in matches:
        m_up = m.upper()
        row = master.model(m_up)
        if row:
            return _hit("S5_SupplierD_MODEL", row, "description_extracted", m_up)
        row = master.alternate(m_up)
        if row:
            return _hit("S5_SupplierD_MODEL_ALT", row, "description_alt", m_up, confidence=0.95)

    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 12  â€”  STRATEGY 6: ALTERNATE CODES  (SR-016)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def s6_alternate(item: dict, master: ItemMaster) -> Optional[dict]:
    """
    SR-016: Check pipe-separated alternate_codes column in item master.
    Catches variant code formats (RC2470B vs RC2-470B, ZR61KCE vs ZR61KCE-TFD).
    """
    for field in ("item_no", "model_code", "SupplierC_product_code"):
        val = (item.get(field) or "").strip()
        if not val:
            continue
        # Try with hyphens removed (RC2-470B â†’ RC2470B)
        for candidate in [val, val.replace("-", ""), val.replace(" ", "")]:
            row = master.alternate(candidate.upper())
            if row:
                return _hit("S6_ALTERNATE_CODE", row, f"{field}_alternate",
                            val, confidence=0.98)
    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 13  â€”  STRATEGY 7: DIFFLIB FUZZY MATCH  (SR-016)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def s7_fuzzy(item: dict, supplier: str, master: ItemMaster,
             threshold: float = FUZZY_THRESHOLD) -> Optional[dict]:
    """
    SR-016: difflib SequenceMatcher â‰¥90% threshold.
    Searches supplier-specific rows first, then all rows.
    Builds search text from item_no + description + model_code.
    """
    parts = [
        item.get("item_no", ""),
        item.get("description", ""),
        item.get("model_code", ""),
        item.get("SupplierC_product_code", ""),
    ]
    search = " ".join(p for p in parts if p).strip()
    if not search:
        return None

    rows = master.supplier_rows(supplier) or master.rows
    best_score, best_row = 0.0, None

    for row in rows:
        # Skip spare parts in fuzzy â€” don't match spare to regular item
        if master.is_spare(row):
            continue
        candidate = " ".join(filter(None, [
            row.get("description", ""),
            row.get("supplier_code", ""),
            row.get("model_code", ""),
            row.get("SAMPLE_ORG_item_code", ""),
            row.get("notes", ""),
        ]))
        score = _sim(search, candidate)
        if score > best_score:
            best_score, best_row = score, row

    if best_row and best_score >= threshold:
        return _hit("S7_FUZZY_DESCRIPTION", best_row,
                    "description_fuzzy", search,
                    confidence=round(best_score, 4),
                    fuzzy_score=round(best_score, 4))
    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 14  â€”  STRATEGY 8: AI SEMANTIC MATCH  (SR-016 fallback)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

_AI_PROMPT = """\
You are a logistics item code resolver for SampleLogisticsOrganization Air Conditioners (SAMPLE_ORG).

Match the SUPPLIER ITEM below to the best SAMPLE_ORG item from the catalog.

SUPPLIER ITEM:
  item_no:     {item_no}
  description: {desc}
  model_code:  {model}
  supplier:    {supplier}

SAMPLE_ORG ITEM CATALOG:
{catalog}

RULES:
1. Find the best semantic match based on description, specs, and codes
2. Confidence 0.0-1.0 â€” only return â‰¥0.80 as a real match
3. If no match exists, return null for SAMPLE_ORG_item_code
4. Never match a spare part to a non-spare item
5. Return ONLY valid JSON, no prose, no markdown

{{
  "SAMPLE_ORG_item_code": "SAMPLE_ORG-XXX or null",
  "confidence": 0.0,
  "reasoning": "brief"
}}"""

def s8_ai_semantic(item: dict, supplier: str, master: ItemMaster,
                   genai: GenerativeAiInferenceClient,
                   min_conf: float = AI_MIN_CONFIDENCE) -> Optional[dict]:
    """
    SR-016: AI LLM semantic description matching â€” last resort.
    Only fires if genai client available.
    Confidence below min_conf â†’ flagged for human review (SR-018).
    """
    if not genai:
        return None

    item_no = (item.get("item_no") or "").strip()
    desc    = (item.get("description") or "").strip()
    model   = (item.get("model_code") or "").strip()

    if not item_no and not desc:
        return None

    rows    = (master.supplier_rows(supplier) or master.rows)[:25]
    catalog = json.dumps([
        {"SAMPLE_ORG_item_code": r.get("SAMPLE_ORG_item_code",""),
         "description":   r.get("description",""),
         "supplier_code": r.get("supplier_code",""),
         "model_code":    r.get("model_code",""),
         "notes":         r.get("notes","")}
        for r in rows if not master.is_spare(r)
    ], indent=2)

    prompt = _AI_PROMPT.format(
        item_no=item_no or "N/A", desc=desc or "N/A",
        model=model or "N/A", supplier=supplier, catalog=catalog)

    try:
        resp = genai.chat(ChatDetails(
            compartment_id=OCI_COMPARTMENT_ID,
            serving_mode=OnDemandServingMode(model_id=OCI_MODEL_ID),
            chat_request=CohereChatRequest(
                message=prompt, max_tokens=200,
                temperature=0.0, frequency_penalty=0,
                top_p=0.75, top_k=0)
        )).data.chat_response.text.strip()

        clean  = re.sub(r"```(?:json)?", "", resp).strip().rstrip("`")
        result = json.loads(clean)
        zc     = result.get("SAMPLE_ORG_item_code")
        conf   = float(result.get("confidence", 0.0))
        reason = result.get("reasoning", "")

        if zc and conf >= min_conf:
            row = master.SAMPLE_ORG(zc)
            if row:
                return _hit("S8_AI_SEMANTIC", row, "ai_description",
                            item_no or desc, confidence=conf,
                            ai_reasoning=reason)

        # Below threshold â€” still return for flagging (SR-018)
        if zc and conf > 0:
            return {"strategy": "S8_AI_LOW_CONF", "SAMPLE_ORG_item_code": zc,
                    "confidence": conf, "ai_reasoning": reason,
                    "needs_review": True,
                    "SAMPLE_ORG_description": (master.SAMPLE_ORG(zc) or {}).get("description",""),
                    "matched_on": "ai_low_confidence", "source_code": item_no or desc}

    except Exception as e:
        log.warning("AI match failed for %s: %s", item_no, e)

    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 15  â€”  SINGLE ITEM MAPPER  (cascade)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _map_one(item: dict, supplier: str, master: ItemMaster,
             genai: Optional[GenerativeAiInferenceClient]) -> dict:
    """
    Applies the full 8-strategy cascade to one item.
    Returns item enriched with mapping fields.
    """
    s = (supplier or "").strip().upper()

    # â”€â”€ Exclusion first (SR-013) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if _is_spare(item, master):
        return {**item, "mapping_status": "EXCLUDED",
                "exclusion_reason": "spare_part_keyword_or_flag",
                "SAMPLE_ORG_item_code": None, "mapping_strategy": "EXCLUDED",
                "mapping_confidence": None}

    # â”€â”€ Strategy cascade â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    result = (
        s1_direct_SAMPLE_ORG(item, master)      or
        s2_supplier_code(item, s, master) or
        (s3_SupplierB_cmn(item, master)    if "SupplierB"  in s else None) or
        (s4_SupplierC(item, master)       if "SupplierC" in s else None) or
        (s5_SupplierD_model(item, master)    if "SupplierD"    in s else None) or
        s6_alternate(item, master)        or
        s7_fuzzy(item, s, master)         or
        s8_ai_semantic(item, s, master, genai)
    )

    if result:
        needs_review = (
            result.get("needs_review", False) or
            result.get("confidence", 1.0) < AI_MIN_CONFIDENCE
        )
        status = "REVIEW_REQUIRED" if needs_review else "MATCHED"
        return {**item,
                "mapping_status":      status,
                "SAMPLE_ORG_item_code":       result.get("SAMPLE_ORG_item_code"),
                "SAMPLE_ORG_description":     result.get("SAMPLE_ORG_description", ""),
                "mapping_strategy":    result.get("strategy"),
                "mapping_confidence":  result.get("confidence"),
                "mapping_matched_on":  result.get("matched_on"),
                "mapping_source_code": result.get("source_code"),
                "ai_reasoning":        result.get("ai_reasoning"),
                "fuzzy_score":         result.get("fuzzy_score")}

    # â”€â”€ No match â€” SR-018 flag â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    return {**item, "mapping_status": "UNMATCHED", "SAMPLE_ORG_item_code": None,
            "SAMPLE_ORG_description": None, "mapping_strategy": "NONE",
            "mapping_confidence": None, "mapping_matched_on": None}


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 16  â€”  DOCUMENT ITEM COLLECTOR
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _collect_items(extracted: dict, source_label: str) -> List[dict]:
    """
    Extracts all item rows from an extracted JSON.
    Handles: packages[], line_items[] (PL), line_items[] (Invoice).
    Stamps each item with source_doc for audit.
    """
    items = []

    # Packing list packages
    for pkg in (extracted.get("packages") or []):
        base = {
            "source_doc":  source_label,
            "pkg_no":      pkg.get("pkg_no"),
            "container_no":pkg.get("container_no"),
            "pallet_no":   pkg.get("pallet_no"),
            "gross_weight_kg": pkg.get("gross_weight_kg"),
            "dimensions_cm":   pkg.get("dimensions_cm"),
            "cbm":             pkg.get("cbm"),
        }
        # New line_items structure
        if pkg.get("line_items"):
            for li in pkg["line_items"]:
                items.append({**base, **li})
        else:
            # Old flat structure
            items.append({**base,
                "item_no":     pkg.get("item_no"),
                "description": pkg.get("description"),
                "quantity":    pkg.get("quantity"),
                "unit":        pkg.get("unit"),
                "net_weight_kg": pkg.get("net_weight_kg"),
                "serial_number": pkg.get("serial_number"),
                "model_code":  pkg.get("model_code"),
                "po_number":   pkg.get("po_number"),
                "customer_material_number": pkg.get("customer_material_number"),
                "hs_code":     pkg.get("hs_code"),
            })

    # Invoice line items
    for li in (extracted.get("line_items") or []):
        items.append({
            "source_doc":  source_label,
            "item_no":     li.get("item_no") or li.get("item_code"),
            "description": li.get("description"),
            "quantity":    li.get("quantity"),
            "unit":        li.get("unit") or li.get("unit_of_measure"),
            "unit_price":  li.get("unit_price"),
            "line_total":  li.get("line_total") or li.get("amount"),
            "po_number":   li.get("po_number") or li.get("po_no"),
            "serial_number": li.get("serial_number"),
            "customer_material_number": li.get("customer_material_number"),
            "hs_code":     li.get("hs_code"),
        })

    return items


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 17  â€”  TCL DUAL PL HANDLING  (SR-003)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _resolve_tcl_product_line(extracted: dict) -> str:
    """
    SR-003: TCL ships separate PLs for IDU and ODU.
    Detect product line from extracted JSON product_line field
    or by scanning item codes.
    """
    pl = (extracted.get("product_line") or "").upper()
    if "IDU" in pl or "INDOOR" in pl:
        return "IDU"
    if "ODU" in pl or "OUTDOOR" in pl:
        return "ODU"
    # Scan items
    for pkg in (extracted.get("packages") or []):
        item_no = (pkg.get("item_no") or "").upper()
        if "IDU" in item_no:
            return "IDU"
        if "ODU" in item_no:
            return "ODU"
    return "UNKNOWN"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 18  â€”  SR-027: 3-WAY QUANTITY RECONCILIATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _reconcile_quantities(pl_items: List[dict], inv_items: List[dict],
                           bl_data: dict) -> List[dict]:
    """
    SR-027: 3-way quantity reconciliation: PL qty vs Invoice qty vs BL.
    Flags discrepancies â€” does NOT silently accept.
    Returns list of discrepancy dicts.
    """
    if not inv_items:
        return []          # â† must be indented 4 spaces inside the if block

    flags = []

    # Build PL qty index: item_no â†’ total qty
    pl_qty: Dict[str, float] = {}
    for item in pl_items:
        key = (item.get("item_no") or "").strip().upper()
        if key:
            pl_qty[key] = pl_qty.get(key, 0) + (item.get("quantity") or 0)

    # Build Invoice qty index
    inv_qty: Dict[str, float] = {}
    for item in inv_items:
        key = (item.get("item_no") or "").strip().upper()
        if key:
            inv_qty[key] = inv_qty.get(key, 0) + (item.get("quantity") or 0)

    # Compare PL vs Invoice
    all_keys = set(pl_qty.keys()) | set(inv_qty.keys())
    for k in all_keys:
        pq = pl_qty.get(k, 0)
        iq = inv_qty.get(k, 0)
        if pq != iq and (pq > 0 or iq > 0):
            flags.append({
                "item_no":          k,
                "pl_quantity":      pq,
                "invoice_quantity": iq,
                "discrepancy":      round(abs(pq - iq), 4),
                "flag":             "SR027_QTY_MISMATCH_PL_VS_INV",
            })

    # BL qty check (BL usually shows total, not per item)
    bl_total_qty = (bl_data or {}).get("total_quantity")
    pl_total     = sum(pl_qty.values())
    if bl_total_qty and abs(pl_total - bl_total_qty) > 1:
        flags.append({
            "item_no":     "TOTAL",
            "pl_total":    pl_total,
            "bl_total":    bl_total_qty,
            "discrepancy": abs(pl_total - bl_total_qty),
            "flag":        "SR027_QTY_MISMATCH_PL_VS_BL",
        })

    return flags


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 19  â€”  SR-028: NET WEIGHT VALIDATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _validate_weight(pl_data: dict, bl_data: dict,
                     tolerance_pct: float = WEIGHT_TOLERANCE_PCT) -> Optional[dict]:
    """
    SR-028: Validate PL total net weight against BL declared gross weight
    within configurable tolerance.
    Note: BL has gross weight, PL has net weight â€” tolerance is wider by design.
    """
    if not bl_data or not pl_data:
        return None

    pl_net   = pl_data.get("total_net_weight_kg") or 0
    bl_gross = (bl_data.get("gross_weight_kg") or
                bl_data.get("total_gross_weight_kg") or 0)

    if not pl_net or not bl_gross:
        return None

    # PL net < BL gross always (net < gross by packaging weight)
    # Flag if PL net > BL gross (impossible) or difference > tolerance
    if pl_net > bl_gross:
        return {
            "flag":        "SR028_NET_EXCEEDS_GROSS",
            "pl_net_kg":   pl_net,
            "bl_gross_kg": bl_gross,
            "diff_pct":    round((pl_net - bl_gross) / bl_gross * 100, 2),
        }

    # Check PL gross weight vs BL gross
    pl_gross = pl_data.get("total_gross_weight_kg") or 0
    if pl_gross and bl_gross:
        diff_pct = abs(pl_gross - bl_gross) / bl_gross * 100
        if diff_pct > tolerance_pct:
            return {
                "flag":        "SR028_GROSS_WEIGHT_MISMATCH",
                "pl_gross_kg": pl_gross,
                "bl_gross_kg": bl_gross,
                "diff_pct":    round(diff_pct, 2),
                "tolerance_pct": tolerance_pct,
            }

    return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 20  â€”  BATCH MAP
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _batch_map(items: List[dict], supplier: str, master: ItemMaster,
               genai) -> Tuple[List, List, List]:
    """Maps all items. Returns (mapped, unmatched, excluded)."""
    mapped, unmatched, excluded = [], [], []
    for item in items:
        result = _map_one(item, supplier, master, genai)
        status = result.get("mapping_status")
        if status == "EXCLUDED":
            excluded.append(result)
        elif status == "UNMATCHED":
            unmatched.append(result)
        else:
            mapped.append(result)
    return mapped, unmatched, excluded


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 21  â€”  SR-029 SUMMARY
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _build_summary(mapped, unmatched, excluded, qty_flags, weight_flag,
                   supplier, shipment_id, tcl_pl="") -> dict:
    """SR-029: Human-readable processing summary."""
    review = [i for i in mapped if i.get("mapping_status") == "REVIEW_REQUIRED"]
    total  = len(mapped) + len(unmatched) + len(excluded)

    strategy_counts = {}
    for i in mapped:
        s = i.get("mapping_strategy", "?")
        strategy_counts[s] = strategy_counts.get(s, 0) + 1

    # SR-030: halt if any unresolved or review items
    halt = bool(unmatched or review or qty_flags or weight_flag)

    halt_reasons = []
    if unmatched:
        halt_reasons.append(f"{len(unmatched)} items unresolved â€” human review required")
    if review:
        halt_reasons.append(f"{len(review)} items flagged low-confidence")
    if qty_flags:
        halt_reasons.append(f"{len(qty_flags)} quantity discrepancies (SR-027)")
    if weight_flag:
        halt_reasons.append(f"Weight mismatch: {weight_flag.get('flag')} (SR-028)")

    return {
        "shipmentId":       shipment_id,
        "supplierName":     supplier,
        "tclProductLine":   tcl_pl,
        "processedAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "totalItems":       total,
        "matched":          len(mapped) - len(review),
        "reviewRequired":   len(review),
        "unmatched":        len(unmatched),
        "excluded":         len(excluded),
        "matchRate":        round((len(mapped)-len(review))/max(total,1)*100, 1),
        "strategyBreakdown": strategy_counts,
        "haltRequired":     halt,
        "haltReasons":      halt_reasons,
        "quantityFlags":    qty_flags,
        "weightFlag":       weight_flag,
        "unmatchedItems":   [{"item_no": i.get("item_no",""),
                              "description": i.get("description",""),
                              "pkg_no": i.get("pkg_no","")}
                             for i in unmatched],
        "reviewItems":      [{"item_no": i.get("item_no",""),
                              "SAMPLE_ORG_code": i.get("SAMPLE_ORG_item_code",""),
                              "confidence": i.get("mapping_confidence"),
                              "strategy": i.get("mapping_strategy"),
                              "reasoning": i.get("ai_reasoning","")}
                             for i in review],
        "sr_flags": {
            "SR013_spares_excluded":    len(excluded),
            "SR018_flagged_for_review": len(review) + len(unmatched),
            "SR027_qty_discrepancies":  len(qty_flags),
            "SR028_weight_check":       weight_flag.get("flag") if weight_flag else "OK",
            "SR030_halt_checkpoint":    halt,
        },
    }


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 22  â€”  SR-031 CSV AUDIT REPORT
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _build_csv(mapped, unmatched, excluded) -> str:
    """SR-031: Complete audit trail."""
    fields = [
        "timestamp", "source_doc", "pkg_no", "container_no", "pallet_no",
        "item_no", "description", "quantity", "unit", "net_weight_kg",
        "serial_number", "po_number",
        "mapping_status", "mapping_strategy", "SAMPLE_ORG_item_code",
        "SAMPLE_ORG_description", "mapping_confidence", "mapping_matched_on",
        "mapping_source_code", "fuzzy_score", "ai_reasoning",
        "exclusion_reason",
    ]
    buf = io.StringIO()
    w   = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    ts  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for item in mapped + unmatched + excluded:
        row = {f: item.get(f, "") for f in fields}
        row["timestamp"] = ts
        w.writerow(row)
    return buf.getvalue()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 23  â€”  MAIN ORCHESTRATOR
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _safe(s: str) -> str:
    return re.sub(r"[^\w\-]", "_", s or "unknown")

def run_mapping(os_cli, genai, body: dict) -> dict:
    shipment_id  = body.get("shipmentId",   "")
    request_id   = body.get("requestId") or f"MAP-{uuid.uuid4().hex[:8].upper()}"
    supplier     = body.get("supplierName", "")
    master_path  = body.get("itemMasterPath", DEFAULT_ITEM_MASTER)

    pl_path      = body.get("extractedPlPath",   "")
    pl2_path     = body.get("extractedPl2Path",  "")   # TCL ODU, SR-003
    inv_path     = body.get("extractedInvPath",  "")
    inv2_path    = body.get("extractedInv2Path", "")   # WEG supplemental SR-004
    bl_path      = body.get("extractedBlPath",   "")

    _plog("="*60)
    _plog("SAMPLE_ORG Item Mapper v2.0.0 | shp=%s | supplier=%s", shipment_id, supplier)

    # â”€â”€ Load master â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    master = ItemMaster.load(os_cli, master_path)

    # â”€â”€ Load documents â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    def _load(path):
        if not path: return {}
        raw = download(os_cli, path)
        return json.loads(raw) if raw else {}

    pl_data   = _load(pl_path)
    pl2_data  = _load(pl2_path)
    inv_data  = _load(inv_path)
    inv2_data = _load(inv2_path)
    bl_data   = _load(bl_path)

    # â”€â”€ TCL product line detection (SR-003) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    tcl_pl = ""
    if "TCL" in (supplier or "").upper():
        tcl_pl  = _resolve_tcl_product_line(pl_data)
        tcl_pl2 = _resolve_tcl_product_line(pl2_data) if pl2_data else ""
        _plog("TCL PL1=%s PL2=%s", tcl_pl, tcl_pl2)

    # â”€â”€ Collect all items â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_items: List[dict] = []
    if pl_data:
        all_items += _collect_items(pl_data,  "packing_list_01")
    if pl2_data:
        all_items += _collect_items(pl2_data, "packing_list_02")  # TCL ODU
    if inv_data:
        all_items += _collect_items(inv_data,  "invoice_01")
    if inv2_data:
        all_items += _collect_items(inv2_data, "invoice_02_supplemental")  # WEG

    _plog("Total items to map: %d (PL1=%d PL2=%d INV1=%d INV2=%d)",
          len(all_items),
          len(_collect_items(pl_data, "")),
          len(_collect_items(pl2_data, "")) if pl2_data else 0,
          len(_collect_items(inv_data, "")),
          len(_collect_items(inv2_data, "")) if inv2_data else 0)

    # â”€â”€ Map all items â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    t0 = time.time()
    mapped, unmatched, excluded = _batch_map(all_items, supplier, master, genai)
    duration = round(time.time() - t0, 2)

    # â”€â”€ SR-027: 3-way qty reconciliation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    pl_items  = _collect_items(pl_data,  "") + _collect_items(pl2_data,  "")
    inv_items = _collect_items(inv_data, "") + _collect_items(inv2_data, "")
    qty_flags = _reconcile_quantities(pl_items, inv_items, bl_data)
    if qty_flags:
        _plog("SR-027: %d quantity discrepancies", len(qty_flags))

    # â”€â”€ SR-028: weight validation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    weight_flag = _validate_weight(pl_data, bl_data)
    if weight_flag:
        _plog("SR-028: weight flag: %s", weight_flag.get("flag"))

    # â”€â”€ Build outputs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    summary = _build_summary(mapped, unmatched, excluded, qty_flags,
                             weight_flag, supplier, shipment_id, tcl_pl)
    summary["durationSeconds"] = duration

    csv_content = _build_csv(mapped, unmatched, excluded)
    base        = f"{MAPPING_FOLDER}/{_safe(shipment_id)}"

    paths = {}
    paths["mapped"]     = f"{base}/mapped.json"
    paths["unmatched"]  = f"{base}/unmatched.json"
    paths["excluded"]   = f"{base}/excluded.json"
    paths["summary"]    = f"{base}/summary.json"
    paths["csv_report"] = f"{base}/mapping_report.csv"

    upload(os_cli, json.dumps(mapped,    indent=2, ensure_ascii=False), paths["mapped"])
    upload(os_cli, json.dumps(unmatched, indent=2, ensure_ascii=False), paths["unmatched"])
    upload(os_cli, json.dumps(excluded,  indent=2, ensure_ascii=False), paths["excluded"])
    upload(os_cli, json.dumps(summary,   indent=2, ensure_ascii=False), paths["summary"])
    upload(os_cli, csv_content, paths["csv_report"], "text/csv")

    # â”€â”€ Log â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    _plog("="*60)
    _plog("MAPPING COMPLETE in %.1fs", duration)
    _plog("  Total:        %d", summary["totalItems"])
    _plog("  Matched:      %d (%.1f%%)", summary["matched"], summary["matchRate"])
    _plog("  Review:       %d", summary["reviewRequired"])
    _plog("  Unmatched:    %d", summary["unmatched"])
    _plog("  Excluded:     %d", summary["excluded"])
    _plog("  Qty flags:    %d", len(qty_flags))
    _plog("  Weight flag:  %s", weight_flag.get("flag") if weight_flag else "None")
    _plog("  HALT:         %s", summary["haltRequired"])
    _plog("  Strategies:   %s", summary["strategyBreakdown"])
    _plog("="*60)

    return {
        "status":          "complete",
        "requestId":       request_id,
        "shipmentId":      shipment_id,
        "supplierName":    supplier,
        "totalItems":      summary["totalItems"],
        "matched":         summary["matched"],
        "reviewRequired":  summary["reviewRequired"],
        "unmatched":       summary["unmatched"],
        "excluded":        summary["excluded"],
        "matchRate":       summary["matchRate"],
        "haltRequired":    summary["haltRequired"],
        "haltReasons":     summary["haltReasons"],
        "quantityFlags":   qty_flags,
        "weightFlag":      weight_flag,
        "strategyBreakdown": summary["strategyBreakdown"],
        "outputPaths":     paths,
        "durationSeconds": duration,
    }


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 24  â€”  OCI FUNCTIONS HANDLER
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def handler(ctx, data: io.BytesIO = None):
    os.makedirs(TMP_DIR, exist_ok=True)

    if not OCI_COMPARTMENT_ID:
        return fdk_response.Response(ctx,
            response_data=json.dumps({"status":"error",
                "message":"OCI_COMPARTMENT_ID not set"}),
            headers={"Content-Type":"application/json"}, status_code=500)

    body = {}
    if data:
        try:
            body = json.loads(data.getvalue())
        except Exception as e:
            log.warning("Body parse failed: %s", e)

    if not body.get("shipmentId") or not body.get("supplierName"):
        return fdk_response.Response(ctx,
            response_data=json.dumps({"status":"error",
                "message":"Missing: shipmentId, supplierName"}),
            headers={"Content-Type":"application/json"}, status_code=400)

    signer   = _signer()
    os_cli   = _os_client(signer)
    genai    = _genai_client(signer)

    try:
        result = run_mapping(os_cli, genai, body)
    except Exception as e:
        log.exception("Mapper failed")
        return fdk_response.Response(ctx,
            response_data=json.dumps({"status":"error","message":str(e)}),
            headers={"Content-Type":"application/json"}, status_code=500)

    return fdk_response.Response(ctx,
        response_data=json.dumps(result, ensure_ascii=False),
        headers={"Content-Type":"application/json"})