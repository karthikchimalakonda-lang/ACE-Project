"""
OTM Packing List Extractor  â€”  OCI FUNCTIONS VERSION  v5.0.0
=============================================================
Changes from v4:
  - Folder structure: all files stored under Packing List/{requestId}/
      chunks  â†’ Packing List/{requestId}/chunks/chunk_{N}.json
      job csv â†’ Packing List/{requestId}/job.csv
      manifestâ†’ Packing List/{requestId}/manifest.json  (written by orchestrator)
      extractedâ†’ Packing List/{requestId}/extracted.json
      otm     â†’ Packing List/{requestId}/otm.json
  - Total net weight always recomputed from full merged package list (not chunk-1 value)
  - CSV fresh-start: chunk 1 always overwrites (no stale rows from prior runs)
  - Inline stdout logging for OCI Code Editor visibility
"""
 
import concurrent.futures
import io
import json
import logging
import math
import os
import random
import re
import sys
import time
from typing import List, Optional, Tuple
 
import oci
from oci.auth.signers import get_resource_principals_signer
from oci.generative_ai_inference import GenerativeAiInferenceClient
from oci.generative_ai_inference.models import (
    ChatDetails,
    OnDemandServingMode,
    CohereChatRequest,
)
from oci.functions.functions_invoke_client import FunctionsInvokeClient
from fdk import response as fdk_response
 
 
 
OCI_GENAI_ENDPOINT = os.getenv(
    "OCI_GENAI_ENDPOINT",
    "https://example.invalid/integration-endpoint",
)
OCI_MODEL_ID = os.getenv(
    "OCI_MODEL_ID",
    "OCI_RESOURCE_OCID_PLACEHOLDER",
)
OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
 
OS_NAMESPACE   = os.getenv("OS_NAMESPACE", "sample_namespace")
OS_BUCKET      = os.getenv("OS_BUCKET",    "sample-workflow-bucket")
PL_BASE_FOLDER = os.getenv("OS_PL_BASE_FOLDER", "Packing List")   # â† single root
 
MAX_TOKENS     = int(os.getenv("MAX_TOKENS",     "4096"))
MAX_WORKERS    = int(os.getenv("MAX_WORKERS",    "10"))
DENSE_MAX_ROWS = int(os.getenv("DENSE_MAX_ROWS", "20"))
 
LLM_MAX_RETRIES   = int(os.getenv("LLM_MAX_RETRIES",   "3"))
LLM_RETRY_BACKOFF = float(os.getenv("LLM_RETRY_BACKOFF", "2.0"))
 
EXTRACTOR_FUNCTION_ID = os.getenv("EXTRACTOR_FUNCTION_ID", "")
EXTRACTOR_ENDPOINT    = os.getenv(
    "EXTRACTOR_ENDPOINT",
    "https://example.invalid/integration-endpoint",
)
 
MERGER_FUNCTION_ID = os.getenv("MERGER_FUNCTION_ID", "")
MERGER_ENDPOINT    = os.getenv("MERGER_ENDPOINT", "https://example.invalid/integration-endpoint")
SHIPMENTS_FOLDER   = os.getenv("SHIPMENTS_FOLDER", "Shipments")
 
OTM_PROCESS_IN_SEQUENCE = os.getenv("OTM_PROCESS_IN_SEQUENCE", "true").lower() == "true"
OTM_CONTENT_TYPE        = os.getenv(
    "OTM_CONTENT_TYPE",
    "application/vnd.oracle.resource+json;type=singular",
)
OTM_HTTP_METHOD        = os.getenv("OTM_HTTP_METHOD",        "PATCH")
OTM_DOMAIN_NAME        = os.getenv("OTM_DOMAIN_NAME",        "SAMPLE_ORG")
OTM_DEFAULT_WEIGHT_UOM = os.getenv("OTM_DEFAULT_WEIGHT_UOM", "KG")
OTM_DEFAULT_VOLUME_UOM = os.getenv("OTM_DEFAULT_VOLUME_UOM", "CBM")
OTM_DEFAULT_QTY_UOM    = os.getenv("OTM_DEFAULT_QTY_UOM",    "PKG")
 
TMP_DIR = "/tmp/pl_extractor"
 
 
 
class _TeeHandler(logging.StreamHandler):
    """Writes to both stderr (OCI log capture) and stdout (Code Editor console)."""
    def emit(self, record):
        super().emit(record)
        msg = self.format(record)
        print(msg, flush=True)           # â† visible in OCI Code Editor invoke panel
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_TeeHandler(sys.stderr)],
)
log = logging.getLogger(__name__)
 
def _plog(msg, *args):
    """Progress log â€” single line printed to stdout with a â–¶ prefix for easy scanning."""
    formatted = msg % args if args else msg
    print(f"â–¶ {formatted}", flush=True)
 
 
 
def _req_folder(safe_req: str) -> str:
    return f"{PL_BASE_FOLDER}/{safe_req}"
 
def _chunk_key(safe_req: str, chunk_idx: int) -> str:
    return f"{_req_folder(safe_req)}/chunks/chunk_{chunk_idx}.json"
 
def _manifest_key(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/manifest.json"
 
def _csv_key(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/job.csv"
 
def _extracted_key(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/extracted.json"
 
def _otm_key(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/otm.json"
 
def _chunk_prefix(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/chunks/chunk_"
 
 
 
def get_genai_client() -> GenerativeAiInferenceClient:
    signer = get_resource_principals_signer()
    client = GenerativeAiInferenceClient(
        config={},
        signer=signer,
        service_endpoint=OCI_GENAI_ENDPOINT,
        retry_strategy=oci.retry.NoneRetryStrategy(),
        timeout=(10, 240),
    )
    log.info("GenAI client ready â†’ %s", OCI_GENAI_ENDPOINT)
    return client
 
def get_object_storage_client():
    signer = get_resource_principals_signer()
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
 
def get_functions_client():
    signer = get_resource_principals_signer()
    return FunctionsInvokeClient(
        config={},
        signer=signer,
        service_endpoint=EXTRACTOR_ENDPOINT,
        timeout=(10, 30),
    )
 
 
 
def upload_to_os(os_client, content, object_name, content_type="application/json"):
    body = content.encode("utf-8") if isinstance(content, str) else content
    try:
        os_client.put_object(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
            object_name=object_name, put_object_body=body, content_type=content_type,
        )
        log.info("Saved â†’ %s", object_name)
        return True
    except Exception as exc:
        log.error("Upload failed '%s': %s", object_name, exc)
        return False
 
def download_from_os(os_client, object_name) -> Optional[str]:
    try:
        resp = os_client.get_object(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=object_name,
        )
        return resp.data.content.decode("utf-8")
    except Exception as exc:
        log.warning("Download failed '%s': %s", object_name, exc)
        return None
 
def list_os_keys(os_client, prefix: str) -> List[str]:
    try:
        resp = os_client.list_objects(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=prefix,
        )
        return [o.name for o in resp.data.objects]
    except Exception as exc:
        log.warning("List failed prefix='%s': %s", prefix, exc)
        return []
 
def write_csv(os_client, csv_key: str, rows: List[dict], fields: List[str]):
    """Write CSV from scratch (always overwrites â€” clean per run)."""
    output = io.StringIO()
    output.write(",".join(fields) + "\n")
    for row in rows:
        output.write(",".join(str(row.get(f, "")) for f in fields) + "\n")
    upload_to_os(os_client, output.getvalue(), csv_key, "text/csv")
 
def append_csv_row(os_client, csv_key: str, row: dict, fields: List[str], chunk_index: int = 2):
    """
    chunk_index=1 â†’ overwrite (fresh start for new run).
    chunk_index>1 â†’ append to existing CSV.
    """
    if chunk_index == 1:
        existing = ""   # discard any rows from previous runs
    else:
        existing = download_from_os(os_client, csv_key) or ""
    output = io.StringIO()
    if not existing.strip():
        output.write(",".join(fields) + "\n")
    else:
        output.write(existing)
        if not existing.endswith("\n"):
            output.write("\n")
    output.write(",".join(str(row.get(f, "")) for f in fields) + "\n")
    upload_to_os(os_client, output.getvalue(), csv_key, "text/csv")
 
 
 
def call_cohere(client, system_prompt, user_message, max_tokens=4096):
    chat_request = CohereChatRequest(
        message=f"{system_prompt}\n\n{user_message}",
        max_tokens=max_tokens,
        temperature=0.0,
        frequency_penalty=0,
        top_p=0.75,
        top_k=0,
    )
    chat_detail = ChatDetails(
        compartment_id=OCI_COMPARTMENT_ID,
        serving_mode=OnDemandServingMode(model_id=OCI_MODEL_ID),
        chat_request=chat_request,
    )
    resp_obj  = client.chat(chat_detail)
    resp_text = resp_obj.data.chat_response.text
    return resp_text
 
def call_cohere_with_retry(client, system_prompt, user_message, max_tokens=4096):
    last_exc = None
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            return call_cohere(client, system_prompt, user_message, max_tokens)
        except Exception as exc:
            last_exc = exc
            base_wait = LLM_RETRY_BACKOFF * (2 ** (attempt - 1))
            jitter    = random.uniform(0, base_wait * 0.5)
            wait      = base_wait + jitter
            log.warning("  [LLM] Attempt %d/%d failed: %s â€” retrying in %.1fs",
                        attempt, LLM_MAX_RETRIES, exc, wait)
            if attempt < LLM_MAX_RETRIES:
                time.sleep(wait)
    log.error("  [LLM] All %d attempts failed. Last: %s", LLM_MAX_RETRIES, last_exc)
    raise last_exc
 
 
 
SYSTEM_PROMPT = """\
You are an expert logistics and trade-document parser.
Extract ALL structured data from shipping documents with complete accuracy.
CRITICAL RULES:
1. Extract EVERY package from packing lists â€” include all package details
2. Always respond with valid JSON only â€” no markdown fences, no prose
3. If a field is not found, use null
4. For arrays (packages), include ALL entries found in the document"""
 
PACKING_LIST_PAGE_PROMPT = """\
Extract ALL packing list rows from the document text below and return JSON.
 
{context}
 
STRUCTURE RULES:
- "packages" array contains one entry per PACKAGE (pkg_no like "1 OF 5", "2 OF 5")
- Each package has a "line_items" array â€” one entry per item number row
- EVERY item number row in the document = ONE entry in line_items, no exceptions
- Steel Structure codes like 1320-2, 1320-4, 1135-27, 1155-31 are ALL individual line items
- gross_weight_kg, dimensions_cm, cbm come from the MAIN bundle header row of that package
- net_weight_kg comes from each individual item row
- pkg_no carries forward â€” rows with no pkg_no printed belong to the last seen pkg_no
- SKIP only: column headers, "SUMMARY PACKING LIST", "Shipment No.", "Vendor:", TOTAL row
 
MANDATORY COMPLETENESS:
- Count every item_no in the text FIRST, then extract them ALL
- If you see 30 item rows on this page, your line_items must contain 30 entries
- Never group, merge, or skip any item row
- Each item_no must appear exactly as printed (e.g. "1155-31", "CLAMP", "1810-11")
 
RANGE-BASED PACKING LISTS (e.g. "Package #1-2", "Package #10-16"):
- Set pkg_no to the range string, quantity to total count, one entry in packages array
 
HEADER TOTALS:
- Extract document-level total gross weight as total_gross_weight_kg
- TOTAL row at bottom â†’ extract to total fields ONLY, not line_items
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "date": null,
  "invoice_reference": null,
  "shipment_number": null,
  "vendor_po_number": null,
  "shipper": {{"name": null, "address": null, "country": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "notify_party": {{"name": null, "address": null, "country": null}},
  "port_of_loading": null,
  "port_of_discharge": null,
  "final_destination": null,
  "vessel_flight_truck": null,
  "sailing_date": null,
  "shipping_marks": null,
  "order_number": null,
  "customer_ref_no": null,
  "warehouse": null,
  "storage_location": null,
  "handling_instructions": null,
  "packages": [
    {{
      "pkg_no": "1 OF 5",
      "packing_style": "Bundle",
      "gross_weight_kg": 10900.0,
      "dimensions_cm": "1170x190x225",
      "cbm": 50.018,
      "line_items": [
        {{
          "item_no": "1320-2",
          "description": "Steel Structure",
          "quantity": 1,
          "unit": "UNIT",
          "net_weight_kg": 496.19
        }},
        {{
          "item_no": "1320-4",
          "description": "Steel Structure",
          "quantity": 1,
          "unit": "UNIT",
          "net_weight_kg": 496.19
        }},
        {{
          "item_no": "1135-27",
          "description": "Steel Structure",
          "quantity": 1,
          "unit": "UNIT",
          "net_weight_kg": 111.46
        }}
      ]
    }},
    {{
      "pkg_no": "2 OF 5",
      "packing_style": "Bundle",
      "gross_weight_kg": 11810.0,
      "dimensions_cm": "1190x190x250",
      "cbm": 56.525,
      "line_items": [
        {{
          "item_no": "1320-1",
          "description": "Steel Structure",
          "quantity": 1,
          "unit": "UNIT",
          "net_weight_kg": 496.19
        }}
      ]
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null,
  "container_number": null,
  "seal_number": null,
  "additional_notes": null
}}
 
CRITICAL:
- Every item_no row â†’ one entry in line_items, no skipping
- pkg_no must NEVER be null â€” carry forward from last seen package header
- TOTAL row at bottom â†’ total fields only, NOT a package entry
- Do not merge rows with same item_no â€” if 1155-26 appears twice, extract twice
 
Document text:
{text}"""
 
 
 
 
 
 
 
 
 
 
 
 
 
 
TCL_PL_PROMPT = """\
Extract ALL packing list rows from the TCL document below and return JSON.
 
{context}
 
CRITICAL TCL RULES:
- TCL ships two separate product lines: IDU (Indoor Unit) and ODU (Outdoor Unit)
- EXCLUDE any rows marked as "SPARE PARTS", "SPARE", "ACCESSORIES" â€” do NOT include them
- SAMPLE_ORG item codes are present on the document â€” extract them as item_no
- Each package row has: pkg_no, item_no (SAMPLE_ORG code), description, quantity, weight, dimensions
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "date": null,
  "invoice_reference": null,
  "product_line": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "packages": [
    {{
      "pkg_no": "1 OF 20",
      "item_no": "SAMPLE_ORG-TCL-IDU-001",
      "description": "Split AC Indoor Unit 1.5T",
      "quantity": 1,
      "unit": "SET",
      "net_weight_kg": 18.5,
      "gross_weight_kg": 22.0,
      "dimensions_cm": "95x35x28",
      "cbm": 0.093,
      "container_no": null,
      "hs_code": null,
      "spare_part": false
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null,
  "container_number": null,
  "seal_number": null
}}
 
CRITICAL:
- SKIP any row where description contains "SPARE PARTS" or "SPARE" or "ACCESSORIES"
- Extract EVERY non-spare package row
- pkg_no must never be null
 
Document text:
{text}"""
 
 
WEG_PL_PROMPT = """\
Extract ALL packing list data from the WEG Electric Motors document below and return JSON.
 
{context}
 
CRITICAL WEG RULES:
- WEG packing lists are HIERARCHICAL: Container â†’ Pallet â†’ Item
- Each container has multiple pallets, each pallet has multiple items
- WEG uses INTERNAL ITEM CODES (format: 18XXXXXX) â€” NOT SAMPLE_ORG codes
- Extract container_no, pallet_no, and item_no for every row
- Serial numbers may be present per motor unit â€” extract them
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "date": null,
  "invoice_reference": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "packages": [
    {{
      "container_no": "TCKU1234567",
      "container_type": "40HQ",
      "pallet_no": "PLT-001",
      "pkg_no": "1 OF 13",
      "item_no": "18ABC00001",
      "description": "Electric Motor WEG 75kW",
      "quantity": 1,
      "unit": "UNIT",
      "serial_number": "SN-2026-00001",
      "net_weight_kg": 485.0,
      "gross_weight_kg": 510.0,
      "dimensions_cm": "120x80x90",
      "cbm": 0.864,
      "hs_code": null
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null,
  "containers": []
}}
 
CRITICAL:
- Extract EVERY item row â€” all 13 pages worth
- container_no and pallet_no must be populated for every row
- serial_number â€” extract if present, null if not
- item_no is WEG internal code (18XXXXXX format)
 
Document text:
{text}"""
 
 
SupplierD_PL_PROMPT = """\
Extract ALL packing list data from the SupplierD AC Compressors document below and return JSON.
 
{context}
 
CRITICAL SupplierD RULES:
- SupplierD uses INTERNAL ITEM CODES + MODEL CODES (e.g. RC2-470B, RC2-550B)
- MODEL CODES like RC2-470B are SAMPLE_ORG model codes â€” extract as model_code field
- SERIAL NUMBERS are present per compressor unit â€” extract all of them
- Transport is LAND via Aramex â€” packages are numbered, NOT containers
- 18 packages total, heavy weights (~34,926 KG total)
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "date": null,
  "awb_number": null,
  "transport_mode": "LAND",
  "carrier": "Aramex",
  "port_of_loading": null,
  "port_of_discharge": null,
  "packages": [
    {{
      "pkg_no": "1",
      "item_no": "SupplierD-RC2-470B-001",
      "model_code": "RC2-470B",
      "description": "AC Compressor RC2-470B",
      "quantity": 1,
      "unit": "UNIT",
      "serial_number": "SN-SupplierD-2026-001",
      "net_weight_kg": 1940.5,
      "gross_weight_kg": 1980.0,
      "dimensions_cm": "180x120x140",
      "cbm": 3.024,
      "hs_code": null
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null
}}
 
CRITICAL:
- model_code (RC2-470B style) must always be extracted â€” it is the SAMPLE_ORG matching key
- serial_number per unit is MANDATORY â€” extract every one
- pkg_no is a simple number (1, 2, 3...) not "X OF Y"
 
Document text:
{text}"""
 
 
SupplierC_PL_PROMPT = """\
Extract ALL packing list data from the SupplierC Compressors document below and return JSON.
 
{context}
 
CRITICAL SupplierC RULES:
- SupplierC uses SupplierC PRODUCT CODES (e.g. ZR61KCE-TFD, ZR72KCE-TFD)
- SAMPLE_ORG PO NUMBER is shown per line â€” extract it as po_number
- SERIAL NUMBERS per compressor unit â€” extract all
- Transport is LAND via Aramex â€” 5 pallets
- Up to 6 different PO numbers in one shipment
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "date": null,
  "awb_number": null,
  "transport_mode": "LAND",
  "carrier": "Aramex",
  "packages": [
    {{
      "pkg_no": "PLT-001",
      "item_no": "ZR61KCE-TFD",
      "description": "SupplierC Scroll Compressor ZR61KCE",
      "po_number": "PO-2026-COP-001",
      "quantity": 6,
      "unit": "UNIT",
      "serial_number": "SN-COP-2026-001",
      "net_weight_kg": 210.0,
      "gross_weight_kg": 225.0,
      "dimensions_cm": "80x60x70",
      "cbm": 0.336,
      "hs_code": null
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null
}}
 
CRITICAL:
- po_number per line is MANDATORY â€” up to 6 different POs
- serial_number per unit must be extracted
- item_no is SupplierC product code (ZR__KCE-___ format)
 
Document text:
{text}"""
 
 
SupplierB_PL_PROMPT = """\
Extract ALL packing list rows from the SupplierB Compressor Parts document below and return JSON.
 
{context}
 
CRITICAL SupplierB RULES:
- SupplierB uses "Customer Material Number" column = SAMPLE_ORG internal code â€” extract as item_no
- Transport is LAND via Aramex â€” 31 pallets
- Serial numbers may be present per compressor unit
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "date": null,
  "awb_number": null,
  "transport_mode": "LAND",
  "carrier": "Aramex",
  "packages": [
    {{
      "pkg_no": "PLT-001",
      "item_no": "SAMPLE_ORG-DAN-COMP-001",
      "customer_material_number": "SAMPLE_ORG-DAN-COMP-001",
      "SupplierB_part_number": "DAN-123456",
      "description": "SupplierB Compressor Part",
      "po_number": null,
      "quantity": 10,
      "unit": "PCS",
      "serial_number": null,
      "net_weight_kg": 45.0,
      "gross_weight_kg": 50.0,
      "dimensions_cm": "60x40x35",
      "cbm": 0.084,
      "hs_code": null
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null
}}
 
CRITICAL:
- customer_material_number = SAMPLE_ORG code â€” always extract it
- serial_number if present per compressor unit
- 31 pallets so there will be many rows â€” extract ALL
 
Document text:
{text}"""

ASN_PL_PROMPT = """\
Extract ALL picking list rows from the ASN document below and return JSON.
{context}
CRITICAL ASN RULES:
- This is an ASN "Picking List" â€” column order is:
  Sl No | Product Description/Code | Pallet | Serial Number | UOM | Batch Number | Quantity Received | Expiry Date
- "Product Description/Code" spans TWO lines:
    LINE 1 = item_description (first line e.g. "SCHIOWIN", "900837", "OLMKS983746")
    LINE 2 = item_code (second line e.g. "89765", "OLWPSKSI", "PSLOED9876OLKI")
- "Pallet" column = pallet_no (a number like 6, 5, 10)
- "Batch Number" = batch_number (e.g. BATSHRI9081) â€” never put this in item_code
- item_no should be null â€” it is not present in this document
- Extract ALL document-level header fields
Return JSON (no markdown fences, no prose):
{{
  "document_type": "packing_list",
  "packing_list_number": null,
  "order_number": null,
  "customer_ref_no": null,
  "date": null,
  "warehouse": null,
  "storage_location": null,
  "handling_instructions": null,
  "invoice_reference": null,
  "shipment_number": null,
  "vendor_po_number": null,
  "shipper": {{"name": null, "address": null, "country": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "notify_party": {{"name": null, "address": null, "country": null}},
  "port_of_loading": null,
  "port_of_discharge": null,
  "final_destination": null,
  "vessel_flight_truck": null,
  "sailing_date": null,
  "shipping_marks": null,
  "packages": [
    {{
      "sl_no": 1,
      "item_description": "SCHIOWIN",
      "item_code": "89765",
      "item_no": null,
      "pallet_no": 6,
      "serial_number": null,
      "unit": "Units",
      "batch_number": "BATSHRI9081",
      "quantity": 300,
      "expiry_date": "11-SEP-2026",
      "pkg_no": null,
      "packing_style": null,
      "gross_weight_kg": null,
      "net_weight_kg": null,
      "dimensions_cm": null,
      "cbm": null,
      "hs_code": null,
      "country_of_origin": null
    }}
  ],
  "total_packages": null,
  "total_quantity": null,
  "total_net_weight_kg": null,
  "total_gross_weight_kg": null,
  "total_cbm": null,
  "container_number": null,
  "seal_number": null,
  "additional_notes": null
}}
CRITICAL:
- item_description = LINE 1 of Product Description/Code column (e.g. SCHIOWIN, 900837)
- item_code = LINE 2 of Product Description/Code column (e.g. 89765, OLWPSKSI)
- pallet_no = the Pallet column number (blue in PDF, e.g. 6, 5, 10)
- batch_number = Batch Number column (e.g. BATSHRI9081) â€” never put this in item_code
- expiry_date = Expiry Date column exactly as printed
- sl_no = Sl No column (1, 2, 3...)
- Extract ALL rows â€” no skipping
Document text:
{text}"""
 
 
def get_pl_prompt(supplier_name: str, context: str, text: str) -> str:
    """Select supplier-specific PL prompt."""
    s = (supplier_name or "").upper()
    if "WEG"      in s: return WEG_PL_PROMPT.format(context=context, text=text)
    if "TCL"      in s: return TCL_PL_PROMPT.format(context=context, text=text)
    if "SupplierD"    in s: return SupplierD_PL_PROMPT.format(context=context, text=text)
    if "SupplierC" in s: return SupplierC_PL_PROMPT.format(context=context, text=text)
    if "SupplierB"  in s: return SupplierB_PL_PROMPT.format(context=context, text=text)
    if "ASN"      in s: return ASN_PL_PROMPT.format(context=context, text=text)   # â† ADD
    return PACKING_LIST_PAGE_PROMPT.format(context=context, text=text)
 
 
 
def parse_json_response(raw: str) -> dict:
    cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            candidate = match.group()
    else:
        candidate = cleaned
    repaired = _repair_truncated_json(candidate)
    try:
        result = json.loads(repaired)
        result["_truncated"] = True
        log.warning("JSON repaired after truncation.")
        return result
    except json.JSONDecodeError:
        pass
    log.warning("Could not parse JSON â€” returning raw.")
    return {"raw_response": raw, "parse_error": True}
 
def _repair_truncated_json(s: str) -> str:
    s = re.sub(r",\s*$", "", s.strip())
    if s.count('"') % 2 != 0:
        s += '"'
    s = re.sub(r",\s*$", "", s.strip())
    stack, in_string, escape_next = [], False, False
    for ch in s:
        if escape_next:
            escape_next = False; continue
        if ch == "\\" and in_string:
            escape_next = True; continue
        if ch == '"':
            in_string = not in_string; continue
        if in_string:
            continue
        if ch == "{": stack.append("}")
        elif ch == "[": stack.append("]")
        elif ch in ("}", "]"):
            if stack and stack[-1] == ch:
                stack.pop()
    s += "".join(reversed(stack))
    return s
 
def sanitize_floats(obj):
    if isinstance(obj, dict):
        return {k: sanitize_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_floats(v) for v in obj]
    elif isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    return obj
 
 
 
PKG_NO_RE = re.compile(r"\b(\d+\s+OF\s+\d+)\b", re.IGNORECASE)
 
def _make_continuation_context(last_pkg_no: Optional[str]) -> str:
    if not last_pkg_no:
        return ""
    return (
        f"CONTINUATION CONTEXT:\n"
        f"The previous chunk ended while still inside package '{last_pkg_no}'.\n"
        f"Any rows at the START of this chunk that do NOT begin with a new 'X OF Y' "
        f"package header belong to package '{last_pkg_no}' â€” assign that as their pkg_no.\n"
        f"Only start a new pkg_no when you encounter an explicit line like '2 OF 5', "
        f"'3 OF 5', etc.\n"
    )
 
def _split_dense_page(page_text: str, max_rows: int) -> List[str]:
    """
    Split a single dense page into sub-chunks of <= max_rows data rows each.
 
    FIX 1 - Broadened row detection:
      Old regex only matched "312.0 KG" (decimal + space + KG label).
      SupplierB rows are "PLT-001  DAN-SC15-001  ...  312.0  350.0  2.4" with NO KG label
      so old regex detected 0 rows -> no split -> 31 rows in one call -> 4K cap hit -> truncation.
      New regex catches: KG-labeled weights, bare decimals, PLT- pallet prefixes.
 
    FIX 2 - Overlap lines for continuity:
      Each sub-chunk now includes the last OVERLAP_ROWS lines of the previous
      sub-chunk, labeled "CONTEXT ONLY". This prevents losing package headers when
      a package spans a chunk boundary (WEG container->pallet->item hierarchy).
      Dedup downstream removes any overlap items that slip through.
 
    FIX 3 - Memory envelope (header) injected into every sub-chunk:
      Document header (column labels, supplier, PO refs) is prepended to every
      sub-chunk so the LLM always has schema context, not just the first chunk.
    """
    OVERLAP_ROWS = 3
 
    lines = page_text.split("\n")
 
    DATA_ROW_RE = re.compile(
        r"(?:"
        r"\d[\d,]*\.\d+\s*KG"          # "312.0 KG" or "312.0KG"
        r"|\b\d{2,}[\d,]*\.\d+\b"      # bare decimal >= 2 integer digits (SupplierB weights/CBM)
        r"|PLT-\d+"                         # SupplierB pallet prefix PLT-001
        r"|\b(?:DAN|WEG|COP|COM)-\S+"      # supplier part code prefix as fallback
        r")",
        re.IGNORECASE,
    )
    data_line_indices = [
        i for i, line in enumerate(lines) if DATA_ROW_RE.search(line)
    ]
 
    if len(data_line_indices) <= max_rows:
        return [page_text]
 
    log.info(
        "  Dense page: %d data rows detected -> splitting into sub-chunks of <=%d rows (overlap=%d)",
        len(data_line_indices), max_rows, OVERLAP_ROWS,
    )
 
    first_data_idx = data_line_indices[0]
    last_data_idx  = data_line_indices[-1]
    header_lines   = lines[:first_data_idx]
    body_lines     = lines[first_data_idx : last_data_idx + 1]
    footer_lines   = lines[last_data_idx + 1:]
 
    header = "\n".join(header_lines)
    footer = "\n".join(footer_lines)
 
    sub_chunks  = []
    batch_start = 0
    while batch_start < len(body_lines):
        batch_end = min(batch_start + max_rows, len(body_lines))
        batch     = body_lines[batch_start:batch_end]
        is_last   = (batch_end >= len(body_lines))
 
        text = header + "\n"
 
        if batch_start > 0:
            overlap_start = max(0, batch_start - OVERLAP_ROWS)
            overlap_lines = body_lines[overlap_start:batch_start]
            text += (
                "--- CONTEXT ONLY: lines already extracted in previous batch, "
                "do NOT repeat in output ---\n"
                + "\n".join(overlap_lines)
                + "\n--- END CONTEXT ---\n"
            )
 
        text += "\n".join(batch)
 
        if is_last:
            text += "\n" + footer
 
        sub_chunks.append(text)
        batch_start = batch_end
 
    log.info("  Dense split -> %d sub-chunks", len(sub_chunks))
    return sub_chunks
 
 
 
 
def _deduplicate_packages(packages: List[dict]) -> List[dict]:
    seen, unique = set(), []
    for pkg in packages:
        key = (
            str(pkg.get("pkg_no",        "")),
            str(pkg.get("item_no",       "")),
            str(pkg.get("quantity",      "")),
            str(pkg.get("net_weight_kg", ""))[:10],
        )
        if key == ("", "", "", ""):
            continue
        if key not in seen:
            seen.add(key)
            unique.append(pkg)
    return unique
 
RANGE_RE = re.compile(r"^\d+\s*[-â€“]\s*\d+$")
 
def _filter_summary_rows(packages: List[dict], totals_found: dict) -> List[dict]:
    filtered = []
    for pkg in packages:
        pn   = str(pkg.get("pkg_no")  or "").strip()
        unit = str(pkg.get("unit")    or "").strip().upper()
        ino  = str(pkg.get("item_no") or "").strip()
        is_range_summary  = RANGE_RE.match(pn) and unit == "PKG" and not re.search(r"\d", ino)
        is_summary_no_pkg = (not pn and unit == "PKG" and (pkg.get("quantity") or 0) > 1)
        if is_range_summary or is_summary_no_pkg:
            gw = pkg.get("gross_weight_kg")
            cb = pkg.get("cbm")
            if gw and not totals_found.get("total_gross_weight_kg"):
                totals_found["total_gross_weight_kg"] = gw
            if cb and not totals_found.get("total_cbm"):
                totals_found["total_cbm"] = cb
            log.info("  Removed summary row: pkg_no='%s'", pn)
        else:
            filtered.append(pkg)
    return filtered
 
 
 
def extract_pages_parallel(
    client: GenerativeAiInferenceClient,
    pages_text: List[str],
    initial_pkg_no: Optional[str] = None,
    supplier_name: str = "",
) -> dict:
    TOTAL_FIELDS = ["total_packages", "total_quantity",
                    "total_net_weight_kg", "total_gross_weight_kg", "total_cbm"]
 
    page_contexts: List[str] = []
    last_pkg_no = initial_pkg_no
    for page_text in pages_text:
        page_contexts.append(_make_continuation_context(last_pkg_no))
        matches = PKG_NO_RE.findall(page_text)
        if matches:
            last_pkg_no = matches[-1].strip()
 
    work_items: List[Tuple[int, int, str, str]] = []
    for page_idx, (page_text, ctx) in enumerate(zip(pages_text, page_contexts), start=1):
        sub_pages = _split_dense_page(page_text, DENSE_MAX_ROWS)
        for sub_idx, sub_text in enumerate(sub_pages, start=1):
            work_items.append((page_idx, sub_idx, sub_text, ctx if sub_idx == 1 else ""))
 
    total_calls    = len(work_items)
    actual_workers = min(total_calls, MAX_WORKERS)
    _plog("Parallel extraction: %d LLM calls, %d workers (carry=%s)",
          total_calls, actual_workers, initial_pkg_no or "none")
 
    def _call_one(item: Tuple[int, int, str, str]) -> Tuple[int, int, dict]:
        page_idx, sub_idx, text, context = item
        label  = f"Page {page_idx}" if sub_idx == 1 else f"Page {page_idx}.{sub_idx}"
        prompt = get_pl_prompt(supplier_name, context, text)
        log.info("  [PAR] %s firing", label)
        try:
            raw    = call_cohere_with_retry(client, SYSTEM_PROMPT, prompt, MAX_TOKENS)
            parsed = parse_json_response(raw)
        except Exception as exc:
            log.error("  [PAR] %s FAILED: %s", label, exc)
            parsed = {"packages": [], "_error": str(exc)}
        n_pkgs  = len(parsed.get("packages", []))
        n_items = sum(len(p.get("line_items", [])) or 1
                      for p in parsed.get("packages", []))
        _plog("  [PAR] %s complete â€” %d packages, %d line items",
              label, n_pkgs, n_items)
        return page_idx, sub_idx, parsed
 
    raw_results: List[Tuple[int, int, dict]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
        future_map = {executor.submit(_call_one, item): item for item in work_items}
        for future in concurrent.futures.as_completed(future_map):
            try:
                raw_results.append(future.result())
            except Exception as exc:
                item = future_map[future]
                log.error("  [PAR] Page %d.%d future raised: %s", item[0], item[1], exc)
                raw_results.append((item[0], item[1], {"packages": []}))
 
    _plog("All %d parallel LLM calls complete", total_calls)
    raw_results.sort(key=lambda x: (x[0], x[1]))
 
    all_packages: List[dict] = []
    totals_found  = {f: None for f in TOTAL_FIELDS}
    base_result: dict = {}
    running_pkg_no = initial_pkg_no
 
 
    for _, _, parsed in raw_results:
        if not base_result:
            base_result = {k: v for k, v in parsed.items() if k != "packages"}
        packages = parsed.get("packages", [])
        for pkg in packages:
            pn = pkg.get("pkg_no")
            if pn and str(pn).strip():
                running_pkg_no = str(pn).strip()
            elif running_pkg_no:
                pn = running_pkg_no
 
            pkg_no        = pn or running_pkg_no
            gross_weight  = pkg.get("gross_weight_kg")
            dimensions    = pkg.get("dimensions_cm")
            cbm           = pkg.get("cbm")
            packing_style = pkg.get("packing_style")
 
            line_items = pkg.get("line_items")
            if line_items:
                for item in line_items:
                    flat = {
                        "sl_no":             pkg.get("sl_no"),
                        "item_description":  pkg.get("item_description"),
                        "item_code":         pkg.get("item_code"),
                        "description":       pkg.get("item_description") or pkg.get("description"),  # â† single
                        "pkg_no":            pkg_no,
                        "packing_style":     packing_style,
                        "pallet_no":         pkg.get("pallet_no"),
                        "gross_weight_kg":   gross_weight,
                        "dimensions_cm":     dimensions,
                        "cbm":               cbm,
                        "item_no":           pkg.get("item_no"),
                        "batch_number":      pkg.get("batch_number"),
                        "serial_number":     pkg.get("serial_number"),
                        "quantity":          pkg.get("quantity"),
                        "unit":              pkg.get("unit"),
                        "expiry_date":       pkg.get("expiry_date"),
                        "net_weight_kg":     pkg.get("net_weight_kg"),
                        "hs_code":           pkg.get("hs_code"),
                        "country_of_origin": pkg.get("country_of_origin"),
                    }
                    all_packages.append(flat)
            else:
                if pkg_no:
                    pkg["pkg_no"] = pkg_no
                all_packages.append(pkg)
        for field in TOTAL_FIELDS:
            val = parsed.get(field)
            if val is not None:
                totals_found[field] = val
 
    log.info("Raw packages before dedup: %d", len(all_packages))
    deduped  = _deduplicate_packages(all_packages)
    log.info("After dedup: %d", len(deduped))
    filtered = _filter_summary_rows(deduped, totals_found)
    log.info("After summary-row removal: %d", len(filtered))
    base_result["packages"] = filtered
 
    s = sum(p.get("net_weight_kg") or 0 for p in filtered if p.get("net_weight_kg"))
    if s > 0:
        totals_found["total_net_weight_kg"] = round(s, 3)
 
    if not totals_found.get("total_gross_weight_kg"):
        seen_pkg  = set()
        gross_sum = 0.0
        cbm_sum   = 0.0
        for p in filtered:
            pn = p.get("pkg_no", "")
            if pn and pn not in seen_pkg:
                g = p.get("gross_weight_kg")
                c = p.get("cbm")
                if g:
                    seen_pkg.add(pn)
                    gross_sum += g
                    cbm_sum   += (c or 0)
        if gross_sum > 0:
            totals_found["total_gross_weight_kg"] = round(gross_sum, 3)
        if cbm_sum > 0:
            totals_found["total_cbm"] = round(cbm_sum, 3)
 
    for field, val in totals_found.items():
        if val is not None:
            base_result[field] = val
 
    return base_result
 
 
 
def merge_all_chunks(os_client, safe_req: str, total_chunks: int) -> dict:
    TOTAL_FIELDS = ["total_packages", "total_quantity",
                    "total_net_weight_kg", "total_gross_weight_kg", "total_cbm"]
 
    all_packages: List[dict] = []
    totals_found = {f: None for f in TOTAL_FIELDS}
    last_pkg_no  = None
    base_result: dict = {}
 
    for chunk_idx in range(1, total_chunks + 1):
        chunk_data = download_from_os(os_client, _chunk_key(safe_req, chunk_idx))
        if not chunk_data:
            log.warning("  [MERGE] Chunk %d missing â€” skipping", chunk_idx)
            continue
        try:
            parsed = json.loads(chunk_data)
        except Exception as exc:
            log.error("  [MERGE] Chunk %d parse failed: %s", chunk_idx, exc)
            continue
 
        if not base_result:
            base_result = {k: v for k, v in parsed.items() if k != "packages"}
 
        packages = parsed.get("packages", [])
        for pkg in packages:
            pn = pkg.get("pkg_no")
            if pn and str(pn).strip():
                last_pkg_no = str(pn).strip()
 
            pkg_no        = pn or last_pkg_no
            gross_weight  = pkg.get("gross_weight_kg")
            dimensions    = pkg.get("dimensions_cm")
            cbm           = pkg.get("cbm")
            packing_style = pkg.get("packing_style")
 
            line_items = pkg.get("line_items")
            if line_items:
                for item in line_items:
                    flat = {
                        "pkg_no":            pkg_no,
                        "packing_style":     packing_style,
                        "gross_weight_kg":   gross_weight,
                        "dimensions_cm":     dimensions,
                        "cbm":               cbm,
                        "item_no":           item.get("item_no"),
                        "description":       item.get("description"),
                        "quantity":          item.get("quantity"),
                        "unit":              item.get("unit"),
                        "net_weight_kg":     item.get("net_weight_kg"),
                        "hs_code":           item.get("hs_code"),
                        "country_of_origin": item.get("country_of_origin"),
                    }
                    all_packages.append(flat)
            else:
                if pkg_no:
                    pkg["pkg_no"] = pkg_no
                pkg.setdefault("item_description", pkg.get("item_description"))
                pkg.setdefault("item_code",        pkg.get("item_code"))
                pkg.setdefault("sl_no",            pkg.get("sl_no"))
                pkg.setdefault("pallet_no",        pkg.get("pallet_no"))
                pkg.setdefault("batch_number",     pkg.get("batch_number"))
                pkg.setdefault("serial_number",    pkg.get("serial_number"))
                pkg.setdefault("expiry_date",      pkg.get("expiry_date"))
                all_packages.append(pkg)
 
        for field in TOTAL_FIELDS:
            val = parsed.get(field)
            if val is not None:
                totals_found[field] = val
 
        _plog("[MERGE] Chunk %d: %d packages (running total: %d)",
              chunk_idx, len(packages), len(all_packages))
 
    _plog("Merge raw: %d | deduping...", len(all_packages))
    deduped  = _deduplicate_packages(all_packages)
    _plog("After dedup: %d", len(deduped))
    filtered = _filter_summary_rows(deduped, totals_found)
    _plog("After summary-row removal: %d", len(filtered))
 
    if not base_result:
        base_result = {}
    base_result["packages"] = filtered
 
 
 
 
 
 
    s = sum(p.get("net_weight_kg") or 0 for p in filtered if p.get("net_weight_kg"))
    if s > 0:
        totals_found["total_net_weight_kg"] = round(s, 3)
 
    seen_pkg  = set()
    gross_sum = 0.0
    cbm_sum   = 0.0
    for p in filtered:
        pn = p.get("pkg_no", "")
        if pn and pn not in seen_pkg:
            g = p.get("gross_weight_kg")
            c = p.get("cbm")
            if g:
                seen_pkg.add(pn)
                gross_sum += g
                cbm_sum   += (c or 0)
    if gross_sum > 0:
        totals_found["total_gross_weight_kg"] = round(gross_sum, 3)
    if cbm_sum > 0:
        totals_found["total_cbm"] = round(cbm_sum, 3)
 
    for field in ["total_packages", "total_quantity"]:
        val = totals_found.get(field)
        if val is not None:
            base_result[field] = val
    for field in ["total_gross_weight_kg", "total_net_weight_kg", "total_cbm"]:
        val = totals_found.get(field)
        if val is not None:
            base_result[field] = val
        elif field in base_result:
            del base_result[field]   # remove potentially wrong LLM value
 
    _plog("âœ“ Merged %d packages | gross=%.2f kg | net=%.2f kg | cbm=%.3f",
          len(filtered),
          base_result.get("total_gross_weight_kg") or 0,
          base_result.get("total_net_weight_kg") or 0,
          base_result.get("total_cbm") or 0)
 
    return base_result
 
 
 
def build_otm_ship_unit_payload(pl_data, request_id, order_base_gid, ob_ship_unit_gid):
    packages        = pl_data.get("packages") or []
    ship_unit_items = []
    for seq, pkg in enumerate(packages, start=1):
        ship_unit_items.append({
            "sequenceNo":       seq,
            "shipUnitCount":    pkg.get("quantity"),
            "shipUnitCountUom": (pkg.get("unit") or OTM_DEFAULT_QTY_UOM).upper(),
            "shipUnitWeight":   pkg.get("gross_weight_kg"),
            "weightUom":        OTM_DEFAULT_WEIGHT_UOM,
            "shipUnitVolume":   pkg.get("cbm"),
            "volumeUom":        OTM_DEFAULT_VOLUME_UOM if pkg.get("cbm") else None,
            "description":      pkg.get("description"),
            "hsCode":           pkg.get("hs_code"),
            "countryOfOrigin":  pkg.get("country_of_origin"),
            "packageNo":        pkg.get("pkg_no"),
            "itemNo":           pkg.get("item_no"),
            "packingStyle":     pkg.get("packing_style"),
            "dimensions":       pkg.get("dimensions_cm"),
            "netWeight":        pkg.get("net_weight_kg"),
            "netWeightUom":     OTM_DEFAULT_WEIGHT_UOM,
            "domainName":       OTM_DOMAIN_NAME,
            "slNo":         pkg.get("sl_no"),
            "productCode":  pkg.get("product_code"),
            "palletNo":     pkg.get("pallet_no"),
            "batchNumber":  pkg.get("batch_number"),
            "serialNumber": pkg.get("serial_number"),
            "expiryDate":   pkg.get("expiry_date"),
        })
    return {
        "referenceTransmissionNo": request_id,
        "senderTransmissionId":    request_id,
        "processInSequence":       OTM_PROCESS_IN_SEQUENCE,
        "transactions": {"items": [{
            "contentType": OTM_CONTENT_TYPE,
            "httpMethod":  OTM_HTTP_METHOD,
            "resourceUrl": f"orderBases/{order_base_gid}/shipUnits/{ob_ship_unit_gid}",
            "body": {
                "orderBaseGid":       order_base_gid,
                "obShipUnitGid":      ob_ship_unit_gid,
                "domainName":         OTM_DOMAIN_NAME,
                "totalPackages":      pl_data.get("total_packages"),
                "totalQuantity":      pl_data.get("total_quantity"),
                "totalGrossWeightKg": pl_data.get("total_gross_weight_kg"),
                "totalNetWeightKg":   pl_data.get("total_net_weight_kg"),
                "totalCbm":           pl_data.get("total_cbm"),
                "containerNumber":    pl_data.get("container_number"),
                "sealNumber":         pl_data.get("seal_number"),
                "portOfLoading":      pl_data.get("port_of_loading"),
                "portOfDischarge":    pl_data.get("port_of_discharge"),
                "vesselFlightTruck":  pl_data.get("vessel_flight_truck"),
                "shipUnits":          {"items": ship_unit_items},
            },
        }]},
    }
 
 
 
def fire_next_extractor(fn_client, manifest_chunk: dict, request_id: str,
                        total_chunks: int, order_base_gid: str, ob_ship_unit_gid: str,
                        shipment_id: str = "", supplier_name: str = "",
                        doc_key: str = "packing_list"):   # â† ADD this line
    payload = {
        "requestId":     request_id,
        "chunkIndex":    manifest_chunk["chunkIndex"],
        "totalChunks":   total_chunks,
        "pages":         manifest_chunk["pages"],
        "lastPkgNo":     manifest_chunk.get("lastPkgNo"),
        "orderBaseGid":  order_base_gid,
        "obShipUnitGid": ob_ship_unit_gid,
        "shipmentId":    shipment_id,    # â† FIXED
        "supplierName":  supplier_name,  # â† FIXED
        "docKey":        doc_key,   # â† ADD
    }
    body_bytes = json.dumps(payload).encode("utf-8")
    fn_client.invoke_function(
        function_id=EXTRACTOR_FUNCTION_ID,
        invoke_function_body=body_bytes,
        fn_invoke_type="detached",
    )
    _plog("[CHAIN] Fired chunk %d/%d (detached)",
          manifest_chunk["chunkIndex"], total_chunks)
 
def _check_and_fire_merger(os_client, shipment_id: str, request_id: str, doc_type: str):
    """
    Race-condition-free completion check using per-document marker files.
    Each extractor writes its own marker â€” no shared-file contention.
    The last extractor to arrive (marker count == total expected) fires the merger.
    """
    if not shipment_id or not MERGER_FUNCTION_ID:
        log.info("Merger check skipped â€” no shipmentId or MERGER_FUNCTION_ID")
        return
 
    safe_shp      = re.sub(r"[^\w\-]", "_", shipment_id)
    manifest_key  = f"{SHIPMENTS_FOLDER}/{safe_shp}.json"
    complete_prefix = f"{SHIPMENTS_FOLDER}/{safe_shp}/complete/"
    marker_key    = f"{complete_prefix}{doc_type}.json"
 
    manifest_raw = None
    for attempt in range(10):
        manifest_raw = download_from_os(os_client, manifest_key)
        if manifest_raw:
            break
        wait = 1.5 * (attempt + 1)
        _plog("Manifest not ready â€” waiting %.0fs (attempt %d/10)", wait, attempt + 1)
        time.sleep(wait)
 
    if not manifest_raw:
        log.error("Shipment manifest never appeared for %s â€” giving up", shipment_id)
        return
 
    try:
        manifest       = json.loads(manifest_raw)
        total_expected = manifest.get("totalDocs") or len(manifest.get("documents", {}))
 
    except Exception as exc:
        log.error("Failed to parse shipment manifest: %s", exc)
        return
 
    if total_expected == 0:
        log.error("Manifest has no documents entry â€” cannot determine total")
        return
 
    marker_payload = json.dumps({
        "docType":   doc_type,
        "requestId": request_id,
        "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "shipmentId": shipment_id,
    })
 
    for attempt in range(3):
        if upload_to_os(os_client, marker_payload, marker_key):
            _plog("Marker written â†’ %s", marker_key)
            break
        log.warning("Marker write attempt %d/3 failed â€” retrying", attempt + 1)
        time.sleep(1)
    else:
        log.error("Failed to write completion marker for %s â€” giving up", doc_type)
        return
 
    jitter_by_type = {
        "bill_of_lading": 3.0,
        "packing_list_01": 2.0, "packing_list": 2.0,
        "invoice_01": 1.0, "invoice": 1.0,
    }
    base_jitter = jitter_by_type.get(doc_type, 2.0)
    time.sleep(base_jitter + random.uniform(0, 1.0))
 
    existing_markers = list_os_keys(os_client, complete_prefix)
    complete_count   = len(existing_markers)
    _plog("Completion markers: %d / %d â€” %s",
          complete_count, total_expected,
          [m.split("/")[-1].replace(".json","") for m in existing_markers])
 
    if complete_count < total_expected:
        _plog("Not all docs done yet â€” merger will be triggered by last extractor")
        return
 
    _plog("All %d docs complete â€” updating manifest and firing merger", total_expected)
 
    request_ids = {}
    for marker_path in existing_markers:
        try:
            raw = download_from_os(os_client, marker_path)
            if raw:
                data = json.loads(raw)
                request_ids[data["docType"]] = data["requestId"]
        except Exception as exc:
            log.warning("Failed to read marker %s: %s", marker_path, exc)
 
    try:
        manifest_raw = download_from_os(os_client, manifest_key)
        manifest     = json.loads(manifest_raw)
        for doc_type_key, doc_info in manifest.get("documents", {}).items():
            if doc_type_key in request_ids:
                doc_info["status"] = "complete"
        manifest["status"] = "complete"
        upload_to_os(
            os_client,
            json.dumps(manifest, indent=2, ensure_ascii=False),
            manifest_key,
        )
        _plog("Manifest updated to complete")
    except Exception as exc:
        log.error("Failed to update manifest to complete: %s", exc)
 
    try:
        signer    = get_resource_principals_signer()
        fn_client = FunctionsInvokeClient(
            config={}, signer=signer,
            service_endpoint=MERGER_ENDPOINT,
            timeout=(10, 240),
        )
        fn_client.invoke_function(
            function_id=MERGER_FUNCTION_ID,
            invoke_function_body=json.dumps({
                "shipmentId": shipment_id,
                "requestIds": request_ids,
            }).encode("utf-8"),
            fn_invoke_type="detached",
        )
        _plog("Merger fired for shipment %s with docs: %s",
              shipment_id, list(request_ids.keys()))
    except Exception as exc:
        log.error("Failed to fire merger: %s", exc)
        
 
 
def handler(ctx, data: io.BytesIO = None):
    os.makedirs(TMP_DIR, exist_ok=True)
    t_start = time.time()
    _plog("=" * 60)
    _plog("PL Extractor v5.0.0 invoked")
 
    if not OCI_COMPARTMENT_ID:
        msg = "OCI_COMPARTMENT_ID env var is not set."
        log.error(msg)
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "message": msg}),
            headers={"Content-Type": "application/json"},
            status_code=500,
        )
 
    body = {}
    if data:
        try:
            body = json.loads(data.getvalue())
        except Exception as exc:
            log.warning("Body parse failed: %s", exc)
 
    request_id       = body.get("requestId",    "unknown")
    chunk_index      = int(body.get("chunkIndex",  1))
    total_chunks     = int(body.get("totalChunks", 1))
    pages            = body.get("pages",        [])
    last_pkg_no      = body.get("lastPkgNo",    None)
    order_base_gid   = body.get("orderBaseGid",  "")
    ob_ship_unit_gid = body.get("obShipUnitGid", "")
    shipment_id      = body.get("shipmentId",    "")
    supplier_name    = body.get("supplierName",  "")
    doc_key          = body.get("docKey", "packing_list") 
 
    safe_req = re.sub(r"[^\w\-]", "_", request_id) or "unknown"
 
    _plog("requestId=%s  chunk=%d/%d  pages=%d  carry=%s",
          request_id, chunk_index, total_chunks, len(pages), last_pkg_no)
    _plog("Folder: %s", _req_folder(safe_req))
 
    if not pages:
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "message": "No pages provided"}),
            headers={"Content-Type": "application/json"},
            status_code=400,
        )
 
    os_client = get_object_storage_client()
 
    try:
        genai_client = get_genai_client()
        chunk_result = extract_pages_parallel(
            client         = genai_client,
            pages_text     = pages,
            initial_pkg_no = last_pkg_no,
            supplier_name  = supplier_name,
        )
        chunk_result["chunkIndex"]  = chunk_index
        chunk_result["totalChunks"] = total_chunks
        chunk_result["requestId"]   = request_id
        chunk_result["pagesCount"]  = len(pages)
        status  = "SUCCESS"
        err_msg = ""
    except Exception as exc:
        log.exception("Extractor chunk %d failed", chunk_index)
        chunk_result = {
            "packages": [], "chunkIndex": chunk_index,
            "totalChunks": total_chunks, "requestId": request_id,
            "pagesCount": len(pages), "_error": str(exc),
        }
        status  = "FAILED"
        err_msg = str(exc)
 
    chunk_result = sanitize_floats(chunk_result)
    duration = round(time.time() - t_start, 2)
    n_pkgs   = len(chunk_result.get("packages", []))
    _plog("Chunk %d/%d done in %.1fs â€” %d packages extracted",
          chunk_index, total_chunks, duration, n_pkgs)
 
 
    for _up in range(3):
        if upload_to_os(os_client,
                        json.dumps(chunk_result, ensure_ascii=False),
                        _chunk_key(safe_req, chunk_index)):
            break
        _plog("Chunk upload attempt %d/3 failed â€” retrying", _up + 1)
        time.sleep(2 ** _up)
    else:
        log.error("All chunk upload attempts failed for chunk %d/%d", chunk_index, total_chunks)
    
 
        _check_and_fire_merger(os_client, shipment_id, request_id, doc_key)   # â† was "packing_list"
 
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "requestId": request_id,
                                    "error": "chunk upload failed after 3 attempts"}),
            headers={"Content-Type": "application/json"},
            status_code=500,
        )
 
    csv_fields = ["chunk_no", "pages_in_chunk", "status", "completed_at",
                  "duration_s", "packages_found", "error"]
    append_csv_row(os_client, _csv_key(safe_req), {
        "chunk_no":       chunk_index,
        "pages_in_chunk": len(pages),
        "status":         status,
        "completed_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_s":     duration,
        "packages_found": n_pkgs,
        "error":          err_msg,
    }, csv_fields, chunk_index=chunk_index)
 
    existing_keys    = list_os_keys(os_client, _chunk_prefix(safe_req))
    completed_chunks = len(existing_keys)
    _plog("Chunks completed: %d / %d", completed_chunks, total_chunks)
 
    if completed_chunks < total_chunks:
        next_chunk_index = chunk_index + 1
        manifest_data    = download_from_os(os_client, _manifest_key(safe_req))
        if manifest_data:
            try:
                manifest       = json.loads(manifest_data)
                next_chunk_def = next(
                    (c for c in manifest["chunks"] if c["chunkIndex"] == next_chunk_index),
                    None,
                )
                if next_chunk_def:
                    fn_client = get_functions_client()
                    fire_next_extractor(
                        fn_client, next_chunk_def, request_id, total_chunks,
                        order_base_gid  or manifest.get("orderBaseGid",  ""),
                        ob_ship_unit_gid or manifest.get("obShipUnitGid", ""),
                        shipment_id     or manifest.get("shipmentId",    ""),  # â† ADD
                        supplier_name   or manifest.get("supplierName",  ""),  # â† ADD
                        doc_key,
                    )
                else:
                    log.error("Chunk %d not found in manifest", next_chunk_index)
            except Exception as exc:
                log.error("Failed to fire next chunk: %s", exc)
        else:
            log.error("Manifest not found â€” cannot chain")
 
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status": "chunk_complete", "requestId": request_id,
                "chunkIndex": chunk_index, "totalChunks": total_chunks,
                "packagesThisChunk": n_pkgs, "completedChunks": completed_chunks,
                "nextChunkFired": next_chunk_index, "durationSeconds": duration,
                "folder": _req_folder(safe_req),
            }, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )
 
    else:
        _plog("All %d chunks complete â€” merging", total_chunks)
        pl_data = merge_all_chunks(os_client, safe_req, total_chunks)
        pl_data["document_type"] = "packing_list"
 
        manifest_data = download_from_os(os_client, _manifest_key(safe_req))
        manifest      = json.loads(manifest_data) if manifest_data else {}
 
        pl_data["_meta"] = {
            "requestId":    request_id,
            "filename":     manifest.get("filename", ""),
            "is_scanned":   manifest.get("isScanned", False),
            "num_chunks":   total_chunks,
            "chunk_size":   manifest.get("chunkSize", 0),
            "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "folder":       _req_folder(safe_req),
        }
 
        upload_to_os(os_client,
                     json.dumps(sanitize_floats(pl_data), indent=2, ensure_ascii=False),
                     _extracted_key(safe_req))
 
        _order_base_gid   = order_base_gid  or manifest.get("orderBaseGid",  "")
        _ob_ship_unit_gid = ob_ship_unit_gid or manifest.get("obShipUnitGid", "")
        otm_payload = build_otm_ship_unit_payload(
            pl_data, request_id, _order_base_gid, _ob_ship_unit_gid,
        )
        otm_clean = sanitize_floats(otm_payload)
        upload_to_os(os_client,
                     json.dumps(otm_clean, indent=2, ensure_ascii=False),
                     _otm_key(safe_req))
 
        _plog("=" * 60)
        _plog("PIPELINE COMPLETE")
        _plog("  Packages : %d", len(pl_data.get("packages", [])))
        _plog("  Gross    : %s kg", pl_data.get("total_gross_weight_kg"))
        _plog("  Net      : %s kg", pl_data.get("total_net_weight_kg"))
        _plog("  CBM      : %s",    pl_data.get("total_cbm"))
        _plog("  Folder   : %s",    _req_folder(safe_req))
        _plog("  extracted: %s",    _extracted_key(safe_req))
        _plog("  otm      : %s",    _otm_key(safe_req))
        _plog("=" * 60)
        _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id, doc_key)
 
 
 
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status":               "complete",
                "requestId":            request_id,
                "totalChunks":          total_chunks,
                "packagesExtracted":    len(pl_data.get("packages", [])),
                "totalGrossWeightKg":   pl_data.get("total_gross_weight_kg"),
                "totalNetWeightKg":     pl_data.get("total_net_weight_kg"),
                "totalCbm":             pl_data.get("total_cbm"),
                "folder":               _req_folder(safe_req),
                "otmSavedTo":           _otm_key(safe_req),
                "extractedJsonSavedTo": _extracted_key(safe_req),
                "jobCsvSavedTo":        _csv_key(safe_req),
                "durationSeconds":      duration,
            }, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )
 