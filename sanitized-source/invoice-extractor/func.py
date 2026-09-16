"""
AI Invoice Extractor  â€”  OCI FUNCTIONS v1.0.0
=============================================
ROLE: Self-chaining parallel chunk processor for Commercial Invoices.
 
Mirrors the PL extractor (ai-pl-extractor v5) pattern exactly.
Each chunk fires ALL its pages in parallel, then chains to the next chunk.
The last chunk merges all results and writes the final extracted JSON.
 
Input:
{
  "requestId":     "INV-REQ-20260616-001-01",
  "chunkIndex":    1,
  "totalChunks":   2,
  "pages":         ["page text...", ...],
  "orderBaseGid":  "SAMPLE_ORG.ORDER_BASE_001",
  "obShipUnitGid": "SAMPLE_ORG.OB_SHIP_UNIT_001",
  "shipmentId":    "SHP-20260616-0007",
  "supplierName":  "SampleSupplier"
}
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
 
OS_NAMESPACE    = os.getenv("OS_NAMESPACE", "sample_namespace")
OS_BUCKET       = os.getenv("OS_BUCKET",    "sample-workflow-bucket")
INV_BASE_FOLDER = os.getenv("OS_INV_BASE_FOLDER", "Invoice")
 
MAX_TOKENS     = int(os.getenv("MAX_TOKENS",     "4096"))
MAX_WORKERS    = int(os.getenv("MAX_WORKERS",    "8"))
DENSE_MAX_ROWS = int(os.getenv("DENSE_MAX_ROWS", "18"))  # rows per sub-chunk
 
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
 
OTM_DOMAIN_NAME = os.getenv("OTM_DOMAIN_NAME", "SAMPLE_ORG")
TMP_DIR = "/tmp/ai_inv_extractor"
 
 
 
class _TeeHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        print(self.format(record), flush=True)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_TeeHandler(sys.stderr)],
)
log = logging.getLogger(__name__)
 
def _plog(msg, *args):
    print(f"â–¶ {msg % args if args else msg}", flush=True)
 
 
 
def _safe(s: str) -> str:
    return re.sub(r"[^\w\-]", "_", s) if s else "unknown"
 
def _req_folder(safe_req: str) -> str:
    return f"{INV_BASE_FOLDER}/{safe_req}"
 
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
    return GenerativeAiInferenceClient(
        config={}, signer=signer,
        service_endpoint=OCI_GENAI_ENDPOINT,
        retry_strategy=oci.retry.NoneRetryStrategy(),
        timeout=(10, 240),
    )
 
def get_object_storage_client():
    signer = get_resource_principals_signer()
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
 
def get_functions_client():
    signer = get_resource_principals_signer()
    return FunctionsInvokeClient(
        config={}, signer=signer,
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
 
def append_csv_row(os_client, csv_key: str, row: dict, fields: List[str], chunk_index: int = 2):
    if chunk_index == 1:
        existing = ""
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
    return resp_obj.data.chat_response.text
 
def call_cohere_with_retry(client, system_prompt, user_message, max_tokens=4096):
    last_exc = None
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            return call_cohere(client, system_prompt, user_message, max_tokens)
        except Exception as exc:
            last_exc = exc
            wait = LLM_RETRY_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1)
            log.warning("  [LLM] Attempt %d/%d failed â€” retrying in %.1fs", attempt, LLM_MAX_RETRIES, wait)
            if attempt < LLM_MAX_RETRIES:
                time.sleep(wait)
    raise last_exc
 
 
 
SYSTEM_PROMPT = """\
You are an expert logistics and trade-document parser.
Extract ALL structured data from commercial invoice documents with complete accuracy.
CRITICAL RULES:
1. Extract EVERY line item from the invoice â€” do not summarize or skip
2. Always respond with valid JSON only â€” no markdown fences, no prose
3. If a field is not found, use null
4. For arrays (line_items), include ALL entries found in the document"""
 
INVOICE_PAGE_PROMPT = """\
Extract ALL fields from the COMMERCIAL INVOICE in this document page/chunk.
 
RULES:
- line_items must ONLY contain rows from the COMMERCIAL INVOICE table
  (columns: HS Code, Description, Quantity, Unit Price, Total Amount)
- Do NOT include packing list rows (they have pkg_no, net weight, dimensions)
- If this chunk contains no invoice data, return all fields as null with empty line_items []
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "seller": {{"name": null, "address": null, "country": null, "vat_tax_id": null, "contact": null}},
  "buyer": {{"name": null, "address": null, "country": null, "vat_tax_id": null, "contact": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "ship_to": {{"name": null, "address": null, "country": null}},
  "currency": null,
  "payment_terms": null,
  "incoterms": null,
  "country_of_origin": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "shipment_date": null,
  "vessel_flight_truck": null,
  "shipment_number": null,
  "vendor_po_number": null,
  "line_items": [
    {{
      "line_no": null,
      "item_no": null,
      "hs_code": null,
      "description": null,
      "origin": null,
      "quantity": null,
      "unit": null,
      "unit_price": null,
      "total_price": null
    }}
  ],
  "subtotal": null,
  "freight": null,
  "insurance": null,
  "other_charges": null,
  "discount": null,
  "total_amount": null,
  "amount_in_words": null,
  "bank_details": {{
    "bank_name": null, "account_name": null, "account_number": null,
    "iban": null, "swift_bic": null, "sort_code": null
  }},
  "additional_notes": null
}}
 
Document text:
{text}"""
 
 
 
WEG_INVOICE_PROMPT = """\
Extract ALL fields from the WEG Electric Motors COMMERCIAL INVOICE below.
 
CRITICAL WEG RULES:
- WEG uses INTERNAL ITEM CODES (18XXXXXX format) â€” NOT SAMPLE_ORG codes
- Multiple SAMPLE_ORG PO numbers per shipment â€” extract po_number per line
- Two invoices exist (main + supplemental) â€” this handles MAIN invoice only
- Extract all line items with WEG internal codes
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "seller": {{"name": null, "address": null, "country": null}},
  "buyer": {{"name": null, "address": null, "country": null}},
  "currency": null,
  "payment_terms": null,
  "incoterms": null,
  "country_of_origin": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "line_items": [
    {{
      "line_no": null,
      "item_no": "18ABC00001",
      "weg_code": "18ABC00001",
      "description": "Electric Motor WEG 75kW",
      "po_number": "PO-2026-WEG-001",
      "quantity": null,
      "unit": null,
      "unit_price": null,
      "total_price": null,
      "hs_code": null,
      "origin": null
    }}
  ],
  "subtotal": null,
  "total_amount": null,
  "currency": null
}}
 
CRITICAL:
- item_no is WEG internal code (18XXXXXX)
- po_number per line â€” extract SAMPLE_ORG PO number shown on each line
- Do NOT skip any line items
 
Document text:
{text}"""
 
 
SupplierB_INVOICE_PROMPT = """\
Extract ALL fields from the SupplierB Compressor Parts COMMERCIAL INVOICE below.
 
CRITICAL SupplierB RULES:
- SupplierB invoice has ~285 line items across 19 pages â€” extract ALL
- "Customer Material Number" column = SAMPLE_ORG internal code â€” extract as item_no
- Also extract SupplierB part number as SupplierB_part_number
- Multiple SAMPLE_ORG PO numbers per shipment
- Serial numbers may appear per compressor unit
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "seller": {{"name": null, "address": null, "country": null}},
  "buyer": {{"name": null, "address": null, "country": null}},
  "currency": null,
  "payment_terms": null,
  "incoterms": null,
  "country_of_origin": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "line_items": [
    {{
      "line_no": null,
      "item_no": "SAMPLE_ORG-DAN-COMP-001",
      "customer_material_number": "SAMPLE_ORG-DAN-COMP-001",
      "SupplierB_part_number": "DAN-123456",
      "description": "SupplierB Compressor Part",
      "po_number": "PO-2026-DAN-001",
      "serial_number": null,
      "quantity": null,
      "unit": null,
      "unit_price": null,
      "total_price": null,
      "hs_code": null,
      "origin": null
    }}
  ],
  "subtotal": null,
  "total_amount": null
}}
 
CRITICAL:
- customer_material_number = SAMPLE_ORG code â€” ALWAYS extract it as item_no
- Extract ALL ~285 line items â€” do not truncate or summarize
- serial_number if present per compressor
 
Document text:
{text}"""
 
 
SupplierD_INVOICE_PROMPT = """\
Extract ALL fields from the SupplierD AC Compressors COMMERCIAL INVOICE below.
 
CRITICAL SupplierD RULES:
- SupplierD uses MODEL CODES like RC2-470B, RC2-550B â€” these ARE SAMPLE_ORG model codes
- Extract model_code as a separate field â€” it is the matching key
- SERIAL NUMBERS per compressor unit are mandatory â€” extract all
- Transport via Aramex (land) â€” AWB number may appear
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "awb_number": null,
  "seller": {{"name": null, "address": null, "country": null}},
  "buyer": {{"name": null, "address": null, "country": null}},
  "currency": null,
  "payment_terms": null,
  "incoterms": null,
  "line_items": [
    {{
      "line_no": null,
      "item_no": "SupplierD-RC2-470B-001",
      "model_code": "RC2-470B",
      "description": "AC Compressor RC2-470B",
      "serial_number": "SN-SupplierD-2026-001",
      "quantity": null,
      "unit": "UNIT",
      "unit_price": null,
      "total_price": null,
      "hs_code": null,
      "origin": null
    }}
  ],
  "subtotal": null,
  "total_amount": null
}}
 
CRITICAL:
- model_code (RC2-470B style) is MANDATORY â€” SAMPLE_ORG matching depends on it
- serial_number per unit MANDATORY
- Extract every line item
 
Document text:
{text}"""
 
 
SupplierC_INVOICE_PROMPT = """\
Extract ALL fields from the SupplierC Compressors/Parts COMMERCIAL INVOICE below.
 
CRITICAL SupplierC RULES:
- SupplierC uses PRODUCT CODES like ZR61KCE-TFD, ZR72KCE-TFD
- SAMPLE_ORG PO NUMBER is shown per line â€” extract as po_number (up to 6 POs)
- SERIAL NUMBERS per compressor unit â€” extract all
- Transport via Aramex (land)
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "awb_number": null,
  "seller": {{"name": null, "address": null, "country": null}},
  "buyer": {{"name": null, "address": null, "country": null}},
  "currency": null,
  "payment_terms": null,
  "incoterms": null,
  "line_items": [
    {{
      "line_no": null,
      "item_no": "ZR61KCE-TFD",
      "SupplierC_code": "ZR61KCE-TFD",
      "description": "SupplierC Scroll Compressor ZR61KCE",
      "po_number": "PO-2026-COP-001",
      "serial_number": "SN-COP-2026-001",
      "quantity": null,
      "unit": "UNIT",
      "unit_price": null,
      "total_price": null,
      "hs_code": null,
      "origin": null
    }}
  ],
  "subtotal": null,
  "total_amount": null
}}
 
CRITICAL:
- po_number per line is MANDATORY â€” up to 6 different POs
- SupplierC_code must always be extracted
- serial_number per unit mandatory
 
Document text:
{text}"""
 
 
TCL_INVOICE_PROMPT = """\
Extract ALL fields from the TCL Air Conditioner COMMERCIAL INVOICE below.
 
CRITICAL TCL RULES:
- TCL ships TWO product lines in one shipment: IDU (Indoor Units) and ODU (Outdoor Units)
- Each product line has its OWN invoice section (Invoice A = IDU, Invoice B = ODU)
- SAMPLE_ORG item codes are printed directly on the document (SAMPLE_ORG-IDU-001, SAMPLE_ORG-ODU-001 etc.)
- Extract ALL line items from BOTH invoice sections into one line_items array
- Detect product_line per item: rows with IDU codes â†’ "IDU", ODU codes â†’ "ODU"
- EXCLUDE any rows marked as "SPARE PARTS", "SPARE", "ACCESSORIES" (SR-013)
- spare_part field: true for spare rows, false for all others
- Extract PO number, invoice number per section if different
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "seller": {{"name": null, "address": null, "country": null, "vat_tax_id": null, "contact": null}},
  "buyer": {{"name": null, "address": null, "country": null, "vat_tax_id": null, "contact": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "ship_to": {{"name": null, "address": null, "country": null}},
  "currency": null,
  "payment_terms": null,
  "incoterms": null,
  "country_of_origin": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "shipment_date": null,
  "vendor_po_number": null,
  "line_items": [
    {{
      "line_no": 1,
      "item_no": "SAMPLE_ORG-IDU-001",
      "hs_code": null,
      "description": "Split AC Indoor Unit 1.5T",
      "product_line": "IDU",
      "origin": null,
      "quantity": 200,
      "unit": "SET",
      "unit_price": 95.0,
      "total_price": 19000.0,
      "po_number": "HSA-PO-88771",
      "spare_part": false
    }},
    {{
      "line_no": 1,
      "item_no": "SAMPLE_ORG-ODU-001",
      "hs_code": null,
      "description": "Split AC Outdoor Unit 1.5T",
      "product_line": "ODU",
      "origin": null,
      "quantity": 200,
      "unit": "SET",
      "unit_price": 125.0,
      "total_price": 25000.0,
      "po_number": "HSA-PO-88771",
      "spare_part": false
    }}
  ],
  "subtotal": null,
  "freight": null,
  "insurance": null,
  "other_charges": null,
  "discount": null,
  "total_amount": null,
  "amount_in_words": null,
  "bank_details": {{
    "bank_name": null, "account_name": null, "account_number": null,
    "iban": null, "swift_bic": null, "sort_code": null
  }},
  "additional_notes": null
}}
 
CRITICAL:
- Extract EVERY line item from BOTH Invoice A (IDU) and Invoice B (ODU)
- Set product_line = "IDU" for indoor unit rows, "ODU" for outdoor unit rows
- Set spare_part = true and SKIP rows with "SPARE PARTS" in description
- SAMPLE_ORG item codes are already on the document â€” use them directly as item_no
 
Document text:
{text}"""
 
 
SupplierA_INVOICE_PROMPT = """\
Extract ALL fields from the SupplierA Window AC COMMERCIAL INVOICE below.
 
CRITICAL SupplierA RULES:
- SupplierA uses SAMPLE_ORG item codes directly on the document (SAMPLE_ORG-WAC-001, SAMPLE_ORG-WAC-002 etc.)
- Document format is clean tabular â€” all fields clearly labelled
- Each line item has: SAMPLE_ORG item code, description, quantity, unit price, total
- HS Code 8415.10 applies to all Window AC SKD items
- PO number HSA-PO-XXXXX format â€” extract per document header
 
Return JSON (no markdown fences, no prose):
{{
  "document_type": "commercial_invoice",
  "invoice_number": null,
  "invoice_date": null,
  "seller": {{"name": null, "address": null, "country": null, "vat_tax_id": null, "contact": null}},
  "buyer": {{"name": null, "address": null, "country": null, "vat_tax_id": null, "contact": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "ship_to": {{"name": null, "address": null, "country": null}},
  "currency": "USD",
  "payment_terms": null,
  "incoterms": null,
  "country_of_origin": "China",
  "port_of_loading": null,
  "port_of_discharge": null,
  "shipment_date": null,
  "vendor_po_number": null,
  "line_items": [
    {{
      "line_no": 1,
      "item_no": "SAMPLE_ORG-WAC-001",
      "hs_code": "8415.10",
      "description": "Window AC 1 Ton SKD Kit",
      "origin": "China",
      "quantity": 500,
      "unit": "SET",
      "unit_price": 85.0,
      "total_price": 42500.0
    }},
    {{
      "line_no": 2,
      "item_no": "SAMPLE_ORG-WAC-002",
      "hs_code": "8415.10",
      "description": "Window AC 1.5 Ton SKD Kit",
      "origin": "China",
      "quantity": 250,
      "unit": "SET",
      "unit_price": 110.0,
      "total_price": 27500.0
    }}
  ],
  "subtotal": null,
  "freight": null,
  "insurance": null,
  "other_charges": null,
  "discount": null,
  "total_amount": null,
  "amount_in_words": null,
  "bank_details": {{
    "bank_name": null, "account_name": null, "account_number": null,
    "iban": null, "swift_bic": null, "sort_code": null
  }},
  "additional_notes": null
}}
 
CRITICAL:
- item_no must be the SAMPLE_ORG code exactly as printed (SAMPLE_ORG-WAC-001 etc.)
- Extract EVERY line item â€” do not skip any
- HS code 8415.10 for all AC items unless stated otherwise
 
Document text:
{text}"""
 
 
def get_invoice_prompt(supplier_name: str, text: str) -> str:
    """Select supplier-specific invoice prompt."""
    s = (supplier_name or "").upper()
    if "WEG"      in s: return WEG_INVOICE_PROMPT.format(text=text)
    if "SupplierB"  in s: return SupplierB_INVOICE_PROMPT.format(text=text)
    if "SupplierD"    in s: return SupplierD_INVOICE_PROMPT.format(text=text)
    if "SupplierC" in s: return SupplierC_INVOICE_PROMPT.format(text=text)
    if "TCL"      in s: return TCL_INVOICE_PROMPT.format(text=text)
    if "SupplierA"    in s: return SupplierA_INVOICE_PROMPT.format(text=text)
    return INVOICE_PAGE_PROMPT.format(text=text)  # generic fallback
 
 
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
        return result
    except json.JSONDecodeError:
        pass
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
 
def _split_dense_invoice_page(page_text: str, max_rows: int) -> list:
    """
    Split a dense invoice page into sub-chunks of <= max_rows line items.
    Fix 1: broadened row detection catches all supplier formats
    Fix 2: overlap lines for continuity
    Fix 3: column header (memory envelope) injected into every sub-chunk
    """
    OVERLAP_ROWS = 3
    lines = page_text.split("\n")

    INV_ROW_RE = re.compile(
        r"(?:"
        r"^\s*\d+\s+\S"
        r"|\b\d+[\d,]*\.\d{2}\b"
        r"|\b(?:SET|UNIT|PCS|KG|PCE|EA)\b"
        r")",
        re.IGNORECASE | re.MULTILINE,
    )
    data_line_indices = [i for i, line in enumerate(lines) if INV_ROW_RE.search(line)]

    if len(data_line_indices) <= max_rows:
        return [page_text]

    log.info(
        "  Dense invoice page: %d data rows -> splitting into sub-chunks of <=%d (overlap=%d)",
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
                "--- CONTEXT ONLY: lines already extracted, do NOT repeat ---\n"
                + "\n".join(overlap_lines)
                + "\n--- END CONTEXT ---\n"
            )
        text += "\n".join(batch)
        if is_last:
            text += "\n" + footer
        sub_chunks.append(text)
        batch_start = batch_end

    log.info("  Dense invoice split -> %d sub-chunks", len(sub_chunks))
    return sub_chunks


def deduplicate_line_items(items: List[dict]) -> List[dict]:
    seen, unique = set(), []
    for item in items:
        key = (
            str(item.get("item_no",      "") or ""),
            str(item.get("hs_code",      "") or ""),
            str(item.get("description",  "") or "")[:40],
            str(item.get("quantity",     "") or ""),
        )
        if key == ("", "", "", ""):
            continue
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique
 
 
 
TOTAL_FIELDS = ["total_amount", "subtotal", "freight", "insurance",
                "other_charges", "discount", "total_gross_weight_kg"]
 
 
def extract_pages_parallel(
    client: GenerativeAiInferenceClient,
    pages_text: List[str],
    supplier_name: str = "",
) -> dict:
    work_items: List[Tuple[int, int, str]] = []
    for page_idx, page_text in enumerate(pages_text, start=1):
        sub_pages = _split_dense_invoice_page(page_text, DENSE_MAX_ROWS)
        for sub_idx, sub_text in enumerate(sub_pages, start=1):
            work_items.append((page_idx, sub_idx, sub_text))

    total_calls    = len(work_items)
    actual_workers = min(total_calls, MAX_WORKERS)
    _plog("Invoice parallel extraction: %d LLM call(s), %d workers", total_calls, actual_workers)

    def _call_one(args: Tuple[int, int, str]) -> Tuple[int, int, dict]:
        page_idx, sub_idx, text = args
        label  = f"Page {page_idx}" if sub_idx == 1 else f"Page {page_idx}.{sub_idx}"
        prompt = get_invoice_prompt(supplier_name, text)
        log.info("  [PAR] %s firing", label)
        try:
            raw    = call_cohere_with_retry(client, SYSTEM_PROMPT, prompt, MAX_TOKENS)
            parsed = parse_json_response(raw)
        except Exception as exc:
            log.error("  [PAR] %s FAILED: %s", label, exc)
            parsed = {"line_items": [], "_error": str(exc)}
        _plog("  [PAR] %s complete â€” %d items", label, len(parsed.get("line_items", [])))
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
                raw_results.append((item[0], item[1], {"line_items": []}))

    _plog("All %d parallel invoice LLM calls complete", total_calls)
    raw_results.sort(key=lambda x: (x[0], x[1]))

    all_line_items: List[dict] = []
    totals_found  = {f: None for f in TOTAL_FIELDS}
    base_result: dict = {}

    for _, _sub, parsed in raw_results:
        if not base_result:
            base_result = {k: v for k, v in parsed.items() if k != "line_items"}
        all_line_items.extend(parsed.get("line_items", []))
        for field in TOTAL_FIELDS:
            val = parsed.get(field)
            if val is not None:
                totals_found[field] = val
 
    deduped = deduplicate_line_items(all_line_items)
    log.info("Line items: raw=%d â†’ dedup=%d", len(all_line_items), len(deduped))
    base_result["line_items"] = deduped
 
    for field, val in totals_found.items():
        if val is not None:
            base_result[field] = val
 
    return base_result
 
 
 
def merge_all_chunks(os_client, safe_req: str, total_chunks: int) -> dict:
    all_line_items: List[dict] = []
    totals_found = {f: None for f in TOTAL_FIELDS}
    base_result: dict = {}
 
    for chunk_idx in range(1, total_chunks + 1):
        chunk_data = download_from_os(os_client, _chunk_key(safe_req, chunk_idx))
        if not chunk_data:
            log.warning("  [MERGE] Chunk %d missing", chunk_idx)
            continue
        try:
            parsed = json.loads(chunk_data)
        except Exception as exc:
            log.error("  [MERGE] Chunk %d parse failed: %s", chunk_idx, exc)
            continue
 
        if not base_result:
            base_result = {k: v for k, v in parsed.items() if k != "line_items"}
 
        all_line_items.extend(parsed.get("line_items", []))
        for field in TOTAL_FIELDS:
            val = parsed.get(field)
            if val is not None:
                totals_found[field] = val
 
        _plog("[MERGE] Chunk %d: %d line items (running: %d)",
              chunk_idx, len(parsed.get("line_items", [])), len(all_line_items))
 
    if not base_result:
        base_result = {}
 
    deduped = deduplicate_line_items(all_line_items)
    _plog("Merge: raw=%d â†’ dedup=%d", len(all_line_items), len(deduped))
    base_result["line_items"] = deduped
 
    for field, val in totals_found.items():
        if val is not None:
            base_result[field] = val
 
    return base_result
 
 
 
def build_otm_invoice_payload(inv_data: dict, request_id: str,
                               order_base_gid: str, ob_ship_unit_gid: str) -> dict:
    line_items = inv_data.get("line_items") or []
    seller     = inv_data.get("seller") or {}
    buyer      = inv_data.get("buyer") or inv_data.get("consignee") or {}
 
    otm_line_items = []
    for seq, item in enumerate(line_items, start=1):
        otm_line_items.append({
            "sequenceNo":      seq,
            "itemNo":          item.get("item_no"),
            "hsCode":          item.get("hs_code"),
            "description":     item.get("description"),
            "countryOfOrigin": item.get("origin"),
            "quantity":        item.get("quantity"),
            "unit":            item.get("unit"),
            "unitPrice":       item.get("unit_price"),
            "totalPrice":      item.get("total_price"),
            "domainName":      OTM_DOMAIN_NAME,
        })
 
    return {
        "referenceTransmissionNo": request_id,
        "senderTransmissionId":    request_id,
        "documentType":            "commercial_invoice",
        "transactions": {"items": [{
            "contentType": "application/vnd.oracle.resource+json;type=singular",
            "httpMethod":  "PATCH",
            "resourceUrl": f"orderBases/{order_base_gid}/invoices/{request_id}",
            "body": {
                "orderBaseGid":      order_base_gid,
                "obShipUnitGid":     ob_ship_unit_gid,
                "domainName":        OTM_DOMAIN_NAME,
                "invoiceNumber":     inv_data.get("invoice_number"),
                "invoiceDate":       inv_data.get("invoice_date"),
                "sellerName":        seller.get("name"),
                "sellerCountry":     seller.get("country"),
                "buyerName":         buyer.get("name"),
                "currency":          inv_data.get("currency"),
                "paymentTerms":      inv_data.get("payment_terms"),
                "incoterms":         inv_data.get("incoterms"),
                "countryOfOrigin":   inv_data.get("country_of_origin"),
                "portOfLoading":     inv_data.get("port_of_loading"),
                "portOfDischarge":   inv_data.get("port_of_discharge"),
                "totalAmount":       inv_data.get("total_amount"),
                "subtotal":          inv_data.get("subtotal"),
                "freight":           inv_data.get("freight"),
                "vendorPoNumber":    inv_data.get("vendor_po_number"),
                "shipmentNumber":    inv_data.get("shipment_number"),
                "lineItems":         {"items": otm_line_items},
            },
        }]},
    }
 
 
 
def fire_next_extractor(fn_client, manifest_chunk: dict, request_id: str,
                        total_chunks: int, order_base_gid: str,
                        ob_ship_unit_gid: str, shipment_id: str, supplier_name: str,
                        doc_key: str = "invoice"):   # â† ADD
    payload = {
        "requestId":     request_id,
        "chunkIndex":    manifest_chunk["chunkIndex"],
        "totalChunks":   total_chunks,
        "pages":         manifest_chunk["pages"],
        "orderBaseGid":  order_base_gid,
        "obShipUnitGid": ob_ship_unit_gid,
        "shipmentId":    shipment_id,
        "supplierName":  supplier_name,
        "docKey":        doc_key,   # â† ADD
    }
    fn_client.invoke_function(
        function_id=EXTRACTOR_FUNCTION_ID,
        invoke_function_body=json.dumps(payload).encode("utf-8"),
        fn_invoke_type="detached",
    )
    _plog("[CHAIN] Fired invoice chunk %d/%d (detached)",
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
    _plog("Invoice Extractor v1.0.0 invoked")
 
    if not OCI_COMPARTMENT_ID:
        msg = "OCI_COMPARTMENT_ID env var not set."
        return fdk_response.Response(
            ctx, response_data=json.dumps({"status": "error", "message": msg}),
            headers={"Content-Type": "application/json"}, status_code=500,
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
    order_base_gid   = body.get("orderBaseGid",  "")
    ob_ship_unit_gid = body.get("obShipUnitGid", "")
    shipment_id      = body.get("shipmentId",    "")
    supplier_name    = body.get("supplierName",  "")
    doc_key          = body.get("docKey", "invoice")   # â† ADD THIS LINE
 
 
    safe_req = _safe(request_id) or "unknown"
    _plog("requestId=%s  chunk=%d/%d  pages=%d", request_id, chunk_index, total_chunks, len(pages))
 
    if not pages:
        return fdk_response.Response(
            ctx, response_data=json.dumps({"status": "error", "message": "No pages provided"}),
            headers={"Content-Type": "application/json"}, status_code=400,
        )
 
    os_client = get_object_storage_client()
 
    try:
        genai_client = get_genai_client()
        chunk_result = extract_pages_parallel(
            client=genai_client, pages_text=pages, supplier_name=supplier_name,
        )
        chunk_result.update({
            "chunkIndex": chunk_index, "totalChunks": total_chunks,
            "requestId": request_id, "pagesCount": len(pages),
        })
        status, err_msg = "SUCCESS", ""
    except Exception as exc:
        log.exception("Invoice extractor chunk %d failed", chunk_index)
        chunk_result = {
            "line_items": [], "chunkIndex": chunk_index,
            "totalChunks": total_chunks, "requestId": request_id,
            "pagesCount": len(pages), "_error": str(exc),
        }
        status, err_msg = "FAILED", str(exc)
 
    chunk_result = sanitize_floats(chunk_result)
    duration = round(time.time() - t_start, 2)
    n_items  = len(chunk_result.get("line_items", []))
    _plog("Chunk %d/%d done in %.1fs â€” %d line items", chunk_index, total_chunks, duration, n_items)
 
 
    for _up in range(3):
        if upload_to_os(os_client, json.dumps(chunk_result, ensure_ascii=False),
                        _chunk_key(safe_req, chunk_index)):
            break
        _plog("Chunk upload attempt %d/3 failed â€” retrying", _up + 1)
        time.sleep(2 ** _up)
    else:
        log.error("All chunk upload attempts failed for chunk %d/%d", chunk_index, total_chunks)
        doc_key = body.get("docKey") or "invoice"
        _check_and_fire_merger(os_client, shipment_id, request_id, doc_key)
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "requestId": request_id,
                                      "error": "chunk upload failed after 3 attempts"}),
            headers={"Content-Type": "application/json"},
            status_code=500,
        )
 
    csv_fields = ["chunk_no", "pages_in_chunk", "status", "completed_at",
                  "duration_s", "line_items_found", "error"]
    append_csv_row(os_client, _csv_key(safe_req), {
        "chunk_no":         chunk_index,
        "pages_in_chunk":   len(pages),
        "status":           status,
        "completed_at":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_s":       duration,
        "line_items_found": n_items,
        "error":            err_msg,
    }, csv_fields, chunk_index=chunk_index)
 
    completed_chunks = len(list_os_keys(os_client, _chunk_prefix(safe_req)))
    _plog("Chunks completed: %d / %d", completed_chunks, total_chunks)
 
    if completed_chunks < total_chunks:
        next_chunk_index = chunk_index + 1
        manifest_data    = download_from_os(os_client, _manifest_key(safe_req))
        if manifest_data:
            try:
                manifest       = json.loads(manifest_data)
                next_chunk_def = next(
                    (c for c in manifest["chunks"] if c["chunkIndex"] == next_chunk_index), None,
                )
                if next_chunk_def:
                    fn_client = get_functions_client()
                    fire_next_extractor(
                        fn_client, next_chunk_def, request_id, total_chunks,
                        order_base_gid  or manifest.get("orderBaseGid",  ""),
                        ob_ship_unit_gid or manifest.get("obShipUnitGid", ""),
                        shipment_id     or manifest.get("shipmentId",    ""),
                        supplier_name   or manifest.get("supplierName",  ""),
                        doc_key,
                    )
            except Exception as exc:
                log.error("Failed to fire next chunk: %s", exc)
 
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status": "chunk_complete", "requestId": request_id,
                "chunkIndex": chunk_index, "totalChunks": total_chunks,
                "lineItemsThisChunk": n_items, "completedChunks": completed_chunks,
                "nextChunkFired": chunk_index + 1, "durationSeconds": duration,
                "folder": _req_folder(safe_req),
            }, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )
 
    else:
        _plog("All %d chunks complete â€” merging", total_chunks)
        inv_data = merge_all_chunks(os_client, safe_req, total_chunks)
        inv_data["document_type"] = "commercial_invoice"
 
        manifest_data = download_from_os(os_client, _manifest_key(safe_req))
        manifest      = json.loads(manifest_data) if manifest_data else {}
 
        inv_data["_meta"] = {
            "requestId":    request_id,
            "filename":     manifest.get("filename", ""),
            "shipmentId":   shipment_id or manifest.get("shipmentId", ""),
            "supplierName": supplier_name or manifest.get("supplierName", ""),
            "num_chunks":   total_chunks,
            "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "folder":       _req_folder(safe_req),
        }
 
        upload_to_os(os_client,
                     json.dumps(sanitize_floats(inv_data), indent=2, ensure_ascii=False),
                     _extracted_key(safe_req))
 
        _order_base_gid   = order_base_gid  or manifest.get("orderBaseGid",  "")
        _ob_ship_unit_gid = ob_ship_unit_gid or manifest.get("obShipUnitGid", "")
        otm_payload = build_otm_invoice_payload(
            inv_data, request_id, _order_base_gid, _ob_ship_unit_gid,
        )
        upload_to_os(os_client,
                     json.dumps(sanitize_floats(otm_payload), indent=2, ensure_ascii=False),
                     _otm_key(safe_req))
 
        _plog("=" * 60)
        _plog("INVOICE PIPELINE COMPLETE")
        _plog("  Line items : %d", len(inv_data.get("line_items", [])))
        _plog("  Total amt  : %s", inv_data.get("total_amount"))
        _plog("  Folder     : %s", _req_folder(safe_req))
        _plog("=" * 60)
 
 
        _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id, doc_key)
 
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status":               "complete",
                "requestId":            request_id,
                "documentType":         "commercial_invoice",
                "totalChunks":          total_chunks,
                "lineItemsExtracted":   len(inv_data.get("line_items", [])),
                "totalAmount":          inv_data.get("total_amount"),
                "folder":               _req_folder(safe_req),
                "extractedJsonSavedTo": _extracted_key(safe_req),
                "otmSavedTo":           _otm_key(safe_req),
                "durationSeconds":      duration,
            }, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )