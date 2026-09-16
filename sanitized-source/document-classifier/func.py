"""
AI SAMPLE_ORG Document Classifier  â€”  OCI FUNCTIONS v2.0.0
=====================================================
CHANGES vs v1.2.0:
  - Removed Tier 1/2 filename matching
  - All documents classified via LLM (per page, parallel)
  - Digital PDFs: pdfplumber page extraction
  - Scanned PDFs: OCI Document Understanding (OCR)
  - Combined documents: pages split by detected type
  - Classified pages saved to OCI OS before firing orchestrators
  - Orchestrators read pre-classified pages (no re-download, no re-classify)
"""
 
import concurrent.futures
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote
 
import pdfplumber
from pypdf import PdfReader
 
import oci
from oci.auth.signers import get_resource_principals_signer
from oci.ai_document import AIServiceDocumentClient
from oci.functions.functions_invoke_client import FunctionsInvokeClient
from oci.generative_ai_inference import GenerativeAiInferenceClient
from oci.generative_ai_inference.models import (
    ChatDetails,
    OnDemandServingMode,
    CohereChatRequest,
)
from fdk import response as fdk_response
from PIL import Image
 
 
 
OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
 
OS_NAMESPACE    = os.getenv("OS_NAMESPACE",    "sample_namespace")
OS_BUCKET       = os.getenv("OS_BUCKET",       "sample-workflow-bucket")
OS_INPUT_BUCKET = os.getenv("OS_INPUT_BUCKET", "sample-workflow-bucket")
 
SHIPMENTS_FOLDER   = os.getenv("OS_SHIPMENTS_FOLDER", "Shipments")
CLASSIFIED_FOLDER  = os.getenv("OS_CLASSIFIED_FOLDER", "Classified")
MIN_CHARS_PER_PAGE = int(os.getenv("MIN_CHARS_PER_PAGE", "50"))
 
OCI_GENAI_ENDPOINT = os.getenv(
    "OCI_GENAI_ENDPOINT",
    "https://example.invalid/integration-endpoint",
)
OCI_MODEL_ID = os.getenv(
    "OCI_MODEL_ID",
    "OCI_RESOURCE_OCID_PLACEHOLDER",
)
OCI_DU_ENDPOINT = os.getenv(
    "OCI_DU_ENDPOINT",
    "https://example.invalid/integration-endpoint",
)
DU_TEMP_FOLDER = os.getenv("OS_DU_TEMP_FOLDER", "Classifier/DU/Temp")
DU_OUT_FOLDER  = os.getenv("OS_DU_OUT_FOLDER",  "Classifier/DU/Output")
 
PL_ORCHESTRATOR_FUNCTION_ID  = os.getenv("PL_ORCHESTRATOR_FUNCTION_ID",  "")
PL_ORCHESTRATOR_ENDPOINT     = os.getenv("PL_ORCHESTRATOR_ENDPOINT",
    "https://example.invalid/integration-endpoint")
INV_ORCHESTRATOR_FUNCTION_ID = os.getenv("INV_ORCHESTRATOR_FUNCTION_ID", "")
INV_ORCHESTRATOR_ENDPOINT    = os.getenv("INV_ORCHESTRATOR_ENDPOINT",
    "https://example.invalid/integration-endpoint")
BL_ORCHESTRATOR_FUNCTION_ID  = os.getenv("BL_ORCHESTRATOR_FUNCTION_ID",  "")
BL_ORCHESTRATOR_ENDPOINT     = os.getenv("BL_ORCHESTRATOR_ENDPOINT",
    "https://example.invalid/integration-endpoint")
 
TMP_DIR = "/tmp/ai_classifier_v2"
 
 
 
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
 
 
 
def get_signer():
    return get_resource_principals_signer()
 
def get_object_storage_client(signer):
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
 
def get_functions_client(signer, endpoint: str) -> FunctionsInvokeClient:
    return FunctionsInvokeClient(
        config={}, signer=signer,
        service_endpoint=endpoint,
        timeout=(10, 300),
    )
 
def get_genai_client(signer) -> GenerativeAiInferenceClient:
    return GenerativeAiInferenceClient(
        config={}, signer=signer,
        service_endpoint=OCI_GENAI_ENDPOINT,
        retry_strategy=oci.retry.NoneRetryStrategy(),
        timeout=(10, 60),
    )
 
def get_du_client(signer) -> AIServiceDocumentClient:
    return AIServiceDocumentClient(
        config={}, signer=signer,
        service_endpoint=OCI_DU_ENDPOINT,
    )
 
 
 
def upload_to_os(os_client, content, object_name,
                 content_type="application/json") -> bool:
    body = content.encode("utf-8") if isinstance(content, str) else content
    try:
        os_client.put_object(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
            object_name=object_name, put_object_body=body,
            content_type=content_type,
        )
        log.info("Saved â†’ %s", object_name)
        return True
    except Exception as exc:
        log.error("Upload failed '%s': %s", object_name, exc)
        return False
 
def download_pdf(os_client, object_path: str) -> bytes:
    os.makedirs(TMP_DIR, exist_ok=True)
    object_key = unquote(object_path.lstrip("/"))
    resp = os_client.get_object(
        namespace_name=OS_NAMESPACE,
        bucket_name=OS_INPUT_BUCKET,
        object_name=object_key,
    )
    chunks = []
    for chunk in resp.data.raw.stream(1024 * 1024, decode_content=False):
        chunks.append(chunk)
    return b"".join(chunks)
 
 
 
def extract_pages_digital(pdf_bytes: bytes) -> List[str]:
    """Extract text page-by-page using pdfplumber, pypdf fallback."""
    pages_text = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                page_text  = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
                table_rows = []
                for table in page.extract_tables():
                    for row in table:
                        table_rows.append(" | ".join(cell or "" for cell in row))
                combined = page_text
                if table_rows:
                    combined += "\n" + "\n".join(table_rows)
                pages_text.append(combined.strip())
        return pages_text
    except Exception as exc:
        log.warning("pdfplumber failed: %s â€” trying pypdf", exc)
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            return [(page.extract_text() or "").strip() for page in reader.pages]
        except Exception as exc2:
            log.error("pypdf also failed: %s", exc2)
            return []
 
def is_scanned(pdf_bytes: bytes) -> bool:
    """Returns True if PDF has little extractable text (likely scanned)."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = pdf.pages[:3]
            if not pages:
                return True
            avg = sum(
                len((p.extract_text(x_tolerance=2, y_tolerance=2) or "").strip())
                for p in pages
            ) / len(pages)
            return avg < MIN_CHARS_PER_PAGE
    except Exception:
        return True
 
def extract_pages_via_du(du_client, os_client,
                          obj_path: str, filename: str) -> List[str]:
    """
    Use OCI Document Understanding to OCR a scanned PDF.
    Returns list of page text strings.
    """
    safe_stem  = re.sub(r"[^\w\-]", "_", Path(filename).stem)
    run_id     = uuid.uuid4().hex[:8]
    temp_key   = f"{DU_TEMP_FOLDER}/{safe_stem}_{run_id}.pdf"
    du_out_pfx = f"{DU_OUT_FOLDER}/{safe_stem}_{run_id}"
 
    pdf_bytes = download_pdf(os_client, obj_path)
    upload_to_os(os_client, pdf_bytes, temp_key, "application/pdf")
 
    job_details = oci.ai_document.models.CreateProcessorJobDetails(
        display_name   = f"classifier-{safe_stem}-{run_id}",
        compartment_id = OCI_COMPARTMENT_ID,
        input_location = oci.ai_document.models.ObjectStorageLocations(
            object_locations=[oci.ai_document.models.ObjectLocation(
                namespace_name=OS_NAMESPACE,
                bucket_name=OS_BUCKET,
                object_name=temp_key,
            )]
        ),
        output_location=oci.ai_document.models.OutputLocation(
            namespace_name=OS_NAMESPACE,
            bucket_name=OS_BUCKET,
            prefix=du_out_pfx,
        ),
        processor_config=oci.ai_document.models.GeneralProcessorConfig(
            features=[oci.ai_document.models.DocumentTextExtractionFeature(
                generate_searchable_pdf=False,
            )],
            is_zip_output_enabled=False,
        ),
    )
 
    job_id = du_client.create_processor_job(
        create_processor_job_details=job_details
    ).data.id
    _plog("[DU] Job created: %s for %s", job_id, filename)
 
    elapsed, status = 0, "SUBMITTED"
    while elapsed < 300:
        time.sleep(5)
        elapsed += 5
        status = du_client.get_processor_job(
            processor_job_id=job_id
        ).data.lifecycle_state
        _plog("[DU] Status: %s (%ds)", status, elapsed)
        if status in ("SUCCEEDED", "FAILED", "CANCELED"):
            break
 
    if status != "SUCCEEDED":
        raise RuntimeError(f"DU job {job_id} ended with status: {status}")
 
    resp       = os_client.list_objects(
        namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=du_out_pfx,
    )
    all_keys   = [o.name for o in resp.data.objects]
    result_key = next((k for k in all_keys if k.endswith("analysedDocument.json")), None) \
              or next((k for k in all_keys if k.endswith(".json")), None)
 
    if not result_key:
        raise FileNotFoundError(f"No DU result under {du_out_pfx}")
 
    raw     = os_client.get_object(
        namespace_name=OS_NAMESPACE,
        bucket_name=OS_BUCKET,
        object_name=result_key,
    ).data.content.decode("utf-8")
    du_data = json.loads(raw)
 
    page_texts = []
    for page in du_data.get("pages", []):
        lines = [
            ln.get("text", "").strip()
            for ln in page.get("lines", [])
            if ln.get("text", "").strip()
        ]
        page_texts.append(
            f"--- Page {page.get('pageNumber', '?')} ---\n" + "\n".join(lines)
        )
 
    _plog("[DU] Extracted %d pages from %s", len(page_texts), filename)
    return page_texts
 
def extract_image_via_du(du_client, os_client, obj_path: str, filename: str) -> List[str]:
    import base64
    safe_stem  = re.sub(r"[^\w\-]", "_", Path(filename).stem)
    run_id     = uuid.uuid4().hex[:8]
    du_out_pfx = f"{DU_OUT_FOLDER}/{safe_stem}_{run_id}"

    img_bytes = download_pdf(os_client, obj_path)
    b64_data  = base64.b64encode(img_bytes).decode("utf-8")
    _plog("[DU-IMG] Using inline base64 content (%d bytes)", len(img_bytes))

    job_details = oci.ai_document.models.CreateProcessorJobDetails(
        display_name   = f"classifier-img-{safe_stem}-{run_id}",
        compartment_id = OCI_COMPARTMENT_ID,
        input_location = oci.ai_document.models.InlineDocumentContent(
            data=b64_data,
        ),
        output_location=oci.ai_document.models.OutputLocation(
            namespace_name=OS_NAMESPACE,
            bucket_name=OS_BUCKET,
            prefix=du_out_pfx,
        ),
        processor_config=oci.ai_document.models.GeneralProcessorConfig(
            features=[oci.ai_document.models.DocumentTextExtractionFeature(
                generate_searchable_pdf=False,
            )],
            is_zip_output_enabled=False,
        ),
    )

    job_id = du_client.create_processor_job(
        create_processor_job_details=job_details
    ).data.id
    _plog("[DU-IMG] Job created: %s", job_id)

    elapsed, status = 0, "SUBMITTED"
    while elapsed < 180:
        time.sleep(5)
        elapsed += 5
        status = du_client.get_processor_job(
            processor_job_id=job_id
        ).data.lifecycle_state
        _plog("[DU-IMG] Status: %s (%ds)", status, elapsed)
        if status in ("SUCCEEDED", "FAILED", "CANCELED"):
            break

    if status != "SUCCEEDED":
        raise RuntimeError(f"DU image job {job_id} ended: {status}")

    resp       = os_client.list_objects(
        namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=du_out_pfx,
    )
    all_keys   = [o.name for o in resp.data.objects]
    result_key = next((k for k in all_keys if k.endswith("analysedDocument.json")), None) \
              or next((k for k in all_keys if k.endswith(".json")), None)

    if not result_key:
        raise FileNotFoundError(f"No DU result under {du_out_pfx}")

    raw     = os_client.get_object(
        namespace_name=OS_NAMESPACE,
        bucket_name=OS_BUCKET,
        object_name=result_key,
    ).data.content.decode("utf-8")
    du_data = json.loads(raw)

    page_texts = []
    for page in du_data.get("pages", []):
        lines = [
            ln.get("text", "").strip()
            for ln in page.get("lines", [])
            if ln.get("text", "").strip()
        ]
        page_texts.append(
            f"--- Page {page.get('pageNumber', 1)} ---\n" + "\n".join(lines)
        )

    _plog("[DU-IMG] Extracted %d page(s)", len(page_texts))
    return page_texts


def extract_excel_file(os_client, obj_path: str, filename: str) -> Tuple[str, List[str]]:
    """
    Read Excel/CSV and detect doc type from column headers.
    Returns (doc_type, [page_text]) â€” no DU, no LLM needed for type detection.
    """
    import pandas as pd

    file_bytes = download_pdf(os_client, obj_path)
    ext = Path(filename).suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(io.BytesIO(file_bytes), header=None)
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), header=None)

    doc_type   = None
    header_row = None
    for i, row in df.iterrows():
        row_vals = [str(v).strip().lower() for v in row if str(v).strip() not in ("", "nan")]
        if "lpn" in row_vals and "locations" in row_vals:
            doc_type   = "lpn_putaway"
            header_row = i
            break
        if "lpn" in row_vals and "split lpn" in row_vals:
            doc_type   = "lpn_split"
            header_row = i
            break

    if not doc_type:
        text = df.to_string(index=False)
        _plog("  %s â†’ unknown Excel, converting to text for LLM", filename)
        return "unknown", [text]

    data_df  = df.iloc[header_row:].reset_index(drop=True)
    text     = data_df.to_string(index=False)
    _plog("  %s â†’ detected as %s", filename, doc_type)
    return doc_type, [text]

 
_PAGE_CLASSIFY_PROMPT = """\
Classify this shipping document page. Reply with exactly one word only:
  invoice
  packing_list
  bill_of_lading
  lpn_putaway
  lpn_split
  other
 
Rules:
- invoice        â†’ commercial invoice, unit price, total amount, payment terms, bank details
- packing_list   â†’ packing list, picking list, net weight, gross weight, CBM, pkg no, dimensions, batch number, expiry date
- bill_of_lading â†’ bill of lading, airway bill, AWB, TCN, truck consignment note, notify party
- lpn_putaway    â†’ LPN numbers paired with location barcodes (e.g. A1-1-10-2), putaway list
- lpn_split      â†’ LPN numbers with split quantities, Split LPN column present
- other          â†’ cover page, blank, terms and conditions, certificate, unrelated
 
Page text:
{text}
 
One word only â€” no explanation:"""
 
LPN_PUTAWAY_PROMPT = """\
Extract all LPN putaway rows from the document below and return JSON.

RULES:
- Each row has an LPN number and a Location barcode
- Strip any leading/trailing whitespace or tab characters from values
- LPN column may be named: LPN, lpn_nbr, LPN Number
- Location column may be named: Locations, Location, location_barcode
- Skip header rows â€” only extract data rows

Return JSON only (no markdown fences, no prose):
{{
  "doc_type": "lpn_putaway",
  "locates": [
    {{
      "lpn_nbr": "CSDEMOTEST_DC00000531_1",
      "location_barcode": "A1-1-10-2"
    }}
  ]
}}

Document text:
{text}"""

LPN_SPLIT_PROMPT = """\
Extract ALL Split LPN rows from the document below and return JSON.

CRITICAL RULES:
- Extract EVERY row â€” do not skip or merge any rows
- The table has exactly these 4 columns in order:
  Column 1: LPN (original LPN number)
  Column 2: Current Qty (number)
  Column 3: Split LPN (new split LPN number)
  Column 4: Split Qty (number)
- current_qty and split_qty must be integers not strings
- Do NOT invent or merge data â€” extract exactly what is in the document
- If you see 5 data rows, return exactly 5 objects

Return JSON only (no markdown fences, no prose):
{{
  "doc_type": "lpn_split",
  "splits": [
    {{
      "lpn_nbr":       "CSDEMOTEST_DC00000369",
      "current_qty":   5,
      "split_lpn_nbr": "CSDEMOTEST_DC00000369_2",
      "split_qty":     3
    }}
  ]
}}

Document text:
{text}"""
 

def extract_lpn_via_llm(genai_client, doc_type: str, text: str) -> dict:
    """Use LLM to structure LPN data into final JSON."""
    import re as _re

    prompt = LPN_PUTAWAY_PROMPT.format(text=text) \
             if doc_type == "lpn_putaway" \
             else LPN_SPLIT_PROMPT.format(text=text)

    chat_request = CohereChatRequest(
        message=prompt,
        max_tokens=2000,
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
    raw = genai_client.chat(chat_detail).data.chat_response.text.strip()

    cleaned = _re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
    try:
        return json.loads(cleaned)
    except Exception:
        match = _re.search(r"\{.*\}", cleaned, _re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
    log.warning("LPN LLM parse failed â€” returning raw")
    return {"raw_response": raw, "parse_error": True}


def classify_page_llm(genai_client, page_text: str) -> str:
    """
    LLM classify one page. 10 output tokens â€” very cheap.
    Returns: 'invoice' | 'packing_list' | 'bill_of_lading' | 'other'
    """
    try:
        prompt = _PAGE_CLASSIFY_PROMPT.format(text=page_text[:1500])
        chat_request = CohereChatRequest(
            message=prompt,
            max_tokens=10,
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
        raw = genai_client.chat(chat_detail).data.chat_response.text.strip().lower()
 
        if "invoice"  in raw: return "invoice"
        if "packing"  in raw: return "packing_list"
        if "picking"  in raw: return "packing_list"
        if "lading"   in raw: return "bill_of_lading"
        if "awb"      in raw: return "bill_of_lading"
        if "tcn"      in raw: return "bill_of_lading"
        if "consignment" in raw: return "bill_of_lading"
        if "lpn_putaway" in raw: return "lpn_putaway"
        if "lpn_split"   in raw: return "lpn_split"
        return "other"
 
    except Exception as exc:
        log.warning("LLM page classify failed: %s", exc)
        return "other"
 
 
def classify_all_pages_parallel(genai_client,
                                  pages_text: List[str]) -> List[str]:
    """
    Classify ALL pages in parallel using LLM.
    10 tokens per page â€” for 20 pages = 200 tokens total output.
    With parallelism: ~8-12s regardless of page count.
    """
    if not pages_text:
        return []
 
    total   = len(pages_text)
    workers = min(total, 10)
    _plog("Classifying %d pages in parallel (%d workers)", total, workers)
 
    def _one(args: Tuple[int, str]) -> Tuple[int, str]:
        idx, text = args
        label = classify_page_llm(genai_client, text)
        _plog("  Page %d â†’ %s", idx + 1, label)
        return idx, label
 
    labels = ["other"] * total
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_one, (i, text)): i
            for i, text in enumerate(pages_text)
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                idx, label = future.result()
                labels[idx] = label
            except Exception as exc:
                log.error("Page classify future failed: %s", exc)
 
    return labels
 
 
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".gif"}
EXCEL_EXTENSIONS = {".xlsx", ".xls", ".csv"}

def process_one_file(args: tuple) -> dict:
    os_client, genai_client, du_client, obj_path, supplier_name = args
    filename = Path(obj_path).name
    ext      = Path(filename).suffix.lower()
    _plog("=" * 40)
    _plog("Processing file: %s", filename)

    if ext in EXCEL_EXTENSIONS:
        _plog("  %s â†’ Excel/CSV file", filename)
        try:
            doc_type, pages_text = extract_excel_file(os_client, obj_path, filename)
        except Exception as exc:
            log.error("Excel extraction failed %s: %s", filename, exc)
            return {
                "filename":    filename,
                "obj_path":    obj_path,
                "error":       f"excel_extraction_failed: {exc}",
                "types_found": {},
            }

        if not pages_text:
            return {
                "filename":    filename,
                "obj_path":    obj_path,
                "error":       "no_text_extracted_from_excel",
                "types_found": {},
            }

        if doc_type in ("lpn_putaway", "lpn_split"):
            _plog("  %s â†’ extracting %s via LLM", filename, doc_type)
            result_json = extract_lpn_via_llm(genai_client, doc_type, pages_text[0])
            _plog("  %s â†’ extracted: %s", filename, json.dumps(result_json)[:100])
            return {
                "filename":       filename,
                "obj_path":       obj_path,
                "scanned":        False,
                "types_found":    {doc_type: pages_text},
                "extracted_data": result_json,   # â† carry extracted JSON
                "doc_type":       doc_type,
            }

        labels = classify_all_pages_parallel(genai_client, pages_text)
        types_found: Dict[str, List[str]] = {}
        for page, label in zip(pages_text, labels):
            if label != "other":
                types_found.setdefault(label, []).append(page)

        _plog("  %s â†’ types found: %s",
              filename, {t: len(p) for t, p in types_found.items()})
        return {
            "filename":    filename,
            "obj_path":    obj_path,
            "scanned":     False,
            "types_found": types_found,
        }

    if ext in IMAGE_EXTENSIONS:
        _plog("  %s â†’ image file, using DU inline OCR", filename)
        try:
            pages_text = extract_image_via_du(
                du_client, os_client, obj_path, filename
            )
            _plog("  DU inline OCR extracted %d pages from image", len(pages_text))
        except Exception as exc:
            log.error("Image DU failed %s: %s", filename, exc)
            return {
                "filename":    filename,
                "obj_path":    obj_path,
                "error":       f"image_du_failed: {exc}",
                "types_found": {},
            }

        if not pages_text:
            return {
                "filename":    filename,
                "obj_path":    obj_path,
                "error":       "no_text_extracted_from_image",
                "types_found": {},
            }

        labels = classify_all_pages_parallel(genai_client, pages_text)
        types_found: Dict[str, List[str]] = {}
        for page, label in zip(pages_text, labels):
            if label != "other":
                types_found.setdefault(label, []).append(page)

        _plog("  %s â†’ types found: %s",
              filename, {t: len(p) for t, p in types_found.items()})
        return {
            "filename":    filename,
            "obj_path":    obj_path,
            "scanned":     True,
            "types_found": types_found,
        }

    try:
        pdf_bytes = download_pdf(os_client, obj_path)
    except Exception as exc:
        log.error("Download failed %s: %s", filename, exc)
        return {
            "filename":    filename,
            "obj_path":    obj_path,
            "error":       f"download_failed: {exc}",
            "types_found": {},
        }

    scanned = is_scanned(pdf_bytes)
    _plog("  %s â†’ scanned=%s", filename, scanned)

    if scanned:
        try:
            pages_text = extract_pages_via_du(
                du_client, os_client, obj_path, filename
            )
            _plog("  DU extracted %d pages", len(pages_text))
        except Exception as exc:
            log.error("DU failed for %s: %s â€” falling back to pdfplumber", filename, exc)
            pages_text = extract_pages_digital(pdf_bytes)
    else:
        pages_text = extract_pages_digital(pdf_bytes)
        _plog("  pdfplumber extracted %d pages", len(pages_text))

    if not pages_text:
        log.error("No text extracted from %s", filename)
        return {
            "filename":    filename,
            "obj_path":    obj_path,
            "error":       "no_text_extracted",
            "types_found": {},
        }

    labels = classify_all_pages_parallel(genai_client, pages_text)
    types_found: Dict[str, List[str]] = {}
    for page, label in zip(pages_text, labels):
        if label != "other":
            types_found.setdefault(label, []).append(page)

    _plog("  %s â†’ types found: %s",
          filename, {t: len(p) for t, p in types_found.items()})
    return {
        "filename":    filename,
        "obj_path":    obj_path,
        "scanned":     scanned,
        "types_found": types_found,
    }

 
 
 
 
 
 
 
 
 
 
 
 
_SUPPLEMENTAL_PATTERNS = [
    r"supplemental\s+invoice",
    r"freight\s+invoice",
    r"additional\s+charges",
    r"debit\s+note",
    r"credit\s+note",
    r"amendment.*invoice",
]
 
SUPPLIER_CONFIG = {
    "WEG":      {"has_supplemental": True},
    "TCL":      {"has_supplemental": False},
    "SupplierA":    {"has_supplemental": False},
    "SupplierB":  {"has_supplemental": False},
    "SupplierD":    {"has_supplemental": False},
    "SupplierC": {"has_supplemental": False},
}
 
def is_supplemental_invoice(pages_text: List[str], supplier_name: str) -> bool:
    s = (supplier_name or "").upper()
    config = next((v for k, v in SUPPLIER_CONFIG.items() if k in s), {})
    if not config.get("has_supplemental", False):
        return False
    full_text = " ".join(pages_text).lower()
    return any(re.search(p, full_text) for p in _SUPPLEMENTAL_PATTERNS)
 
def get_doc_key(doc_type: str, counter: int) -> str:
    if doc_type == "bill_of_lading":
        return "bill_of_lading"
    return f"{doc_type}_{counter:02d}"
 
def _safe(s: str) -> str:
    return re.sub(r"[^\w\-]", "_", s) if s else "unknown"
 
def write_shipment_manifest(os_client, shipment_id: str,
                             doc_entries: dict) -> str:
    manifest = {
        "shipmentId": shipment_id,
        "documents":  doc_entries,
        "totalDocs":  len(doc_entries),
        "status":     "in_progress",
        "createdAt":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    path = f"{SHIPMENTS_FOLDER}/{_safe(shipment_id)}.json"
    upload_to_os(os_client, json.dumps(manifest, indent=2, ensure_ascii=False), path)
    return path
 
 
 
def _fire(signer, function_id: str, endpoint: str, payload: dict) -> bool:
    if not function_id:
        log.error("Cannot fire: function_id not set for endpoint %s", endpoint)
        return False
    for attempt in range(1, 4):
        try:
            fn_client = get_functions_client(signer, endpoint)
            fn_client.invoke_function(
                function_id=function_id,
                invoke_function_body=json.dumps(payload).encode("utf-8"),
                fn_invoke_type="detached",
            )
            log.info("Fired â†’ %s (detached)", endpoint)
            return True
        except Exception as exc:
            log.warning("Fire attempt %d/3 failed [%s]: %s", attempt, endpoint, exc)
            if attempt < 3:
                time.sleep(attempt * 2)
    log.error("All 3 fire attempts failed for %s", endpoint)
    return False
 
def fire_pl_orchestrator(signer, request_id, classified_path, shipment_id,
                          order_base_gid, ob_ship_unit_gid, supplier_name,
                          doc_key):
    return _fire(signer, PL_ORCHESTRATOR_FUNCTION_ID, PL_ORCHESTRATOR_ENDPOINT, {
        "requestId":            request_id,
        "classified_pages_path": classified_path,   # â† NEW: pre-classified pages
        "shipmentId":           shipment_id,
        "supplierName":         supplier_name,
        "shipment":             order_base_gid,
        "supplier":             ob_ship_unit_gid,
        "docKey":               doc_key,
    })
 
def fire_invoice_orchestrator(signer, request_id, classified_path, shipment_id,
                               order_base_gid, ob_ship_unit_gid, supplier_name,
                               doc_key):
    return _fire(signer, INV_ORCHESTRATOR_FUNCTION_ID, INV_ORCHESTRATOR_ENDPOINT, {
        "requestId":            request_id,
        "classified_pages_path": classified_path,
        "shipmentId":           shipment_id,
        "supplierName":         supplier_name,
        "shipment":             order_base_gid,
        "supplier":             ob_ship_unit_gid,
        "docKey":               doc_key,
    })
 
def fire_bl_orchestrator(signer, request_id, classified_path, shipment_id,
                          order_base_gid, ob_ship_unit_gid, supplier_name,
                          doc_key):
    return _fire(signer, BL_ORCHESTRATOR_FUNCTION_ID, BL_ORCHESTRATOR_ENDPOINT, {
        "requestId":            request_id,
        "classified_pages_path": classified_path,
        "shipmentId":           shipment_id,
        "supplierName":         supplier_name,
        "shipment":             order_base_gid,
        "supplier":             ob_ship_unit_gid,
        "docKey":               doc_key,
    })
 
 
 
def classify_and_dispatch(os_client, signer, genai_client,
                           du_client, body: dict) -> dict:
 
    batch_request_id = body.get("requestId") or \
                       f"REQ-{uuid.uuid4().hex[:8].upper()}"
    shipment_id      = f"SHP-{batch_request_id}"
    supplier_name    = body.get("supplierName") or ""
    order_base_gid   = body.get("orderBaseGid") or body.get("shipment") or ""
    ob_ship_unit_gid = body.get("obShipUnitGid") or body.get("supplier") or ""
    files            = body.get("files") or []
 
    _plog("=" * 60)
    _plog("Classifier v2.0.0 â€” requestId=%s  files=%d",
          batch_request_id, len(files))
 
    classify_args = [
        (
            os_client, genai_client, du_client,
            f.get("path") or f.get("object_storage_path") or "",
            supplier_name,
        )
        for f in files
        if f.get("path") or f.get("object_storage_path")
    ]
 
    file_results = []
    if classify_args:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(classify_args)
        ) as ex:
            futures = {
                ex.submit(process_one_file, a): a
                for a in classify_args
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    file_results.append(future.result())
                except Exception as exc:
                    arg = futures[future]
                    log.error("process_one_file failed for %s: %s", arg[2], exc)
                    file_results.append({
                        "filename":    Path(arg[2]).name,
                        "obj_path":    arg[2],
                        "error":       str(exc),
                        "types_found": {},
                    })
 
    lpn_saved    = []
    remaining_results = []
    for result in file_results:
        if result.get("extracted_data") and result.get("doc_type") in ("lpn_putaway", "lpn_split"):
            doc_type  = result["doc_type"]
            extracted = result["extracted_data"]
            req_id    = f"LPN-{batch_request_id}-{uuid.uuid4().hex[:4].upper()}"
            output_key = f"LPN/{_safe(shipment_id)}/{doc_type}_{req_id}.json"

            extracted["_meta"] = {
                "requestId":   req_id,
                "shipmentId":  shipment_id,
                "sourceFile":  result["obj_path"],
                "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            upload_to_os(
                os_client,
                json.dumps(extracted, ensure_ascii=False, indent=2),
                output_key,
            )
            _plog("Saved %s â†’ %s", doc_type, output_key)
            lpn_saved.append({
                "docType":   doc_type,
                "requestId": req_id,
                "savedTo":   output_key,
                "fired":     False,
            })
        else:
            remaining_results.append(result)

    file_results = remaining_results  # only non-LPN results go through normal flow

    for result in file_results:
        types_found = result.get("types_found", {})
        for doc_type, pages in list(types_found.items()):
            if doc_type in ("lpn_putaway", "lpn_split"):
                _plog("LPN image detected â†’ extracting %s via LLM", doc_type)
                extracted = extract_lpn_via_llm(genai_client, doc_type, pages[0])
                req_id     = f"LPN-{batch_request_id}-{uuid.uuid4().hex[:4].upper()}"
                output_key = f"LPN/{_safe(shipment_id)}/{doc_type}_{req_id}.json"
                extracted["_meta"] = {
                    "requestId":   req_id,
                    "shipmentId":  shipment_id,
                    "sourceFile":  result["obj_path"],
                    "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                upload_to_os(
                    os_client,
                    json.dumps(extracted, ensure_ascii=False, indent=2),
                    output_key,
                )
                _plog("Saved %s â†’ %s", doc_type, output_key)
                lpn_saved.append({
                    "docType":   doc_type,
                    "requestId": req_id,
                    "savedTo":   output_key,
                    "fired":     False,
                })
                del types_found[doc_type]


    merged_types: Dict[str, dict] = {}
    type_counters = {
        "packing_list":  0,
        "invoice":       0,
        "bill_of_lading":0,
        "lpn_putaway":   0,   # â† ADD
        "lpn_split":     0,   # â† ADD
    }
    prefix_map = {
        "packing_list":   "PL",
        "invoice":        "INV",
        "bill_of_lading": "BL",
        "lpn_putaway":    "LPN",   # â† ADD
        "lpn_split":      "SLPN",  # â† ADD
    }
 
    for result in file_results:
        if result.get("error"):
            _plog("Skipping %s â€” error: %s",
                  result["filename"], result["error"])
            continue
 
        for doc_type, pages in result.get("types_found", {}).items():
            if doc_type not in type_counters:
                continue
 
            if doc_type == "invoice":
                if is_supplemental_invoice(pages, supplier_name):
                    _plog("Supplemental invoice detected in %s â€” routing as invoice_02",
                          result["filename"])
                    sup_doc_key = "invoice_02"
                    sup_req_id  = f"INV-{batch_request_id}-02"
                    sup_path    = f"{CLASSIFIED_FOLDER}/{_safe(shipment_id)}/{sup_doc_key}.json"
                    sup_payload = {
                        "shipmentId":    shipment_id,
                        "doc_key":       sup_doc_key,
                        "doc_type":      "invoice",
                        "req_id":        sup_req_id,
                        "supplier_name": supplier_name,
                        "page_count":    len(pages),
                        "pages":         pages,
                        "source_file":   result["obj_path"],
                    }
                    upload_to_os(
                        os_client,
                        json.dumps(sup_payload, ensure_ascii=False),
                        sup_path,
                    )
                    _plog("Saved classified pages â†’ %s (%d pages)",
                          sup_path, len(pages))
                    merged_types[sup_doc_key] = {
                        "doc_type": "invoice",
                        "req_id":   sup_req_id,
                        "pages":    pages,
                        "obj_path": result["obj_path"],
                    }
                    continue
 
            type_counters[doc_type] += 1
            count   = type_counters[doc_type]
            doc_key = get_doc_key(doc_type, count)
            req_id  = f"{prefix_map[doc_type]}-{batch_request_id}-{count:02d}"
 
            if doc_key in merged_types:
                merged_types[doc_key]["pages"].extend(pages)
                _plog("Merged %d more pages into %s (total=%d)",
                      len(pages), doc_key, len(merged_types[doc_key]["pages"]))
            else:
                merged_types[doc_key] = {
                    "doc_type": doc_type,
                    "req_id":   req_id,
                    "pages":    pages,
                    "obj_path": result["obj_path"],
                }
 
    _plog("Classified doc types: %s",
          {k: len(v["pages"]) for k, v in merged_types.items()})
 
    classified_base = f"{CLASSIFIED_FOLDER}/{_safe(shipment_id)}"
    classified_paths = {}
 
    for doc_key, info in merged_types.items():
        classified_path = f"{classified_base}/{doc_key}.json"
        payload = {
            "shipmentId":   shipment_id,
            "doc_key":      doc_key,
            "doc_type":     info["doc_type"],
            "req_id":       info["req_id"],
            "supplier_name": supplier_name,
            "page_count":   len(info["pages"]),
            "pages":        info["pages"],
            "source_file":  info["obj_path"],
        }
        upload_to_os(
            os_client,
            json.dumps(payload, ensure_ascii=False),
            classified_path,
        )
        classified_paths[doc_key] = classified_path
        _plog("Saved classified pages â†’ %s (%d pages)",
              classified_path, len(info["pages"]))
 
    doc_entries = {
        doc_key: {
            "requestId": info["req_id"],
            "status":    "pending",
        }
        for doc_key, info in merged_types.items()
    }
    shipment_path = write_shipment_manifest(
        os_client, shipment_id, doc_entries
    )
    _plog("Manifest written â†’ %s (totalDocs=%d)",
          shipment_path, len(doc_entries))
 
    fire_fn_map = {
        "packing_list":   fire_pl_orchestrator,
        "invoice":        fire_invoice_orchestrator,
        "bill_of_lading": fire_bl_orchestrator,
    }

    dispatched = []
    first = True

    for doc_key, info in merged_types.items():
        if not first:
            time.sleep(3)
        first = False

        fire_fn = fire_fn_map.get(info["doc_type"])  # â† .get() not []
        if not fire_fn:
            _plog("No orchestrator for doc_type=%s â€” skipping", info["doc_type"])
            dispatched.append({
                "docType":        doc_key,
                "requestId":      info["req_id"],
                "pages":          len(info["pages"]),
                "classifiedPath": classified_paths.get(doc_key, ""),
                "fired":          False,
                "reason":         "no_orchestrator",
            })
            continue

        req_id          = info["req_id"]
        classified_path = classified_paths[doc_key]

        ok = fire_fn(
            signer, req_id, classified_path, shipment_id,
            order_base_gid, ob_ship_unit_gid, supplier_name, doc_key,
        )

        if not ok and doc_key in doc_entries:
            doc_entries[doc_key]["status"] = "fire_failed"

        dispatched.append({
            "docType":          doc_key,
            "requestId":        req_id,
            "pages":            len(info["pages"]),
            "classifiedPath":   classified_path,
            "fired":            ok,
        })
        _plog("Fired %s orchestrator â†’ req=%s pages=%d %s",
              info["doc_type"], req_id, len(info["pages"]),
              "âœ“" if ok else "âœ—")
 
    expected = {"packing_list", "invoice", "bill_of_lading"}
    found    = {info["doc_type"] for info in merged_types.values()}
    missing  = expected - found
    if missing:
        _plog("WARNING: Missing doc types: %s", missing)
 
    return {
            "status":           "processing",
            "batchRequestId":   batch_request_id,
            "shipmentId":       shipment_id,
            "supplierName":     supplier_name,
            "filesProcessed":   len(files),
            "dispatched":       dispatched + lpn_saved,                                    # â† ADD lpn_saved
            "shipmentManifest": shipment_path,
            "documentsRouted":  list(merged_types.keys()) + [r["docType"] for r in lpn_saved],  # â† ADD
            "missingDocTypes":  list(missing),
        }
 
 
 
def handler(ctx, data: io.BytesIO = None):
    os.makedirs(TMP_DIR, exist_ok=True)
    t_start = time.time()
    _plog("=" * 60)
    _plog("AI SAMPLE_ORG Classifier v2.0.0 invoked")
 
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
            _plog("Input keys: %s", list(body.keys()))
        except Exception as exc:
            log.warning("Body parse failed: %s", exc)
 
    if not body.get("files"):
        msg = "Missing required field: files (list of {path})"
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "message": msg}),
            headers={"Content-Type": "application/json"},
            status_code=400,
        )
 
    signer       = get_signer()
    os_client    = get_object_storage_client(signer)
    genai_client = get_genai_client(signer)
    du_client    = get_du_client(signer)
 
    try:
        result = classify_and_dispatch(
            os_client, signer, genai_client, du_client, body
        )
    except Exception as exc:
        log.exception("Classifier failed")
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "message": str(exc)}),
            headers={"Content-Type": "application/json"},
            status_code=500,
        )
 
    result["durationSeconds"] = round(time.time() - t_start, 2)
    _plog("Classifier done in %.1fs", result["durationSeconds"])
    _plog("=" * 60)
 
    return fdk_response.Response(
        ctx,
        response_data=json.dumps(result, ensure_ascii=False),
        headers={"Content-Type": "application/json"},
    )