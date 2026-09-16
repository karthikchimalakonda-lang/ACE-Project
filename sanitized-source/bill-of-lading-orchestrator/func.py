# """
# AI Bill of Lading Orchestrator  â€”  OCI FUNCTIONS v1.0.0
# ========================================================
# ROLE: Orchestrator for Bill of Lading extraction.
# Mirrors the PL orchestrator pattern exactly.

# Pipeline:
# 1.  Download PDF from Object Storage
# 2.  Extract text page-by-page
# 3.  Classify pages â†’ keep bill_of_lading pages
# 4.  Split into chunks (default 3 â€” BOLs are typically 1-3 pages)
# 5.  Save manifest â†’ OS: Bill of Lading/{req}/manifest.json
# 6.  Fire ai-bl-extractor chunk 1 DETACHED â†’ return immediately
# """

# import io
# import json
# import logging
# import os
# import re
# import sys
# import time
# import uuid
# from pathlib import Path
# from typing import List, Optional
# from urllib.parse import unquote

# import pdfplumber
# from pypdf import PdfReader

# import oci
# from oci.auth.signers import get_resource_principals_signer
# from oci.functions.functions_invoke_client import FunctionsInvokeClient
# from fdk import response as fdk_response


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  CONFIGURATION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
# OS_NAMESPACE       = os.getenv("OS_NAMESPACE",    "sample_namespace")
# OS_BUCKET          = os.getenv("OS_BUCKET",       "sample-workflow-bucket")
# OS_INPUT_BUCKET    = os.getenv("OS_INPUT_BUCKET", "sample-workflow-bucket")

# BL_BASE_FOLDER     = os.getenv("OS_BL_BASE_FOLDER", "Bill of Lading")
# MIN_CHARS_PER_PAGE = int(os.getenv("MIN_CHARS_PER_PAGE", "80"))
# CHUNK_SIZE         = int(os.getenv("CHUNK_SIZE", "3"))

# EXTRACTOR_FUNCTION_ID = os.getenv("EXTRACTOR_FUNCTION_ID", "")
# EXTRACTOR_ENDPOINT    = os.getenv(
#     "EXTRACTOR_ENDPOINT",
#     "https://example.invalid/integration-endpoint",
# )

# OTM_DOMAIN_NAME = os.getenv("OTM_DOMAIN_NAME", "SAMPLE_ORG")
# TMP_DIR = "/tmp/ai_bl_orchestrator"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  LOGGING
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# class _TeeHandler(logging.StreamHandler):
#     def emit(self, record):
#         super().emit(record)
#         print(self.format(record), flush=True)

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[_TeeHandler(sys.stderr)],
# )
# log = logging.getLogger(__name__)

# def _plog(msg, *args):
#     print(f"â–¶ {msg % args if args else msg}", flush=True)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  FOLDER HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _safe(s: str) -> str:
#     return re.sub(r"[^\w\-]", "_", s) if s else "unknown"

# def _req_folder(safe_req: str) -> str:
#     return f"{BL_BASE_FOLDER}/{safe_req}"

# def _manifest_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/manifest.json"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 1  â€”  OCI CLIENTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def get_signer():
#     return get_resource_principals_signer()

# def get_object_storage_client(signer):
#     return oci.object_storage.ObjectStorageClient(config={}, signer=signer)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 2  â€”  OBJECT STORAGE HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def upload_to_object_storage(os_client, content, object_name, content_type="application/json"):
#     body = content.encode("utf-8") if isinstance(content, str) else content
#     try:
#         os_client.put_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
#             object_name=object_name, put_object_body=body, content_type=content_type,
#         )
#         log.info("Uploaded â†’ %s", object_name)
#         return True
#     except Exception as exc:
#         log.error("Upload failed '%s': %s", object_name, exc)
#         return False

# def download_pdf(os_client, object_path: str) -> bytes:
#     os.makedirs(TMP_DIR, exist_ok=True)
#     object_key = unquote(object_path.lstrip("/"))
#     log.info("Downloading: %s", object_key)
#     resp = os_client.get_object(
#         namespace_name=OS_NAMESPACE, bucket_name=OS_INPUT_BUCKET, object_name=object_key,
#     )
#     chunks = []
#     for chunk in resp.data.raw.stream(1024 * 1024, decode_content=False):
#         chunks.append(chunk)
#     return b"".join(chunks)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 3  â€”  TEXT EXTRACTION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def extract_pages(pdf_bytes: bytes) -> List[str]:
#     pages_text = []
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             for i, page in enumerate(pdf.pages, start=1):
#                 page_text  = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
#                 table_rows = []
#                 for table in page.extract_tables():
#                     for row in table:
#                         table_rows.append(" | ".join(cell or "" for cell in row))
#                 combined = page_text
#                 if table_rows:
#                     combined += "\n" + "\n".join(table_rows)
#                 pages_text.append(combined.strip())
#                 log.info("  Page %d: %d chars", i, len(combined))
#         return pages_text
#     except Exception as exc:
#         log.warning("pdfplumber failed: %s â€” trying pypdf", exc)
#         try:
#             reader = PdfReader(io.BytesIO(pdf_bytes))
#             return [(page.extract_text() or "").strip() for page in reader.pages]
#         except Exception as exc2:
#             log.error("pypdf also failed: %s", exc2)
#             return []

# def is_scanned(pdf_bytes: bytes) -> bool:
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             pages = pdf.pages[:3]
#             if not pages:
#                 return True
#             avg = sum(
#                 len((p.extract_text(x_tolerance=2, y_tolerance=2) or "").strip())
#                 for p in pages
#             ) / len(pages)
#             return avg < MIN_CHARS_PER_PAGE
#     except Exception:
#         return False


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 4  â€”  PAGE CLASSIFIER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# _BL_PATTERNS = [
#     r"bill\s+of\s+lading", r"\bb/l\b", r"\bbol\b",
#     r"airway\s+bill", r"\bawb\b", r"ocean\s+bill",
#     r"notify\s+party", r"place\s+of\s+receipt",
#     r"on\s+board\s+date", r"freight\s+(prepaid|collect)",
#     r"original.*bill",
# ]
# _INVOICE_PATTERNS = [
#     r"commercial\s+invoice", r"invoice\s+n[o0]", r"unit\s+price",
#     r"total\s+amount", r"\bfob\b|\bcfr\b", r"payment\s+terms",
# ]

# def classify_pages_by_regex(pages_text: List[str]) -> List[str]:
#     labels = []
#     for i, page in enumerate(pages_text):
#         lower    = page.lower()
#         bl_score = sum(1 for p in _BL_PATTERNS      if re.search(p, lower))
#         ot_score = sum(1 for p in _INVOICE_PATTERNS  if re.search(p, lower))
#         if bl_score == 0 and ot_score == 0:
#             label = "other"
#         elif bl_score >= ot_score:
#             label = "bill_of_lading"
#         else:
#             label = "other"
#         log.info("  Page %d â†’ %s (bl=%d ot=%d)", i + 1, label, bl_score, ot_score)
#         labels.append(label)
#     return labels

# def keep_bl_pages(pages_text: List[str], labels: List[str]) -> List[str]:
#     """Keep bill_of_lading + other pages (invoice/PL go elsewhere)."""
#     return [p for p, l in zip(pages_text, labels) if l in ("bill_of_lading", "other")]


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 5  â€”  FIRE EXTRACTOR
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def fire_extractor_async(signer, payload: dict):
#     fn_client = FunctionsInvokeClient(
#         config={}, signer=signer,
#         service_endpoint=EXTRACTOR_ENDPOINT,
#         timeout=(10, 240),
#     )
#     fn_client.invoke_function(
#         function_id=EXTRACTOR_FUNCTION_ID,
#         invoke_function_body=json.dumps(payload).encode("utf-8"),
#         fn_invoke_type="detached",
#     )
#     _plog("[FIRE] BL extractor chunk %d/%d (detached)",
#           payload["chunkIndex"], payload["totalChunks"])


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 6  â€”  MAIN ORCHESTRATION LOGIC
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def orchestrate(os_client, signer, body: dict) -> dict:
#     pipeline_start = time.time()

#     def pick(*keys):
#         for k in keys:
#             v = body.get(k)
#             if v is not None and str(v).strip():
#                 return str(v).strip()
#         return None

#     request_id       = pick("requestId", "request_id") or f"BL-{uuid.uuid4().hex[:8].upper()}"
#     object_path      = pick("object_storage_path", "objectStoragePath", "url")
#     order_base_gid   = pick("shipment", "orderBaseGid", "order_base_gid") or ""
#     ob_ship_unit_gid = pick("supplier", "obShipUnitGid", "ob_ship_unit_gid") or ""
#     shipment_id      = pick("shipmentId", "shipment_id") or ""
#     supplier_name    = pick("supplierName", "supplier_name") or ""

#     if not object_path:
#         raise ValueError("Missing required field: object_storage_path")

#     safe_req = _safe(request_id)
#     filename = Path(object_path).name
#     _plog("requestId=%s  file=%s", request_id, filename)

#     pdf_bytes = download_pdf(os_client, object_path)
#     scanned   = is_scanned(pdf_bytes)
#     _plog("Scanned: %s", scanned)

#     pages_text = extract_pages(pdf_bytes)
#     if not pages_text:
#         raise RuntimeError(f"No text extracted from {filename}")
#     _plog("Total pages: %d", len(pages_text))

#     if scanned:
#         bl_pages = pages_text
#     else:
#         labels   = classify_pages_by_regex(pages_text)
#         bl_pages = keep_bl_pages(pages_text, labels)

#     if not bl_pages:
#         return {"filename": filename, "error": "no bill of lading content found"}

#     _plog("BL pages: %d", len(bl_pages))

#     page_chunks  = [bl_pages[i: i + CHUNK_SIZE] for i in range(0, len(bl_pages), CHUNK_SIZE)]
#     total_chunks = len(page_chunks)
#     _plog("Split into %d chunk(s) of â‰¤%d pages", total_chunks, CHUNK_SIZE)

#     manifest = {
#         "requestId":      request_id,
#         "safeReqId":      safe_req,
#         "documentType":   "bill_of_lading",
#         "totalChunks":    total_chunks,
#         "chunkSize":      CHUNK_SIZE,
#         "filename":       filename,
#         "isScanned":      scanned,
#         "shipmentId":     shipment_id,
#         "supplierName":   supplier_name,
#         "orderBaseGid":   order_base_gid,
#         "obShipUnitGid":  ob_ship_unit_gid,
#         "createdAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "folder":         _req_folder(safe_req),
#         "chunks": [
#             {"chunkIndex": i + 1, "pages": chunk}
#             for i, chunk in enumerate(page_chunks)
#         ],
#     }
#     upload_to_object_storage(
#         os_client,
#         json.dumps(manifest, indent=2, ensure_ascii=False),
#         _manifest_key(safe_req),
#     )
#     _plog("Manifest â†’ %s", _manifest_key(safe_req))

#     fire_extractor_async(signer, {
#         "requestId":     request_id,
#         "chunkIndex":    1,
#         "totalChunks":   total_chunks,
#         "pages":         page_chunks[0],
#         "orderBaseGid":  order_base_gid,
#         "obShipUnitGid": ob_ship_unit_gid,
#         "shipmentId":    shipment_id,
#         "supplierName":  supplier_name,
#     })

#     duration = round(time.time() - pipeline_start, 2)
#     _plog("BL Orchestrator done in %.1fs", duration)

#     return {
#         "status":          "processing",
#         "requestId":       request_id,
#         "documentType":    "bill_of_lading",
#         "totalChunks":     total_chunks,
#         "totalPages":      len(bl_pages),
#         "filename":        filename,
#         "folder":          _req_folder(safe_req),
#         "manifestSavedTo": _manifest_key(safe_req),
#         "message": f"BL extraction started. {total_chunks} chunk(s).",
#     }


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 7  â€”  OCI FUNCTIONS ENTRY POINT
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def handler(ctx, data: io.BytesIO = None):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     _plog("=" * 60)
#     _plog("BL Orchestrator v1.0.0 invoked")

#     for var, name in [(OCI_COMPARTMENT_ID, "OCI_COMPARTMENT_ID"),
#                       (EXTRACTOR_FUNCTION_ID, "EXTRACTOR_FUNCTION_ID")]:
#         if not var:
#             msg = f"{name} env var not set."
#             return fdk_response.Response(
#                 ctx, response_data=json.dumps({"status": "error", "message": msg}),
#                 headers={"Content-Type": "application/json"}, status_code=500,
#             )

#     body = {}
#     if data:
#         try:
#             body = json.loads(data.getvalue())
#         except Exception as exc:
#             log.warning("Body parse failed: %s", exc)

#     signer    = get_signer()
#     os_client = get_object_storage_client(signer)

#     try:
#         result = orchestrate(os_client, signer, body)
#     except Exception as exc:
#         log.exception("BL orchestrator failed")
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": str(exc)}),
#             headers={"Content-Type": "application/json"}, status_code=500,
#         )

#     return fdk_response.Response(
#         ctx, response_data=json.dumps(result, ensure_ascii=False),
#         headers={"Content-Type": "application/json"},
#     )







"""
AI Bill of Lading Orchestrator  â€”  OCI FUNCTIONS v1.0.0
========================================================
ROLE: Orchestrator for Bill of Lading extraction.
Mirrors the PL orchestrator pattern exactly.

Pipeline:
1.  Download PDF from Object Storage
2.  Extract text page-by-page
3.  Classify pages â†’ keep bill_of_lading pages
4.  Split into chunks (default 3 â€” BOLs are typically 1-3 pages)
5.  Save manifest â†’ OS: Bill of Lading/{req}/manifest.json
6.  Fire ai-bl-extractor chunk 1 DETACHED â†’ return immediately
"""

import io
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import List, Optional
from urllib.parse import unquote

import pdfplumber
from pypdf import PdfReader

import oci
from oci.auth.signers import get_resource_principals_signer
from oci.functions.functions_invoke_client import FunctionsInvokeClient
from fdk import response as fdk_response


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
OS_NAMESPACE       = os.getenv("OS_NAMESPACE",    "sample_namespace")
OS_BUCKET          = os.getenv("OS_BUCKET",       "sample-workflow-bucket")
OS_INPUT_BUCKET    = os.getenv("OS_INPUT_BUCKET", "sample-workflow-bucket")

BL_BASE_FOLDER     = os.getenv("OS_BL_BASE_FOLDER", "Bill of Lading")
MIN_CHARS_PER_PAGE = int(os.getenv("MIN_CHARS_PER_PAGE", "80"))
CHUNK_SIZE         = int(os.getenv("CHUNK_SIZE", "3"))

EXTRACTOR_FUNCTION_ID = os.getenv("EXTRACTOR_FUNCTION_ID", "")
EXTRACTOR_ENDPOINT    = os.getenv(
    "EXTRACTOR_ENDPOINT",
    "https://example.invalid/integration-endpoint",
)

OTM_DOMAIN_NAME = os.getenv("OTM_DOMAIN_NAME", "SAMPLE_ORG")
# TMP_DIR = "/tmp/ai_bl_orchestrator"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  LOGGING
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# class _TeeHandler(logging.StreamHandler):
#     def emit(self, record):
#         super().emit(record)
#         print(self.format(record), flush=True)

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[_TeeHandler(sys.stderr)],
# )
# log = logging.getLogger(__name__)

# def _plog(msg, *args):
#     print(f"â–¶ {msg % args if args else msg}", flush=True)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  FOLDER HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _safe(s: str) -> str:
#     return re.sub(r"[^\w\-]", "_", s) if s else "unknown"

# def _req_folder(safe_req: str) -> str:
#     return f"{BL_BASE_FOLDER}/{safe_req}"

# def _manifest_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/manifest.json"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 1  â€”  OCI CLIENTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def get_signer():
#     return get_resource_principals_signer()

# def get_object_storage_client(signer):
#     return oci.object_storage.ObjectStorageClient(config={}, signer=signer)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 2  â€”  OBJECT STORAGE HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def upload_to_object_storage(os_client, content, object_name, content_type="application/json"):
#     body = content.encode("utf-8") if isinstance(content, str) else content
#     try:
#         os_client.put_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
#             object_name=object_name, put_object_body=body, content_type=content_type,
#         )
#         log.info("Uploaded â†’ %s", object_name)
#         return True
#     except Exception as exc:
#         log.error("Upload failed '%s': %s", object_name, exc)
#         return False

# def download_pdf(os_client, object_path: str) -> bytes:
#     os.makedirs(TMP_DIR, exist_ok=True)
#     object_key = unquote(object_path.lstrip("/"))
#     log.info("Downloading: %s", object_key)
#     resp = os_client.get_object(
#         namespace_name=OS_NAMESPACE, bucket_name=OS_INPUT_BUCKET, object_name=object_key,
#     )
#     chunks = []
#     for chunk in resp.data.raw.stream(1024 * 1024, decode_content=False):
#         chunks.append(chunk)
#     return b"".join(chunks)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 3  â€”  TEXT EXTRACTION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def extract_pages(pdf_bytes: bytes) -> List[str]:
#     pages_text = []
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             for i, page in enumerate(pdf.pages, start=1):
#                 page_text  = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
#                 table_rows = []
#                 for table in page.extract_tables():
#                     for row in table:
#                         table_rows.append(" | ".join(cell or "" for cell in row))
#                 combined = page_text
#                 if table_rows:
#                     combined += "\n" + "\n".join(table_rows)
#                 pages_text.append(combined.strip())
#                 log.info("  Page %d: %d chars", i, len(combined))
#         return pages_text
#     except Exception as exc:
#         log.warning("pdfplumber failed: %s â€” trying pypdf", exc)
#         try:
#             reader = PdfReader(io.BytesIO(pdf_bytes))
#             return [(page.extract_text() or "").strip() for page in reader.pages]
#         except Exception as exc2:
#             log.error("pypdf also failed: %s", exc2)
#             return []

# def is_scanned(pdf_bytes: bytes) -> bool:
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             pages = pdf.pages[:3]
#             if not pages:
#                 return True
#             avg = sum(
#                 len((p.extract_text(x_tolerance=2, y_tolerance=2) or "").strip())
#                 for p in pages
#             ) / len(pages)
#             return avg < MIN_CHARS_PER_PAGE
#     except Exception:
#         return False


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 4  â€”  PAGE CLASSIFIER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# _BL_PATTERNS = [
#     r"bill\s+of\s+lading", r"\bb/l\b", r"\bbol\b",
#     r"airway\s+bill", r"\bawb\b", r"ocean\s+bill",
#     r"notify\s+party", r"place\s+of\s+receipt",
#     r"on\s+board\s+date", r"freight\s+(prepaid|collect)",
#     r"original.*bill",
# ]
# _INVOICE_PATTERNS = [
#     r"commercial\s+invoice", r"invoice\s+n[o0]", r"unit\s+price",
#     r"total\s+amount", r"\bfob\b|\bcfr\b", r"payment\s+terms",
# ]

# def classify_pages_by_regex(pages_text: List[str]) -> List[str]:
#     labels = []
#     for i, page in enumerate(pages_text):
#         lower    = page.lower()
#         bl_score = sum(1 for p in _BL_PATTERNS      if re.search(p, lower))
#         ot_score = sum(1 for p in _INVOICE_PATTERNS  if re.search(p, lower))
#         if bl_score == 0 and ot_score == 0:
#             label = "other"
#         elif bl_score >= ot_score:
#             label = "bill_of_lading"
#         else:
#             label = "other"
#         log.info("  Page %d â†’ %s (bl=%d ot=%d)", i + 1, label, bl_score, ot_score)
#         labels.append(label)
#     return labels

# def keep_bl_pages(pages_text: List[str], labels: List[str]) -> List[str]:
#     """Keep bill_of_lading + other pages (invoice/PL go elsewhere)."""
#     return [p for p, l in zip(pages_text, labels) if l in ("bill_of_lading", "other")]


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 5  â€”  FIRE EXTRACTOR
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# # def fire_extractor_async(signer, payload: dict):
# #     fn_client = FunctionsInvokeClient(
# #         config={}, signer=signer,
# #         service_endpoint=EXTRACTOR_ENDPOINT,
# #         timeout=(10, 240),
# #     )
# #     fn_client.invoke_function(
# #         function_id=EXTRACTOR_FUNCTION_ID,
# #         invoke_function_body=json.dumps(payload).encode("utf-8"),
# #         fn_invoke_type="detached",
# #     )
# #     _plog("[FIRE] BL extractor chunk %d/%d (detached)",
# #           payload["chunkIndex"], payload["totalChunks"])

# # REPLACE WITH â€” 3 retries with backoff on 503/5xx
# def fire_extractor_async(signer, payload: dict):
#     fn_client = FunctionsInvokeClient(
#         config={}, signer=signer,
#         service_endpoint=EXTRACTOR_ENDPOINT,
#         timeout=(10, 240),
#     )
#     last_exc = None
#     for attempt in range(1, 4):
#         try:
#             fn_client.invoke_function(
#                 function_id=EXTRACTOR_FUNCTION_ID,
#                 invoke_function_body=json.dumps(payload).encode("utf-8"),
#                 fn_invoke_type="detached",
#             )
#             _plog("[FIRE] Extractor chunk %d/%d fired (attempt %d)",
#                   payload["chunkIndex"], payload["totalChunks"], attempt)
#             return
#         except Exception as exc:
#             last_exc = exc
#             wait = 5 * attempt  # 5s, 10s, 15s
#             _plog("[FIRE] Attempt %d/3 failed: %s â€” retrying in %ds", attempt, exc, wait)
#             if attempt < 3:
#                 time.sleep(wait)
#     log.error("[FIRE] All 3 attempts failed â€” extractor not started: %s", last_exc)
#     raise last_exc
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 6  â€”  MAIN ORCHESTRATION LOGIC
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def orchestrate(os_client, signer, body: dict) -> dict:
#     pipeline_start = time.time()

#     def pick(*keys):
#         for k in keys:
#             v = body.get(k)
#             if v is not None and str(v).strip():
#                 return str(v).strip()
#         return None

#     request_id       = pick("requestId", "request_id") or f"BL-{uuid.uuid4().hex[:8].upper()}"
#     object_path      = pick("object_storage_path", "objectStoragePath", "url")
#     order_base_gid   = pick("shipment", "orderBaseGid", "order_base_gid") or ""
#     ob_ship_unit_gid = pick("supplier", "obShipUnitGid", "ob_ship_unit_gid") or ""
#     shipment_id      = pick("shipmentId", "shipment_id") or ""
#     supplier_name    = pick("supplierName", "supplier_name") or ""
#     doc_key = pick("docKey", "doc_key") or "bill_of_lading"   # â† add

#     if not object_path:
#         raise ValueError("Missing required field: object_storage_path")

#     safe_req = _safe(request_id)
#     filename = Path(object_path).name
#     _plog("requestId=%s  file=%s", request_id, filename)

#     pdf_bytes = download_pdf(os_client, object_path)
#     scanned   = is_scanned(pdf_bytes)
#     _plog("Scanned: %s", scanned)

#     pages_text = extract_pages(pdf_bytes)
#     if not pages_text:
#         raise RuntimeError(f"No text extracted from {filename}")
#     _plog("Total pages: %d", len(pages_text))

#     if scanned:
#         bl_pages = pages_text
#     else:
#         labels   = classify_pages_by_regex(pages_text)
#         bl_pages = keep_bl_pages(pages_text, labels)

#     if not bl_pages:
#         return {"filename": filename, "error": "no bill of lading content found"}

#     _plog("BL pages: %d", len(bl_pages))

#     page_chunks  = [bl_pages[i: i + CHUNK_SIZE] for i in range(0, len(bl_pages), CHUNK_SIZE)]
#     total_chunks = len(page_chunks)
#     _plog("Split into %d chunk(s) of â‰¤%d pages", total_chunks, CHUNK_SIZE)

#     manifest = {
#         "requestId":      request_id,
#         "safeReqId":      safe_req,
#         "documentType":   "bill_of_lading",
#         "totalChunks":    total_chunks,
#         "chunkSize":      CHUNK_SIZE,
#         "filename":       filename,
#         "isScanned":      scanned,
#         "shipmentId":     shipment_id,
#         "supplierName":   supplier_name,
#         "orderBaseGid":   order_base_gid,
#         "obShipUnitGid":  ob_ship_unit_gid,
#         "createdAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "folder":         _req_folder(safe_req),
#         "chunks": [
#             {"chunkIndex": i + 1, "pages": chunk}
#             for i, chunk in enumerate(page_chunks)
#         ],
#     }
#     upload_to_object_storage(
#         os_client,
#         json.dumps(manifest, indent=2, ensure_ascii=False),
#         _manifest_key(safe_req),
#     )
#     _plog("Manifest â†’ %s", _manifest_key(safe_req))

#     fire_extractor_async(signer, {
#         "requestId":     request_id,
#         "chunkIndex":    1,
#         "totalChunks":   total_chunks,
#         "pages":         page_chunks[0],
#         "orderBaseGid":  order_base_gid,
#         "obShipUnitGid": ob_ship_unit_gid,
#         "shipmentId":    shipment_id,
#         "supplierName":  supplier_name,
#         "docKey":        doc_key,   # â† add
#     })

#     duration = round(time.time() - pipeline_start, 2)
#     _plog("BL Orchestrator done in %.1fs", duration)

#     return {
#         "status":          "processing",
#         "requestId":       request_id,
#         "documentType":    "bill_of_lading",
#         "totalChunks":     total_chunks,
#         "totalPages":      len(bl_pages),
#         "filename":        filename,
#         "folder":          _req_folder(safe_req),
#         "manifestSavedTo": _manifest_key(safe_req),
#         "message": f"BL extraction started. {total_chunks} chunk(s).",
#     }


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 7  â€”  OCI FUNCTIONS ENTRY POINT
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def handler(ctx, data: io.BytesIO = None):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     _plog("=" * 60)
#     _plog("BL Orchestrator v1.0.0 invoked")

#     for var, name in [(OCI_COMPARTMENT_ID, "OCI_COMPARTMENT_ID"),
#                       (EXTRACTOR_FUNCTION_ID, "EXTRACTOR_FUNCTION_ID")]:
#         if not var:
#             msg = f"{name} env var not set."
#             return fdk_response.Response(
#                 ctx, response_data=json.dumps({"status": "error", "message": msg}),
#                 headers={"Content-Type": "application/json"}, status_code=500,
#             )

#     body = {}
#     if data:
#         try:
#             body = json.loads(data.getvalue())
#         except Exception as exc:
#             log.warning("Body parse failed: %s", exc)

#     signer    = get_signer()
#     os_client = get_object_storage_client(signer)

#     try:
#         result = orchestrate(os_client, signer, body)
#     except Exception as exc:
#         log.exception("BL orchestrator failed")
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": str(exc)}),
#             headers={"Content-Type": "application/json"}, status_code=500,
#         )

#     return fdk_response.Response(
#         ctx, response_data=json.dumps(result, ensure_ascii=False),
#         headers={"Content-Type": "application/json"},
#     )



TMP_DIR = "/tmp/bl_orchestrator_v2"
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  LOGGING
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
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
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  FOLDER HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
def _safe(s: str) -> str:
    return re.sub(r"[^\w\-]", "_", s) if s else "unknown"
 
def _req_folder(safe_req: str) -> str:
    return f"{BL_BASE_FOLDER}/{safe_req}"
 
def _manifest_key(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/manifest.json"
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  OCI CLIENTS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
def get_signer():
    return get_resource_principals_signer()
 
def get_object_storage_client(signer):
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  OBJECT STORAGE HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
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
 
def download_from_os(os_client, object_name) -> Optional[str]:
    try:
        resp = os_client.get_object(
            namespace_name=OS_NAMESPACE,
            bucket_name=OS_BUCKET,
            object_name=object_name,
        )
        return resp.data.content.decode("utf-8")
    except Exception as exc:
        log.warning("Download failed '%s': %s", object_name, exc)
        return None
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  FIRE EXTRACTOR â€” with retry
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
def fire_extractor_async(signer, payload: dict):
    fn_client = FunctionsInvokeClient(
        config={}, signer=signer,
        service_endpoint=EXTRACTOR_ENDPOINT,
        timeout=(10, 240),
    )
    last_exc = None
    for attempt in range(1, 4):
        try:
            fn_client.invoke_function(
                function_id=EXTRACTOR_FUNCTION_ID,
                invoke_function_body=json.dumps(payload).encode("utf-8"),
                fn_invoke_type="detached",
            )
            _plog("[FIRE] Extractor chunk %d/%d fired (attempt %d)",
                  payload["chunkIndex"], payload["totalChunks"], attempt)
            return
        except Exception as exc:
            last_exc = exc
            wait = 5 * attempt
            _plog("[FIRE] Attempt %d/3 failed: %s â€” retrying in %ds",
                  attempt, exc, wait)
            if attempt < 3:
                time.sleep(wait)
    log.error("[FIRE] All 3 attempts failed: %s", last_exc)
    raise last_exc
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  MAIN PIPELINE
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
def process_classified(signer, os_client, classified_pages_path: str,
                        request_id: str, order_base_gid: str,
                        ob_ship_unit_gid: str, shipment_id: str,
                        supplier_name: str, doc_key: str) -> dict:
    """
    Reads pre-classified BL pages from OCI OS.
    Chunks them and fires the BL extractor.
    """
    pipeline_start = time.time()
    safe_req = _safe(request_id)
 
    _plog("=" * 60)
    _plog("BL Orchestrator v2.0.0")
    _plog("requestId=%s  classified=%s", request_id, classified_pages_path)
 
    # STEP 1: Read pre-classified pages
    raw = download_from_os(os_client, classified_pages_path)
    if not raw:
        raise RuntimeError(
            f"Classified pages not found: {classified_pages_path}"
        )
 
    classified = json.loads(raw)
    pages_text  = classified.get("pages", [])
    filename    = Path(classified.get("source_file", "unknown")).name
 
    if not pages_text:
        return {"filename": filename, "error": "no BL pages in classified file"}
 
    _plog("Read %d pre-classified BL pages", len(pages_text))
 
    # STEP 2: Split into chunks
    page_chunks  = [
        pages_text[i: i + CHUNK_SIZE]
        for i in range(0, len(pages_text), CHUNK_SIZE)
    ]
    total_chunks = len(page_chunks)
    _plog("Split into %d chunk(s) of â‰¤%d pages", total_chunks, CHUNK_SIZE)
 
    # STEP 3: Write manifest
    manifest = {
        "requestId":      request_id,
        "safeReqId":      safe_req,
        "documentType":   "bill_of_lading",
        "totalChunks":    total_chunks,
        "chunkSize":      CHUNK_SIZE,
        "filename":       filename,
        "isScanned":      False,
        "shipmentId":     shipment_id,
        "supplierName":   supplier_name,
        "orderBaseGid":   order_base_gid,
        "obShipUnitGid":  ob_ship_unit_gid,
        "createdAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "folder":         _req_folder(safe_req),
        "docKey":         doc_key,
        "chunks": [
            {"chunkIndex": i + 1, "pages": chunk}
            for i, chunk in enumerate(page_chunks)
        ],
    }
    upload_to_os(
        os_client,
        json.dumps(manifest, indent=2, ensure_ascii=False),
        _manifest_key(safe_req),
    )
    _plog("Manifest â†’ %s", _manifest_key(safe_req))
 
    # STEP 4: Fire extractor chunk 1 (detached)
    fire_extractor_async(signer, {
        "requestId":     request_id,
        "chunkIndex":    1,
        "totalChunks":   total_chunks,
        "pages":         page_chunks[0],
        "orderBaseGid":  order_base_gid,
        "obShipUnitGid": ob_ship_unit_gid,
        "shipmentId":    shipment_id,
        "supplierName":  supplier_name,
        "docKey":        doc_key,
    })
 
    duration = round(time.time() - pipeline_start, 2)
    _plog("BL Orchestrator done in %.1fs", duration)
 
    return {
        "status":       "processing",
        "requestId":    request_id,
        "documentType": "bill_of_lading",
        "totalChunks":  total_chunks,
        "totalPages":   len(pages_text),
        "filename":     filename,
        "folder":       _req_folder(safe_req),
        "durationSeconds": duration,
    }
 
 
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  OCI FUNCTIONS ENTRY POINT
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
def handler(ctx, data: io.BytesIO = None):
    os.makedirs(TMP_DIR, exist_ok=True)
    _plog("=" * 60)
    _plog("BL Orchestrator v2.0.0 invoked")
 
    body = {}
    if data:
        try:
            body = json.loads(data.getvalue())
        except Exception as exc:
            log.warning("Body parse failed: %s", exc)
 
    def pick(*keys):
        for k in keys:
            v = body.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return None
 
    request_id       = pick("requestId", "request_id") or \
                       f"BL-{uuid.uuid4().hex[:8].upper()}"
    classified_path  = pick("classified_pages_path", "classified_path")
    order_base_gid   = pick("shipment", "orderBaseGid", "order_base_gid") or ""
    ob_ship_unit_gid = pick("supplier", "obShipUnitGid", "ob_ship_unit_gid") or ""
    shipment_id      = pick("shipmentId", "shipment_id") or ""
    supplier_name    = pick("supplierName", "supplier_name") or ""
    doc_key          = pick("docKey", "doc_key") or "bill_of_lading"
 
    if not classified_path:
        msg = "Missing required field: classified_pages_path"
        log.error(msg)
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "message": msg}),
            headers={"Content-Type": "application/json"},
            status_code=400,
        )
 
    try:
        signer    = get_signer()
        os_client = get_object_storage_client(signer)
 
        result = process_classified(
            signer, os_client, classified_path,
            request_id, order_base_gid, ob_ship_unit_gid,
            shipment_id, supplier_name, doc_key,
        )
 
        if isinstance(result, dict) and result.get("error"):
            return fdk_response.Response(
                ctx,
                response_data=json.dumps({
                    "status": "error",
                    "requestId": request_id,
                    "message": result.get("error"),
                }),
                headers={"Content-Type": "application/json"},
                status_code=422,
            )
 
        return fdk_response.Response(
            ctx,
            response_data=json.dumps(result, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )
 
    except Exception as exc:
        log.exception("BL Orchestrator failed")
        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status": "error",
                "requestId": request_id,
                "message": str(exc),
            }),
            headers={"Content-Type": "application/json"},
            status_code=500,
        )
 