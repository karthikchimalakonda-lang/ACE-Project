

# """
# OTM Packing List Orchestrator  â€”  OCI FUNCTIONS VERSION  v2.0.0
# ================================================================
# ROLE: Orchestrator / Splitter  (NO WAITING â€” fire-and-forget)

# New Pipeline (Self-Chaining via Object Storage)
# ------------------------------------------------
# 1.  Download PDF from Object Storage
# 2.  Extract text page-by-page (pdfplumber / OCI DU for scanned)
# 3.  Classify pages â†’ keep packing-list pages only
# 4.  Split packing pages into chunks of CHUNK_SIZE (default 6)
# 5.  Save job MANIFEST to OS  â†’  Packing List/Jobs/{safe_req}_manifest.json
#     {requestId, totalChunks, chunkSize, orderBaseGid, obShipUnitGid,
#      chunks: [{chunkIndex, pages: [...], lastPkgNo}]}
# 6.  Fire Extractor for chunk 1 ASYNC (fn_invoke_type=detached)
# 7.  Return immediately: {"status":"processing", "requestId":..., "totalChunks":...}

# The Extractor self-chains: each finished chunk fires the next one,
# and the last chunk merges all results and saves the final OTM JSON.

# Input (JSON body)
# -----------------
# {
#   "requestId":           "PL-20260609-0001",
#   "object_storage_path": "AI_Invoices/packing_list.pdf",
#   "shipment":            "SAMPLE_ORG.ORDER_BASE_001",
#   "supplier":            "SAMPLE_ORG.OB_SHIP_UNIT_001"
# }
# """

# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  IMPORTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# import io
# import json
# import logging
# import os
# import re
# import time
# import uuid
# from pathlib import Path
# from typing import List, Optional
# from urllib.parse import urlparse, unquote

# import pdfplumber
# from pypdf import PdfReader

# import oci
# from oci.auth.signers import get_resource_principals_signer
# from oci.ai_document import AIServiceDocumentClient
# from oci.functions.functions_invoke_client import FunctionsInvokeClient
# from fdk import response as fdk_response


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  CONFIGURATION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
# OCI_DU_ENDPOINT    = os.getenv(
#     "OCI_DU_ENDPOINT",
#     "https://example.invalid/integration-endpoint",
# )

# OS_NAMESPACE    = os.getenv("OS_NAMESPACE",    "sample_namespace")
# OS_BUCKET       = os.getenv("OS_BUCKET",       "sample-workflow-bucket")
# OS_INPUT_BUCKET = os.getenv("OS_INPUT_BUCKET", "sample-workflow-bucket")

# PL_TEXT_FOLDER   = os.getenv("OS_PL_TEXT_FOLDER",   "Packing List/Text")
# PL_TEMP_FOLDER   = os.getenv("OS_PL_TEMP_FOLDER",   "Packing List/Temp")
# PL_DU_OUTPUT     = os.getenv("OS_PL_DU_OUTPUT",     "Packing List/DU Output")
# PL_JOBS_FOLDER   = os.getenv("OS_PL_JOBS_FOLDER",   "Packing List/Jobs")
# PL_CHUNKS_FOLDER = os.getenv("OS_PL_CHUNKS_FOLDER", "Packing List/Chunks")

# MIN_CHARS_PER_PAGE = int(os.getenv("MIN_CHARS_PER_PAGE", "80"))
# CHUNK_SIZE         = int(os.getenv("CHUNK_SIZE", "6"))

# EXTRACTOR_FUNCTION_ID = os.getenv("EXTRACTOR_FUNCTION_ID", "")
# EXTRACTOR_ENDPOINT    = os.getenv(
#     "EXTRACTOR_ENDPOINT",
#     "https://example.invalid/integration-endpoint",
# )

# OTM_DOMAIN_NAME = os.getenv("OTM_DOMAIN_NAME", "SAMPLE_ORG")

# TMP_DIR = "/tmp/pl_orchestrator"

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# log = logging.getLogger(__name__)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 1  â€”  OCI CLIENTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def get_signer():
#     return get_resource_principals_signer()

# def get_object_storage_client(signer):
#     return oci.object_storage.ObjectStorageClient(config={}, signer=signer)

# def get_document_understanding_client(signer) -> AIServiceDocumentClient:
#     return AIServiceDocumentClient(
#         config={}, signer=signer, service_endpoint=OCI_DU_ENDPOINT,
#     )


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 2  â€”  OBJECT STORAGE HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def upload_to_object_storage(os_client, content, object_name, content_type="application/octet-stream"):
#     body = content.encode("utf-8") if isinstance(content, str) else content
#     try:
#         os_client.put_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
#             object_name=object_name, put_object_body=body, content_type=content_type,
#         )
#         log.info("Uploaded â†’ %s", object_name)
#         return True
#     except Exception as exc:
#         log.error("Upload failed for '%s': %s", object_name, exc)
#         return False

# def parse_os_url(os_url, default_bucket=None):
#     if default_bucket is None:
#         default_bucket = OS_BUCKET
#     if not os_url:
#         raise ValueError("Empty Object Storage URL/path provided.")
#     if "://" not in os_url:
#         return OS_NAMESPACE, default_bucket, unquote(os_url.lstrip("/"))
#     path  = urlparse(os_url).path
#     parts = [p for p in path.split("/") if p]
#     ns, bucket, obj = OS_NAMESPACE, default_bucket, None
#     if "n" in parts:
#         ns = parts[parts.index("n") + 1]
#     if "b" in parts:
#         bucket = parts[parts.index("b") + 1]
#     if "o" in parts:
#         obj = "/".join(parts[parts.index("o") + 1:])
#     if not obj:
#         obj = path.lstrip("/")
#     return OS_NAMESPACE, bucket, unquote(obj)

# def download_pdf_by_url(os_client, os_url):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     namespace, bucket, object_key = parse_os_url(os_url, default_bucket=OS_INPUT_BUCKET)
#     pdf_filename = Path(object_key).name or f"download_{uuid.uuid4().hex[:8]}.pdf"
#     local_path   = Path(TMP_DIR) / pdf_filename
#     log.info("Downloading: %s/%s/%s", namespace, bucket, object_key)
#     get_resp = os_client.get_object(
#         namespace_name=namespace, bucket_name=bucket, object_name=object_key,
#     )
#     with open(local_path, "wb") as f:
#         for chunk in get_resp.data.raw.stream(1024 * 1024, decode_content=False):
#             f.write(chunk)
#     log.info("Downloaded â†’ %s (%d bytes)", local_path, local_path.stat().st_size)
#     return local_path


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 3  â€”  TEXT EXTRACTION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def is_scanned_pdf(pdf_bytes):
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             pages = pdf.pages[:3]
#             if not pages:
#                 return True
#             total_chars = sum(
#                 len((p.extract_text(x_tolerance=2, y_tolerance=2) or "").strip())
#                 for p in pages
#             )
#             avg = total_chars / len(pages)
#             log.info("Scanned detection: avg_chars=%.1f", avg)
#             return avg < MIN_CHARS_PER_PAGE
#     except Exception as exc:
#         log.warning("Scanned detection failed: %s", exc)
#         return False

# def extract_pages_digital(pdf_bytes):
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
#                 log.info("  [digital] Page %d: %d chars", i, len(combined))
#     except Exception as exc:
#         log.error("pdfplumber failed: %s â€” trying pypdf", exc)
#         try:
#             reader = PdfReader(io.BytesIO(pdf_bytes))
#             for i, page in enumerate(reader.pages, start=1):
#                 text = (page.extract_text() or "").strip()
#                 pages_text.append(text)
#         except Exception as exc2:
#             log.error("pypdf fallback also failed: %s", exc2)
#     return pages_text

# def extract_text_via_du(du_client, os_client, pdf_path):
#     safe_stem  = re.sub(r"[^\w\-]", "_", pdf_path.stem)
#     temp_key   = f"{PL_TEMP_FOLDER}/{safe_stem}_{uuid.uuid4().hex[:8]}.pdf"
#     du_out_pfx = f"{PL_DU_OUTPUT}/{safe_stem}_{uuid.uuid4().hex[:8]}"

#     pdf_bytes = pdf_path.read_bytes()
#     upload_to_object_storage(os_client, pdf_bytes, temp_key, "application/pdf")

#     job_details = oci.ai_document.models.CreateProcessorJobDetails(
#         display_name   = f"pl-orch-{safe_stem}",
#         compartment_id = OCI_COMPARTMENT_ID,
#         input_location = oci.ai_document.models.ObjectStorageLocations(
#             object_locations=[oci.ai_document.models.ObjectLocation(
#                 namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=temp_key,
#             )]
#         ),
#         output_location=oci.ai_document.models.OutputLocation(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=du_out_pfx,
#         ),
#         processor_config=oci.ai_document.models.GeneralProcessorConfig(
#             features=[oci.ai_document.models.DocumentTextExtractionFeature(
#                 generate_searchable_pdf=False,
#             )],
#             is_zip_output_enabled=False,
#         ),
#     )
#     job_id = du_client.create_processor_job(
#         create_processor_job_details=job_details
#     ).data.id
#     log.info("[DU] Job: %s", job_id)

#     elapsed, status = 0, "SUBMITTED"
#     while elapsed < 300:
#         time.sleep(5); elapsed += 5
#         status = du_client.get_processor_job(processor_job_id=job_id).data.lifecycle_state
#         if status in ("SUCCEEDED", "FAILED", "CANCELED"):
#             break

#     if status != "SUCCEEDED":
#         raise RuntimeError(f"DU job {job_id} ended: {status}")

#     resp     = os_client.list_objects(namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=du_out_pfx)
#     all_keys = [o.name for o in resp.data.objects]
#     result_key = next((k for k in all_keys if k.endswith("analysedDocument.json")), None) \
#               or next((k for k in all_keys if k.endswith(".json")), None)
#     if not result_key:
#         raise FileNotFoundError(f"No DU result under {du_out_pfx}")

#     raw = os_client.get_object(
#         namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=result_key,
#     ).data.content.decode("utf-8")
#     du_data = json.loads(raw)

#     page_texts = []
#     for page in du_data.get("pages", []):
#         lines = [ln.get("text", "").strip() for ln in page.get("lines", []) if ln.get("text", "").strip()]
#         page_texts.append(f"--- Page {page.get('pageNumber','?')} ---\n" + "\n".join(lines))
#     return "\n\n".join(page_texts)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 4  â€”  PAGE CLASSIFIER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# _INVOICE_PATTERNS = [
#     r"commercial\s+invoice", r"invoice\s+n[o0]", r"unit\s+price",
#     r"total\s+amount", r"seller|shipper\s*/\s*exporter",
#     r"\bfob\b|\bcfr\b|\bcif\b", r"amount\s+in\s+words",
#     r"bank\s+details|swift|iban", r"payment\s+terms",
# ]
# _PACKING_PATTERNS = [
#     r"\bpacking\s+list\b", r"\d+\s+of\s+\d+", r"pkg\s*no\.?|package\s+no",
#     r"net\s+weight.*gross\s+weight", r"gross\s+weight.*kgs?",
#     r"dimensions?\s*[:\-]?\s*\d", r"\bcbm\b", r"summary\s+packing",
#     r"packing\s+style", r"l\s*x\s*w\s*x\s*h",
# ]

# def classify_pages_by_regex(pages_text):
#     labels = []
#     for i, page in enumerate(pages_text):
#         lower     = page.lower()
#         inv_score = sum(1 for p in _INVOICE_PATTERNS if re.search(p, lower))
#         pl_score  = sum(1 for p in _PACKING_PATTERNS if re.search(p, lower))
#         if inv_score == 0 and pl_score == 0:
#             label = "other"
#         elif pl_score > inv_score:
#             label = "packing_list"
#         elif inv_score > pl_score:
#             label = "invoice"
#         else:
#             label = "packing_list" if re.search(r"\d+\s+of\s+\d+", lower) else "invoice"
#         log.info("  Page %d â†’ %s (inv=%d, pl=%d)", i + 1, label, inv_score, pl_score)
#         labels.append(label)
#     return labels

# def keep_packing_pages(pages_text, labels):
#     packing = [p for p, l in zip(pages_text, labels) if l == "packing_list"]
#     if not packing:
#         packing = [p for p, l in zip(pages_text, labels) if l != "invoice"]
#         log.warning("No explicit PL pages â€” using %d non-invoice pages", len(packing))
#     log.info("Page split â†’ PL: %d | invoice: %d | other: %d",
#              sum(1 for l in labels if l == "packing_list"),
#              sum(1 for l in labels if l == "invoice"),
#              sum(1 for l in labels if l == "other"))
#     return packing


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 5  â€”  CARRY-FORWARD PKG_NO
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _precompute_last_pkg_no(pages_chunk):
#     PKG_NO_RE = re.compile(r"\b(\d+\s+OF\s+\d+)\b", re.IGNORECASE)
#     last = None
#     for page in pages_chunk:
#         matches = PKG_NO_RE.findall(page)
#         if matches:
#             last = matches[-1].strip()
#     return last


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 6  â€”  FIRE EXTRACTOR (ASYNC / DETACHED)
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def fire_extractor_async(signer, payload: dict):
#     """
#     Fire the extractor OCI Function in DETACHED (async) mode.
#     Returns immediately â€” extractor runs independently.
#     """
#     invoke_client = FunctionsInvokeClient(
#         config={},
#         signer=signer,
#         service_endpoint=EXTRACTOR_ENDPOINT,
#         timeout=(10, 30),
#     )
#     body_bytes = json.dumps(payload).encode("utf-8")
#     invoke_client.invoke_function(
#         function_id=EXTRACTOR_FUNCTION_ID,
#         invoke_function_body=body_bytes,
#         fn_invoke_type="detached",   # â† fire-and-forget
#     )
#     log.info("  [ORCH] Fired extractor chunk %d/%d (detached)",
#              payload["chunkIndex"], payload["totalChunks"])


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 7  â€”  MAIN PIPELINE
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def process_document(signer, os_client, du_client, pdf_path, request_id,
#                      order_base_gid, ob_ship_unit_gid):
#     pipeline_start = time.time()
#     filename  = pdf_path.name
#     safe_stem = re.sub(r"[^\w\-]", "_", pdf_path.stem)
#     safe_req  = re.sub(r"[^\w\-]", "_", request_id) or safe_stem
#     log.info("=== Orchestrator v2 processing: %s ===", filename)

#     pdf_bytes = pdf_path.read_bytes()

#     # STEP 1: Detect scanned vs digital
#     scanned = is_scanned_pdf(pdf_bytes)
#     log.info("Detection: %s", "scanned" if scanned else "digital")

#     # STEP 2: Extract text
#     if scanned:
#         full_text  = extract_text_via_du(du_client, os_client, pdf_path)
#         pages_text = [full_text]
#     else:
#         pages_text = extract_pages_digital(pdf_bytes)
#         full_text  = "\n\n".join(
#             f"--- Page {i} ---\n{p}" for i, p in enumerate(pages_text, start=1)
#         )

#     if not full_text.strip():
#         return {"filename": filename, "error": "no text extracted"}

#     log.info("Extracted %d chars across %d pages", len(full_text), len(pages_text))

#     # Save raw text
#     upload_to_object_storage(
#         os_client, full_text,
#         f"{PL_TEXT_FOLDER}/{safe_stem}_raw.txt", "text/plain",
#     )

#     # STEP 3: Classify pages
#     if scanned:
#         packing_pages = pages_text
#     else:
#         labels        = classify_pages_by_regex(pages_text)
#         packing_pages = keep_packing_pages(pages_text, labels)

#     if not packing_pages:
#         return {"filename": filename, "error": "no packing list content found"}

#     log.info("Packing-list pages to process: %d", len(packing_pages))

#     # STEP 4: Split into chunks
#     page_chunks = [
#         packing_pages[i: i + CHUNK_SIZE]
#         for i in range(0, len(packing_pages), CHUNK_SIZE)
#     ]
#     total_chunks = len(page_chunks)
#     log.info("Split into %d chunks of â‰¤%d pages", total_chunks, CHUNK_SIZE)

#     # Pre-compute carry-forward pkg_no per chunk
#     last_pkg_nos = [None]
#     for chunk in page_chunks[:-1]:
#         last_pkg_nos.append(_precompute_last_pkg_no(chunk))

#     # STEP 5: Build and save MANIFEST to Object Storage
#     # Extractor reads this to know what to do next
#     manifest = {
#         "requestId":      request_id,
#         "safeReqId":      safe_req,
#         "totalChunks":    total_chunks,
#         "chunkSize":      CHUNK_SIZE,
#         "filename":       filename,
#         "isScanned":      scanned,
#         "orderBaseGid":   order_base_gid,
#         "obShipUnitGid":  ob_ship_unit_gid,
#         "createdAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "chunks": [
#             {
#                 "chunkIndex": i + 1,
#                 "pages":      chunk,
#                 "lastPkgNo":  last_pkg_nos[i],
#             }
#             for i, chunk in enumerate(page_chunks)
#         ],
#     }
#     manifest_key = f"{PL_JOBS_FOLDER}/{safe_req}_manifest.json"
#     upload_to_object_storage(
#         os_client,
#         json.dumps(manifest, indent=2, ensure_ascii=False),
#         manifest_key,
#         "application/json",
#     )
#     log.info("Manifest saved â†’ %s", manifest_key)

#     # STEP 6: Fire extractor chunk 1 (detached / async)
#     fire_extractor_async(signer, {
#         "requestId":    request_id,
#         "chunkIndex":   1,
#         "totalChunks":  total_chunks,
#         "pages":        page_chunks[0],
#         "lastPkgNo":    last_pkg_nos[0],
#         "orderBaseGid": order_base_gid,
#         "obShipUnitGid": ob_ship_unit_gid,
#     })

#     pipeline_duration = round(time.time() - pipeline_start, 2)
#     log.info("Orchestrator done in %.1fs â€” extractor chain started", pipeline_duration)

#     return {
#         "status":         "processing",
#         "requestId":      request_id,
#         "totalChunks":    total_chunks,
#         "chunkSize":      CHUNK_SIZE,
#         "totalPages":     len(packing_pages),
#         "filename":       filename,
#         "manifestSavedTo": manifest_key,
#         "message": (
#             f"Extraction started. {total_chunks} extractor function(s) will run "
#             f"sequentially. Final OTM JSON will be saved to "
#             f"Packing List/Json/{safe_req}_otm.json when complete."
#         ),
#         "_debug": {
#             "orchestrator_duration_s": pipeline_duration,
#             "total_pages_in_pdf":      len(pages_text),
#             "packing_pages_used":      len(packing_pages),
#             "chunks_total":            total_chunks,
#             "chunk_size":              CHUNK_SIZE,
#             "is_scanned":              scanned,
#         },
#     }


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 8  â€”  OCI FUNCTIONS ENTRY POINT
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def handler(ctx, data: io.BytesIO = None):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     log.info("OCI Function (PL Orchestrator v2.0.0) invoked.")

#     for var, name in [(OCI_COMPARTMENT_ID, "OCI_COMPARTMENT_ID"),
#                       (EXTRACTOR_FUNCTION_ID, "EXTRACTOR_FUNCTION_ID")]:
#         if not var:
#             msg = f"{name} env var is not set."
#             log.error(msg)
#             return fdk_response.Response(
#                 ctx,
#                 response_data=json.dumps({"status": "error", "message": msg}),
#                 headers={"Content-Type": "application/json"},
#                 status_code=500,
#             )

#     body = {}
#     if data:
#         try:
#             body = json.loads(data.getvalue())
#             log.info("PARSED keys: %s", list(body.keys()))
#         except Exception as exc:
#             log.warning("Body parse failed: %s", exc)

#     def pick(*keys):
#         for k in keys:
#             v = body.get(k)
#             if v is not None and str(v).strip():
#                 return str(v).strip()
#         return None

#     request_id         = pick("requestId", "request_id", "referenceTransmissionNo")
#     object_storage_url = pick("object_storage_path", "object_storage_url", "objectStoragePath",
#                                "objectStorageUrl", "object_url", "file_url", "url")
#     order_base_gid     = pick("shipment", "shipment_xid", "shipmentXid", "orderBaseGid", "order_base_gid")
#     ob_ship_unit_gid   = pick("supplier", "servprov_alias_value", "servprovAliasValue",
#                                "obShipUnitGid", "ob_ship_unit_gid")

#     missing = [name for name, val in [
#         ("requestId",               request_id),
#         ("object_storage_path",     object_storage_url),
#         ("shipment (orderBaseGid)", order_base_gid),
#         ("supplier (obShipUnitGid)",ob_ship_unit_gid),
#     ] if not val]

#     if missing:
#         msg = f"Missing required input(s): {', '.join(missing)}"
#         log.error(msg)
#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({"status": "error", "message": msg}),
#             headers={"Content-Type": "application/json"},
#             status_code=400,
#         )

#     try:
#         signer    = get_signer()
#         os_client = get_object_storage_client(signer)
#         du_client = get_document_understanding_client(signer)
#         pdf_path  = download_pdf_by_url(os_client, object_storage_url)

#         result = process_document(
#             signer, os_client, du_client, pdf_path,
#             request_id, order_base_gid, ob_ship_unit_gid,
#         )

#         if isinstance(result, dict) and result.get("error"):
#             return fdk_response.Response(
#                 ctx,
#                 response_data=json.dumps({
#                     "status": "error", "request_id": request_id,
#                     "message": result.get("error"),
#                 }),
#                 headers={"Content-Type": "application/json"},
#                 status_code=422,
#             )

#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps(result, ensure_ascii=False),
#             headers={"Content-Type": "application/json"},
#         )

#     except Exception as exc:
#         log.exception("Orchestrator pipeline failed")
#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({
#                 "status": "error", "request_id": request_id, "message": str(exc),
#             }),
#             headers={"Content-Type": "application/json"},
#             status_code=500,
#         )
#     finally:
#         import shutil
#         try:
#             shutil.rmtree(TMP_DIR)
#         except Exception:
#             pass



##the abvoe is working fine below is tweake dand professional
"""
OTM Packing List Orchestrator  â€”  OCI FUNCTIONS VERSION  v3.0.0
================================================================
Changes from v2:
  - Folder structure: all files stored under Packing List/{requestId}/
      manifest â†’ Packing List/{requestId}/manifest.json
      raw text â†’ Packing List/{requestId}/raw_text.txt
  - Inline stdout logging for OCI Code Editor visibility
  - EXTRACTOR_ENDPOINT default updated to correct hostname
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
from urllib.parse import urlparse, unquote

import pdfplumber
from pypdf import PdfReader

import oci
from oci.auth.signers import get_resource_principals_signer
from oci.ai_document import AIServiceDocumentClient
from oci.functions.functions_invoke_client import FunctionsInvokeClient
from fdk import response as fdk_response


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
OCI_DU_ENDPOINT    = os.getenv(
    "OCI_DU_ENDPOINT",
    "https://example.invalid/integration-endpoint",
)

OS_NAMESPACE    = os.getenv("OS_NAMESPACE",    "sample_namespace")
OS_BUCKET       = os.getenv("OS_BUCKET",       "sample-workflow-bucket")
OS_INPUT_BUCKET = os.getenv("OS_INPUT_BUCKET", "sample-workflow-bucket")

PL_BASE_FOLDER = os.getenv("OS_PL_BASE_FOLDER", "Packing List")   # â† single root
PL_TEMP_FOLDER = os.getenv("OS_PL_TEMP_FOLDER", "Packing List/Temp")
PL_DU_OUTPUT   = os.getenv("OS_PL_DU_OUTPUT",   "Packing List/DU Output")

MIN_CHARS_PER_PAGE = int(os.getenv("MIN_CHARS_PER_PAGE", "80"))
CHUNK_SIZE         = int(os.getenv("CHUNK_SIZE", "6"))

EXTRACTOR_FUNCTION_ID = os.getenv("EXTRACTOR_FUNCTION_ID", "")
EXTRACTOR_ENDPOINT    = os.getenv(
    "EXTRACTOR_ENDPOINT",
    "https://example.invalid/integration-endpoint",  # â† corrected default
)

OTM_DOMAIN_NAME = os.getenv("OTM_DOMAIN_NAME", "SAMPLE_ORG")
# TMP_DIR = "/tmp/pl_orchestrator"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  LOGGING  â€” stdout visible in OCI Code Editor
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
#     formatted = msg % args if args else msg
#     print(f"â–¶ {formatted}", flush=True)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  FOLDER HELPERS  â€” rooted at Packing List/{safe_req}/
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _req_folder(safe_req: str) -> str:
#     return f"{PL_BASE_FOLDER}/{safe_req}"

# def _manifest_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/manifest.json"

# def _raw_text_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/raw_text.txt"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 1  â€”  OCI CLIENTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def get_signer():
#     return get_resource_principals_signer()

# def get_object_storage_client(signer):
#     return oci.object_storage.ObjectStorageClient(config={}, signer=signer)

# def get_document_understanding_client(signer) -> AIServiceDocumentClient:
#     return AIServiceDocumentClient(
#         config={}, signer=signer, service_endpoint=OCI_DU_ENDPOINT,
#     )


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 2  â€”  OBJECT STORAGE HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def upload_to_object_storage(os_client, content, object_name, content_type="application/octet-stream"):
#     body = content.encode("utf-8") if isinstance(content, str) else content
#     try:
#         os_client.put_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
#             object_name=object_name, put_object_body=body, content_type=content_type,
#         )
#         log.info("Uploaded â†’ %s", object_name)
#         return True
#     except Exception as exc:
#         log.error("Upload failed for '%s': %s", object_name, exc)
#         return False

# def parse_os_url(os_url, default_bucket=None):
#     if default_bucket is None:
#         default_bucket = OS_BUCKET
#     if not os_url:
#         raise ValueError("Empty Object Storage URL/path provided.")
#     if "://" not in os_url:
#         return OS_NAMESPACE, default_bucket, unquote(os_url.lstrip("/"))
#     path  = urlparse(os_url).path
#     parts = [p for p in path.split("/") if p]
#     ns, bucket, obj = OS_NAMESPACE, default_bucket, None
#     if "n" in parts:
#         ns = parts[parts.index("n") + 1]
#     if "b" in parts:
#         bucket = parts[parts.index("b") + 1]
#     if "o" in parts:
#         obj = "/".join(parts[parts.index("o") + 1:])
#     if not obj:
#         obj = path.lstrip("/")
#     return OS_NAMESPACE, bucket, unquote(obj)

# def download_pdf_by_url(os_client, os_url):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     namespace, bucket, object_key = parse_os_url(os_url, default_bucket=OS_INPUT_BUCKET)
#     pdf_filename = Path(object_key).name or f"download_{uuid.uuid4().hex[:8]}.pdf"
#     local_path   = Path(TMP_DIR) / pdf_filename
#     _plog("Downloading: %s", object_key)
#     get_resp = os_client.get_object(
#         namespace_name=namespace, bucket_name=bucket, object_name=object_key,
#     )
#     with open(local_path, "wb") as f:
#         for chunk in get_resp.data.raw.stream(1024 * 1024, decode_content=False):
#             f.write(chunk)
#     _plog("Downloaded â†’ %s (%d bytes)", local_path, local_path.stat().st_size)
#     return local_path


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 3  â€”  TEXT EXTRACTION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def is_scanned_pdf(pdf_bytes):
#     try:
#         with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
#             pages = pdf.pages[:3]
#             if not pages:
#                 return True
#             total_chars = sum(
#                 len((p.extract_text(x_tolerance=2, y_tolerance=2) or "").strip())
#                 for p in pages
#             )
#             avg = total_chars / len(pages)
#             _plog("Scanned detection: avg_chars=%.1f â†’ %s", avg,
#                   "SCANNED" if avg < MIN_CHARS_PER_PAGE else "digital")
#             return avg < MIN_CHARS_PER_PAGE
#     except Exception as exc:
#         log.warning("Scanned detection failed: %s", exc)
#         return False

# def extract_pages_digital(pdf_bytes):
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
#                 _plog("  Page %d: %d chars", i, len(combined))
#     except Exception as exc:
#         log.error("pdfplumber failed: %s â€” trying pypdf", exc)
#         try:
#             reader = PdfReader(io.BytesIO(pdf_bytes))
#             for i, page in enumerate(reader.pages, start=1):
#                 text = (page.extract_text() or "").strip()
#                 pages_text.append(text)
#         except Exception as exc2:
#             log.error("pypdf fallback also failed: %s", exc2)
#     return pages_text

# def extract_text_via_du(du_client, os_client, pdf_path):
#     safe_stem  = re.sub(r"[^\w\-]", "_", pdf_path.stem)
#     temp_key   = f"{PL_TEMP_FOLDER}/{safe_stem}_{uuid.uuid4().hex[:8]}.pdf"
#     du_out_pfx = f"{PL_DU_OUTPUT}/{safe_stem}_{uuid.uuid4().hex[:8]}"

#     pdf_bytes = pdf_path.read_bytes()
#     upload_to_object_storage(os_client, pdf_bytes, temp_key, "application/pdf")

#     job_details = oci.ai_document.models.CreateProcessorJobDetails(
#         display_name   = f"pl-orch-{safe_stem}",
#         compartment_id = OCI_COMPARTMENT_ID,
#         input_location = oci.ai_document.models.ObjectStorageLocations(
#             object_locations=[oci.ai_document.models.ObjectLocation(
#                 namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=temp_key,
#             )]
#         ),
#         output_location=oci.ai_document.models.OutputLocation(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=du_out_pfx,
#         ),
#         processor_config=oci.ai_document.models.GeneralProcessorConfig(
#             features=[oci.ai_document.models.DocumentTextExtractionFeature(
#                 generate_searchable_pdf=False,
#             )],
#             is_zip_output_enabled=False,
#         ),
#     )
#     job_id = du_client.create_processor_job(
#         create_processor_job_details=job_details
#     ).data.id
#     _plog("[DU] Job created: %s", job_id)

#     elapsed, status = 0, "SUBMITTED"
#     while elapsed < 300:
#         time.sleep(5); elapsed += 5
#         status = du_client.get_processor_job(processor_job_id=job_id).data.lifecycle_state
#         _plog("[DU] Status: %s (%ds)", status, elapsed)
#         if status in ("SUCCEEDED", "FAILED", "CANCELED"):
#             break

#     if status != "SUCCEEDED":
#         raise RuntimeError(f"DU job {job_id} ended: {status}")

#     resp     = os_client.list_objects(namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=du_out_pfx)
#     all_keys = [o.name for o in resp.data.objects]
#     result_key = next((k for k in all_keys if k.endswith("analysedDocument.json")), None) \
#               or next((k for k in all_keys if k.endswith(".json")), None)
#     if not result_key:
#         raise FileNotFoundError(f"No DU result under {du_out_pfx}")

#     raw = os_client.get_object(
#         namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=result_key,
#     ).data.content.decode("utf-8")
#     du_data = json.loads(raw)

#     page_texts = []
#     for page in du_data.get("pages", []):
#         lines = [ln.get("text", "").strip() for ln in page.get("lines", []) if ln.get("text", "").strip()]
#         page_texts.append(f"--- Page {page.get('pageNumber','?')} ---\n" + "\n".join(lines))
#     return "\n\n".join(page_texts)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 4  â€”  PAGE CLASSIFIER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# _INVOICE_PATTERNS = [
#     r"commercial\s+invoice", r"invoice\s+n[o0]", r"unit\s+price",
#     r"total\s+amount", r"seller|shipper\s*/\s*exporter",
#     r"\bfob\b|\bcfr\b|\bcif\b", r"amount\s+in\s+words",
#     r"bank\s+details|swift|iban", r"payment\s+terms",
# ]
# _PACKING_PATTERNS = [
#     r"\bpacking\s+list\b", r"\d+\s+of\s+\d+", r"pkg\s*no\.?|package\s+no",
#     r"net\s+weight.*gross\s+weight", r"gross\s+weight.*kgs?",
#     r"dimensions?\s*[:\-]?\s*\d", r"\bcbm\b", r"summary\s+packing",
#     r"packing\s+style", r"l\s*x\s*w\s*x\s*h",
# ]

# def classify_pages_by_regex(pages_text):
#     labels = []
#     for i, page in enumerate(pages_text):
#         lower     = page.lower()
#         inv_score = sum(1 for p in _INVOICE_PATTERNS if re.search(p, lower))
#         pl_score  = sum(1 for p in _PACKING_PATTERNS if re.search(p, lower))
#         if inv_score == 0 and pl_score == 0:
#             label = "other"
#         elif pl_score > inv_score:
#             label = "packing_list"
#         elif inv_score > pl_score:
#             label = "invoice"
#         else:
#             label = "packing_list" if re.search(r"\d+\s+of\s+\d+", lower) else "invoice"
#         _plog("  Page %d â†’ %s (inv=%d, pl=%d)", i + 1, label, inv_score, pl_score)
#         labels.append(label)
#     return labels

# def keep_packing_pages(pages_text, labels):
#     packing = [p for p, l in zip(pages_text, labels) if l == "packing_list"]
#     if not packing:
#         packing = [p for p, l in zip(pages_text, labels) if l != "invoice"]
#         log.warning("No explicit PL pages â€” using %d non-invoice pages", len(packing))
#     _plog("Page split â†’ PL: %d | invoice: %d | other: %d",
#           sum(1 for l in labels if l == "packing_list"),
#           sum(1 for l in labels if l == "invoice"),
#           sum(1 for l in labels if l == "other"))
#     return packing


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 5  â€”  CARRY-FORWARD PKG_NO
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _precompute_last_pkg_no(pages_chunk):
#     PKG_NO_RE = re.compile(r"\b(\d+\s+OF\s+\d+)\b", re.IGNORECASE)
#     last = None
#     for page in pages_chunk:
#         matches = PKG_NO_RE.findall(page)
#         if matches:
#             last = matches[-1].strip()
#     return last


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 6  â€”  FIRE EXTRACTOR (ASYNC / DETACHED)
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# # def fire_extractor_async(signer, payload: dict):
# #     invoke_client = FunctionsInvokeClient(
# #         config={},
# #         signer=signer,
# #         service_endpoint=EXTRACTOR_ENDPOINT,
# #         timeout=(10, 240),
# #     )
# #     body_bytes = json.dumps(payload).encode("utf-8")
# #     invoke_client.invoke_function(
# #         function_id=EXTRACTOR_FUNCTION_ID,
# #         invoke_function_body=body_bytes,
# #         fn_invoke_type="detached",
# #     )
# #     _plog("[ORCH] Fired extractor chunk %d/%d (detached)",
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
# #  SECTION 7  â€”  MAIN PIPELINE
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# # def process_document(signer, os_client, du_client, pdf_path, request_id,
# #                      order_base_gid, ob_ship_unit_gid):
# def process_document(signer, os_client, du_client, pdf_path, request_id,
#                      order_base_gid, ob_ship_unit_gid,
#                      shipment_id="", supplier_name="", doc_key="packing_list"):
#     pipeline_start = time.time()
#     filename  = pdf_path.name
#     safe_stem = re.sub(r"[^\w\-]", "_", pdf_path.stem)
#     safe_req  = re.sub(r"[^\w\-]", "_", request_id) or safe_stem

#     _plog("=" * 60)
#     _plog("Orchestrator v3.0.0 â€” %s", filename)
#     _plog("requestId=%s  folder=%s", request_id, _req_folder(safe_req))

#     pdf_bytes = pdf_path.read_bytes()

#     # STEP 1: Detect scanned vs digital
#     scanned = is_scanned_pdf(pdf_bytes)

#     # STEP 2: Extract text
#     if scanned:
#         full_text  = extract_text_via_du(du_client, os_client, pdf_path)
#         pages_text = [full_text]
#     else:
#         pages_text = extract_pages_digital(pdf_bytes)
#         full_text  = "\n\n".join(
#             f"--- Page {i} ---\n{p}" for i, p in enumerate(pages_text, start=1)
#         )

#     if not full_text.strip():
#         return {"filename": filename, "error": "no text extracted"}

#     _plog("Extracted %d chars across %d pages", len(full_text), len(pages_text))

#     # Save raw text â†’ Packing List/{req}/raw_text.txt
#     upload_to_object_storage(
#         os_client, full_text, _raw_text_key(safe_req), "text/plain",
#     )

#     # STEP 3: Classify pages
#     if scanned:
#         packing_pages = pages_text
#     else:
#         labels        = classify_pages_by_regex(pages_text)
#         packing_pages = keep_packing_pages(pages_text, labels)

#     if not packing_pages:
#         return {"filename": filename, "error": "no packing list content found"}

#     _plog("Packing-list pages: %d", len(packing_pages))

#     # STEP 4: Split into chunks
#     page_chunks = [
#         packing_pages[i: i + CHUNK_SIZE]
#         for i in range(0, len(packing_pages), CHUNK_SIZE)
#     ]
#     total_chunks = len(page_chunks)
#     _plog("Split into %d chunks of â‰¤%d pages", total_chunks, CHUNK_SIZE)

#     # Pre-compute carry-forward pkg_no per chunk
#     last_pkg_nos = [None]
#     for chunk in page_chunks[:-1]:
#         last_pkg_nos.append(_precompute_last_pkg_no(chunk))

#     # STEP 5: Build and save MANIFEST â†’ Packing List/{req}/manifest.json
#     manifest = {
#         "requestId":      request_id,
#         "safeReqId":      safe_req,
#         "totalChunks":    total_chunks,
#         "chunkSize":      CHUNK_SIZE,
#         "filename":       filename,
#         "isScanned":      scanned,
#         "orderBaseGid":   order_base_gid,
#         "obShipUnitGid":  ob_ship_unit_gid,
#         "createdAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "folder":         _req_folder(safe_req),
#         "shipmentId":     shipment_id,    # â† add
#         "supplierName":   supplier_name,  # â† add
#         "chunks": [
#             {
#                 "chunkIndex": i + 1,
#                 "pages":      chunk,
#                 "lastPkgNo":  last_pkg_nos[i],
#             }
#             for i, chunk in enumerate(page_chunks)
#         ],
#     }
#     upload_to_object_storage(
#         os_client,
#         json.dumps(manifest, indent=2, ensure_ascii=False),
#         _manifest_key(safe_req),
#         "application/json",
#     )
#     _plog("Manifest saved â†’ %s", _manifest_key(safe_req))

#     # STEP 6: Fire extractor chunk 1 (detached)
#     # fire_extractor_async(signer, {
#     #     "requestId":     request_id,
#     #     "chunkIndex":    1,
#     #     "totalChunks":   total_chunks,
#     #     "pages":         page_chunks[0],
#     #     "lastPkgNo":     last_pkg_nos[0],
#     #     "orderBaseGid":  order_base_gid,
#     #     "obShipUnitGid": ob_ship_unit_gid,
#     # })

#     fire_extractor_async(signer, {
#         "requestId":     request_id,
#         "chunkIndex":    1,
#         "totalChunks":   total_chunks,
#         "pages":         page_chunks[0],
#         "lastPkgNo":     last_pkg_nos[0],
#         "orderBaseGid":  order_base_gid,
#         "obShipUnitGid": ob_ship_unit_gid,
#         "shipmentId":    shipment_id,     # â† add
#         "supplierName":  supplier_name,   # â† add
#         "docKey":        doc_key,          # â† NEW
#     })

#     pipeline_duration = round(time.time() - pipeline_start, 2)
#     _plog("Orchestrator done in %.1fs â€” extractor chain started", pipeline_duration)
#     _plog("All output will be under: %s", _req_folder(safe_req))

#     return {
#         "status":          "processing",
#         "requestId":       request_id,
#         "totalChunks":     total_chunks,
#         "chunkSize":       CHUNK_SIZE,
#         "totalPages":      len(packing_pages),
#         "filename":        filename,
#         "folder":          _req_folder(safe_req),
#         "manifestSavedTo": _manifest_key(safe_req),
#         "message": (
#             f"Extraction started. {total_chunks} chunk(s) will run sequentially. "
#             f"All output under: {_req_folder(safe_req)}"
#         ),
#         "_debug": {
#             "orchestrator_duration_s": pipeline_duration,
#             "total_pages_in_pdf":      len(pages_text),
#             "packing_pages_used":      len(packing_pages),
#             "chunks_total":            total_chunks,
#             "chunk_size":              CHUNK_SIZE,
#             "is_scanned":              scanned,
#         },
#     }


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 8  â€”  OCI FUNCTIONS ENTRY POINT
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def handler(ctx, data: io.BytesIO = None):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     _plog("=" * 60)
#     _plog("PL Orchestrator v3.0.0 invoked")

#     for var, name in [(OCI_COMPARTMENT_ID, "OCI_COMPARTMENT_ID"),
#                       (EXTRACTOR_FUNCTION_ID, "EXTRACTOR_FUNCTION_ID")]:
#         if not var:
#             msg = f"{name} env var is not set."
#             log.error(msg)
#             return fdk_response.Response(
#                 ctx,
#                 response_data=json.dumps({"status": "error", "message": msg}),
#                 headers={"Content-Type": "application/json"},
#                 status_code=500,
#             )

#     body = {}
#     if data:
#         try:
#             body = json.loads(data.getvalue())
#             _plog("Input keys: %s", list(body.keys()))
#         except Exception as exc:
#             log.warning("Body parse failed: %s", exc)

#     def pick(*keys):
#         for k in keys:
#             v = body.get(k)
#             if v is not None and str(v).strip():
#                 return str(v).strip()
#         return None

#     request_id         = pick("requestId", "request_id", "referenceTransmissionNo")
#     object_storage_url = pick("object_storage_path", "object_storage_url", "objectStoragePath",
#                                "objectStorageUrl", "object_url", "file_url", "url")
#     order_base_gid     = pick("shipment", "shipment_xid", "shipmentXid", "orderBaseGid", "order_base_gid")
#     ob_ship_unit_gid   = pick("supplier", "servprov_alias_value", "servprovAliasValue",
#                                "obShipUnitGid", "ob_ship_unit_gid")
#     shipment_id        = pick("shipmentId", "shipment_id") or ""
#     supplier_name      = pick("supplierName", "supplier_name") or ""

#     missing = [name for name, val in [
#         ("requestId",               request_id),
#         ("object_storage_path",     object_storage_url),
#         ("shipment (orderBaseGid)", order_base_gid),
#         ("supplier (obShipUnitGid)",ob_ship_unit_gid),
#     ] if not val]

#     if missing:
#         msg = f"Missing required input(s): {', '.join(missing)}"
#         log.error(msg)
#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({"status": "error", "message": msg}),
#             headers={"Content-Type": "application/json"},
#             status_code=400,
#         )

#     _plog("requestId=%s  pdf=%s", request_id, object_storage_url)

# # Extract doc_key here where pick() is available
#     doc_key = pick("docKey", "doc_key") or "packing_list"

#     try:
#         signer    = get_signer()
#         os_client = get_object_storage_client(signer)
#         du_client = get_document_understanding_client(signer)
#         pdf_path  = download_pdf_by_url(os_client, object_storage_url)

#         result = process_document(
#             signer, os_client, du_client, pdf_path,
#             request_id, order_base_gid, ob_ship_unit_gid,
#             shipment_id=shipment_id,
#             supplier_name=supplier_name,
#             doc_key=doc_key,
#         )
#         # result = process_document(
#         #     signer, os_client, du_client, pdf_path,
#         #     request_id, order_base_gid, ob_ship_unit_gid,
#         #     shipment_id, supplier_name, doc_key,    # â† doc_key added
#         # )

#         if isinstance(result, dict) and result.get("error"):
#             return fdk_response.Response(
#                 ctx,
#                 response_data=json.dumps({
#                     "status": "error", "request_id": request_id,
#                     "message": result.get("error"),
#                 }),
#                 headers={"Content-Type": "application/json"},
#                 status_code=422,
#             )

#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps(result, ensure_ascii=False),
#             headers={"Content-Type": "application/json"},
#         )

#     except Exception as exc:
#         log.exception("Orchestrator pipeline failed")
#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({
#                 "status": "error", "request_id": request_id, "message": str(exc),
#             }),
#             headers={"Content-Type": "application/json"},
#             status_code=500,
#         )
#     finally:
#         import shutil
#         try:
#             shutil.rmtree(TMP_DIR)
#         except Exception:
#             pass

TMP_DIR = "/tmp/pl_orchestrator_v4"


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
    return f"{PL_BASE_FOLDER}/{safe_req}"
 
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
#  HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 
def _precompute_last_pkg_no(pages_chunk: List[str]) -> Optional[str]:
    PKG_NO_RE = re.compile(r"\b(\d+\s+OF\s+\d+)\b", re.IGNORECASE)
    last = None
    for page in pages_chunk:
        matches = PKG_NO_RE.findall(page)
        if matches:
            last = matches[-1].strip()
    return last
 
 
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
    Reads pre-classified pages from OCI OS (written by Classifier v2.0.0).
    Chunks them and fires the PL extractor.
    No PDF download. No text extraction. No page classification.
    """
    pipeline_start = time.time()
    safe_req = _safe(request_id)
 
    _plog("=" * 60)
    _plog("PL Orchestrator v4.0.0")
    _plog("requestId=%s  classified=%s", request_id, classified_pages_path)
 
    # STEP 1: Read pre-classified pages from OCI OS
    raw = download_from_os(os_client, classified_pages_path)
    if not raw:
        raise RuntimeError(
            f"Classified pages not found: {classified_pages_path}"
        )
 
    classified = json.loads(raw)
    pages_text  = classified.get("pages", [])
    filename    = Path(classified.get("source_file", "unknown")).name
 
    if not pages_text:
        return {"filename": filename, "error": "no packing list pages in classified file"}
 
    _plog("Read %d pre-classified PL pages from classifier", len(pages_text))
 
    # STEP 2: Split into chunks
    page_chunks  = [
        pages_text[i: i + CHUNK_SIZE]
        for i in range(0, len(pages_text), CHUNK_SIZE)
    ]
    total_chunks = len(page_chunks)
    _plog("Split into %d chunk(s) of â‰¤%d pages", total_chunks, CHUNK_SIZE)
 
    # Pre-compute carry-forward pkg_no per chunk (for PL continuity)
    last_pkg_nos = [None]
    for chunk in page_chunks[:-1]:
        last_pkg_nos.append(_precompute_last_pkg_no(chunk))
 
    # STEP 3: Write manifest
    manifest = {
        "requestId":      request_id,
        "safeReqId":      safe_req,
        "totalChunks":    total_chunks,
        "chunkSize":      CHUNK_SIZE,
        "filename":       filename,
        "isScanned":      False,   # classifier handled scanned via DU
        "orderBaseGid":   order_base_gid,
        "obShipUnitGid":  ob_ship_unit_gid,
        "createdAt":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "folder":         _req_folder(safe_req),
        "shipmentId":     shipment_id,
        "supplierName":   supplier_name,
        "docKey":         doc_key,
        "chunks": [
            {
                "chunkIndex": i + 1,
                "pages":      chunk,
                "lastPkgNo":  last_pkg_nos[i],
            }
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
        "lastPkgNo":     last_pkg_nos[0],
        "orderBaseGid":  order_base_gid,
        "obShipUnitGid": ob_ship_unit_gid,
        "shipmentId":    shipment_id,
        "supplierName":  supplier_name,
        "docKey":        doc_key,
    })
 
    duration = round(time.time() - pipeline_start, 2)
    _plog("PL Orchestrator done in %.1fs", duration)
 
    return {
        "status":       "processing",
        "requestId":    request_id,
        "documentType": "packing_list",
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
    _plog("PL Orchestrator v4.0.0 invoked")
 
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
 
    request_id          = pick("requestId", "request_id") or \
                          f"PL-{uuid.uuid4().hex[:8].upper()}"
    classified_path     = pick("classified_pages_path", "classified_path")
    order_base_gid      = pick("shipment", "orderBaseGid", "order_base_gid") or ""
    ob_ship_unit_gid    = pick("supplier", "obShipUnitGid", "ob_ship_unit_gid") or ""
    shipment_id         = pick("shipmentId", "shipment_id") or ""
    supplier_name       = pick("supplierName", "supplier_name") or ""
    doc_key             = pick("docKey", "doc_key") or "packing_list_01"
 
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
        log.exception("PL Orchestrator failed")
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