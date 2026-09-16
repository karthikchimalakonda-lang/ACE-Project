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

























































TMP_DIR = "/tmp/bl_orchestrator_v2"
 
 
 
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
    return f"{BL_BASE_FOLDER}/{safe_req}"
 
def _manifest_key(safe_req: str) -> str:
    return f"{_req_folder(safe_req)}/manifest.json"
 
 
 
def get_signer():
    return get_resource_principals_signer()
 
def get_object_storage_client(signer):
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
 
 
 
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
 
    page_chunks  = [
        pages_text[i: i + CHUNK_SIZE]
        for i in range(0, len(pages_text), CHUNK_SIZE)
    ]
    total_chunks = len(page_chunks)
    _plog("Split into %d chunk(s) of â‰¤%d pages", total_chunks, CHUNK_SIZE)
 
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
 