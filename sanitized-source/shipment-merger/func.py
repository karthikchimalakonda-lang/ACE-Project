# """
# AI Shipment Merger  â€”  OCI FUNCTIONS v1.1.0
# ============================================
# CHANGES vs v1.0.0:
#   - update_shipment_manifest_status now marks EVERY individual document
#     as complete (not just the top-level status field), so manual invocations
#     and automatic invocations both produce a fully consistent manifest.
#   - Cleans up completion marker files after successful merge so the
#     Shipments/{id}/complete/ prefix stays tidy on retries.

# ROLE: Final step â€” merge extracted JSONs (PL + Invoice + BL) for a shipment
# into a single unified shipment JSON.

# Can be called:
#   a) Automatically when the last extractor finishes (via marker-file count)
#   b) Manually at any time once all three are done

# Input:
# {
#   "shipmentId":   "SHP-REQ-REALTIME-014",
#   "requestIds": {
#     "packing_list":   "PL-REQ-REALTIME-014-01",
#     "invoice":        "INV-REQ-REALTIME-014-01",
#     "bill_of_lading": "BL-REQ-REALTIME-014-01"
#   }
# }

# Output (also saved to OS: Shipments/{shipmentId}/merged.json):
# {
#   "shipmentId":   "...",
#   "mergedAt":     "...",
#   "packing_list": { ...full extracted PL JSON... },
#   "invoice":      { ...full extracted invoice JSON... },
#   "bill_of_lading": { ...full extracted BL JSON... },
#   "summary": {
#     "supplierName":        "...",
#     "invoiceNumber":       "...",
#     "blNumber":            "...",
#     "portOfLoading":       "...",
#     "portOfDischarge":     "...",
#     "totalGrossWeightKg":  ...,
#     "totalCbm":            ...,
#     "totalLineItems":      ...,
#     "totalPackages":       ...,
#     "currency":            "...",
#     "totalInvoiceAmount":  ...
#   }
# }
# """

# import io
# import json
# import logging
# import os
# import re
# import sys
# import time
# from typing import Optional

# import oci
# from oci.auth.signers import get_resource_principals_signer
# from fdk import response as fdk_response


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  CONFIGURATION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
# OS_NAMESPACE       = os.getenv("OS_NAMESPACE", "sample_namespace")
# OS_BUCKET          = os.getenv("OS_BUCKET",    "sample-workflow-bucket")

# PL_BASE_FOLDER   = os.getenv("OS_PL_BASE_FOLDER",  "Packing List")
# INV_BASE_FOLDER  = os.getenv("OS_INV_BASE_FOLDER",  "Invoice")
# BL_BASE_FOLDER   = os.getenv("OS_BL_BASE_FOLDER",   "Bill of Lading")
# SHIPMENTS_FOLDER = os.getenv("OS_SHIPMENTS_FOLDER", "Shipments")

# TMP_DIR = "/tmp/ai_shipment_merger"


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
# #  HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _safe(s: str) -> str:
#     return re.sub(r"[^\w\-]", "_", s) if s else "unknown"

# def get_object_storage_client():
#     signer = get_resource_principals_signer()
#     return oci.object_storage.ObjectStorageClient(config={}, signer=signer)

# def upload_to_os(os_client, content, object_name, content_type="application/json"):
#     body = content.encode("utf-8") if isinstance(content, str) else content
#     try:
#         os_client.put_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
#             object_name=object_name, put_object_body=body, content_type=content_type,
#         )
#         log.info("Saved â†’ %s", object_name)
#         return True
#     except Exception as exc:
#         log.error("Upload failed '%s': %s", object_name, exc)
#         return False

# def download_from_os(os_client, object_name) -> Optional[str]:
#     try:
#         resp = os_client.get_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=object_name,
#         )
#         return resp.data.content.decode("utf-8")
#     except Exception as exc:
#         log.warning("Not found '%s': %s", object_name, exc)
#         return None

# def list_os_keys(os_client, prefix: str):
#     try:
#         resp = os_client.list_objects(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=prefix,
#         )
#         return [o.name for o in resp.data.objects]
#     except Exception as exc:
#         log.warning("List failed prefix='%s': %s", prefix, exc)
#         return []

# def delete_from_os(os_client, object_name):
#     try:
#         os_client.delete_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=object_name,
#         )
#         log.info("Deleted â†’ %s", object_name)
#     except Exception as exc:
#         log.warning("Delete failed '%s': %s", object_name, exc)

# def _extracted_key(base_folder: str, safe_req: str) -> str:
#     return f"{base_folder}/{safe_req}/extracted.json"

# def _shipment_manifest_key(safe_shp: str) -> str:
#     return f"{SHIPMENTS_FOLDER}/{safe_shp}.json"

# def _merged_key(safe_shp: str) -> str:
#     return f"{SHIPMENTS_FOLDER}/{safe_shp}/merged.json"

# def _complete_prefix(safe_shp: str) -> str:
#     return f"{SHIPMENTS_FOLDER}/{safe_shp}/complete/"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  MANIFEST STATUS UPDATE
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def update_shipment_manifest_status(os_client, safe_shp: str):
#     """
#     Mark the shipment manifest as fully complete:
#       - sets every individual document's status â†’ 'complete'
#       - sets top-level status â†’ 'complete'
#       - adds completedAt timestamp

#     â”€â”€ FIX vs v1.0.0: v1.0.0 only set top-level status, leaving individual
#        document statuses as 'pending' when merger was invoked manually.
#        Now both levels are updated consistently. â”€â”€
#     """
#     raw = download_from_os(os_client, _shipment_manifest_key(safe_shp))
#     if not raw:
#         log.warning("Shipment manifest not found at %s â€” cannot mark complete", _shipment_manifest_key(safe_shp))
#         return
#     try:
#         manifest = json.loads(raw)

#         # â”€â”€ FIX: mark every individual document complete â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#         for doc_type, doc_info in manifest.get("documents", {}).items():
#             if isinstance(doc_info, dict):
#                 doc_info["status"] = "complete"
#                 _plog("  Marked document '%s' â†’ complete", doc_type)

#         # â”€â”€ Mark top-level status â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#         manifest["status"]      = "complete"
#         manifest["completedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

#         upload_to_os(
#             os_client,
#             json.dumps(manifest, indent=2, ensure_ascii=False),
#             _shipment_manifest_key(safe_shp),
#         )
#         _plog("Shipment manifest fully marked complete")
#     except Exception as exc:
#         log.warning("Could not update shipment manifest: %s", exc)


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  MERGE LOGIC
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def _load_doc(os_client, base_folder: str, request_id: str) -> Optional[dict]:
#     """Load extracted.json for a given request_id."""
#     if not request_id:
#         return None
#     key = _extracted_key(base_folder, _safe(request_id))
#     raw = download_from_os(os_client, key)
#     if not raw:
#         log.warning("extracted.json not found for requestId=%s at %s", request_id, key)
#         return None
#     try:
#         return json.loads(raw)
#     except Exception as exc:
#         log.error("JSON parse failed for %s: %s", key, exc)
#         return None

# def build_summary(pl_data: Optional[dict], inv_data: Optional[dict], bl_data: Optional[dict]) -> dict:
#     """Distil key cross-document fields into one summary block."""
#     supplier_name  = None
#     invoice_number = None
#     bl_number      = None
#     port_loading   = None
#     port_discharge = None
#     total_gw       = None
#     total_cbm      = None
#     total_li       = 0
#     total_pkgs     = None
#     currency       = None
#     total_amount   = None

#     if pl_data:
#         total_gw       = pl_data.get("total_gross_weight_kg")
#         total_cbm      = pl_data.get("total_cbm")
#         total_pkgs     = pl_data.get("total_packages")
#         port_loading   = pl_data.get("port_of_loading")
#         port_discharge = pl_data.get("port_of_discharge")
#         total_li       = len(pl_data.get("packages", []))
#         shipper        = pl_data.get("shipper") or {}
#         supplier_name  = shipper.get("name")

#     if inv_data:
#         invoice_number = inv_data.get("invoice_number")
#         currency       = inv_data.get("currency")
#         total_amount   = inv_data.get("total_amount")
#         if not port_loading:
#             port_loading   = inv_data.get("port_of_loading")
#         if not port_discharge:
#             port_discharge = inv_data.get("port_of_discharge")
#         if not supplier_name:
#             seller = inv_data.get("seller") or {}
#             supplier_name = seller.get("name")
#         if not total_li:
#             total_li = len(inv_data.get("line_items", []))

#     if bl_data:
#         bl_number = bl_data.get("bl_number")
#         if not port_loading:
#             port_loading   = bl_data.get("port_of_loading")
#         if not port_discharge:
#             port_discharge = bl_data.get("port_of_discharge")
#         if not total_gw:
#             total_gw  = bl_data.get("total_gross_weight_kg")
#         if not total_cbm:
#             total_cbm = bl_data.get("total_measurement_cbm")
#         if not total_pkgs:
#             total_pkgs = bl_data.get("total_packages")

#     return {
#         "supplierName":       supplier_name,
#         "invoiceNumber":      invoice_number,
#         "blNumber":           bl_number,
#         "portOfLoading":      port_loading,
#         "portOfDischarge":    port_discharge,
#         "totalGrossWeightKg": total_gw,
#         "totalCbm":           total_cbm,
#         "totalLineItems":     total_li,
#         "totalPackages":      total_pkgs,
#         "currency":           currency,
#         "totalInvoiceAmount": total_amount,
#     }


# def merge_shipment(os_client, shipment_id: str, request_ids: dict) -> tuple:
#     safe_shp = _safe(shipment_id)

#     pl_req  = request_ids.get("packing_list")
#     inv_req = request_ids.get("invoice")
#     bl_req  = request_ids.get("bill_of_lading")

#     _plog("Loading documents for shipment %s", shipment_id)
#     _plog("  PL  requestId: %s", pl_req  or "â€”")
#     _plog("  INV requestId: %s", inv_req or "â€”")
#     _plog("  BL  requestId: %s", bl_req  or "â€”")

#     pl_data  = _load_doc(os_client, PL_BASE_FOLDER,  pl_req)  if pl_req  else None
#     inv_data = _load_doc(os_client, INV_BASE_FOLDER, inv_req) if inv_req else None
#     bl_data  = _load_doc(os_client, BL_BASE_FOLDER,  bl_req)  if bl_req  else None

#     docs_found = sum(1 for d in [pl_data, inv_data, bl_data] if d is not None)
#     _plog("Documents loaded: %d / %d", docs_found,
#           sum(1 for r in [pl_req, inv_req, bl_req] if r))

#     summary = build_summary(pl_data, inv_data, bl_data)

#     merged = {
#         "shipmentId":     shipment_id,
#         "mergedAt":       time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "summary":        summary,
#         "packing_list":   pl_data,
#         "invoice":        inv_data,
#         "bill_of_lading": bl_data,
#         "_meta": {
#             "requestIds": request_ids,
#             "docsLoaded": docs_found,
#             "folder":     f"{SHIPMENTS_FOLDER}/{safe_shp}",
#         },
#     }

#     merged_path = _merged_key(safe_shp)
#     upload_to_os(os_client, json.dumps(merged, indent=2, ensure_ascii=False), merged_path)
#     _plog("Merged JSON saved â†’ %s", merged_path)

#     # â”€â”€ Mark manifest fully complete (both individual docs + top-level) â”€â”€â”€
#     update_shipment_manifest_status(os_client, safe_shp)

#     # â”€â”€ Clean up completion marker files now that merge is done â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#     # This keeps the complete/ prefix clean on retries / re-runs.
#     marker_prefix = _complete_prefix(safe_shp)
#     for marker_key in list_os_keys(os_client, marker_prefix):
#         delete_from_os(os_client, marker_key)
#     _plog("Cleaned up completion markers under %s", marker_prefix)

#     return merged, merged_path


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  OCI FUNCTIONS ENTRY POINT
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def handler(ctx, data: io.BytesIO = None):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     t_start = time.time()
#     _plog("=" * 60)
#     _plog("Shipment Merger v1.1.0 invoked")

#     body = {}
#     if data:
#         try:
#             body = json.loads(data.getvalue())
#         except Exception as exc:
#             log.warning("Body parse failed: %s", exc)

#     shipment_id = body.get("shipmentId") or body.get("shipment_id")
#     request_ids = body.get("requestIds") or body.get("request_ids") or {}

#     if not shipment_id and body.get("requestId"):
#         shipment_id = f"SHP-{body.get('requestId')}"

#     if not shipment_id:
#         msg = "Missing required field: shipmentId"
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": msg}),
#             headers={"Content-Type": "application/json"}, status_code=400,
#         )

#     os_client = get_object_storage_client()

#     if not request_ids:
#         # Fall back to reading requestIds from the shipment manifest
#         safe_shp     = _safe(shipment_id)
#         manifest_raw = download_from_os(os_client, _shipment_manifest_key(safe_shp))
#         if manifest_raw:
#             manifest    = json.loads(manifest_raw)
#             docs        = manifest.get("documents") or {}
#             request_ids = {k: v.get("requestId") for k, v in docs.items() if v.get("requestId")}
#             _plog("Loaded requestIds from shipment manifest: %s", request_ids)

#     if not request_ids:
#         msg = "Missing required field: requestIds and no shipment manifest found"
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": msg}),
#             headers={"Content-Type": "application/json"}, status_code=400,
#         )

#     try:
#         merged, merged_path = merge_shipment(os_client, shipment_id, request_ids)
#     except Exception as exc:
#         log.exception("Shipment merger failed")
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": str(exc)}),
#             headers={"Content-Type": "application/json"}, status_code=500,
#         )

#     duration = round(time.time() - t_start, 2)
#     summary  = merged.get("summary", {})

#     _plog("=" * 60)
#     _plog("MERGE COMPLETE for shipment %s", shipment_id)
#     _plog("  Supplier      : %s", summary.get("supplierName"))
#     _plog("  Invoice No    : %s", summary.get("invoiceNumber"))
#     _plog("  B/L No        : %s", summary.get("blNumber"))
#     _plog("  Port Loading  : %s", summary.get("portOfLoading"))
#     _plog("  Port Discharge: %s", summary.get("portOfDischarge"))
#     _plog("  Gross Weight  : %s kg", summary.get("totalGrossWeightKg"))
#     _plog("  CBM           : %s",    summary.get("totalCbm"))
#     _plog("  Line Items    : %s",    summary.get("totalLineItems"))
#     _plog("  Invoice Amt   : %s %s", summary.get("currency"), summary.get("totalInvoiceAmount"))
#     _plog("  Saved to      : %s", merged_path)
#     _plog("=" * 60)

#     return fdk_response.Response(
#         ctx,
#         response_data=json.dumps({
#             "status":          "complete",
#             "shipmentId":      shipment_id,
#             "mergedSavedTo":   merged_path,
#             "summary":         summary,
#             "docsLoaded":      merged["_meta"]["docsLoaded"],
#             "durationSeconds": duration,
#         }, ensure_ascii=False),
#         headers={"Content-Type": "application/json"},
#     )



"""
AI Shipment Merger  â€”  OCI FUNCTIONS v1.1.0
============================================
CHANGES vs v1.0.0:
  - update_shipment_manifest_status now marks EVERY individual document
    as complete (not just the top-level status field), so manual invocations
    and automatic invocations both produce a fully consistent manifest.
  - Cleans up completion marker files after successful merge so the
    Shipments/{id}/complete/ prefix stays tidy on retries.

ROLE: Final step â€” merge extracted JSONs (PL + Invoice + BL) for a shipment
into a single unified shipment JSON.

Can be called:
  a) Automatically when the last extractor finishes (via marker-file count)
  b) Manually at any time once all three are done

Input:
{
  "shipmentId":   "SHP-REQ-REALTIME-014",
  "requestIds": {
    "packing_list":   "PL-REQ-REALTIME-014-01",
    "invoice":        "INV-REQ-REALTIME-014-01",
    "bill_of_lading": "BL-REQ-REALTIME-014-01"
  }
}

Output (also saved to OS: Shipments/{shipmentId}/merged.json):
{
  "shipmentId":   "...",
  "mergedAt":     "...",
  "packing_list": { ...full extracted PL JSON... },
  "invoice":      { ...full extracted invoice JSON... },
  "bill_of_lading": { ...full extracted BL JSON... },
  "summary": {
    "supplierName":        "...",
    "invoiceNumber":       "...",
    "blNumber":            "...",
    "portOfLoading":       "...",
    "portOfDischarge":     "...",
    "totalGrossWeightKg":  ...,
    "totalCbm":            ...,
    "totalLineItems":      ...,
    "totalPackages":       ...,
    "currency":            "...",
    "totalInvoiceAmount":  ...
  }
}
"""

import io
import json
import logging
import os
import re
import sys
import time
from typing import Optional

import oci
from oci.auth.signers import get_resource_principals_signer
from fdk import response as fdk_response


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")
OS_NAMESPACE       = os.getenv("OS_NAMESPACE", "sample_namespace")
OS_BUCKET          = os.getenv("OS_BUCKET",    "sample-workflow-bucket")

PL_BASE_FOLDER   = os.getenv("OS_PL_BASE_FOLDER",  "Packing List")
INV_BASE_FOLDER  = os.getenv("OS_INV_BASE_FOLDER",  "Invoice")
BL_BASE_FOLDER   = os.getenv("OS_BL_BASE_FOLDER",   "Bill of Lading")
SHIPMENTS_FOLDER = os.getenv("OS_SHIPMENTS_FOLDER", "Shipments")

TMP_DIR = "/tmp/ai_shipment_merger"


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
#  HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _safe(s: str) -> str:
    return re.sub(r"[^\w\-]", "_", s) if s else "unknown"

def get_object_storage_client():
    signer = get_resource_principals_signer()
    return oci.object_storage.ObjectStorageClient(config={}, signer=signer)

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
        log.warning("Not found '%s': %s", object_name, exc)
        return None

def list_os_keys(os_client, prefix: str):
    try:
        resp = os_client.list_objects(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=prefix,
        )
        return [o.name for o in resp.data.objects]
    except Exception as exc:
        log.warning("List failed prefix='%s': %s", prefix, exc)
        return []

def delete_from_os(os_client, object_name):
    try:
        os_client.delete_object(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, object_name=object_name,
        )
        log.info("Deleted â†’ %s", object_name)
    except Exception as exc:
        log.warning("Delete failed '%s': %s", object_name, exc)

def _extracted_key(base_folder: str, safe_req: str) -> str:
    return f"{base_folder}/{safe_req}/extracted.json"

def _shipment_manifest_key(safe_shp: str) -> str:
    return f"{SHIPMENTS_FOLDER}/{safe_shp}.json"

def _merged_key(safe_shp: str) -> str:
    return f"{SHIPMENTS_FOLDER}/{safe_shp}/merged.json"

def _complete_prefix(safe_shp: str) -> str:
    return f"{SHIPMENTS_FOLDER}/{safe_shp}/complete/"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  MANIFEST STATUS UPDATE
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def update_shipment_manifest_status(os_client, safe_shp: str):
    """
    Mark the shipment manifest as fully complete:
      - sets every individual document's status â†’ 'complete'
      - sets top-level status â†’ 'complete'
      - adds completedAt timestamp

    â”€â”€ FIX vs v1.0.0: v1.0.0 only set top-level status, leaving individual
       document statuses as 'pending' when merger was invoked manually.
       Now both levels are updated consistently. â”€â”€
    """
    raw = download_from_os(os_client, _shipment_manifest_key(safe_shp))
    if not raw:
        log.warning("Shipment manifest not found at %s â€” cannot mark complete", _shipment_manifest_key(safe_shp))
        return
    try:
        manifest = json.loads(raw)

        # â”€â”€ FIX: mark every individual document complete â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        for doc_type, doc_info in manifest.get("documents", {}).items():
            if isinstance(doc_info, dict):
                doc_info["status"] = "complete"
                _plog("  Marked document '%s' â†’ complete", doc_type)

        # â”€â”€ Mark top-level status â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        manifest["status"]      = "complete"
        manifest["completedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        upload_to_os(
            os_client,
            json.dumps(manifest, indent=2, ensure_ascii=False),
            _shipment_manifest_key(safe_shp),
        )
        _plog("Shipment manifest fully marked complete")
    except Exception as exc:
        log.warning("Could not update shipment manifest: %s", exc)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  MERGE LOGIC
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _load_doc(os_client, base_folder: str, request_id: str) -> Optional[dict]:
    """Load extracted.json for a given request_id."""
    if not request_id:
        return None
    key = _extracted_key(base_folder, _safe(request_id))
    raw = download_from_os(os_client, key)
    if not raw:
        log.warning("extracted.json not found for requestId=%s at %s", request_id, key)
        return None
    try:
        return json.loads(raw)
    except Exception as exc:
        log.error("JSON parse failed for %s: %s", key, exc)
        return None

def build_summary(pl_data: Optional[dict], inv_data: Optional[dict], bl_data: Optional[dict]) -> dict:
    """Distil key cross-document fields into one summary block."""
    supplier_name  = None
    invoice_number = None
    bl_number      = None
    port_loading   = None
    port_discharge = None
    total_gw       = None
    total_cbm      = None
    total_li       = 0
    total_pkgs     = None
    currency       = None
    total_amount   = None

    if pl_data:
        total_gw       = pl_data.get("total_gross_weight_kg")
        total_cbm      = pl_data.get("total_cbm")
        total_pkgs     = pl_data.get("total_packages")
        port_loading   = pl_data.get("port_of_loading")
        port_discharge = pl_data.get("port_of_discharge")
        total_li       = len(pl_data.get("packages", []))
        shipper        = pl_data.get("shipper") or {}
        supplier_name  = shipper.get("name")

    if inv_data:
        invoice_number = inv_data.get("invoice_number")
        currency       = inv_data.get("currency")
        total_amount   = inv_data.get("total_amount")
        if not port_loading:
            port_loading   = inv_data.get("port_of_loading")
        if not port_discharge:
            port_discharge = inv_data.get("port_of_discharge")
        if not supplier_name:
            seller = inv_data.get("seller") or {}
            supplier_name = seller.get("name")
        if not total_li:
            total_li = len(inv_data.get("line_items", []))

    if bl_data:
        bl_number = bl_data.get("bl_number")
        if not port_loading:
            port_loading   = bl_data.get("port_of_loading")
        if not port_discharge:
            port_discharge = bl_data.get("port_of_discharge")
        if not total_gw:
            total_gw  = bl_data.get("total_gross_weight_kg")
        if not total_cbm:
            total_cbm = bl_data.get("total_measurement_cbm")
        if not total_pkgs:
            total_pkgs = bl_data.get("total_packages")

    return {
        "supplierName":       supplier_name,
        "invoiceNumber":      invoice_number,
        "blNumber":           bl_number,
        "portOfLoading":      port_loading,
        "portOfDischarge":    port_discharge,
        "totalGrossWeightKg": total_gw,
        "totalCbm":           total_cbm,
        "totalLineItems":     total_li,
        "totalPackages":      total_pkgs,
        "currency":           currency,
        "totalInvoiceAmount": total_amount,
    }


def merge_shipment(os_client, shipment_id: str, request_ids: dict) -> tuple:
    safe_shp = _safe(shipment_id)

    # Support multiple PLs (packing_list_01, packing_list_02) and invoices (TCL)
    pl_reqs  = [v for k, v in request_ids.items() if "packing_list" in k]
    inv_reqs = [v for k, v in request_ids.items() if k.startswith("invoice")]
    bl_req   = request_ids.get("bill_of_lading")

    # Fallback for legacy single-key format
    if not pl_reqs  and request_ids.get("packing_list"):
        pl_reqs  = [request_ids["packing_list"]]
    if not inv_reqs and request_ids.get("invoice"):
        inv_reqs = [request_ids["invoice"]]

    _plog("Loading documents for shipment %s", shipment_id)
    _plog("  PL  requestIds: %s", pl_reqs  or ["â€”"])
    _plog("  INV requestIds: %s", inv_reqs or ["â€”"])
    _plog("  BL  requestId:  %s", bl_req   or "â€”")

    # Load all PLs and merge packages
    all_pl_data = [_load_doc(os_client, PL_BASE_FOLDER, r) for r in pl_reqs if r]
    all_pl_data = [d for d in all_pl_data if d is not None]

    # Load all invoices and merge line items
    all_inv_data = [_load_doc(os_client, INV_BASE_FOLDER, r) for r in inv_reqs if r]
    all_inv_data = [d for d in all_inv_data if d is not None]

    bl_data = _load_doc(os_client, BL_BASE_FOLDER, bl_req) if bl_req else None

    # Merge multiple PLs into one
    if len(all_pl_data) == 1:
        pl_data = all_pl_data[0]
    elif len(all_pl_data) > 1:
        pl_data = all_pl_data[0].copy()
        merged_packages = []
        for pl in all_pl_data:
            merged_packages.extend(pl.get("packages", []))
        pl_data["packages"] = merged_packages
        pl_data["total_packages"] = len(merged_packages)
        _plog("Merged %d PL docs â†’ %d total packages", len(all_pl_data), len(merged_packages))
    else:
        pl_data = None

    # Merge multiple invoices into one
    if len(all_inv_data) == 1:
        inv_data = all_inv_data[0]
    elif len(all_inv_data) > 1:
        inv_data = all_inv_data[0].copy()
        merged_items = []
        total_amount = 0
        for inv in all_inv_data:
            merged_items.extend(inv.get("line_items", []))
            total_amount += inv.get("total_amount") or 0
        inv_data["line_items"] = merged_items
        inv_data["total_amount"] = total_amount
        _plog("Merged %d invoice docs â†’ %d total line items", len(all_inv_data), len(merged_items))
    else:
        inv_data = None

    docs_found = sum(1 for d in [pl_data, inv_data, bl_data] if d is not None)

    summary = build_summary(pl_data, inv_data, bl_data)

    merged = {
        "shipmentId":     shipment_id,
        "mergedAt":       time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "summary":        summary,
        "packing_list":   pl_data,
        "invoice":        inv_data,
        "bill_of_lading": bl_data,
        "_meta": {
            "requestIds":  request_ids,
            "docsLoaded":  docs_found,
            "plCount":     len(all_pl_data),
            "invCount":    len(all_inv_data),
            "folder":      f"{SHIPMENTS_FOLDER}/{safe_shp}",
        },
    }


    merged_path = _merged_key(safe_shp)
    upload_to_os(os_client, json.dumps(merged, indent=2, ensure_ascii=False), merged_path)
    _plog("Merged JSON saved â†’ %s", merged_path)

    # â”€â”€ Mark manifest fully complete (both individual docs + top-level) â”€â”€â”€
    update_shipment_manifest_status(os_client, safe_shp)

    # â”€â”€ Clean up completion marker files now that merge is done â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # This keeps the complete/ prefix clean on retries / re-runs.
    marker_prefix = _complete_prefix(safe_shp)
    for marker_key in list_os_keys(os_client, marker_prefix):
        delete_from_os(os_client, marker_key)
    _plog("Cleaned up completion markers under %s", marker_prefix)

    return merged, merged_path


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  OCI FUNCTIONS ENTRY POINT
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def handler(ctx, data: io.BytesIO = None):
    os.makedirs(TMP_DIR, exist_ok=True)
    t_start = time.time()
    _plog("=" * 60)
    _plog("Shipment Merger v1.1.0 invoked")

    body = {}
    if data:
        try:
            body = json.loads(data.getvalue())
        except Exception as exc:
            log.warning("Body parse failed: %s", exc)

    shipment_id = body.get("shipmentId") or body.get("shipment_id")
    request_ids = body.get("requestIds") or body.get("request_ids") or {}

    if not shipment_id and body.get("requestId"):
        shipment_id = f"SHP-{body.get('requestId')}"

    if not shipment_id:
        msg = "Missing required field: shipmentId"
        return fdk_response.Response(
            ctx, response_data=json.dumps({"status": "error", "message": msg}),
            headers={"Content-Type": "application/json"}, status_code=400,
        )

    os_client = get_object_storage_client()

    if not request_ids:
        # Fall back to reading requestIds from the shipment manifest
        safe_shp     = _safe(shipment_id)
        manifest_raw = download_from_os(os_client, _shipment_manifest_key(safe_shp))
        if manifest_raw:
            manifest    = json.loads(manifest_raw)
            docs        = manifest.get("documents") or {}
            request_ids = {k: v.get("requestId") for k, v in docs.items() if v.get("requestId")}
            _plog("Loaded requestIds from shipment manifest: %s", request_ids)

    if not request_ids:
        msg = "Missing required field: requestIds and no shipment manifest found"
        return fdk_response.Response(
            ctx, response_data=json.dumps({"status": "error", "message": msg}),
            headers={"Content-Type": "application/json"}, status_code=400,
        )

    try:
        merged, merged_path = merge_shipment(os_client, shipment_id, request_ids)
    except Exception as exc:
        log.exception("Shipment merger failed")
        return fdk_response.Response(
            ctx, response_data=json.dumps({"status": "error", "message": str(exc)}),
            headers={"Content-Type": "application/json"}, status_code=500,
        )

    duration = round(time.time() - t_start, 2)
    summary  = merged.get("summary", {})

    _plog("=" * 60)
    _plog("MERGE COMPLETE for shipment %s", shipment_id)
    _plog("  Supplier      : %s", summary.get("supplierName"))
    _plog("  Invoice No    : %s", summary.get("invoiceNumber"))
    _plog("  B/L No        : %s", summary.get("blNumber"))
    _plog("  Port Loading  : %s", summary.get("portOfLoading"))
    _plog("  Port Discharge: %s", summary.get("portOfDischarge"))
    _plog("  Gross Weight  : %s kg", summary.get("totalGrossWeightKg"))
    _plog("  CBM           : %s",    summary.get("totalCbm"))
    _plog("  Line Items    : %s",    summary.get("totalLineItems"))
    _plog("  Invoice Amt   : %s %s", summary.get("currency"), summary.get("totalInvoiceAmount"))
    _plog("  Saved to      : %s", merged_path)
    _plog("=" * 60)

    return fdk_response.Response(
        ctx,
        response_data=json.dumps({
            "status":          "complete",
            "shipmentId":      shipment_id,
            "mergedSavedTo":   merged_path,
            "summary":         summary,
            "docsLoaded":      merged["_meta"]["docsLoaded"],
            "durationSeconds": duration,
        }, ensure_ascii=False),
        headers={"Content-Type": "application/json"},
    )

