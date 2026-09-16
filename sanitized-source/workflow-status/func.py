import io
import json
import os
import logging

import oci
from fdk import response
from oci.auth.signers import get_resource_principals_signer

log          = logging.getLogger(__name__)
OS_NAMESPACE = os.getenv("OS_NAMESPACE", "sample_namespace")
OS_BUCKET    = os.getenv("OS_BUCKET",    "sample-workflow-bucket")


def get_os_client():
    return oci.object_storage.ObjectStorageClient(
        config={}, signer=get_resource_principals_signer()
    )


def read_object(os_client, key: str):
    try:
        raw = os_client.get_object(
            namespace_name=OS_NAMESPACE,
            bucket_name=OS_BUCKET,
            object_name=key,
        ).data.content.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def list_objects(os_client, prefix: str):
    try:
        resp = os_client.list_objects(
            namespace_name=OS_NAMESPACE,
            bucket_name=OS_BUCKET,
            prefix=prefix,
        )
        return [o.name for o in resp.data.objects]
    except Exception:
        return []


def handler(ctx, data: io.BytesIO = None):
    try:
        body = {}
        if data:
            try:
                body = json.loads(data.getvalue())
            except Exception:
                pass

        shipment_id = body.get("shipmentId")
        doc_type    = body.get("docType", "packing_list")

        if not shipment_id:
            return response.Response(
                ctx,
                response_data=json.dumps({"error": "shipmentId is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        os_client = get_os_client()

        # â”€â”€ Check shipment manifest â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        manifest = read_object(os_client, f"Shipments/{shipment_id}.json")
        if not manifest:
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "status":     "pending",
                    "shipmentId": shipment_id,
                    "message":    "Shipment not found yet â€” still processing",
                }),
                status_code=202,
                headers={"Content-Type": "application/json"},
            )

        # â”€â”€ LPN files â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if doc_type in ("lpn_putaway", "lpn_split", "lpn"):
            keys    = list_objects(os_client, f"LPN/{shipment_id}/")
            results = []
            for key in keys:
                if key.endswith(".json"):
                    obj = read_object(os_client, key)
                    if obj:
                        results.append(obj)
            if results:
                return response.Response(
                    ctx,
                    response_data=json.dumps({
                        "status":     "completed",
                        "shipmentId": shipment_id,
                        "results":    results,
                    }),
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                )
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "status":     "pending",
                    "shipmentId": shipment_id,
                    "message":    "LPN extraction in progress",
                }),
                status_code=202,
                headers={"Content-Type": "application/json"},
            )

        # â”€â”€ Packing list / invoice / BOL â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        docs     = manifest.get("documents", {})
        results  = {}
        all_done = True

        for doc_key, doc_info in docs.items():
            req_id = doc_info.get("requestId")
            dtype  = doc_key.rsplit("_", 1)[0]  # packing_list, invoice, lpn_putaway, lpn_split

            # â”€â”€ LPN types â€” saved directly in LPN/ folder â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            if dtype in ("lpn_putaway", "lpn_split"):
                keys = list_objects(os_client, f"LPN/{shipment_id}/")
                lpn_files = [k for k in keys if k.endswith(".json") and dtype in k]
                if lpn_files:
                    obj = read_object(os_client, lpn_files[0])
                    results[doc_key] = {
                        "status":    "completed",
                        "extracted": obj,
                    }
                else:
                    all_done = False
                    results[doc_key] = {
                        "status":  "pending",
                        "message": "Extraction in progress",
                    }
                continue

            # â”€â”€ Packing list / invoice / BOL â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            if dtype == "packing_list":
                folder = f"Packing List/{req_id}"
            elif dtype == "invoice":
                folder = f"Invoice/{req_id}"
            elif dtype == "bill_of_lading":
                folder = f"BL/{req_id}"
            else:
                folder = f"{dtype}/{req_id}"

            extracted = read_object(os_client, f"{folder}/extracted.json")
            otm       = read_object(os_client, f"{folder}/otm.json")

            if extracted:
                results[doc_key] = {
                    "status":    "completed",
                    "extracted": extracted,
                    "otm":       otm,
                }
            else:
                all_done = False
                results[doc_key] = {
                    "status":  "pending",
                    "message": "Extraction in progress",
                }

        return response.Response(
            ctx,
            response_data=json.dumps({
                "status":     "completed" if all_done else "pending",
                "shipmentId": shipment_id,
                "documents":  results,
            }),
            status_code=200 if all_done else 202,
            headers={"Content-Type": "application/json"},
        )

    except Exception as exc:
        log.error("Status handler error: %s", exc)
        return response.Response(
            ctx,
            response_data=json.dumps({"error": str(exc)}),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )
