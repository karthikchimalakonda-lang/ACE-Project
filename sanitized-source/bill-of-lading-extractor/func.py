# """
# AI Bill of Lading Extractor  â€”  OCI FUNCTIONS v1.0.0
# =====================================================
# ROLE: Self-chaining parallel chunk processor for Bill of Lading documents.
# Mirrors the PL extractor and Invoice extractor patterns exactly.
# """

# import concurrent.futures
# import io
# import json
# import logging
# import math
# import os
# import random
# import re
# import sys
# import time
# from typing import List, Optional, Tuple

# import oci
# from oci.auth.signers import get_resource_principals_signer
# from oci.generative_ai_inference import GenerativeAiInferenceClient
# from oci.generative_ai_inference.models import (
#     ChatDetails,
#     OnDemandServingMode,
#     CohereChatRequest,
# )
# from oci.functions.functions_invoke_client import FunctionsInvokeClient
# from fdk import response as fdk_response


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  CONFIGURATION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# OCI_GENAI_ENDPOINT = os.getenv(
#     "OCI_GENAI_ENDPOINT",
#     "https://example.invalid/integration-endpoint",
# )
# OCI_MODEL_ID = os.getenv(
#     "OCI_MODEL_ID",
#     "OCI_RESOURCE_OCID_PLACEHOLDER",
# )
# OCI_COMPARTMENT_ID = os.getenv("OCI_COMPARTMENT_ID", "")

# OS_NAMESPACE   = os.getenv("OS_NAMESPACE", "sample_namespace")
# OS_BUCKET      = os.getenv("OS_BUCKET",    "sample-workflow-bucket")
# BL_BASE_FOLDER = os.getenv("OS_BL_BASE_FOLDER", "Bill of Lading")

# MAX_TOKENS     = int(os.getenv("MAX_TOKENS",  "4096"))
# MAX_WORKERS    = int(os.getenv("MAX_WORKERS", "6"))

# LLM_MAX_RETRIES   = int(os.getenv("LLM_MAX_RETRIES",   "3"))
# LLM_RETRY_BACKOFF = float(os.getenv("LLM_RETRY_BACKOFF", "2.0"))

# EXTRACTOR_FUNCTION_ID = os.getenv("EXTRACTOR_FUNCTION_ID", "")
# EXTRACTOR_ENDPOINT    = os.getenv(
#     "EXTRACTOR_ENDPOINT",
#     "https://example.invalid/integration-endpoint",
# )

# MERGER_FUNCTION_ID = os.getenv("MERGER_FUNCTION_ID", "")
# MERGER_ENDPOINT    = os.getenv("MERGER_ENDPOINT", "https://example.invalid/integration-endpoint")
# SHIPMENTS_FOLDER   = os.getenv("SHIPMENTS_FOLDER", "Shipments")

# OTM_DOMAIN_NAME = os.getenv("OTM_DOMAIN_NAME", "SAMPLE_ORG")
# TMP_DIR = "/tmp/ai_bl_extractor"


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

# def _chunk_key(safe_req: str, chunk_idx: int) -> str:
#     return f"{_req_folder(safe_req)}/chunks/chunk_{chunk_idx}.json"

# def _manifest_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/manifest.json"

# def _csv_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/job.csv"

# def _extracted_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/extracted.json"

# def _otm_key(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/otm.json"

# def _chunk_prefix(safe_req: str) -> str:
#     return f"{_req_folder(safe_req)}/chunks/chunk_"


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 1  â€”  OCI CLIENTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def get_genai_client():
#     signer = get_resource_principals_signer()
#     return GenerativeAiInferenceClient(
#         config={}, signer=signer,
#         service_endpoint=OCI_GENAI_ENDPOINT,
#         retry_strategy=oci.retry.NoneRetryStrategy(),
#         timeout=(10, 240),
#     )

# def get_object_storage_client():
#     signer = get_resource_principals_signer()
#     return oci.object_storage.ObjectStorageClient(config={}, signer=signer)

# def get_functions_client():
#     signer = get_resource_principals_signer()
#     return FunctionsInvokeClient(
#         config={}, signer=signer,
#         service_endpoint=EXTRACTOR_ENDPOINT,
#         timeout=(10, 30),
#     )


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 2  â€”  OBJECT STORAGE HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def upload_to_os(os_client, content, object_name, content_type="application/json"):
#     body = content.encode("utf-8") if isinstance(content, str) else content
#     try:
#         os_client.put_object(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
#             object_name=object_name, put_object_body=body, content_type=content_type,
#         )
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
#         log.warning("Download failed '%s': %s", object_name, exc)
#         return None

# def list_os_keys(os_client, prefix: str) -> List[str]:
#     try:
#         resp = os_client.list_objects(
#             namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET, prefix=prefix,
#         )
#         return [o.name for o in resp.data.objects]
#     except Exception as exc:
#         log.warning("List failed prefix='%s': %s", prefix, exc)
#         return []

# def append_csv_row(os_client, csv_key: str, row: dict, fields: List[str], chunk_index: int = 2):
#     existing = "" if chunk_index == 1 else (download_from_os(os_client, csv_key) or "")
#     output = io.StringIO()
#     if not existing.strip():
#         output.write(",".join(fields) + "\n")
#     else:
#         output.write(existing)
#         if not existing.endswith("\n"):
#             output.write("\n")
#     output.write(",".join(str(row.get(f, "")) for f in fields) + "\n")
#     upload_to_os(os_client, output.getvalue(), csv_key, "text/csv")


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 3  â€”  LLM WRAPPER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def call_cohere(client, system_prompt, user_message, max_tokens=4096):
#     chat_request = CohereChatRequest(
#         message=f"{system_prompt}\n\n{user_message}",
#         max_tokens=max_tokens, temperature=0.0, frequency_penalty=0, top_p=0.75, top_k=0,
#     )
#     chat_detail = ChatDetails(
#         compartment_id=OCI_COMPARTMENT_ID,
#         serving_mode=OnDemandServingMode(model_id=OCI_MODEL_ID),
#         chat_request=chat_request,
#     )
#     return client.chat(chat_detail).data.chat_response.text

# def call_cohere_with_retry(client, system_prompt, user_message, max_tokens=4096):
#     last_exc = None
#     for attempt in range(1, LLM_MAX_RETRIES + 1):
#         try:
#             return call_cohere(client, system_prompt, user_message, max_tokens)
#         except Exception as exc:
#             last_exc = exc
#             wait = LLM_RETRY_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1)
#             if attempt < LLM_MAX_RETRIES:
#                 time.sleep(wait)
#     raise last_exc


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 4  â€”  PROMPTS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# SYSTEM_PROMPT = """\
# You are an expert logistics and trade-document parser.
# Extract ALL structured data from Bill of Lading documents with complete accuracy.
# CRITICAL RULES:
# 1. Extract ALL cargo description rows â€” do not skip any
# 2. Always respond with valid JSON only â€” no markdown fences, no prose
# 3. If a field is not found, use null"""

# BL_PAGE_PROMPT = """\
# Extract ALL fields from the Bill of Lading in this document page/chunk.

# Return JSON (no markdown fences, no prose):
# {{
#   "document_type": "bill_of_lading",
#   "bl_number": null,
#   "bl_date": null,
#   "bl_type": null,
#   "shipper": {{"name": null, "address": null, "country": null}},
#   "consignee": {{"name": null, "address": null, "country": null}},
#   "notify_party": {{"name": null, "address": null, "country": null}},
#   "also_notify": {{"name": null, "address": null, "country": null}},
#   "vessel_name": null,
#   "voyage_number": null,
#   "port_of_loading": null,
#   "port_of_discharge": null,
#   "place_of_receipt": null,
#   "place_of_delivery": null,
#   "on_board_date": null,
#   "sailing_date": null,
#   "eta": null,
#   "freight_terms": null,
#   "freight_amount": null,
#   "currency": null,
#   "number_of_originals": null,
#   "container_number": null,
#   "seal_number": null,
#   "shipping_marks": null,
#   "cargo_items": [
#     {{
#       "container_no": null,
#       "seal_no": null,
#       "marks_and_numbers": null,
#       "description": null,
#       "number_of_packages": null,
#       "package_type": null,
#       "gross_weight_kg": null,
#       "measurement_cbm": null,
#       "hs_code": null
#     }}
#   ],
#   "total_packages": null,
#   "total_gross_weight_kg": null,
#   "total_measurement_cbm": null,
#   "terms_and_conditions": null,
#   "place_of_issue": null,
#   "date_of_issue": null,
#   "carrier_name": null,
#   "carrier_signature": null,
#   "additional_notes": null
# }}

# CRITICAL:
# - Extract EVERY cargo row â€” do not skip any
# - If this chunk has no BL data, return null fields with empty cargo_items []

# Document text:
# {text}"""


# # â”€â”€ SUPPLIER-SPECIFIC BL PROMPTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# ARAMEX_BL_PROMPT = """\
# Extract ALL fields from the Aramex Air Waybill or Land Transport document below.

# CRITICAL ARAMEX/LAND RULES:
# - This is a LAND or AIR transport document â€” NOT an ocean Bill of Lading
# - Extract AWB NUMBER (Air Waybill) or TCN NUMBER instead of BOL number
# - Packages are PALLETS or BOXES â€” NOT containers
# - Carrier is Aramex
# - Applies to: SupplierD, SupplierC, SupplierB shipments

# Return JSON (no markdown fences, no prose):
# {{
#   "document_type": "airway_bill",
#   "awb_number": null,
#   "tcn_number": null,
#   "bl_number": null,
#   "bl_date": null,
#   "transport_mode": "LAND",
#   "carrier": "Aramex",
#   "shipper": {{"name": null, "address": null, "country": null}},
#   "consignee": {{"name": null, "address": null, "country": null}},
#   "notify_party": {{"name": null, "address": null, "country": null}},
#   "origin_airport_port": null,
#   "destination_airport_port": null,
#   "port_of_loading": null,
#   "port_of_discharge": null,
#   "shipment_date": null,
#   "eta": null,
#   "freight_terms": null,
#   "cargo_items": [
#     {{
#       "package_no": null,
#       "package_type": "PALLET",
#       "description": null,
#       "number_of_packages": null,
#       "gross_weight_kg": null,
#       "net_weight_kg": null,
#       "measurement_cbm": null,
#       "hs_code": null
#     }}
#   ],
#   "total_packages": null,
#   "total_gross_weight_kg": null,
#   "total_net_weight_kg": null,
#   "total_measurement_cbm": null,
#   "additional_notes": null
# }}

# CRITICAL:
# - awb_number or tcn_number must be extracted â€” it replaces bl_number
# - No container numbers â€” these are pallets/boxes
# - Extract every cargo row

# Document text:
# {text}"""


# def get_bl_prompt(supplier_name: str, text: str) -> str:
#     """Select supplier-specific BL prompt."""
#     s = (supplier_name or "").upper()
#     if "SupplierD"    in s: return ARAMEX_BL_PROMPT.format(text=text)
#     if "SupplierC" in s: return ARAMEX_BL_PROMPT.format(text=text)
#     if "SupplierB"  in s: return ARAMEX_BL_PROMPT.format(text=text)
#     return BL_PAGE_PROMPT.format(text=text)  # SupplierA, TCL, WEG (ocean)

# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 5  â€”  JSON HELPERS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def parse_json_response(raw: str) -> dict:
#     cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
#     try:
#         return json.loads(cleaned)
#     except json.JSONDecodeError:
#         pass
#     match = re.search(r"\{.*\}", cleaned, re.DOTALL)
#     candidate = match.group() if match else cleaned
#     if match:
#         try:
#             return json.loads(candidate)
#         except json.JSONDecodeError:
#             pass
#     repaired = _repair_json(candidate)
#     try:
#         result = json.loads(repaired)
#         result["_truncated"] = True
#         return result
#     except json.JSONDecodeError:
#         return {"raw_response": raw, "parse_error": True}

# def _repair_json(s: str) -> str:
#     s = re.sub(r",\s*$", "", s.strip())
#     if s.count('"') % 2 != 0:
#         s += '"'
#     s = re.sub(r",\s*$", "", s.strip())
#     stack, in_string, esc = [], False, False
#     for ch in s:
#         if esc:
#             esc = False; continue
#         if ch == "\\" and in_string:
#             esc = True; continue
#         if ch == '"':
#             in_string = not in_string; continue
#         if in_string:
#             continue
#         if ch == "{": stack.append("}")
#         elif ch == "[": stack.append("]")
#         elif ch in ("}", "]"):
#             if stack and stack[-1] == ch:
#                 stack.pop()
#     s += "".join(reversed(stack))
#     return s

# def sanitize_floats(obj):
#     if isinstance(obj, dict):
#         return {k: sanitize_floats(v) for k, v in obj.items()}
#     elif isinstance(obj, list):
#         return [sanitize_floats(v) for v in obj]
#     elif isinstance(obj, float):
#         return None if (math.isnan(obj) or math.isinf(obj)) else obj
#     return obj

# def deduplicate_cargo_items(items: List[dict]) -> List[dict]:
#     seen, unique = set(), []
#     for item in items:
#         key = (
#             str(item.get("container_no",     "") or ""),
#             str(item.get("description",      "") or "")[:40],
#             str(item.get("gross_weight_kg",  "") or ""),
#         )
#         if key == ("", "", ""):
#             continue
#         if key not in seen:
#             seen.add(key)
#             unique.append(item)
#     return unique


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 6  â€”  PARALLEL LLM EXTRACTION
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# BL_TOTAL_FIELDS = ["total_packages", "total_gross_weight_kg", "total_measurement_cbm"]

# def extract_pages_parallel(client, pages_text: List[str], supplier_name: str = "") -> dict:
#     total_calls    = len(pages_text)
#     actual_workers = min(total_calls, MAX_WORKERS)
#     _plog("BL parallel extraction: %d pages, %d workers", total_calls, actual_workers)


#     def _call_one(args: Tuple[int, str]) -> Tuple[int, dict]:
#         page_idx, text = args
#         # prompt = BL_PAGE_PROMPT.format(text=text)
#         prompt = get_bl_prompt(supplier_name, text)
#         log.info("  [PAR] BL Page %d firing", page_idx)
#         try:
#             raw    = call_cohere_with_retry(client, SYSTEM_PROMPT, prompt, MAX_TOKENS)
#             parsed = parse_json_response(raw)
#         except Exception as exc:
#             log.error("  [PAR] BL Page %d FAILED: %s", page_idx, exc)
#             parsed = {"cargo_items": [], "_error": str(exc)}
#         _plog("  [PAR] BL Page %d complete â€” %d cargo rows", page_idx, len(parsed.get("cargo_items", [])))
#         return page_idx, parsed

#     work_items = [(i + 1, text) for i, text in enumerate(pages_text)]
#     raw_results: List[Tuple[int, dict]] = []

#     with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
#         future_map = {executor.submit(_call_one, item): item for item in work_items}
#         for future in concurrent.futures.as_completed(future_map):
#             try:
#                 raw_results.append(future.result())
#             except Exception as exc:
#                 item = future_map[future]
#                 log.error("  [PAR] BL Page %d future raised: %s", item[0], exc)
#                 raw_results.append((item[0], {"cargo_items": []}))

#     raw_results.sort(key=lambda x: x[0])

#     all_cargo: List[dict] = []
#     totals_found = {f: None for f in BL_TOTAL_FIELDS}
#     base_result: dict = {}

#     for _, parsed in raw_results:
#         if not base_result:
#             base_result = {k: v for k, v in parsed.items() if k != "cargo_items"}
#         all_cargo.extend(parsed.get("cargo_items", []))
#         for field in BL_TOTAL_FIELDS:
#             val = parsed.get(field)
#             if val is not None:
#                 totals_found[field] = val

#     deduped = deduplicate_cargo_items(all_cargo)
#     base_result["cargo_items"] = deduped
#     for field, val in totals_found.items():
#         if val is not None:
#             base_result[field] = val

#     return base_result


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 7  â€”  MERGE ALL CHUNKS
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def merge_all_chunks(os_client, safe_req: str, total_chunks: int) -> dict:
#     all_cargo: List[dict] = []
#     totals_found = {f: None for f in BL_TOTAL_FIELDS}
#     base_result: dict = {}

#     for chunk_idx in range(1, total_chunks + 1):
#         chunk_data = download_from_os(os_client, _chunk_key(safe_req, chunk_idx))
#         if not chunk_data:
#             log.warning("  [MERGE] BL Chunk %d missing", chunk_idx)
#             continue
#         try:
#             parsed = json.loads(chunk_data)
#         except Exception as exc:
#             log.error("  [MERGE] BL Chunk %d parse failed: %s", chunk_idx, exc)
#             continue
#         if not base_result:
#             base_result = {k: v for k, v in parsed.items() if k != "cargo_items"}
#         all_cargo.extend(parsed.get("cargo_items", []))
#         for field in BL_TOTAL_FIELDS:
#             val = parsed.get(field)
#             if val is not None:
#                 totals_found[field] = val

#     deduped = deduplicate_cargo_items(all_cargo)
#     if not base_result:
#         base_result = {}
#     base_result["cargo_items"] = deduped

#     # Fallback totals
#     if not totals_found.get("total_gross_weight_kg"):
#         s = sum(c.get("gross_weight_kg") or 0 for c in deduped if c.get("gross_weight_kg"))
#         if s > 0:
#             totals_found["total_gross_weight_kg"] = round(s, 3)
#     if not totals_found.get("total_measurement_cbm"):
#         s = sum(c.get("measurement_cbm") or 0 for c in deduped if c.get("measurement_cbm"))
#         if s > 0:
#             totals_found["total_measurement_cbm"] = round(s, 3)

#     for field, val in totals_found.items():
#         if val is not None:
#             base_result[field] = val

#     return base_result


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 8  â€”  OTM PAYLOAD BUILDER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def build_otm_bl_payload(bl_data: dict, request_id: str,
#                           order_base_gid: str, ob_ship_unit_gid: str) -> dict:
#     cargo_items = bl_data.get("cargo_items") or []
#     otm_cargo   = []
#     for seq, item in enumerate(cargo_items, start=1):
#         otm_cargo.append({
#             "sequenceNo":     seq,
#             "containerNo":    item.get("container_no"),
#             "sealNo":         item.get("seal_no"),
#             "description":    item.get("description"),
#             "packages":       item.get("number_of_packages"),
#             "packageType":    item.get("package_type"),
#             "grossWeightKg":  item.get("gross_weight_kg"),
#             "measurementCbm": item.get("measurement_cbm"),
#             "hsCode":         item.get("hs_code"),
#             "domainName":     OTM_DOMAIN_NAME,
#         })

#     return {
#         "referenceTransmissionNo": request_id,
#         "senderTransmissionId":    request_id,
#         "documentType":            "bill_of_lading",
#         "transactions": {"items": [{
#             "contentType": "application/vnd.oracle.resource+json;type=singular",
#             "httpMethod":  "PATCH",
#             "resourceUrl": f"orderBases/{order_base_gid}/billsOfLading/{request_id}",
#             "body": {
#                 "orderBaseGid":         order_base_gid,
#                 "obShipUnitGid":        ob_ship_unit_gid,
#                 "domainName":           OTM_DOMAIN_NAME,
#                 "blNumber":             bl_data.get("bl_number"),
#                 "blDate":               bl_data.get("bl_date"),
#                 "blType":               bl_data.get("bl_type"),
#                 "vesselName":           bl_data.get("vessel_name"),
#                 "voyageNumber":         bl_data.get("voyage_number"),
#                 "portOfLoading":        bl_data.get("port_of_loading"),
#                 "portOfDischarge":      bl_data.get("port_of_discharge"),
#                 "placeOfReceipt":       bl_data.get("place_of_receipt"),
#                 "onBoardDate":          bl_data.get("on_board_date"),
#                 "freightTerms":         bl_data.get("freight_terms"),
#                 "containerNumber":      bl_data.get("container_number"),
#                 "totalPackages":        bl_data.get("total_packages"),
#                 "totalGrossWeightKg":   bl_data.get("total_gross_weight_kg"),
#                 "totalMeasurementCbm":  bl_data.get("total_measurement_cbm"),
#                 "cargoItems":           {"items": otm_cargo},
#             },
#         }]},
#     }


# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 9  â€”  SELF-CHAIN
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def fire_next_extractor(fn_client, manifest_chunk: dict, request_id: str, total_chunks: int,
#                         order_base_gid: str, ob_ship_unit_gid: str,
#                         shipment_id: str, supplier_name: str):
#     payload = {
#         "requestId":     request_id,
#         "chunkIndex":    manifest_chunk["chunkIndex"],
#         "totalChunks":   total_chunks,
#         "pages":         manifest_chunk["pages"],
#         "orderBaseGid":  order_base_gid,
#         "obShipUnitGid": ob_ship_unit_gid,
#         "shipmentId":    shipment_id,
#         "supplierName":  supplier_name,
#     }
#     fn_client.invoke_function(
#         function_id=EXTRACTOR_FUNCTION_ID,
#         invoke_function_body=json.dumps(payload).encode("utf-8"),
#         fn_invoke_type="detached",
#     )
#     _plog("[CHAIN] Fired BL chunk %d/%d (detached)", manifest_chunk["chunkIndex"], total_chunks)



# def _check_and_fire_merger(os_client, shipment_id: str, request_id: str, doc_type: str):
#     """
#     Race-condition-free completion check using per-document marker files.
#     Each extractor writes its own marker â€” no shared-file contention.
#     The last extractor to arrive (marker count == total expected) fires the merger.
#     """
#     if not shipment_id or not MERGER_FUNCTION_ID:
#         log.info("Merger check skipped â€” no shipmentId or MERGER_FUNCTION_ID")
#         return

#     safe_shp      = re.sub(r"[^\w\-]", "_", shipment_id)
#     manifest_key  = f"{SHIPMENTS_FOLDER}/{safe_shp}.json"
#     complete_prefix = f"{SHIPMENTS_FOLDER}/{safe_shp}/complete/"
#     marker_key    = f"{complete_prefix}{doc_type}.json"

#     # â”€â”€ Step 1: wait for shipment manifest to exist â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#     manifest_raw = None
#     for attempt in range(10):
#         manifest_raw = download_from_os(os_client, manifest_key)
#         if manifest_raw:
#             break
#         wait = 1.5 * (attempt + 1)
#         _plog("Manifest not ready â€” waiting %.0fs (attempt %d/10)", wait, attempt + 1)
#         time.sleep(wait)

#     if not manifest_raw:
#         log.error("Shipment manifest never appeared for %s â€” giving up", shipment_id)
#         return

#     try:
#         manifest       = json.loads(manifest_raw)
#         total_expected = len(manifest.get("documents", {}))
#     except Exception as exc:
#         log.error("Failed to parse shipment manifest: %s", exc)
#         return

#     if total_expected == 0:
#         log.error("Manifest has no documents entry â€” cannot determine total")
#         return

#     # â”€â”€ Step 2: write THIS extractor's marker (atomic, no contention) â”€â”€â”€â”€
#     marker_payload = json.dumps({
#         "docType":   doc_type,
#         "requestId": request_id,
#         "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "shipmentId": shipment_id,
#     })

#     for attempt in range(3):
#         if upload_to_os(os_client, marker_payload, marker_key):
#             _plog("Marker written â†’ %s", marker_key)
#             break
#         log.warning("Marker write attempt %d/3 failed â€” retrying", attempt + 1)
#         time.sleep(1)
#     else:
#         log.error("Failed to write completion marker for %s â€” giving up", doc_type)
#         return

#     # â”€â”€ Step 3: small jitter to let other near-simultaneous writes land â”€â”€
#     time.sleep(1.0 + random.uniform(0, 1.0))

#     # â”€â”€ Step 4: count markers (each doc type has its own file â€” no race) â”€
#     existing_markers = list_os_keys(os_client, complete_prefix)
#     complete_count   = len(existing_markers)
#     _plog("Completion markers: %d / %d â€” %s",
#           complete_count, total_expected,
#           [m.split("/")[-1].replace(".json","") for m in existing_markers])

#     if complete_count < total_expected:
#         _plog("Not all docs done yet â€” merger will be triggered by last extractor")
#         return

#     # â”€â”€ Step 5: we are the last â€” update manifest status then fire merger â”€
#     _plog("All %d docs complete â€” updating manifest and firing merger", total_expected)

#     # Read all markers to build the complete requestIds map
#     request_ids = {}
#     for marker_path in existing_markers:
#         try:
#             raw = download_from_os(os_client, marker_path)
#             if raw:
#                 data = json.loads(raw)
#                 request_ids[data["docType"]] = data["requestId"]
#         except Exception as exc:
#             log.warning("Failed to read marker %s: %s", marker_path, exc)

#     # Update manifest to complete
#     try:
#         manifest_raw = download_from_os(os_client, manifest_key)
#         manifest     = json.loads(manifest_raw)
#         for doc_type_key, doc_info in manifest.get("documents", {}).items():
#             if doc_type_key in request_ids:
#                 doc_info["status"] = "complete"
#         manifest["status"] = "complete"
#         upload_to_os(
#             os_client,
#             json.dumps(manifest, indent=2, ensure_ascii=False),
#             manifest_key,
#         )
#         _plog("Manifest updated to complete")
#     except Exception as exc:
#         log.error("Failed to update manifest to complete: %s", exc)

#     # Fire merger
#     try:
#         signer    = get_resource_principals_signer()
#         fn_client = FunctionsInvokeClient(
#             config={}, signer=signer,
#             service_endpoint=MERGER_ENDPOINT,
#             timeout=(10, 240),
#         )
#         fn_client.invoke_function(
#             function_id=MERGER_FUNCTION_ID,
#             invoke_function_body=json.dumps({
#                 "shipmentId": shipment_id,
#                 "requestIds": request_ids,
#             }).encode("utf-8"),
#             fn_invoke_type="detached",
#         )
#         _plog("Merger fired for shipment %s with docs: %s",
#               shipment_id, list(request_ids.keys()))
#     except Exception as exc:
#         log.error("Failed to fire merger: %s", exc)



# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# #  SECTION 10  â€”  MAIN HANDLER
# # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# def handler(ctx, data: io.BytesIO = None):
#     os.makedirs(TMP_DIR, exist_ok=True)
#     t_start = time.time()
#     _plog("=" * 60)
#     _plog("BL Extractor v1.0.0 invoked")

#     if not OCI_COMPARTMENT_ID:
#         msg = "OCI_COMPARTMENT_ID env var not set."
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": msg}),
#             headers={"Content-Type": "application/json"}, status_code=500,
#         )

#     body = {}
#     if data:
#         try:
#             body = json.loads(data.getvalue())
#         except Exception as exc:
#             log.warning("Body parse failed: %s", exc)

#     request_id       = body.get("requestId",    "unknown")
#     chunk_index      = int(body.get("chunkIndex",  1))
#     total_chunks     = int(body.get("totalChunks", 1))
#     pages            = body.get("pages",        [])
#     order_base_gid   = body.get("orderBaseGid",  "")
#     ob_ship_unit_gid = body.get("obShipUnitGid", "")
#     shipment_id      = body.get("shipmentId",    "")
#     supplier_name    = body.get("supplierName",  "")

#     safe_req = _safe(request_id) or "unknown"
#     _plog("requestId=%s  chunk=%d/%d  pages=%d", request_id, chunk_index, total_chunks, len(pages))

#     if not pages:
#         return fdk_response.Response(
#             ctx, response_data=json.dumps({"status": "error", "message": "No pages provided"}),
#             headers={"Content-Type": "application/json"}, status_code=400,
#         )

#     os_client = get_object_storage_client()

#     try:
#         genai_client = get_genai_client()
#         # chunk_result = extract_pages_parallel(client=genai_client, pages_text=pages)
#         chunk_result = extract_pages_parallel(
#             client=genai_client, pages_text=pages, supplier_name=supplier_name,
#         )
#         chunk_result.update({
#             "chunkIndex": chunk_index, "totalChunks": total_chunks,
#             "requestId": request_id, "pagesCount": len(pages),
#         })
#         status, err_msg = "SUCCESS", ""
#     except Exception as exc:
#         log.exception("BL extractor chunk %d failed", chunk_index)
#         chunk_result = {
#             "cargo_items": [], "chunkIndex": chunk_index,
#             "totalChunks": total_chunks, "requestId": request_id,
#             "pagesCount": len(pages), "_error": str(exc),
#         }
#         status, err_msg = "FAILED", str(exc)

#     chunk_result = sanitize_floats(chunk_result)
#     duration = round(time.time() - t_start, 2)
#     n_items  = len(chunk_result.get("cargo_items", []))
#     _plog("Chunk %d/%d done in %.1fs â€” %d cargo rows", chunk_index, total_chunks, duration, n_items)

#     # upload_to_os(os_client, json.dumps(chunk_result, ensure_ascii=False),
#     #              _chunk_key(safe_req, chunk_index))

#     # csv_fields = ["chunk_no", "pages_in_chunk", "status", "completed_at",
#     #               "duration_s", "cargo_rows_found", "error"]

#     for _up in range(3):
#         if upload_to_os(os_client, json.dumps(chunk_result, ensure_ascii=False),
#                         _chunk_key(safe_req, chunk_index)):
#             break
#         _plog("Chunk upload attempt %d/3 failed â€” retrying", _up + 1)
#         time.sleep(2 ** _up)
#     else:
#         log.error("All chunk upload attempts failed for chunk %d/%d", chunk_index, total_chunks)
#         _check_and_fire_merger(os_client, shipment_id, request_id, "bill_of_lading")
#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({"status": "error", "requestId": request_id,
#                                       "error": "chunk upload failed after 3 attempts"}),
#             headers={"Content-Type": "application/json"},
#             status_code=500,
#         )

#     csv_fields = ["chunk_no", "pages_in_chunk", "status", "completed_at",
#                   "duration_s", "cargo_rows_found", "error"]

#     append_csv_row(os_client, _csv_key(safe_req), {
#         "chunk_no":        chunk_index, "pages_in_chunk":  len(pages),
#         "status":          status, "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#         "duration_s":      duration, "cargo_rows_found":  n_items, "error": err_msg,
#     }, csv_fields, chunk_index=chunk_index)

#     completed_chunks = len(list_os_keys(os_client, _chunk_prefix(safe_req)))
#     _plog("Chunks completed: %d / %d", completed_chunks, total_chunks)

#     if completed_chunks < total_chunks:
#         next_idx      = chunk_index + 1
#         manifest_data = download_from_os(os_client, _manifest_key(safe_req))
#         if manifest_data:
#             try:
#                 manifest       = json.loads(manifest_data)
#                 next_chunk_def = next(
#                     (c for c in manifest["chunks"] if c["chunkIndex"] == next_idx), None,
#                 )
#                 if next_chunk_def:
#                     fn_client = get_functions_client()
#                     fire_next_extractor(
#                         fn_client, next_chunk_def, request_id, total_chunks,
#                         order_base_gid  or manifest.get("orderBaseGid",  ""),
#                         ob_ship_unit_gid or manifest.get("obShipUnitGid", ""),
#                         shipment_id     or manifest.get("shipmentId",    ""),
#                         supplier_name   or manifest.get("supplierName",  ""),
#                     )
#             except Exception as exc:
#                 log.error("Failed to fire next BL chunk: %s", exc)

#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({
#                 "status": "chunk_complete", "requestId": request_id,
#                 "chunkIndex": chunk_index, "totalChunks": total_chunks,
#                 "cargoRowsThisChunk": n_items, "completedChunks": completed_chunks,
#                 "nextChunkFired": chunk_index + 1, "durationSeconds": duration,
#             }, ensure_ascii=False),
#             headers={"Content-Type": "application/json"},
#         )

#     else:
#         _plog("All %d chunks complete â€” merging BL", total_chunks)
#         bl_data = merge_all_chunks(os_client, safe_req, total_chunks)
#         bl_data["document_type"] = "bill_of_lading"

#         manifest_data = download_from_os(os_client, _manifest_key(safe_req))
#         manifest      = json.loads(manifest_data) if manifest_data else {}

#         bl_data["_meta"] = {
#             "requestId":    request_id,
#             "filename":     manifest.get("filename", ""),
#             "shipmentId":   shipment_id or manifest.get("shipmentId", ""),
#             "supplierName": supplier_name or manifest.get("supplierName", ""),
#             "num_chunks":   total_chunks,
#             "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#             "folder":       _req_folder(safe_req),
#         }

#         upload_to_os(os_client,
#                      json.dumps(sanitize_floats(bl_data), indent=2, ensure_ascii=False),
#                      _extracted_key(safe_req))

#         _order_base_gid   = order_base_gid  or manifest.get("orderBaseGid",  "")
#         _ob_ship_unit_gid = ob_ship_unit_gid or manifest.get("obShipUnitGid", "")
#         otm_payload = build_otm_bl_payload(bl_data, request_id, _order_base_gid, _ob_ship_unit_gid)
#         upload_to_os(os_client,
#                      json.dumps(sanitize_floats(otm_payload), indent=2, ensure_ascii=False),
#                      _otm_key(safe_req))

#         _plog("=" * 60)
#         _plog("BL PIPELINE COMPLETE")
#         _plog("  Cargo rows : %d", len(bl_data.get("cargo_items", [])))
#         _plog("  Gross wt   : %s kg", bl_data.get("total_gross_weight_kg"))
#         _plog("  CBM        : %s", bl_data.get("total_measurement_cbm"))
#         _plog("  Folder     : %s", _req_folder(safe_req))
#         _plog("=" * 60)

#         # _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id)
#         # _check_and_fire_merger(os_client, shipment_id, request_id)
#         _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id, "bill_of_lading")


#         return fdk_response.Response(
#             ctx,
#             response_data=json.dumps({
#                 "status":               "complete",
#                 "requestId":            request_id,
#                 "documentType":         "bill_of_lading",
#                 "totalChunks":          total_chunks,
#                 "cargoRowsExtracted":   len(bl_data.get("cargo_items", [])),
#                 "totalGrossWeightKg":   bl_data.get("total_gross_weight_kg"),
#                 "totalCbm":             bl_data.get("total_measurement_cbm"),
#                 "folder":               _req_folder(safe_req),
#                 "extractedJsonSavedTo": _extracted_key(safe_req),
#                 "otmSavedTo":           _otm_key(safe_req),
#                 "durationSeconds":      duration,
#             }, ensure_ascii=False),
#             headers={"Content-Type": "application/json"},
#         )








"""
AI Bill of Lading Extractor  â€”  OCI FUNCTIONS v1.0.0
=====================================================
ROLE: Self-chaining parallel chunk processor for Bill of Lading documents.
Mirrors the PL extractor and Invoice extractor patterns exactly.
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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

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
BL_BASE_FOLDER = os.getenv("OS_BL_BASE_FOLDER", "Bill of Lading")

MAX_TOKENS     = int(os.getenv("MAX_TOKENS",  "4096"))
MAX_WORKERS    = int(os.getenv("MAX_WORKERS", "6"))

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
TMP_DIR = "/tmp/ai_bl_extractor"


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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 1  â€”  OCI CLIENTS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def get_genai_client():
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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 2  â€”  OBJECT STORAGE HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def upload_to_os(os_client, content, object_name, content_type="application/json"):
    body = content.encode("utf-8") if isinstance(content, str) else content
    try:
        os_client.put_object(
            namespace_name=OS_NAMESPACE, bucket_name=OS_BUCKET,
            object_name=object_name, put_object_body=body, content_type=content_type,
        )
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
    existing = "" if chunk_index == 1 else (download_from_os(os_client, csv_key) or "")
    output = io.StringIO()
    if not existing.strip():
        output.write(",".join(fields) + "\n")
    else:
        output.write(existing)
        if not existing.endswith("\n"):
            output.write("\n")
    output.write(",".join(str(row.get(f, "")) for f in fields) + "\n")
    upload_to_os(os_client, output.getvalue(), csv_key, "text/csv")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 3  â€”  LLM WRAPPER
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def call_cohere(client, system_prompt, user_message, max_tokens=4096):
    chat_request = CohereChatRequest(
        message=f"{system_prompt}\n\n{user_message}",
        max_tokens=max_tokens, temperature=0.0, frequency_penalty=0, top_p=0.75, top_k=0,
    )
    chat_detail = ChatDetails(
        compartment_id=OCI_COMPARTMENT_ID,
        serving_mode=OnDemandServingMode(model_id=OCI_MODEL_ID),
        chat_request=chat_request,
    )
    return client.chat(chat_detail).data.chat_response.text

def call_cohere_with_retry(client, system_prompt, user_message, max_tokens=4096):
    last_exc = None
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            return call_cohere(client, system_prompt, user_message, max_tokens)
        except Exception as exc:
            last_exc = exc
            wait = LLM_RETRY_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1)
            if attempt < LLM_MAX_RETRIES:
                time.sleep(wait)
    raise last_exc


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 4  â€”  PROMPTS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

SYSTEM_PROMPT = """\
You are an expert logistics and trade-document parser.
Extract ALL structured data from Bill of Lading documents with complete accuracy.
CRITICAL RULES:
1. Extract ALL cargo description rows â€” do not skip any
2. Always respond with valid JSON only â€” no markdown fences, no prose
3. If a field is not found, use null"""

BL_PAGE_PROMPT = """\
Extract ALL fields from the Bill of Lading in this document page/chunk.

Return JSON (no markdown fences, no prose):
{{
  "document_type": "bill_of_lading",
  "bl_number": null,
  "bl_date": null,
  "bl_type": null,
  "shipper": {{"name": null, "address": null, "country": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "notify_party": {{"name": null, "address": null, "country": null}},
  "also_notify": {{"name": null, "address": null, "country": null}},
  "vessel_name": null,
  "voyage_number": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "place_of_receipt": null,
  "place_of_delivery": null,
  "on_board_date": null,
  "sailing_date": null,
  "eta": null,
  "freight_terms": null,
  "freight_amount": null,
  "currency": null,
  "number_of_originals": null,
  "container_number": null,
  "seal_number": null,
  "shipping_marks": null,
  "cargo_items": [
    {{
      "container_no": null,
      "seal_no": null,
      "marks_and_numbers": null,
      "description": null,
      "number_of_packages": null,
      "package_type": null,
      "gross_weight_kg": null,
      "measurement_cbm": null,
      "hs_code": null
    }}
  ],
  "total_packages": null,
  "total_gross_weight_kg": null,
  "total_measurement_cbm": null,
  "terms_and_conditions": null,
  "place_of_issue": null,
  "date_of_issue": null,
  "carrier_name": null,
  "carrier_signature": null,
  "additional_notes": null
}}

CRITICAL:
- Extract EVERY cargo row â€” do not skip any
- If this chunk has no BL data, return null fields with empty cargo_items []

Document text:
{text}"""


# â”€â”€ SUPPLIER-SPECIFIC BL PROMPTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

ARAMEX_BL_PROMPT = """\
Extract ALL fields from the Aramex Air Waybill or Land Transport document below.

CRITICAL ARAMEX/LAND RULES:
- This is a LAND or AIR transport document â€” NOT an ocean Bill of Lading
- Extract AWB NUMBER (Air Waybill) or TCN NUMBER instead of BOL number
- Packages are PALLETS or BOXES â€” NOT containers
- Carrier is Aramex
- Applies to: SupplierD, SupplierC, SupplierB shipments

Return JSON (no markdown fences, no prose):
{{
  "document_type": "airway_bill",
  "awb_number": null,
  "tcn_number": null,
  "bl_number": null,
  "bl_date": null,
  "transport_mode": "LAND",
  "carrier": "Aramex",
  "shipper": {{"name": null, "address": null, "country": null}},
  "consignee": {{"name": null, "address": null, "country": null}},
  "notify_party": {{"name": null, "address": null, "country": null}},
  "origin_airport_port": null,
  "destination_airport_port": null,
  "port_of_loading": null,
  "port_of_discharge": null,
  "shipment_date": null,
  "eta": null,
  "freight_terms": null,
  "cargo_items": [
    {{
      "package_no": null,
      "package_type": "PALLET",
      "description": null,
      "number_of_packages": null,
      "gross_weight_kg": null,
      "net_weight_kg": null,
      "measurement_cbm": null,
      "hs_code": null
    }}
  ],
  "total_packages": null,
  "total_gross_weight_kg": null,
  "total_net_weight_kg": null,
  "total_measurement_cbm": null,
  "additional_notes": null
}}

CRITICAL:
- awb_number or tcn_number must be extracted â€” it replaces bl_number
- No container numbers â€” these are pallets/boxes
- Extract every cargo row

Document text:
{text}"""


def get_bl_prompt(supplier_name: str, text: str) -> str:
    """Select supplier-specific BL prompt."""
    s = (supplier_name or "").upper()
    if "SupplierD"    in s: return ARAMEX_BL_PROMPT.format(text=text)
    if "SupplierC" in s: return ARAMEX_BL_PROMPT.format(text=text)
    if "SupplierB"  in s: return ARAMEX_BL_PROMPT.format(text=text)
    return BL_PAGE_PROMPT.format(text=text)  # SupplierA, TCL, WEG (ocean)

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 5  â€”  JSON HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def parse_json_response(raw: str) -> dict:
    cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    candidate = match.group() if match else cleaned
    if match:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    repaired = _repair_json(candidate)
    try:
        result = json.loads(repaired)
        result["_truncated"] = True
        return result
    except json.JSONDecodeError:
        return {"raw_response": raw, "parse_error": True}

def _repair_json(s: str) -> str:
    s = re.sub(r",\s*$", "", s.strip())
    if s.count('"') % 2 != 0:
        s += '"'
    s = re.sub(r",\s*$", "", s.strip())
    stack, in_string, esc = [], False, False
    for ch in s:
        if esc:
            esc = False; continue
        if ch == "\\" and in_string:
            esc = True; continue
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

def deduplicate_cargo_items(items: List[dict]) -> List[dict]:
    seen, unique = set(), []
    for item in items:
        key = (
            str(item.get("container_no",     "") or ""),
            str(item.get("description",      "") or "")[:40],
            str(item.get("gross_weight_kg",  "") or ""),
        )
        if key == ("", "", ""):
            continue
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 6  â€”  PARALLEL LLM EXTRACTION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

BL_TOTAL_FIELDS = ["total_packages", "total_gross_weight_kg", "total_measurement_cbm"]

def extract_pages_parallel(client, pages_text: List[str], supplier_name: str = "") -> dict:
    total_calls    = len(pages_text)
    actual_workers = min(total_calls, MAX_WORKERS)
    _plog("BL parallel extraction: %d pages, %d workers", total_calls, actual_workers)


    def _call_one(args: Tuple[int, str]) -> Tuple[int, dict]:
        page_idx, text = args
        # prompt = BL_PAGE_PROMPT.format(text=text)
        prompt = get_bl_prompt(supplier_name, text)
        log.info("  [PAR] BL Page %d firing", page_idx)
        try:
            raw    = call_cohere_with_retry(client, SYSTEM_PROMPT, prompt, MAX_TOKENS)
            parsed = parse_json_response(raw)
        except Exception as exc:
            log.error("  [PAR] BL Page %d FAILED: %s", page_idx, exc)
            parsed = {"cargo_items": [], "_error": str(exc)}
        _plog("  [PAR] BL Page %d complete â€” %d cargo rows", page_idx, len(parsed.get("cargo_items", [])))
        return page_idx, parsed

    work_items = [(i + 1, text) for i, text in enumerate(pages_text)]
    raw_results: List[Tuple[int, dict]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
        future_map = {executor.submit(_call_one, item): item for item in work_items}
        for future in concurrent.futures.as_completed(future_map):
            try:
                raw_results.append(future.result())
            except Exception as exc:
                item = future_map[future]
                log.error("  [PAR] BL Page %d future raised: %s", item[0], exc)
                raw_results.append((item[0], {"cargo_items": []}))

    raw_results.sort(key=lambda x: x[0])

    all_cargo: List[dict] = []
    totals_found = {f: None for f in BL_TOTAL_FIELDS}
    base_result: dict = {}

    for _, parsed in raw_results:
        if not base_result:
            base_result = {k: v for k, v in parsed.items() if k != "cargo_items"}
        all_cargo.extend(parsed.get("cargo_items", []))
        for field in BL_TOTAL_FIELDS:
            val = parsed.get(field)
            if val is not None:
                totals_found[field] = val

    deduped = deduplicate_cargo_items(all_cargo)
    base_result["cargo_items"] = deduped
    for field, val in totals_found.items():
        if val is not None:
            base_result[field] = val

    return base_result


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 7  â€”  MERGE ALL CHUNKS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def merge_all_chunks(os_client, safe_req: str, total_chunks: int) -> dict:
    all_cargo: List[dict] = []
    totals_found = {f: None for f in BL_TOTAL_FIELDS}
    base_result: dict = {}

    for chunk_idx in range(1, total_chunks + 1):
        chunk_data = download_from_os(os_client, _chunk_key(safe_req, chunk_idx))
        if not chunk_data:
            log.warning("  [MERGE] BL Chunk %d missing", chunk_idx)
            continue
        try:
            parsed = json.loads(chunk_data)
        except Exception as exc:
            log.error("  [MERGE] BL Chunk %d parse failed: %s", chunk_idx, exc)
            continue
        if not base_result:
            base_result = {k: v for k, v in parsed.items() if k != "cargo_items"}
        all_cargo.extend(parsed.get("cargo_items", []))
        for field in BL_TOTAL_FIELDS:
            val = parsed.get(field)
            if val is not None:
                totals_found[field] = val

    deduped = deduplicate_cargo_items(all_cargo)
    if not base_result:
        base_result = {}
    base_result["cargo_items"] = deduped

    # Fallback totals
    if not totals_found.get("total_gross_weight_kg"):
        s = sum(c.get("gross_weight_kg") or 0 for c in deduped if c.get("gross_weight_kg"))
        if s > 0:
            totals_found["total_gross_weight_kg"] = round(s, 3)
    if not totals_found.get("total_measurement_cbm"):
        s = sum(c.get("measurement_cbm") or 0 for c in deduped if c.get("measurement_cbm"))
        if s > 0:
            totals_found["total_measurement_cbm"] = round(s, 3)

    for field, val in totals_found.items():
        if val is not None:
            base_result[field] = val

    return base_result


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 8  â€”  OTM PAYLOAD BUILDER
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def build_otm_bl_payload(bl_data: dict, request_id: str,
                          order_base_gid: str, ob_ship_unit_gid: str) -> dict:
    cargo_items = bl_data.get("cargo_items") or []
    otm_cargo   = []
    for seq, item in enumerate(cargo_items, start=1):
        otm_cargo.append({
            "sequenceNo":     seq,
            "containerNo":    item.get("container_no"),
            "sealNo":         item.get("seal_no"),
            "description":    item.get("description"),
            "packages":       item.get("number_of_packages"),
            "packageType":    item.get("package_type"),
            "grossWeightKg":  item.get("gross_weight_kg"),
            "measurementCbm": item.get("measurement_cbm"),
            "hsCode":         item.get("hs_code"),
            "domainName":     OTM_DOMAIN_NAME,
        })

    return {
        "referenceTransmissionNo": request_id,
        "senderTransmissionId":    request_id,
        "documentType":            "bill_of_lading",
        "transactions": {"items": [{
            "contentType": "application/vnd.oracle.resource+json;type=singular",
            "httpMethod":  "PATCH",
            "resourceUrl": f"orderBases/{order_base_gid}/billsOfLading/{request_id}",
            "body": {
                "orderBaseGid":         order_base_gid,
                "obShipUnitGid":        ob_ship_unit_gid,
                "domainName":           OTM_DOMAIN_NAME,
                "blNumber":             bl_data.get("bl_number"),
                "blDate":               bl_data.get("bl_date"),
                "blType":               bl_data.get("bl_type"),
                "vesselName":           bl_data.get("vessel_name"),
                "voyageNumber":         bl_data.get("voyage_number"),
                "portOfLoading":        bl_data.get("port_of_loading"),
                "portOfDischarge":      bl_data.get("port_of_discharge"),
                "placeOfReceipt":       bl_data.get("place_of_receipt"),
                "onBoardDate":          bl_data.get("on_board_date"),
                "freightTerms":         bl_data.get("freight_terms"),
                "containerNumber":      bl_data.get("container_number"),
                "totalPackages":        bl_data.get("total_packages"),
                "totalGrossWeightKg":   bl_data.get("total_gross_weight_kg"),
                "totalMeasurementCbm":  bl_data.get("total_measurement_cbm"),
                "cargoItems":           {"items": otm_cargo},
            },
        }]},
    }


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 9  â€”  SELF-CHAIN
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def fire_next_extractor(fn_client, manifest_chunk: dict, request_id: str, total_chunks: int,
                        order_base_gid: str, ob_ship_unit_gid: str,
                        shipment_id: str, supplier_name: str,
                        doc_key: str = "bill_of_lading"): 
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
    _plog("[CHAIN] Fired BL chunk %d/%d (detached)", manifest_chunk["chunkIndex"], total_chunks)



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

    # â”€â”€ Step 1: wait for shipment manifest to exist â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
        # total_expected = len(manifest.get("documents", {}))
        total_expected = manifest.get("totalDocs") or len(manifest.get("documents", {}))
    except Exception as exc:
        log.error("Failed to parse shipment manifest: %s", exc)
        return

    if total_expected == 0:
        log.error("Manifest has no documents entry â€” cannot determine total")
        return

    # â”€â”€ Step 2: write THIS extractor's marker (atomic, no contention) â”€â”€â”€â”€
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

    # â”€â”€ Step 3: small jitter to let other near-simultaneous writes land â”€â”€
    # time.sleep(1.0 + random.uniform(0, 1.0))
    # â”€â”€ Step 3: staggered jitter by doc_type to reduce simultaneous counts â”€â”€
    # bill_of_lading waits longest â†’ most likely to be last and fire merger
    jitter_by_type = {
        "bill_of_lading": 3.0,
        "packing_list_01": 2.0, "packing_list": 2.0,
        "invoice_01": 1.0, "invoice": 1.0,
    }
    base_jitter = jitter_by_type.get(doc_type, 2.0)
    time.sleep(base_jitter + random.uniform(0, 1.0))

    # â”€â”€ Step 4: count markers (each doc type has its own file â€” no race) â”€
    existing_markers = list_os_keys(os_client, complete_prefix)
    complete_count   = len(existing_markers)
    _plog("Completion markers: %d / %d â€” %s",
          complete_count, total_expected,
          [m.split("/")[-1].replace(".json","") for m in existing_markers])

    if complete_count < total_expected:
        _plog("Not all docs done yet â€” merger will be triggered by last extractor")
        return

    # â”€â”€ Step 5: we are the last â€” update manifest status then fire merger â”€
    _plog("All %d docs complete â€” updating manifest and firing merger", total_expected)

    # Read all markers to build the complete requestIds map
    request_ids = {}
    for marker_path in existing_markers:
        try:
            raw = download_from_os(os_client, marker_path)
            if raw:
                data = json.loads(raw)
                request_ids[data["docType"]] = data["requestId"]
        except Exception as exc:
            log.warning("Failed to read marker %s: %s", marker_path, exc)

    # Update manifest to complete
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

    # Fire merger
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



# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  SECTION 10  â€”  MAIN HANDLER
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def handler(ctx, data: io.BytesIO = None):
    os.makedirs(TMP_DIR, exist_ok=True)
    t_start = time.time()
    _plog("=" * 60)
    _plog("BL Extractor v1.0.0 invoked")

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
    doc_key          = body.get("docKey", "bill_of_lading")   # â† ADD THIS LINE

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
        # chunk_result = extract_pages_parallel(client=genai_client, pages_text=pages)
        chunk_result = extract_pages_parallel(
            client=genai_client, pages_text=pages, supplier_name=supplier_name,
        )
        chunk_result.update({
            "chunkIndex": chunk_index, "totalChunks": total_chunks,
            "requestId": request_id, "pagesCount": len(pages),
        })
        status, err_msg = "SUCCESS", ""
    except Exception as exc:
        log.exception("BL extractor chunk %d failed", chunk_index)
        chunk_result = {
            "cargo_items": [], "chunkIndex": chunk_index,
            "totalChunks": total_chunks, "requestId": request_id,
            "pagesCount": len(pages), "_error": str(exc),
        }
        status, err_msg = "FAILED", str(exc)

    chunk_result = sanitize_floats(chunk_result)
    duration = round(time.time() - t_start, 2)
    n_items  = len(chunk_result.get("cargo_items", []))
    _plog("Chunk %d/%d done in %.1fs â€” %d cargo rows", chunk_index, total_chunks, duration, n_items)

    # upload_to_os(os_client, json.dumps(chunk_result, ensure_ascii=False),
    #              _chunk_key(safe_req, chunk_index))

    # csv_fields = ["chunk_no", "pages_in_chunk", "status", "completed_at",
    #               "duration_s", "cargo_rows_found", "error"]

    for _up in range(3):
        if upload_to_os(os_client, json.dumps(chunk_result, ensure_ascii=False),
                        _chunk_key(safe_req, chunk_index)):
            break
        _plog("Chunk upload attempt %d/3 failed â€” retrying", _up + 1)
        time.sleep(2 ** _up)
    else:
        log.error("All chunk upload attempts failed for chunk %d/%d", chunk_index, total_chunks)
        # _check_and_fire_merger(os_client, shipment_id, request_id, "bill_of_lading")
        _check_and_fire_merger(os_client, shipment_id, request_id, doc_key)

        return fdk_response.Response(
            ctx,
            response_data=json.dumps({"status": "error", "requestId": request_id,
                                      "error": "chunk upload failed after 3 attempts"}),
            headers={"Content-Type": "application/json"},
            status_code=500,
        )

    csv_fields = ["chunk_no", "pages_in_chunk", "status", "completed_at",
                  "duration_s", "cargo_rows_found", "error"]

    append_csv_row(os_client, _csv_key(safe_req), {
        "chunk_no":        chunk_index, "pages_in_chunk":  len(pages),
        "status":          status, "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_s":      duration, "cargo_rows_found":  n_items, "error": err_msg,
    }, csv_fields, chunk_index=chunk_index)

    completed_chunks = len(list_os_keys(os_client, _chunk_prefix(safe_req)))
    _plog("Chunks completed: %d / %d", completed_chunks, total_chunks)

    if completed_chunks < total_chunks:
        next_idx      = chunk_index + 1
        manifest_data = download_from_os(os_client, _manifest_key(safe_req))
        if manifest_data:
            try:
                manifest       = json.loads(manifest_data)
                next_chunk_def = next(
                    (c for c in manifest["chunks"] if c["chunkIndex"] == next_idx), None,
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
                log.error("Failed to fire next BL chunk: %s", exc)

        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status": "chunk_complete", "requestId": request_id,
                "chunkIndex": chunk_index, "totalChunks": total_chunks,
                "cargoRowsThisChunk": n_items, "completedChunks": completed_chunks,
                "nextChunkFired": chunk_index + 1, "durationSeconds": duration,
            }, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )

    else:
        _plog("All %d chunks complete â€” merging BL", total_chunks)
        bl_data = merge_all_chunks(os_client, safe_req, total_chunks)
        bl_data["document_type"] = "bill_of_lading"

        manifest_data = download_from_os(os_client, _manifest_key(safe_req))
        manifest      = json.loads(manifest_data) if manifest_data else {}

        bl_data["_meta"] = {
            "requestId":    request_id,
            "filename":     manifest.get("filename", ""),
            "shipmentId":   shipment_id or manifest.get("shipmentId", ""),
            "supplierName": supplier_name or manifest.get("supplierName", ""),
            "num_chunks":   total_chunks,
            "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "folder":       _req_folder(safe_req),
        }

        upload_to_os(os_client,
                     json.dumps(sanitize_floats(bl_data), indent=2, ensure_ascii=False),
                     _extracted_key(safe_req))

        _order_base_gid   = order_base_gid  or manifest.get("orderBaseGid",  "")
        _ob_ship_unit_gid = ob_ship_unit_gid or manifest.get("obShipUnitGid", "")
        otm_payload = build_otm_bl_payload(bl_data, request_id, _order_base_gid, _ob_ship_unit_gid)
        upload_to_os(os_client,
                     json.dumps(sanitize_floats(otm_payload), indent=2, ensure_ascii=False),
                     _otm_key(safe_req))

        _plog("=" * 60)
        _plog("BL PIPELINE COMPLETE")
        _plog("  Cargo rows : %d", len(bl_data.get("cargo_items", [])))
        _plog("  Gross wt   : %s kg", bl_data.get("total_gross_weight_kg"))
        _plog("  CBM        : %s", bl_data.get("total_measurement_cbm"))
        _plog("  Folder     : %s", _req_folder(safe_req))
        _plog("=" * 60)

        # _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id)
        # _check_and_fire_merger(os_client, shipment_id, request_id)
        # _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id, "bill_of_lading")
        _check_and_fire_merger(os_client, shipment_id or manifest.get("shipmentId", ""), request_id, doc_key)



        return fdk_response.Response(
            ctx,
            response_data=json.dumps({
                "status":               "complete",
                "requestId":            request_id,
                "documentType":         "bill_of_lading",
                "totalChunks":          total_chunks,
                "cargoRowsExtracted":   len(bl_data.get("cargo_items", [])),
                "totalGrossWeightKg":   bl_data.get("total_gross_weight_kg"),
                "totalCbm":             bl_data.get("total_measurement_cbm"),
                "folder":               _req_folder(safe_req),
                "extractedJsonSavedTo": _extracted_key(safe_req),
                "otmSavedTo":           _otm_key(safe_req),
                "durationSeconds":      duration,
            }, ensure_ascii=False),
            headers={"Content-Type": "application/json"},
        )
