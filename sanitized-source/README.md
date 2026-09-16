# Sanitized source intake

Copy reviewed function folders here only after sanitization. Use these generic folder names:

| Original capability | Sanitized folder |
| --- | --- |
| Classifier | `document-classifier` |
| Packing-list orchestration | `packing-list-orchestrator` |
| Packing-list extraction | `packing-list-extractor` |
| Invoice orchestration | `invoice-orchestrator` |
| Invoice extraction | `invoice-extractor` |
| Bill-of-lading orchestration | `bill-of-lading-orchestrator` |
| Bill-of-lading extraction | `bill-of-lading-extractor` |
| Shipment merge | `shipment-merger` |
| Item mapping/validation | `item-mapper` |
| Status API | `workflow-status` |

Before adding any file, replace organization and client names with generic terms, convert all endpoints and IDs to environment-variable placeholders, and remove comments containing business-specific references. Do not add real input/output samples.
