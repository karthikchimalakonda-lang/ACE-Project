# ACE Product Usage submission draft

## Suggested title

Serverless Fan-Out/Fan-In Shipment Document Automation with OCI Functions and Generative AI

## Contribution description

I built an original, serverless shipment-document automation workflow on Oracle Cloud Infrastructure. The solution accepts shipment documents through a Streamlit interface and uses OCI Functions to classify documents, dispatch document-specific extraction work in parallel, and merge results once all required document types are complete.

The workflow processes packing lists, commercial invoices, and bills of lading. OCI Object Storage holds the source documents, workflow manifests, intermediate chunk results, completion markers, and final structured output. OCI Generative AI assists with field extraction and is used as a fallback for semantic item mapping. The workflow then reconciles cross-document quantities and weights, flags unmatched items for review, and creates an integration-ready payload for an Oracle Transportation Management target.

I implemented the function-based orchestration, asynchronous fan-out/fan-in design, Object Storage state management, validation and mapping logic, and Streamlit interface. The attached repository and evidence show the architecture, sanitized implementation, and a recorded end-to-end demonstration using synthetic documents.

## Evidence to attach

- Repository link: `REPLACE_WITH_SANITIZED_REPOSITORY_URL`
- Demo video link: `REPLACE_WITH_DEMO_VIDEO_URL`
- OCI Functions application screenshot with IDs blurred
- Streamlit UI screenshots showing upload, progress, and final result
- Object Storage workflow-artifact screenshots with identifiers blurred
- Final validation/integration-ready-output screenshot using synthetic data

## Reviewer notes

All source code, screenshots, and test inputs have been sanitized. Client names, production identifiers, credentials, and real shipment data are excluded.
