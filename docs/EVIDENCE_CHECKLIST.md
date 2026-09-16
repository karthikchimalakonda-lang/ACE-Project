# Evidence checklist

## Before publishing or submitting

- [ ] Every source file has been reviewed for client names, customer data, production URLs, OCIDs, namespaces, bucket names, tokens, API keys, and passwords.
- [ ] Private keys, OAuth credentials, OCI config files, `.env` files, and real input/output JSON are excluded.
- [ ] All names are generic: `Sample Logistics Organization`, `Supplier A`, `Customer A`, and `sample-bucket`.
- [ ] Screenshots use synthetic documents and have identifiers blurred.
- [ ] The Git repository is accessible to ACE reviewers.
- [ ] The README renders correctly on GitHub and the Mermaid diagram displays.

## Recommended screenshots

1. Streamlit upload page with a synthetic PDF.
2. OCI Functions application overview, with all OCIDs and compartment/bucket information blurred.
3. Function list showing generic function names.
4. Object Storage manifest/chunk-output view with object names and IDs masked.
5. Streamlit status/result screen displaying extraction and validation outcome.
6. Final merged JSON or OTM-ready payload, using synthetic values only.

## Recommended demonstration flow

1. Explain the business problem and architecture in under 30 seconds.
2. Upload a synthetic shipment document.
3. Show the classifier and asynchronous routing.
4. Show parallel extraction outputs and completion state.
5. Show the merge, validation, item mapping, and final integration-ready payload.
6. Close by identifying the OCI services used and the operational benefit.
