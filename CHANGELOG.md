# Changelog

## 1.0.0 (unreleased)

- Require Go 1.26 and use Apache Arrow Go v18 for public Arrow types.
- Add Storage API V2 clients for reading SQL instance and table results and for staging, resuming, committing, and aborting batch writes.
- Validate all Storage API V2 stream versions before explicit commits and use the Storage quota when discovering the Tunnel endpoint.
- Match the Storage API V2 wire format for table snapshots, Arrow streams, and write-session resumption.
- Move table preview to Storage API V2. Replace the former Table Tunnel Arrow methods with the Storage API V2 session methods.
- Decode both boolean and string `Transactional` metadata.
- Accept successful creation and replacement responses for multipart resource uploads.
- Preserve a resource's name when the metadata response omits it.
- Return an explicit error for unsupported `database/sql` transactions and prepared statements.
- Bind multiple positional `database/sql` arguments in order and reject mismatched argument counts.
- Serialize credential refresh within each Aliyun STS account used by concurrent requests.
- Treat a nil Tea API query value as a flag during request signing.
- Keep DNS cache entries isolated across concurrent requests and use cached IPv6 addresses correctly.
- Preserve an explicitly supplied request project and generate GMT request dates on minimal images.
- Return the failed task's result on multi-task jobs and poll MaxQA queries on their own endpoint.
- Apply function updates to the requested schema, propagate metadata errors and close mutation responses.
- Discover Tunnel routes by project and quota, and send P2P stream writes to the selected worker.
- Apply SQL context cancellation to HTTP requests and retry delays, and preserve the instance ID when available.
- Add MaxCompute Emulator regressions for Arrow, offset retries, and lost write and commit responses.

See the [Storage API V2 migration guide](docs/storage-v2.md) for the updated interfaces.
