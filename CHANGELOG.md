# Changelog

## 1.0.0 (unreleased)

- Require Go 1.26 and use Apache Arrow Go v18 for public Arrow types.
- Add Storage API V2 clients for reading SQL instance and table results and for staging, resuming, committing, and aborting batch writes.
- Commit selected Storage API V2 stream versions and use the Storage quota when discovering the Tunnel endpoint.
- Move table preview to Storage API V2. Replace the former Table Tunnel Arrow methods with the Storage API V2 session methods.
- Decode both boolean and string `Transactional` metadata.
- Accept successful creation and replacement responses for multipart resource uploads.
- Preserve a resource's name when the metadata response omits it.
- Return an explicit error for unsupported `database/sql` transactions and prepared statements.
- Apply SQL context cancellation to HTTP requests and retry delays, and preserve the instance ID when available.
- Add MaxCompute Emulator regressions for Arrow, offset retries, and lost write and commit responses.

See the [Storage API V2 migration guide](docs/storage-v2.md) for the updated interfaces.
