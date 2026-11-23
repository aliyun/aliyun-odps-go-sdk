# Changelog

## 1.0.0 (unreleased)

- Require Go 1.26.11 or later and use Apache Arrow Go v18 for public Arrow types.
- Add a scoped client with validated construction, context-aware catalog pages, and `SubmitSQL`, `Wait`, and `Cancel` for SQL jobs.
- Add versioned Storage read and write descriptors. Preserve initializing snapshots across canceled waits and expose read metadata and closed stream versions.
- Bind SQL values with type-aware escaping and perform a service request for database/sql ping.
- Validate Protobuf values against the column schema before writing and reject out-of-range integers when reading.
- Resolve DSN credentials from a single source, require complete matching security configuration snapshots before updates, and preserve view types and quotas during listing and Tunnel resumption.
- Add Storage API V2 clients for reading SQL instance and table results and for staging, resuming, committing, and aborting batch writes.
- Validate all Storage API V2 stream versions before explicit commits and use the Storage quota when discovering the Tunnel endpoint.
- Match the Storage API V2 wire format for table snapshots, Arrow streams, and write-session resumption.
- Wait for asynchronous Storage API V2 table snapshots to become ready, with context cancellation.
- Preserve multibyte `CHAR` and `VARCHAR` values and MAP keys in Protobuf Tunnel. Report values that exceed the declared width.
- Allow callers to supply a custom HTTP transport for signed ODPS, Tunnel, and Storage requests.
- Send the named quota on Storage operations and SQL driver result transfers.
- Return an empty Arrow reader with the projected schema for empty table snapshots.
- Honor context cancellation while waiting for credential refresh and release unused request bodies.
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
- Sign ODPS v4 requests using their request date and normalize endpoints with a trailing slash.
- Support empty resource uploads and stop when an input reader makes no progress.
- Resolve SQL schema hints consistently and preserve the caller's options and external table aliases.
- Read the requested SQL task's CSV result and report malformed headers and task errors.
- Make LogView discovery safe for concurrent requests and use the instance project's region.
- Preserve full DECIMAL precision and report integer conversions that would lose data.
- Apply the configured Deflate level and close the streaming request when a compressor fails.
- Check table and project existence using basic metadata and submit schema changes in the target project.
- Read and write the service's project protection XML and retain compatibility with older responses.
- Validate Storage API V2 stream IDs against the production service's allowed characters.
- Apply SQL context cancellation to HTTP requests and retry delays, and preserve the instance ID when available.
- Add MaxCompute Emulator regressions for Arrow, offset retries, and lost write and commit responses.

See the [Storage API V2 migration guide](docs/storage-v2.md) for the updated interfaces.
