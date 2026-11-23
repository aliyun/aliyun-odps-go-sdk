# Storage API V2

Storage API V2 reads and writes Apache Arrow IPC. A client shares credentials
with an existing `*odps.Client`. Set `storage.ClientOptions.QuotaName` to select a
quota for Tunnel endpoint discovery and Storage requests. Set `TunnelEndpoint`
to connect directly to a specific address.

## Read a SQL result

Submit SQL with the core client and wait for its result with `job.Wait(ctx)`.
Open a Storage read session using the resulting job reference. A reader owns an HTTP stream and one Arrow reference.
Call `Release` when done and check `Err` after the read loop. Retain a batch to
keep it after advancing the reader and release that reference when done.

```go
client, err := storage.Open(ctx, core, storage.ClientOptions{})
if err != nil {
    return err
}
defer client.Close()

session, err := client.OpenSQLResult(ctx, result.Ref(), storage.InstanceReadOptions{})
if err != nil {
    return err
}
reader, err := session.OpenReader(ctx, nil)
if err != nil {
    return err
}
defer reader.Release()
for reader.Next() {
    batch := reader.RecordBatch()
    // Process batch before advancing the reader.
    fmt.Println(batch)
}
return reader.Err()
```

Pass `&storage.RowRange{Offset: start, Count: count}` to read a range. Use the
serializable `session.Descriptor()` with `ResumeRead` to reopen a result. See the
[runnable instance reader](../examples/sdk/storage/instance_result/main.go).

## Read and write tables

- Create a table snapshot with `StartTableRead` and save its descriptor.
  Select columns and partitions in `TableReadOptions`. Call `WaitReady` with a
  context deadline, then use `OpenReader`. If waiting fails, it returns the
  latest handle for `ResumeRead` with a new context. Inspect `Plan` before
  opening the stream for the projected schema and row count when available.
  Supply an explicit row range when the count is unknown.
- Create a batch upload with `CreateTableWriteSession` and open one or more
  streams with `CreateStream`. Use ASCII letters, digits, and hyphens in stream
  IDs and positive stream versions. Write each batch at the previously
  acknowledged offset. `InitialOffset`
  on a resumed stream is the server's durable offset.
- After an ambiguous transport error, resend the same batch at the same offset.
  Keep the original payload until the write is acknowledged. The service may
  deduplicate by offset without verifying the payload.
- Finish creating, writing, and closing every stream in the session. Call
  `CommitStreams(ctx, streams...)` with all original `storage.Stream`
  descriptors. The SDK checks these against the closed streams reported by
  the server before sending the IDs and versions required by some transactional
  tables. Track every intended stream and close them all before committing, since
  the server may omit open streams from that check. Use a
  separate session for each group of rows you want to commit independently.
  `Commit(ctx)` commits all closed streams on tables that support implicit
  selection. Call `Abort` to discard an open upload.

Save the JSON-serializable write-session descriptor and all intended stream
versions before submitting a commit. If the outcome is uncertain, call
`ResumeWrite` using the same project and quota and repeat the commit with the
same versions. `Refresh` and `ClosedStreams` let the coordinator inspect
acknowledged streams. For a
partitioned upload, supply the partition spec in `TableWriteOptions.Partition`.
Set `Overwrite` when the commit should replace the target's rows.

Use `TableReadOptions.PartitionColumns` to include partition keys in Arrow
results. `PreviewTable` accepts a partition filter and streams a sample of data
columns.

## Upgrade from the embedded Arrow APIs

| Earlier API | Go SDK 1.0 API |
| --- | --- |
| `DownloadSession.OpenRecordArrowReader` | `storage.Client.StartTableRead`, then `WaitReady` and `OpenReader` |
| `UploadSession.OpenRecordArrowWriter` | `storage.Client.CreateTableWriteSession`, then `CreateStream` and `WriteBatch` |
| `TableSchema.ToArrowSchema` | Returns `(*arrow.Schema, error)` with Go types from `github.com/apache/arrow-go/v18/arrow` |
| `Tunnel.Preview` | Returns MaxCompute records through Storage API V2 |

The row-oriented `OpenRecordReader` and `OpenRecordWriter` use the Protobuf
Tunnel APIs. `odps/storage` exposes an `array.RecordReader` for Arrow consumers.

## Verify a deployment

Run the [Emulator suite](../README.md) for local protocol and fault regression
tests. In a MaxCompute test project, repeat the following with the authentication
and data transmission quota used by the application.

1. Submit a SQL query with known values, including a null and a decimal. Read
   its full result and a row range through Storage API V2, and compare them with
   the SQL result.
2. Write two streams into a test table, close both, and reopen the session.
   Verify that a commit listing just one stream is rejected. Commit with both
   IDs and versions and check that each row appears exactly once. Repeat on a
   transactional table and check neighboring partitions for a partitioned target.
3. Retry a write at its original row offset and repeat the same commit after
   reopening. Compare the final row count and values with the inputs.
