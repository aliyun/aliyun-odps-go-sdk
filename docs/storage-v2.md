# Storage API V2

Storage API V2 reads and writes Apache Arrow IPC. A client shares credentials
with an existing `*odps.Odps`. It connects to that project's Tunnel endpoint,
which can be resolved from the project and quota or supplied with
`storage.ClientOptions.TunnelEndpoint`.

## Read a SQL result

Submit SQL with the ODPS client, wait for the instance to finish, then open a
Storage read session. A reader owns an HTTP stream and one Arrow reference.
Call `Release` when done and check `Err` after the read loop. Retain a batch to
keep it after advancing the reader and release that reference when done.

```go
client, err := storage.NewClient(ctx, service, storage.ClientOptions{})
if err != nil {
    return err
}
defer client.Close()

session, err := client.CreateInstanceReadSession(ctx, instance.Id(), storage.InstanceReadOptions{})
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
session descriptor with `GetInstanceReadSession` to reopen a result. See the
[runnable instance reader](../examples/sdk/storage/instance_result/main.go).

## Read and write tables

- Create a table snapshot with `CreateTableReadSession`. Select columns and
  partitions in `TableReadOptions`, then use `OpenReader` to stream rows.
- Create a batch upload with `CreateTableWriteSession` and open one or more
  streams with `CreateStream`. Write each batch at the previously acknowledged
  offset. `InitialOffset` on a resumed stream is the server's durable offset.
- After an ambiguous transport error, resend the same batch at the same offset.
  A different batch at that offset is rejected. Close each stream and call
  `Commit` to publish the staged rows. Call `Abort` to discard an open upload.

Keep the session descriptor, including any routing token, when reopening a
read or write session. For a partitioned upload, supply the partition spec in
`TableWriteOptions.Partition`. Set `Overwrite` when the commit should replace
the target's rows.

Use `TableReadOptions.PartitionColumns` to include partition keys in Arrow
results. `PreviewTable` accepts a partition filter and streams a sample of data
columns.

## Upgrade from the embedded Arrow APIs

| Earlier API | Go SDK 1.0 API |
| --- | --- |
| `DownloadSession.OpenRecordArrowReader` | `storage.Client.CreateTableReadSession`, then `ReadSession.OpenReader` |
| `UploadSession.OpenRecordArrowWriter` | `storage.Client.CreateTableWriteSession`, then `CreateStream` and `WriteBatch` |
| `TableSchema.ToArrowSchema` | Returns `(*arrow.Schema, error)` with Go types from `github.com/apache/arrow-go/v18/arrow` |
| `Tunnel.Preview` | Returns MaxCompute records through Storage API V2 |

The row-oriented `OpenRecordReader` and `OpenRecordWriter` use the Protobuf
Tunnel APIs. `odps/storage` exposes an `array.RecordReader` for Arrow consumers.
