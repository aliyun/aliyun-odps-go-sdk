# Upgrade to Go SDK 1.0

The module path is unchanged. Go 1.26.11 or later is required for the main SDK.
The separately versioned Tea module continues to support Go 1.15.

| Previous entry point | 1.0 entry point |
| --- | --- |
| `NewOdps` followed by project, schema and timeout setters | `odps.NewClient(account, odps.ClientOptions{...})` |
| Changing the default project or schema in place | `client.WithScope(odps.Scope{...})` |
| `Tables.Get(name)` followed by `Load` | `client.Tables().Get(ctx, odps.TableRef{Name: name})` |
| `Tables.List` with a callback | `ListPage` for one page or `List` for a lazy Go iterator |
| `ExecSQl` or `ExecSQlWithOption` for job submission | `client.SubmitSQL(ctx, odps.SQLRequest{...})` |
| `Instance.WaitForSuccess` and `Instance.Terminate` | `job.Wait(ctx)` and `job.Cancel(ctx)` |
| Constructing a DSN to reuse SDK credentials | `sqldriver.NewConnector(client, options)` and `sql.OpenDB` |
| Persisting an ID and route token separately | `ReadSession.Descriptor` or `WriteSession.Descriptor` |
| Embedded Arrow and Arrow Tunnel readers and writers | [Storage API V2 and Arrow Go v18](storage-v2.md) |

## Client and metadata

Client settings and scope are fixed at construction. A scoped client shares
the core connection pool and account. A supplied HTTP transport remains under
its caller's ownership. Use request contexts to bound individual operations.
`odps.WithRequestMetadata` carries a call context, W3C tracing headers and a
User-Agent suffix through core, Tunnel and Storage requests. A later call
replaces the metadata for that context. `ClientOptions.Region` selects the
LogView region. Configure the signing region on the account.

Object references and `WithScope` inherit the schema only within the same
project. From `alpha.analytics`, both `TableRef{Project: "beta", Name: "t"}`
and `WithScope(Scope{Project: "beta"})` followed by `TableRef{Name: "t"}`
select the default schema in `beta`. Set `Schema` explicitly to select a
named schema, including `"default"` in a schema-enabled project. To discover
projects before configuring a scoped client, use
`odps.NewOdps(account, endpoint).Projects().List()` first.

Table metadata `Get` requests the basic schema. Use `GetExtended` to request
additional statistics. Check `metadata.ExtendedLoaded` before interpreting
extended fields such as `Schema.FileNum` or `Schema.Transactional`.
Schema lists return `SchemaSummary`. Use `Schemas().Get` for timestamps.
Resource size is optional. A non-nil `SizeBytes` with value zero identifies
an empty resource. `FunctionMetadata.Resources` contains resource names.

`ListPage` returns lightweight entries and an opaque
`NextPageToken`. `List` fetches pages as needed and stops when the loop ends.
Projects, schemas, resources and instances follow the same pagination pattern. Use
`client.Functions().Get` to read one scoped function.

Existing catalog and Tunnel entry points remain available for operations such
as DDL, permission management and writes through the Protobuf protocol. The
removed project methods were `CreateExternalProject`, `DeleteExternalProject`
and `UpdateProject`.

For example, a caller can create a table with the existing DDL API and fetch
its metadata with the scoped client using the same account and project:

```go
func createAndInspect(ctx context.Context, credentials account.Account, endpoint, project string, schema tableschema.TableSchema) (odps.TableMetadata, error) {
    core, err := odps.NewClient(credentials, odps.ClientOptions{Endpoint: endpoint, Project: project})
    if err != nil {
        return odps.TableMetadata{}, err
    }
    defer core.CloseIdleConnections()
    ddl := odps.NewOdps(credentials, endpoint)
    transport := ddl.RestClient()
    defer transport.CloseIdleConnections()
    ddl.SetDefaultProjectName(project)
    if err := ddl.Tables().Create(schema, true, nil, nil); err != nil {
        return odps.TableMetadata{}, err
    }
    return core.Tables().Get(ctx, odps.TableRef{Name: schema.TableName})
}
```

The `storage.Table` compatibility name now accepts a project in addition to a
schema and name. Use keyed fields with `odps.TableRef`.

## SQL jobs and results

`SubmitSQL` returns a job once its submission has been acknowledged. Save
`job.Ref()` to attach to a standard SQL job with `client.ResumeSQL`. Canceling
a wait ends local polling. Use `job.Cancel` with its own context to request
remote termination. If a submission or wait fails after acknowledgement,
`*odps.SQLJobError` carries the known job reference and wraps the cause.
`SubmitSQL` can return both a job and an error.

Keep the live `SQLJob` for a MaxQA submission. Its reference records
`MaxQA: true` and omits session credentials. Passing that reference to
`ResumeSQL` or either result-transfer adapter returns `errors.ErrUnsupported`
(test with `errors.Is`). Standard SQL references saved without the MaxQA field
still work. Discard MaxQA references saved by an earlier 1.0 candidate, which
could not distinguish them from standard jobs.

After `job.Wait`, use `result.Inline(ctx)` for inline outputs, or open a
result transfer with `tunnel.OpenSQLResult` or `storage.Client.OpenSQLResult`.
Pass the result's reference to identify the same instance. Arrow and cached Protobuf reads use the
referenced task. Passing a conflicting task name returns an error before a
request. For Tunnel, a reference with a task name alone still opens a standard
download. Use `InstanceSessionCfg.WithQueryId` for an incremental result. If you opt into
SDK connection pooling on a Protobuf result session, call
`CloseIdleConnections` after closing its readers.

## Database/sql changes

Placeholders take values. Pass ordinary strings and put `?` or `@name`
outside quotes. For example, use `WHERE name = ?` with `"O'Reilly"`.
Keep identifiers, type declarations and other SQL syntax in the statement.
For compound values, bind their elements inside SQL constructors.

A query can use indexed positional values or named values. The driver ignores
placeholder-like text inside strings, identifiers and comments. It rejects
missing, duplicate or unused arguments. `nil`, integers, finite doubles,
booleans, strings, byte slices, timestamps and `data.Decimal` are supported.
Timestamp and binary values require the corresponding MaxCompute data types.
Use `PingContext` to check that the credentials can read the configured
project. Replace the former `NewNamedArgQuery` and `NewPositionArgQuery`
builders and their setters with `QueryContext` or `ExecContext` arguments.

Scalar result values use standard `database/sql/driver` types, with integers as
`int64`, floats as `float64` and decimal as exact text. Existing SDK complex
value scanners remain available. Scanning a DECIMAL into `data.Decimal`
infers precision and scale from its text. For example, `1.020` has precision
4 and scale 3. To obtain its declared column type, such as DECIMAL(15,3),
call `rows.ColumnTypes()` and then `column.DecimalSize()`.

Use `Config.FormatDSN` to validate and
format DSNs. DSN credentials are read as a group, either entirely from the DSN
or entirely from the environment. Choose one SQL result route, either
`tunnelEndpoint` or `tunnelQuotaName`. Timeout parameters specify whole
seconds. An omitted TCP dial timeout uses 30 seconds and an explicit
`tcpConnectionTimeout=0` disables the dial timeout. With `odps.ClientOptions`,
leave `TCPConnectionTimeout` nil for the default or pass a pointer to the duration.

## Map mutations and Protobuf uploads

`data.Map.ToGoMap()` returns a shallow copy. Replace
`delete(m.ToGoMap(), key)` with `m.Delete(key)` and check the returned error.
Protobuf uploads reject duplicate keys after conversion to the column's key
type. In a BIGINT-keyed map, `data.Int(1)` and `data.BigInt(1)` identify the
same key. CHAR keys ignore trailing padding spaces. VARCHAR keys retain them.
For slice-backed BINARY keys, use pointers to `data.Binary` with `Map.Set`.
The Protobuf codec compares their bytes and returns pointers when decoding.
Scalar floating-point NaN keys can be replaced or deleted using the same type
and IEEE bit pattern. A rejected row writes no bytes and can be corrected on
the same writer.
