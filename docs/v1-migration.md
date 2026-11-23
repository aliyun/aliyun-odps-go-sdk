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
User-Agent suffix through core, Tunnel and Storage requests.

Table metadata `Get` requests the basic schema. Use `GetExtended` to request
additional statistics. `ListPage` returns lightweight entries and an opaque
`NextPageToken`. `List` fetches pages as needed and stops when the loop ends.
Projects, schemas and resources follow the same pagination pattern. Use
`client.Functions().Get` to read one scoped function.

Existing catalog and Tunnel entry points remain available for operations such
as DDL, permission management and writes through the Protobuf protocol. The
three project mutation methods that always returned an unimplemented error
have been removed.

## SQL jobs and results

`SubmitSQL` returns a job once its submission has been acknowledged. Save
`job.Ref()` to attach to a standard SQL job with `client.ResumeSQL`. Canceling
a wait ends local polling. Use `job.Cancel` with its own context to request
remote termination. If a submission or wait fails after acknowledgement,
`*odps.SQLJobError` carries the known job reference and wraps the cause.
`SubmitSQL` can return both a job and an error.

After `job.Wait`, use `result.Inline(ctx)` for inline outputs, or open a
result transfer with `tunnel.OpenSQLResult` or `storage.Client.OpenSQLResult`.
Pass the result's reference to identify the same instance.

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
project.

Scalar result values use standard `database/sql/driver` types, with integers as
`int64`, floats as `float64` and decimal as exact text. Existing SDK complex
value scanners remain available. Use `Config.FormatDSN` to validate and
format DSNs. DSN credentials are read as a group, either entirely from the DSN
or entirely from the environment. Timeout parameters specify whole seconds.
