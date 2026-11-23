// Licensed under the Apache License, Version 2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// Session identifies a read or write session that can be reopened.
// Keep RouteToken with ID when a server supplies it.
type Session struct {
	ID         string
	RouteToken string
}

func (s Session) operation(action, target string) operation {
	return operation{action: action, target: target, route: s.RouteToken, params: url.Values{"SessionId": {s.ID}}}
}

// TableReadOptions restricts the columns and partitions in a table snapshot.
// Required partition specs use the MaxCompute form "ds='20261001',hour='01'".
type TableReadOptions struct {
	Columns          []string `json:"RequiredDataColumns,omitempty"`
	PartitionColumns []string `json:"RequiredPartitionColumns,omitempty"`
	Partitions       []string `json:"RequiredPartitions,omitempty"`
}

type tableReadRequest struct {
	RequiredDataColumns      []string
	RequiredPartitionColumns []string
	RequiredPartitions       []string
	RequiredBucketIDs        []int `json:"RequiredBucketIds"`
	SplitOptions             struct {
		SplitMode      string
		SplitUnit      string
		SplitNumber    int
		CrossPartition bool
	}
	ArrowOptions            struct{ TimestampUnit, DatetimeUnit string }
	FilterPredicate         string
	FilterPredicateFallback bool
	SplitMaxFileNum         int
	IncrementalRead         bool
}

type tableReadStreamRequest struct {
	MaxBatchRows         int
	SkipRowNum           int
	MaxBatchRawSize      int64
	DataFormat           struct{ Type, Version string }
	DataColumns          []string
	DataColumnsUnordered bool
}

const (
	tableReadBatchRows    = 4096
	tableReadPollInterval = time.Second
)

// InstanceReadOptions selects the result in a SQL instance.
type InstanceReadOptions struct {
	TaskName    string `json:",omitempty"`
	QueryID     *int64 `json:"QueryId,omitempty"`
	EnableLimit bool
}

// RowRange selects Count rows starting at the zero-based Offset.
type RowRange struct {
	Offset int64
	Count  int64
}

func (r RowRange) addTo(params url.Values) error {
	if r.Offset < 0 || r.Count < 0 {
		return fmt.Errorf("storage: offset and count must be nonnegative")
	}
	params.Set("Offset", strconv.FormatInt(r.Offset, 10))
	params.Set("Count", strconv.FormatInt(r.Count, 10))
	return nil
}

type readResponse struct {
	SessionID   string `json:"SessionId"`
	DownloadID  string `json:"DownloadID"`
	RecordCount *int64
	Status      string
	TableStatus string `json:"SessionStatus"`
	DataSchema  *ReadSchema
}

// ReadColumn describes a column reported by a Storage session.
type ReadColumn struct {
	Name     string
	Type     string
	Nullable bool
}

// ReadSchema contains the projected columns, in their Storage data groups.
type ReadSchema struct {
	DataColumns      []ReadColumn
	SystemColumns    []ReadColumn
	PartitionColumns []ReadColumn
}

// ArrowSchema converts the reported column metadata to the default Arrow schema.
func (schema ReadSchema) ArrowSchema() (*arrow.Schema, error) {
	columns := slices.Concat(schema.DataColumns, schema.SystemColumns, schema.PartitionColumns)
	table := tableschema.TableSchema{Columns: make([]tableschema.Column, len(columns))}
	for i, column := range columns {
		typ, err := datatype.ParseDataType(column.Type)
		if err != nil {
			return nil, fmt.Errorf("storage: column %q: %w", column.Name, err)
		}
		table.Columns[i] = tableschema.Column{Name: column.Name, Type: typ, NotNull: !column.Nullable}
	}
	return table.ToArrowSchema()
}

func (schema ReadSchema) emptyReader() (*ArrowReader, error) {
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	if err := ipc.NewWriter(&buffer, ipc.WithSchema(arrowSchema)).Close(); err != nil {
		return nil, err
	}
	return newArrowReader(io.NopCloser(bytes.NewReader(buffer.Bytes())), 0)
}

// ReadSession represents a table or SQL result snapshot. A table snapshot can
// be pending. WaitReady returns a refreshed snapshot without changing its receiver.
type ReadSession struct {
	client     *Client
	descriptor ReadSessionDescriptor
	target     string
	action     string
	rows       int64
	status     string
	input      any
	schema     *ReadSchema
}

// Session returns a compatibility identifier. Persist Descriptor instead to
// retain the target and quota when resuming a snapshot.
func (s *ReadSession) Session() Session { return s.descriptor.session() }

// Descriptor identifies the exact target and route for ResumeRead.
func (s *ReadSession) Descriptor() ReadSessionDescriptor { return s.descriptor.clone() }

// RecordCount returns the number of rows reported by the server, or -1 when unknown.
func (s *ReadSession) RecordCount() int64 { return s.rows }

// Status returns the server's session status as reported at open time.
func (s *ReadSession) Status() string { return s.status }

// ReadPlan describes the negotiated row-offset read. Schema may be nil if the
// service only returns it when opening an Arrow stream.
type ReadPlan struct {
	SplitMode   ReadSplitMode
	RecordCount int64
	CountKnown  bool
	Schema      *ReadSchema
}

// ReadSplitMode identifies how the session addresses ranges.
type ReadSplitMode string

// ReadSplitRowOffset addresses each range by its start row and count.
const ReadSplitRowOffset ReadSplitMode = "RowOffset"

// Plan returns the row count and an owned copy of the metadata reported by the service.
func (s *ReadSession) Plan() ReadPlan {
	plan := ReadPlan{SplitMode: ReadSplitRowOffset, RecordCount: s.rows, CountKnown: s.rows >= 0 && s.status != "INIT"}
	if s.schema != nil {
		plan.Schema = &ReadSchema{
			DataColumns:      slices.Clone(s.schema.DataColumns),
			PartitionColumns: slices.Clone(s.schema.PartitionColumns),
			SystemColumns:    slices.Clone(s.schema.SystemColumns),
		}
	}
	return plan
}

// StartTableRead creates a snapshot and returns as soon as the service
// acknowledges it. Save Descriptor before waiting for the snapshot to be ready.
func (c *Client) StartTableRead(ctx context.Context, table odps.TableRef, opts TableReadOptions) (*ReadSession, error) {
	table, err := c.tableRef(table)
	if err != nil {
		return nil, err
	}
	// The Storage API expects explicit defaults and empty arrays in this request.
	input := tableReadRequest{
		RequiredDataColumns:      append([]string{}, opts.Columns...),
		RequiredPartitionColumns: append([]string{}, opts.PartitionColumns...),
		RequiredPartitions:       make([]string, 0, len(opts.Partitions)),
		RequiredBucketIDs:        []int{},
	}
	for _, spec := range opts.Partitions {
		partition, err := normalizePartitionSpec(spec)
		if err != nil {
			return nil, err
		}
		input.RequiredPartitions = append(input.RequiredPartitions, partition)
	}
	input.SplitOptions.SplitMode = string(ReadSplitRowOffset)
	input.SplitOptions.SplitUnit = "ByteSize"
	input.SplitOptions.SplitNumber = tableReadBatchRows
	input.SplitOptions.CrossPartition = true
	input.ArrowOptions.TimestampUnit = "nano"
	input.ArrowOptions.DatetimeUnit = "milli"

	desc := ReadSessionDescriptor{Version: descriptorVersion, Kind: TableSnapshot, Table: table, QuotaName: c.quota}
	op := operation{action: "TableCreateReadSession", target: tableResource(table)}
	return c.tableReadSession(ctx, desc, op, input)
}

// CreateTableReadSession starts a row-offset snapshot and waits for readiness.
// An error after creation is accompanied by the latest recoverable handle.
func (c *Client) CreateTableReadSession(ctx context.Context, table Table, opts TableReadOptions) (*ReadSession, error) {
	snapshot, err := c.StartTableRead(ctx, table, opts)
	if err != nil {
		return snapshot, err
	}
	return snapshot.WaitReady(ctx)
}

// GetTableReadSession reopens a table snapshot, waiting for readiness with ctx.
// Use ResumeRead with a saved descriptor to preserve the original scope.
func (c *Client) GetTableReadSession(ctx context.Context, table Table, session Session) (*ReadSession, error) {
	table, err := c.tableRef(table)
	if err != nil {
		return nil, err
	}
	snapshot, err := c.ResumeRead(ctx, ReadSessionDescriptor{
		Version: descriptorVersion, ID: session.ID, RouteToken: session.RouteToken,
		Kind: TableSnapshot, Table: table, QuotaName: c.quota,
	})
	if err != nil {
		return snapshot, err
	}
	return snapshot.WaitReady(ctx)
}

// ResumeRead opens a saved snapshot in one request. A table snapshot may still
// be initializing. Call WaitReady for a snapshot that can open Arrow readers.
func (c *Client) ResumeRead(ctx context.Context, desc ReadSessionDescriptor) (*ReadSession, error) {
	switch desc.Kind {
	case TableSnapshot:
		if desc.Job != (odps.SQLJobRef{}) || desc.QueryID != nil || desc.EnableLimit || desc.Table.Schema == "" {
			return nil, fmt.Errorf("storage: invalid table read descriptor")
		}
		if err := c.validateDescriptor(desc.Version, desc.ID, desc.Table.Project, desc.QuotaName); err != nil {
			return nil, err
		}
		table, err := c.tableRef(desc.Table)
		if err != nil {
			return nil, err
		}
		op := desc.session().operation("TableGetReadSession", tableResource(table))
		op.params.Set("session_refresh", "false")
		return c.tableReadSession(ctx, desc, op, struct{}{})
	case InstanceSnapshot:
		if desc.Table != (odps.TableRef{}) || desc.Job.TaskName == "" {
			return nil, fmt.Errorf("storage: invalid instance read descriptor")
		}
		if err := c.validateDescriptor(desc.Version, desc.ID, desc.Job.Project, desc.QuotaName); err != nil {
			return nil, err
		}
		return c.instanceReadSession(ctx, http.MethodGet, "InstanceGetReadSession", desc)
	default:
		return nil, fmt.Errorf("storage: unsupported read session kind %q", desc.Kind)
	}
}

// WaitReady waits for a table snapshot to become readable. On failure it
// returns the latest handle so the caller can retain or resume the snapshot.
func (s *ReadSession) WaitReady(ctx context.Context) (*ReadSession, error) {
	current := s
	for current.status == "INIT" {
		if err := common.SleepContext(ctx, tableReadPollInterval); err != nil {
			return current, fmt.Errorf("storage: wait for read session %q: %w", current.descriptor.ID, err)
		}
		next, err := current.client.ResumeRead(ctx, current.descriptor)
		if next != nil {
			current = next
		}
		if err != nil {
			return current, err
		}
	}
	if current.descriptor.Kind == TableSnapshot && current.status != "NORMAL" {
		return current, fmt.Errorf("storage: read session is not ready in status %q", current.status)
	}
	return current, ctx.Err()
}

func (c *Client) tableReadSession(ctx context.Context, desc ReadSessionDescriptor, op operation, input any) (*ReadSession, error) {
	var reply readResponse
	headers, err := c.jsonRequest(ctx, http.MethodPost, op, input, &reply)
	if err != nil {
		return nil, err
	}
	stream := tableReadStreamRequest{MaxBatchRows: tableReadBatchRows, DataColumns: []string{}}
	stream.DataFormat.Type, stream.DataFormat.Version = "Arrow", "V5"
	snapshot, err := c.newReadSession(desc, op, "TableRead", reply.SessionID, reply.TableStatus, reply.RecordCount, headers, stream, reply.DataSchema)
	if err != nil {
		return nil, err
	}
	if reply.TableStatus != "NORMAL" && reply.TableStatus != "INIT" {
		return snapshot, fmt.Errorf("storage %s: read session %q has status %q", op.action, snapshot.descriptor.ID, reply.TableStatus)
	}
	return snapshot, nil
}

// OpenSQLResult opens an Arrow snapshot of a completed SQL job in the client's project.
func (c *Client) OpenSQLResult(ctx context.Context, job odps.SQLJobRef, opts InstanceReadOptions) (*ReadSession, error) {
	if job.Project == "" {
		job.Project = c.project
	}
	if job.Project != c.project {
		return nil, fmt.Errorf("storage: SQL result project does not match the client")
	}
	if opts.TaskName != "" {
		job.TaskName = opts.TaskName
	}
	if job.TaskName == "" {
		job.TaskName = "AnonymousSQLTask"
	}
	desc := ReadSessionDescriptor{
		Version: descriptorVersion, Kind: InstanceSnapshot, Job: job,
		QuotaName: c.quota, QueryID: opts.QueryID, EnableLimit: opts.EnableLimit,
	}
	return c.instanceReadSession(ctx, http.MethodPost, "InstanceCreateReadSession", desc)
}

// CreateInstanceReadSession opens the result of a completed SQL instance.
func (c *Client) CreateInstanceReadSession(ctx context.Context, id string, opts InstanceReadOptions) (*ReadSession, error) {
	return c.OpenSQLResult(ctx, odps.SQLJobRef{InstanceID: id}, opts)
}

// GetInstanceReadSession reopens an existing instance result snapshot.
func (c *Client) GetInstanceReadSession(ctx context.Context, id string, session Session, opts InstanceReadOptions) (*ReadSession, error) {
	if opts.TaskName == "" {
		opts.TaskName = "AnonymousSQLTask"
	}
	return c.ResumeRead(ctx, ReadSessionDescriptor{
		Version: descriptorVersion, Kind: InstanceSnapshot, QuotaName: c.quota,
		ID: session.ID, RouteToken: session.RouteToken, QueryID: opts.QueryID, EnableLimit: opts.EnableLimit,
		Job: odps.SQLJobRef{Project: c.project, InstanceID: id, TaskName: opts.TaskName},
	})
}

func (c *Client) instanceReadSession(ctx context.Context, method, action string, desc ReadSessionDescriptor) (*ReadSession, error) {
	if err := validateIdentifier("instance", desc.Job.InstanceID); err != nil {
		return nil, err
	}
	desc = desc.clone()
	op := desc.session().operation(action, fmt.Sprintf("projects.%s.instances.%s", c.project, desc.Job.InstanceID))
	var input any
	if method == http.MethodPost {
		input = struct{ EnableLimit bool }{desc.EnableLimit}
		op.params.Del("SessionId")
	}
	var reply readResponse
	headers, err := c.jsonRequest(ctx, method, op, input, &reply)
	if err != nil {
		return nil, err
	}
	return c.newReadSession(desc, op, "InstanceRead", reply.DownloadID, reply.Status, reply.RecordCount, headers,
		InstanceReadOptions{TaskName: desc.Job.TaskName, QueryID: desc.QueryID, EnableLimit: desc.EnableLimit}, reply.DataSchema)
}

func (c *Client) newReadSession(desc ReadSessionDescriptor, op operation, action, id, status string, count *int64, headers http.Header, input any, schema *ReadSchema) (*ReadSession, error) {
	if id == "" {
		id = desc.ID
	}
	if id == "" || (desc.ID != "" && desc.ID != id) || (count != nil && *count < -1) {
		return nil, fmt.Errorf("storage %s: invalid session descriptor", op.action)
	}
	rows := int64(-1)
	if count != nil && status != "INIT" {
		rows = *count
	}
	desc.ID, desc.RouteToken = id, updatedRoute(headers, op.route)
	return &ReadSession{client: c, descriptor: desc, target: op.target, action: action, rows: rows, status: status, input: input, schema: schema}, nil
}

// OpenReader streams a range of a ready snapshot. Pass nil for the entire
// snapshot when a row count is available. The caller must Release the reader.
func (s *ReadSession) OpenReader(ctx context.Context, rows *RowRange) (*ArrowReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.status == "INIT" {
		return nil, fmt.Errorf("storage: read session is not ready; call WaitReady")
	}
	if s.descriptor.Kind == TableSnapshot && s.status != "NORMAL" {
		return nil, fmt.Errorf("storage: read session is not readable in status %q", s.status)
	}
	if rows == nil && s.rows < 0 {
		return nil, fmt.Errorf("storage: row count is unknown; supply an explicit range")
	}
	rangeToRead := RowRange{Count: max(s.rows, 1)}
	if rows != nil {
		rangeToRead = *rows
	}
	op := s.descriptor.session().operation(s.action, s.target)
	if err := rangeToRead.addTo(op.params); err != nil {
		return nil, err
	}
	if (rangeToRead.Count == 0 || s.rows == 0 && rangeToRead.Offset == 0) && s.schema != nil {
		return s.schema.emptyReader()
	}
	expected := int64(-1)
	if s.rows >= 0 {
		expected = min(rangeToRead.Count, max(s.rows-rangeToRead.Offset, 0))
	}
	return s.client.arrowRequest(ctx, op, s.input, expected)
}

// PreviewTable reads the first limit rows without creating a persistent snapshot.
// A negative or zero limit uses the server default, and partition may be empty.
func (c *Client) PreviewTable(ctx context.Context, table Table, partition string, limit int64) (*ArrowReader, error) {
	target, err := c.tableTarget(table)
	if err != nil {
		return nil, err
	}
	op := operation{
		action: "TablePreview", target: target,
		params: url.Values{"Limit": {strconv.FormatInt(limit, 10)}},
	}
	if partition != "" {
		op.params.Set("Partition", partition)
	}
	input := struct {
		Partition string `json:",omitempty"`
		Limit     int64
	}{partition, limit}
	return c.arrowRequest(ctx, op, input, -1)
}

func (c *Client) arrowRequest(ctx context.Context, op operation, input any, expected int64) (*ArrowReader, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	resp, err := c.request(ctx, http.MethodPost, op, bytes.NewReader(encoded), map[string]string{
		"Content-Type": "application/json; charset=utf-8", "Accept-Encoding": "x-lz4-frame",
	})
	if err != nil {
		return nil, err
	}
	reader, err := newArrowReader(resp.Body, expected)
	if err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("storage %s: %w", op.action, err)
	}
	return reader, nil
}

// ArrowReader is a reference-counted Arrow IPC reader. Release closes the HTTP
// stream when the last reference is released. Only one goroutine may read it.
type ArrowReader struct {
	ipc      *ipc.Reader
	body     io.ReadCloser
	refs     atomic.Int64
	expected int64
	rows     int64
	err      error
}

var _ array.RecordReader = (*ArrowReader)(nil)

func newArrowReader(body io.ReadCloser, expected int64) (*ArrowReader, error) {
	decoded, err := ipc.NewReader(body)
	if err != nil {
		return nil, err
	}
	r := &ArrowReader{ipc: decoded, body: body, expected: expected}
	r.refs.Store(1)
	return r, nil
}

func (r *ArrowReader) Retain() { r.refs.Add(1) }

func (r *ArrowReader) Release() {
	if r.refs.Add(-1) == 0 {
		r.ipc.Release()
		_ = r.body.Close()
	}
}

func (r *ArrowReader) Schema() *arrow.Schema { return r.ipc.Schema() }

func (r *ArrowReader) RecordBatch() arrow.RecordBatch { return r.ipc.RecordBatch() }

// Record implements the compatibility method of array.RecordReader.
func (r *ArrowReader) Record() arrow.Record { return r.RecordBatch() }

func (r *ArrowReader) Next() bool {
	if r.err != nil {
		return false
	}
	if !r.ipc.Next() {
		r.err = r.ipc.Err()
		if r.err == nil && r.expected >= 0 && r.rows != r.expected {
			r.err = fmt.Errorf("storage: read %d of %d rows: %w", r.rows, r.expected, io.ErrUnexpectedEOF)
		}
		return false
	}
	r.rows += r.RecordBatch().NumRows()
	if r.expected >= 0 && r.rows > r.expected {
		r.err = fmt.Errorf("storage: received %d rows for a range of %d rows", r.rows, r.expected)
		return false
	}
	return true
}

func (r *ArrowReader) Err() error { return r.err }
