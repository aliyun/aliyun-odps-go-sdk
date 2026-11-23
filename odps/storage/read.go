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
	RecordCount int64
	Status      string
	TableStatus string `json:"SessionStatus"`
	DataSchema  *tableReadSchema
}

type readColumn struct {
	Name     string
	Type     string
	Nullable bool
}

type tableReadSchema struct {
	DataColumns      []readColumn
	SystemColumns    []readColumn
	PartitionColumns []readColumn
}

func (schema tableReadSchema) emptyReader() (*ArrowReader, error) {
	columns := slices.Concat(schema.DataColumns, schema.SystemColumns, schema.PartitionColumns)
	table := tableschema.TableSchema{Columns: make([]tableschema.Column, len(columns))}
	for i, column := range columns {
		typ, err := datatype.ParseDataType(column.Type)
		if err != nil {
			return nil, fmt.Errorf("storage: column %q: %w", column.Name, err)
		}
		table.Columns[i] = tableschema.Column{Name: column.Name, Type: typ, NotNull: !column.Nullable}
	}
	arrowSchema, err := table.ToArrowSchema()
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	if err := ipc.NewWriter(&buffer, ipc.WithSchema(arrowSchema)).Close(); err != nil {
		return nil, err
	}
	return newArrowReader(io.NopCloser(bytes.NewReader(buffer.Bytes())), 0)
}

// ReadSession is a snapshot of a table or SQL instance result.
type ReadSession struct {
	client      *Client
	session     Session
	target      string
	action      string
	rows        int64
	status      string
	input       any
	tableSchema *tableReadSchema
}

// Session returns the descriptor for reopening the snapshot.
func (s *ReadSession) Session() Session { return s.session }

// RecordCount returns the number of rows reported by the server.
func (s *ReadSession) RecordCount() int64 { return s.rows }

// Status returns the server's session status as reported at open time.
func (s *ReadSession) Status() string { return s.status }

// CreateTableReadSession opens a row-offset snapshot of a table.
// For asynchronous creation, it waits for the snapshot to become ready or ctx to end.
func (c *Client) CreateTableReadSession(ctx context.Context, table Table, opts TableReadOptions) (*ReadSession, error) {
	target, err := c.tableTarget(table)
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
	input.SplitOptions.SplitMode = "RowOffset"
	input.SplitOptions.SplitUnit = "ByteSize"
	input.SplitOptions.SplitNumber = tableReadBatchRows
	input.SplitOptions.CrossPartition = true
	input.ArrowOptions.TimestampUnit = "nano"
	input.ArrowOptions.DatetimeUnit = "milli"
	return c.tableReadSession(ctx, operation{action: "TableCreateReadSession", target: target}, input)
}

// GetTableReadSession reopens a table snapshot, waiting for readiness with ctx.
func (c *Client) GetTableReadSession(ctx context.Context, table Table, session Session) (*ReadSession, error) {
	target, err := c.tableTarget(table)
	if err != nil {
		return nil, err
	}
	if session.ID == "" {
		return nil, fmt.Errorf("storage: session ID is required")
	}
	return c.tableReadSession(ctx, tableReadLookup(target, session), struct{}{})
}

func tableReadLookup(target string, session Session) operation {
	op := session.operation("TableGetReadSession", target)
	op.params.Set("session_refresh", "false")
	return op
}

func (c *Client) tableReadSession(ctx context.Context, op operation, input any) (*ReadSession, error) {
	stream := tableReadStreamRequest{MaxBatchRows: tableReadBatchRows, DataColumns: []string{}}
	stream.DataFormat.Type = "Arrow"
	stream.DataFormat.Version = "V5"
	for {
		var reply readResponse
		headers, err := c.jsonRequest(ctx, http.MethodPost, op, input, &reply)
		if err != nil {
			return nil, err
		}
		requestedID := op.params.Get("SessionId")
		id := reply.SessionID
		if id == "" {
			id = requestedID
		}
		switch reply.TableStatus {
		case "NORMAL", "INIT":
		default:
			return nil, fmt.Errorf("storage %s: read session %q has status %s", op.action, id, reply.TableStatus)
		}
		if id == "" || (requestedID != "" && requestedID != id) {
			return nil, fmt.Errorf("storage %s: invalid session descriptor", op.action)
		}
		if reply.TableStatus != "INIT" {
			snapshot, err := c.newReadSession(op, "TableRead", id, reply.TableStatus, reply.RecordCount, headers, stream)
			if err != nil {
				return nil, err
			}
			snapshot.tableSchema = reply.DataSchema
			return snapshot, nil
		}
		session := Session{ID: id, RouteToken: updatedRoute(headers, op.route)}
		op = tableReadLookup(op.target, session)
		input = struct{}{}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("storage: wait for read session %q: %w", id, ctx.Err())
		case <-time.After(tableReadPollInterval):
		}
	}
}

// CreateInstanceReadSession opens the result of a completed SQL instance.
func (c *Client) CreateInstanceReadSession(ctx context.Context, id string, opts InstanceReadOptions) (*ReadSession, error) {
	return c.instanceReadSession(ctx, http.MethodPost, "InstanceCreateReadSession", id, Session{}, opts)
}

// GetInstanceReadSession reopens an existing instance result snapshot.
func (c *Client) GetInstanceReadSession(ctx context.Context, id string, session Session, opts InstanceReadOptions) (*ReadSession, error) {
	if session.ID == "" {
		return nil, fmt.Errorf("storage: session ID is required")
	}
	return c.instanceReadSession(ctx, http.MethodGet, "InstanceGetReadSession", id, session, opts)
}

func (c *Client) instanceReadSession(ctx context.Context, method, action, id string, session Session, opts InstanceReadOptions) (*ReadSession, error) {
	if err := validateIdentifier("instance", id); err != nil {
		return nil, err
	}
	if opts.TaskName == "" {
		opts.TaskName = "AnonymousSQLTask"
	}
	op := session.operation(action, fmt.Sprintf("projects.%s.instances.%s", c.project, id))
	var input any
	if method == http.MethodPost {
		input = struct{ EnableLimit bool }{opts.EnableLimit}
		op.params.Del("SessionId")
	}
	var reply readResponse
	headers, err := c.jsonRequest(ctx, method, op, input, &reply)
	if err != nil {
		return nil, err
	}
	return c.newReadSession(op, "InstanceRead", reply.DownloadID, reply.Status, reply.RecordCount, headers, opts)
}

func (c *Client) newReadSession(op operation, action, id, status string, count int64, headers http.Header, input any) (*ReadSession, error) {
	if id == "" || count < 0 {
		return nil, fmt.Errorf("storage %s: invalid session descriptor", op.action)
	}
	session := Session{ID: id, RouteToken: updatedRoute(headers, op.route)}
	return &ReadSession{client: c, session: session, target: op.target, action: action, rows: count, status: status, input: input}, nil
}

// OpenReader streams a range of the snapshot. Pass nil for the entire snapshot.
// The caller owns one reference and must call Release.
func (s *ReadSession) OpenReader(ctx context.Context, rows *RowRange) (*ArrowReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Use a positive wire count when retrieving a schema from the server.
	rangeToRead := RowRange{Count: max(s.rows, 1)}
	if rows != nil {
		rangeToRead = *rows
	}
	op := s.session.operation(s.action, s.target)
	if err := rangeToRead.addTo(op.params); err != nil {
		return nil, err
	}
	// Some deployments reject every offset for an empty table snapshot.
	if s.rows == 0 && rangeToRead.Offset == 0 && s.tableSchema != nil {
		return s.tableSchema.emptyReader()
	}
	expected := min(rangeToRead.Count, max(s.rows-rangeToRead.Offset, 0))
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
