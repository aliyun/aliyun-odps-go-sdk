// Licensed under the Apache License, Version 2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// TableWriteOptions configures a batch upload and its atomic commit.
type TableWriteOptions struct {
	Partition string // Partition spec, for example ds='20261120',hour='00'.
	Overwrite bool
}

// WriteSession stages one or more Arrow streams for an atomic table commit.
type WriteSession struct {
	client     *Client
	descriptor WriteSessionDescriptor
	status     string
	streams    map[string]int64
}

// Session returns a compatibility identifier. Persist Descriptor instead when
// restoring a write session in another process.
func (s *WriteSession) Session() Session { return s.descriptor.session() }

// Descriptor identifies the table, mode and route for ResumeWrite.
func (s *WriteSession) Descriptor() WriteSessionDescriptor { return s.descriptor }

// Status returns the server's reported status. An empty value means it was not reported.
func (s *WriteSession) Status() string { return s.status }

// ClosedStreams returns the stream versions reported at open or refresh time,
// ordered by stream ID. It cannot enumerate streams that are still open.
func (s *WriteSession) ClosedStreams() []Stream {
	streams := make([]Stream, 0, len(s.streams))
	for id, version := range s.streams {
		streams = append(streams, Stream{ID: id, Version: version})
	}
	slices.SortFunc(streams, func(a, b Stream) int { return strings.Compare(a.ID, b.ID) })
	return streams
}

// Refresh looks up the current status and closed streams without changing the receiver.
func (s *WriteSession) Refresh(ctx context.Context) (*WriteSession, error) {
	return s.client.ResumeWrite(ctx, s.descriptor)
}

// CreateTableWriteSession opens a batch upload.
func (c *Client) CreateTableWriteSession(ctx context.Context, table Table, opts TableWriteOptions) (*WriteSession, error) {
	table, err := c.tableRef(table)
	if err != nil {
		return nil, err
	}
	partition := ""
	if opts.Partition != "" {
		partition, err = normalizePartitionSpec(opts.Partition)
		if err != nil {
			return nil, err
		}
	}
	input := struct {
		PartialPartitionSpec string
		Flags                map[string]string
	}{partition, map[string]string{"overwrite": strconv.FormatBool(opts.Overwrite)}}
	descriptor := WriteSessionDescriptor{Version: descriptorVersion, Table: table, QuotaName: c.quota, Mode: WriteModeBatch}
	op := writeOperation(descriptor, "TableCreateWriteSession")
	op.params.Del("SessionId")
	return c.requestWriteSession(ctx, descriptor, op, input)
}

// ResumeWrite reopens an upload using the original table, quota and write mode.
func (c *Client) ResumeWrite(ctx context.Context, descriptor WriteSessionDescriptor) (*WriteSession, error) {
	if err := c.validateDescriptor(descriptor.Version, descriptor.ID, descriptor.Table.Project, descriptor.QuotaName); err != nil {
		return nil, err
	}
	if descriptor.Mode != WriteModeBatch || descriptor.Table.Schema == "" {
		return nil, fmt.Errorf("storage: invalid batch write descriptor")
	}
	if _, err := c.tableRef(descriptor.Table); err != nil {
		return nil, err
	}
	return c.requestWriteSession(ctx, descriptor, writeOperation(descriptor, "TableGetWriteSession"), struct{}{})
}

// GetTableWriteSession is the compatibility form of ResumeWrite.
func (c *Client) GetTableWriteSession(ctx context.Context, table Table, session Session) (*WriteSession, error) {
	if session.ID == "" {
		return nil, fmt.Errorf("storage: session ID is required")
	}
	table, err := c.tableRef(table)
	if err != nil {
		return nil, err
	}
	return c.ResumeWrite(ctx, WriteSessionDescriptor{
		Version: descriptorVersion, Table: table, QuotaName: c.quota, Mode: WriteModeBatch,
		ID: session.ID, RouteToken: session.RouteToken,
	})
}

func (c *Client) requestWriteSession(ctx context.Context, descriptor WriteSessionDescriptor, op operation, input any) (*WriteSession, error) {
	reply := struct {
		ID           string `json:"SessionId"`
		SessionState string `json:"SessionStatus"`
		Status       string
		Streams      map[string]int64
	}{}
	headers, err := c.jsonRequest(ctx, http.MethodPost, op, input, &reply)
	if err != nil {
		return nil, err
	}
	// A lookup can omit the session's ID and status.
	if reply.ID != "" {
		if descriptor.ID != "" && descriptor.ID != reply.ID {
			return nil, fmt.Errorf("storage %s: unexpected session ID", op.action)
		}
		descriptor.ID = reply.ID
	}
	if descriptor.ID == "" {
		return nil, fmt.Errorf("storage %s: missing session ID", op.action)
	}
	for id, version := range reply.Streams {
		if err := (Stream{ID: id, Version: version}).validate(); err != nil {
			return nil, fmt.Errorf("storage %s: invalid closed stream: %w", op.action, err)
		}
	}
	descriptor.RouteToken = updatedRoute(headers, descriptor.RouteToken)
	status := reply.SessionState
	if status == "" {
		status = reply.Status
	}
	return &WriteSession{client: c, descriptor: descriptor, status: status, streams: reply.Streams}, nil
}

func updatedRoute(headers http.Header, previous string) string {
	if route := headers.Get(routeTokenHeader); route != "" {
		return route
	}
	return previous
}

func writeOperation(descriptor WriteSessionDescriptor, action string) operation {
	op := descriptor.session().operation(action, tableResource(descriptor.Table))
	op.params.Set("WriteMode", string(descriptor.Mode))
	return op
}

func (s *WriteSession) operation(action string) operation {
	return writeOperation(s.descriptor, action)
}

// Commit publishes all closed streams on tables that support implicit stream
// selection. Use CommitStreams when the table requires explicit stream versions.
// Repeat the same commit on the same session after an ambiguous transport failure.
func (s *WriteSession) Commit(ctx context.Context) error {
	_, err := s.client.jsonRequest(ctx, http.MethodPost, s.operation("TableCommitWriteSession"), struct{}{}, nil)
	return err
}

// CommitStreams commits a session with explicit stream versions. Finish creating
// and closing streams before calling it. Pass the complete list of intended
// streams. The SDK compares it with the closed streams reported by the service.
// Open streams may be absent from that response. Use a separate session for
// each independently committed set of rows.
func (s *WriteSession) CommitStreams(ctx context.Context, streams ...Stream) error {
	if len(streams) == 0 {
		return fmt.Errorf("storage: at least one stream is required to commit")
	}
	input := struct {
		StreamIDs      []string `json:"StreamIds"`
		StreamVersions []int64
	}{
		StreamIDs:      make([]string, len(streams)),
		StreamVersions: make([]int64, len(streams)),
	}
	versions := make(map[string]int64, len(streams))
	for i, stream := range streams {
		if err := stream.validate(); err != nil {
			return err
		}
		if _, duplicate := versions[stream.ID]; duplicate {
			return fmt.Errorf("storage: stream %q can only be committed once per request", stream.ID)
		}
		versions[stream.ID] = stream.Version
		input.StreamIDs[i] = stream.ID
		input.StreamVersions[i] = stream.Version
	}
	current, err := s.Refresh(ctx)
	if err != nil {
		return err
	}
	if !maps.Equal(current.streams, versions) {
		return fmt.Errorf("storage: commit descriptors must match all streams and versions in the write session")
	}
	_, err = s.client.jsonRequest(ctx, http.MethodPost, current.operation("TableCommitWriteSession"), input, nil)
	return err
}

// Abort discards the staged rows.
func (s *WriteSession) Abort(ctx context.Context) error {
	_, err := s.client.jsonRequest(ctx, http.MethodPost, s.operation("TableAbortWriteSession"), struct{}{}, nil)
	return err
}

// Stream identifies a writer attempt within a session.
type Stream struct {
	// ID must contain only ASCII letters, digits, and hyphens.
	ID string `json:"StreamId"`
	// Version must be positive.
	Version int64 `json:"StreamVersion"`
}

var streamIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

func (s Stream) validate() error {
	if s.ID == "" || s.Version <= 0 {
		return fmt.Errorf("storage: stream ID is required and version must be positive")
	}
	if !streamIDPattern.MatchString(s.ID) {
		return fmt.Errorf("storage: stream ID must contain only ASCII letters, digits, and hyphens")
	}
	return nil
}

// WriteStream stages Arrow batches using explicit, replayable row offsets.
type WriteStream struct {
	session     *WriteSession
	stream      Stream
	route       string
	accessToken string
	offset      int64
}

// InitialOffset is the durable row offset reported when the stream was opened.
func (w *WriteStream) InitialOffset() int64 { return w.offset }

// CreateStream opens an exactly-once write stream.
func (s *WriteSession) CreateStream(ctx context.Context, stream Stream) (*WriteStream, error) {
	return s.openStream(ctx, stream, "TableCreateWriteStream")
}

// GetStream reopens a stream and retrieves the server's durable row offset.
func (s *WriteSession) GetStream(ctx context.Context, stream Stream) (*WriteStream, error) {
	return s.openStream(ctx, stream, "TableGetWriteStream")
}

func (s *WriteSession) openStream(ctx context.Context, stream Stream, action string) (*WriteStream, error) {
	if err := stream.validate(); err != nil {
		return nil, err
	}
	op := s.operation(action)
	input := struct {
		Stream
		ExactlyOnceMode bool
	}{stream, true}
	if action == "TableGetWriteStream" {
		setStreamParams(op, stream)
		op.params.Set("ExactlyOnceMode", "true")
	}
	reply := struct {
		AccessToken string
		RowOffset   int64
	}{}
	headers, err := s.client.jsonRequest(ctx, http.MethodPost, op, input, &reply)
	if err != nil {
		return nil, err
	}
	if reply.RowOffset < 0 {
		return nil, fmt.Errorf("storage: negative durable row offset")
	}
	return &WriteStream{
		session: s, stream: stream, route: updatedRoute(headers, s.descriptor.RouteToken),
		accessToken: reply.AccessToken, offset: reply.RowOffset,
	}, nil
}

func setStreamParams(op operation, stream Stream) {
	op.params.Set("StreamId", stream.ID)
	op.params.Set("StreamVersion", strconv.FormatInt(stream.Version, 10))
}

// WriteBatch stages one nonempty batch at offset and returns the next durable
// offset. On an ambiguous failure retry the same batch at the same offset.
// The service may deduplicate by offset without verifying the payload.
// The caller retains ownership of batch.
func (w *WriteStream) WriteBatch(ctx context.Context, offset int64, batch arrow.RecordBatch) (int64, error) {
	if batch == nil || batch.NumRows() <= 0 {
		return 0, fmt.Errorf("storage: a nonempty Arrow batch is required")
	}
	count := batch.NumRows()
	if offset < 0 || offset > math.MaxInt64-count {
		return 0, fmt.Errorf("storage: invalid write offset")
	}

	var body bytes.Buffer
	ipcWriter := ipc.NewWriter(&body)
	encodeErr := ipcWriter.Write(batch)
	if closeErr := ipcWriter.Close(); encodeErr == nil {
		encodeErr = closeErr
	}
	if encodeErr != nil {
		return 0, fmt.Errorf("storage: encode Arrow batch: %w", encodeErr)
	}
	op := w.session.operation("TableWrite")
	op.route = w.route
	setStreamParams(op, w.stream)
	op.params.Set("Count", strconv.FormatInt(count, 10))
	op.params.Set("RowOffset", strconv.FormatInt(offset, 10))
	headers := map[string]string{"Content-Type": "application/octet-stream"}
	if w.accessToken != "" {
		headers["x-odps-max-storage-write-access-token"] = w.accessToken
	}
	resp, err := w.session.client.request(ctx, http.MethodPost, op, bytes.NewReader(body.Bytes()), headers)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	reply := struct{ ExactlyOnceRowOffset int64 }{}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return 0, fmt.Errorf("storage TableWrite: decode response: %w", err)
	}
	if reply.ExactlyOnceRowOffset < offset+count {
		return 0, fmt.Errorf("storage TableWrite: server offset %d is before the end of the batch", reply.ExactlyOnceRowOffset)
	}
	return reply.ExactlyOnceRowOffset, nil
}

// Close marks a stream ready for commit.
func (w *WriteStream) Close(ctx context.Context) error {
	op := w.session.operation("TableCloseWriteStream")
	op.route = w.route
	input := struct {
		SessionID string `json:"SessionId"`
		Stream
	}{w.session.descriptor.ID, w.stream}
	_, err := w.session.client.jsonRequest(ctx, http.MethodPost, op, input, nil)
	return err
}
