// Licensed under the Apache License, Version 2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// TableWriteOptions configures a batch upload and its atomic commit.
type TableWriteOptions struct {
	Partition string
	Overwrite bool
}

// WriteSession stages one or more Arrow streams for an atomic table commit.
type WriteSession struct {
	client  *Client
	session Session
	target  string
	status  string
}

// Session returns the descriptor for reopening the upload.
func (s *WriteSession) Session() Session { return s.session }

// Status returns the server's status as reported at open time.
func (s *WriteSession) Status() string { return s.status }

// CreateTableWriteSession opens a batch upload.
func (c *Client) CreateTableWriteSession(ctx context.Context, table Table, opts TableWriteOptions) (*WriteSession, error) {
	input := struct {
		PartialPartitionSpec string
		Flags                map[string]string
	}{opts.Partition, map[string]string{"overwrite": strconv.FormatBool(opts.Overwrite)}}
	return c.tableWriteSession(ctx, table, "TableCreateWriteSession", Session{}, input)
}

// GetTableWriteSession reopens an upload, for example after a process restart.
func (c *Client) GetTableWriteSession(ctx context.Context, table Table, session Session) (*WriteSession, error) {
	if session.ID == "" {
		return nil, fmt.Errorf("storage: session ID is required")
	}
	return c.tableWriteSession(ctx, table, "TableGetWriteSession", session, struct{}{})
}

func (c *Client) tableWriteSession(ctx context.Context, table Table, action string, session Session, input any) (*WriteSession, error) {
	target, err := c.tableTarget(table)
	if err != nil {
		return nil, err
	}
	op := session.operation(action, target)
	if session.ID == "" {
		op.params.Del("SessionId")
	}
	op.params.Set("WriteMode", "Batch")
	reply := struct {
		ID     string `json:"SessionId"`
		Status string `json:"SessionStatus"`
	}{}
	headers, err := c.jsonRequest(ctx, http.MethodPost, op, input, &reply)
	if err != nil {
		return nil, err
	}
	if reply.ID == "" {
		return nil, fmt.Errorf("storage %s: missing session ID", action)
	}
	return &WriteSession{
		client: c, target: target, status: reply.Status,
		session: Session{ID: reply.ID, RouteToken: updatedRoute(headers, session.RouteToken)},
	}, nil
}

func updatedRoute(headers http.Header, previous string) string {
	if route := headers.Get(routeTokenHeader); route != "" {
		return route
	}
	return previous
}

func (s *WriteSession) operation(action string) operation {
	op := s.session.operation(action, s.target)
	op.params.Set("WriteMode", "Batch")
	return op
}

// Commit publishes all closed streams. Repeat a commit to resolve an ambiguous
// transport failure, using the same session descriptor.
func (s *WriteSession) Commit(ctx context.Context) error {
	_, err := s.client.jsonRequest(ctx, http.MethodPost, s.operation("TableCommitWriteSession"), struct{}{}, nil)
	return err
}

// Abort discards the staged rows.
func (s *WriteSession) Abort(ctx context.Context) error {
	_, err := s.client.jsonRequest(ctx, http.MethodPost, s.operation("TableAbortWriteSession"), struct{}{}, nil)
	return err
}

// Stream identifies a writer attempt within a session.
type Stream struct {
	ID      string `json:"StreamId"`
	Version int64  `json:"StreamVersion"`
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
	if stream.ID == "" || stream.Version < 0 {
		return nil, fmt.Errorf("storage: stream ID is required and version must be nonnegative")
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
	return &WriteStream{
		session: s, stream: stream, route: updatedRoute(headers, s.session.RouteToken),
		accessToken: reply.AccessToken, offset: reply.RowOffset,
	}, nil
}

func setStreamParams(op operation, stream Stream) {
	op.params.Set("StreamId", stream.ID)
	op.params.Set("StreamVersion", strconv.FormatInt(stream.Version, 10))
}

// WriteBatch stages one nonempty batch at offset and returns the next durable
// offset. On an ambiguous failure retry the same batch at the same offset.
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
	}{w.session.session.ID, w.stream}
	_, err := w.session.client.jsonRequest(ctx, http.MethodPost, op, input, nil)
	return err
}
