package ipc

import (
	"bytes"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/internal/debug"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"golang.org/x/xerrors"
	"io"
	"sync/atomic"
)

type RecordBatchReader struct {
	msgReader MessageReader
	schema    *arrow.Schema

	refCount int64
	rec      array.Record
	err      error

	types dictTypeMap
	memo  dictMemo

	mem memory.Allocator

	done bool
}

// NewRecordBatchReader returns a reader that reads records from an input stream.
func NewRecordBatchReader(r io.Reader, schema *arrow.Schema, opts ...Option) *RecordBatchReader {
	cfg := newConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &RecordBatchReader{
		msgReader: NewMessageReader(r, opts...),
		schema:    schema,
		refCount:  1,
		types:     make(dictTypeMap),
		memo:      newMemo(),
		mem:       cfg.alloc,
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *RecordBatchReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *RecordBatchReader) Release() {
	debug.Assert(atomic.LoadInt64(&r.refCount) > 0, "too many releases")

	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
			r.rec = nil
		}
		if r.msgReader != nil {
			r.msgReader.Release()
			r.msgReader = nil
		}
	}
}

// Next returns whether a Record could be extracted from the underlying stream.
func (r *RecordBatchReader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.err != nil || r.done {
		return false
	}

	return r.next()
}

func (r *RecordBatchReader) next() bool {
	var msg *Message
	msg, r.err = r.msgReader.Message()
	if r.err != nil {
		r.done = true
		if r.err == io.EOF {
			r.err = nil
		}
		return false
	}

	if got, want := msg.Type(), MessageRecordBatch; got != want {
		r.err = xerrors.Errorf("arrow/ipc: invalid message type (got=%v, want=%v", got, want)
		return false
	}

	r.rec = newRecord(r.schema, msg.meta, bytes.NewReader(msg.body.Bytes()), r.mem)
	return true
}

// Record returns the current record that has been extracted from the
// underlying stream.
// It is valid until the next call to Next.
func (r *RecordBatchReader) Record() array.Record {
	return r.rec
}

// Read reads the current record from the underlying stream and an error, if any.
// When the Reader reaches the end of the underlying stream, it returns (nil, io.EOF).
func (r *RecordBatchReader) Read() (array.Record, error) {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if !r.next() {
		if r.done {
			return nil, io.EOF
		}
		return nil, r.err
	}

	return r.rec, nil
}
