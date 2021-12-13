package ipc

import (
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/internal/flatbuf"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"golang.org/x/xerrors"
	"io"
)

type RecordBatchWriter struct {
	w io.Writer

	mem memory.Allocator
	pw  PayloadWriter

	schema     *arrow.Schema
	codec      flatbuf.CompressionType
	compressNP int
}

func NewRecordBatchWriter(w io.Writer, opts ...Option) *RecordBatchWriter {
	cfg := newConfig(opts...)
	return &RecordBatchWriter{
		w:          w,
		mem:        cfg.alloc,
		pw:         &swriter{w: w},
		schema:     cfg.schema,
		codec:      cfg.codec,
		compressNP: cfg.compressNP,
	}
}

func (w *RecordBatchWriter) Write(rec array.Record) error {
	schema := rec.Schema()
	if schema == nil || !schema.Equal(w.schema) {
		return errInconsistentSchema
	}

	const allow64b = true
	var (
		data = Payload{msg: MessageRecordBatch}
		enc  = newRecordEncoder(w.mem, 0, kMaxNestingDepth, allow64b, w.codec, w.compressNP)
	)
	defer data.Release()

	if err := enc.Encode(&data, rec); err != nil {
		return xerrors.Errorf("arrow/ipc: could not encode record to payload: %w", err)
	}

	return w.pw.WritePayload(data)
}

func (w *RecordBatchWriter) Close() error {
	w.pw = nil
	return nil
}
