package tunnel

import (
	"io"
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/pkg/errors"
)

type RecordArrowReader struct {
	httpRes           *http.Response
	recordBatchReader *ipc.RecordBatchReader
	arrowReader       *ArrowStreamReader
}

func newRecordArrowReader(res *http.Response, schema *arrow.Schema) RecordArrowReader {
	httpReader := NewArrowStreamReader(res.Body)

	return RecordArrowReader{
		httpRes:           res,
		recordBatchReader: ipc.NewRecordBatchReader(httpReader, schema),
		arrowReader:       httpReader,
	}
}

func (r *RecordArrowReader) HttpRes() *http.Response {
	return r.httpRes
}

func (r *RecordArrowReader) RecordBatchReader() *ipc.RecordBatchReader {
	return r.recordBatchReader
}

func (r *RecordArrowReader) Iterator(f func(array.Record, error)) {
	for {
		record, err := r.recordBatchReader.Read()
		isEOF := errors.Is(err, io.EOF)
		if isEOF {
			return
		}
		if err != nil {
			f(record, err)
			return
		}

		record.Retain()
		f(record, err)
	}
}

func (r *RecordArrowReader) Read() (array.Record, error) {
	record, err := r.recordBatchReader.Read()
	return record, errors.WithStack(err)
}

func (r *RecordArrowReader) Close() error {
	r.recordBatchReader.Release()
	return errors.WithStack(r.arrowReader.Close())
}
