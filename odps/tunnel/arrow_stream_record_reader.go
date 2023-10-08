package tunnel

import (
	"io"
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/pkg/errors"
)

type ArrowStreamRecordReader struct {
	httpRes           *http.Response
	recordBatchReader *ipc.Reader
	columnNames       []string
}

func newArrowStreamRecordReader(res *http.Response, schema *arrow.Schema) (*ArrowStreamRecordReader, error) {
	reader, err := ipc.NewReader(res.Body, ipc.WithSchema(schema))
	if err != nil {
		return nil, err
	}

	return &ArrowStreamRecordReader{
		httpRes:           res,
		recordBatchReader: reader,
	}, nil
}

func (ar *ArrowStreamRecordReader) HttpRes() *http.Response {
	return ar.httpRes
}

func (ar *ArrowStreamRecordReader) RecordBatchReader() *ipc.Reader {
	return ar.recordBatchReader
}

func (ar *ArrowStreamRecordReader) Iterator(f func(array.Record, error)) {
	for {
		record, err := ar.recordBatchReader.Read()
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

func (ar *ArrowStreamRecordReader) Read() (array.Record, error) {
	record, err := ar.recordBatchReader.Read()
	return record, errors.WithStack(err)
}

func (ar *ArrowStreamRecordReader) Close() error {
	ar.recordBatchReader.Release()
	return errors.WithStack(ar.httpRes.Body.Close())
}
