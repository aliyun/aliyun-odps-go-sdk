package tunnel

import (
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
	"io"
	"net/http"
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

func (r *RecordArrowReader) Iterator() <-chan common.Result {
	records := make(chan common.Result)

	go func() {
		defer close(records)

		record, err := r.recordBatchReader.Read()
		result := common.Result{}

		isEOF := errors.Is(err, io.EOF)

		if err != nil && !isEOF {
			result.Error = err
			records <- result
			return
		}

		if isEOF {
			return
		}

		record.Retain()
		result.Data = record
		records <- result
	}()

	return records
}

func (r *RecordArrowReader) Read() (array.Record, error) {
	record, err := r.recordBatchReader.Read()
	return record, errors.WithStack(err)
}

func (r *RecordArrowReader) Close() error {
	r.recordBatchReader.Release()
	return errors.WithStack(r.arrowReader.Close())
}
