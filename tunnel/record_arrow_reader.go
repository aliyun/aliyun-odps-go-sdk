package tunnel

import (
	"github.com/fetchadd/arrow"
	"github.com/fetchadd/arrow/array"
	"github.com/fetchadd/arrow/ipc"
	"net/http"
)

type RecordArrowReader struct {
	httpRes *http.Response
	recordBatchReader *ipc.RecordBatchReader
	arrowReader       *ArrowStreamReader
}

func newRecordArrowReader(res *http.Response, schema *arrow.Schema) RecordArrowReader  {
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

func (r *RecordArrowReader) Iterator() <-chan array.Record {
	records := make(chan array.Record)

	go func() {
		defer close(records)

		for r.recordBatchReader.Next() {
			record := r.recordBatchReader.Record()
			record.Retain()
			records <- record
		}
	}()

	return records
}

func (r *RecordArrowReader) Read() (array.Record, error) {
	return r.recordBatchReader.Read()
}

func (r *RecordArrowReader) Close() error {
	r.recordBatchReader.Release()
	return r.arrowReader.Close()
}




