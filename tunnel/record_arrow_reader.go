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
	httpReader *ArrowHttpReader
}

func newRecordArrowReader(res *http.Response, schema *arrow.Schema) RecordArrowReader  {
	httpReader := NewArrowHttpReader(res.Body)

	return RecordArrowReader{
		httpRes: res,
		recordBatchReader: ipc.NewRecordBatchReader(httpReader, schema),
		httpReader: httpReader,
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
		for r.recordBatchReader.Next() {
			record := r.recordBatchReader.Record()
			record.Retain()
			records <- record
		}

		close(records)
	}()

	return records
}

func (r *RecordArrowReader) Read() (array.Record, error) {
	return r.recordBatchReader.Read()
}


func (r *RecordArrowReader) Close() error {
	r.recordBatchReader.Release()

	return r.httpReader.Close()
}




