package tunnel

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/fetchadd/arrow"
	"github.com/fetchadd/arrow/array"
	"github.com/fetchadd/arrow/ipc"
)

type RecordArrowWriter struct {
	resChan           <-chan resOrErr
	schema            *arrow.Schema
	recordBatchWriter *ipc.RecordBatchWriter
	httpWriter        *ArrowStreamWriter
}

func newRecordArrowWriter(conn *httpConnection, schema *arrow.Schema) RecordArrowWriter {
	httpWriter := NewArrowStreamWriter(conn.Writer)

	return RecordArrowWriter{
		resChan:           conn.resChan,
		schema:            schema,
		recordBatchWriter: ipc.NewRecordBatchWriter(httpWriter, ipc.WithSchema(schema)),
		httpWriter:        httpWriter,
	}
}

func (writer *RecordArrowWriter) WriteArrowRecord(record array.Record) error {
	return writer.recordBatchWriter.Write(record)
}

func (writer *RecordArrowWriter) GetHttpError() error {
	rOrE := <-writer.resChan

	if rOrE.err != nil {
		return rOrE.err
	}

	res := rOrE.res
	if res.StatusCode/100 != 2 {
		return odps.NewHttpNotOk(res)
	}

	return nil
}

func (writer *RecordArrowWriter) Close() error {
	err1 := writer.recordBatchWriter.Close()
	err2 := writer.httpWriter.Close()

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	return writer.GetHttpError()
}
