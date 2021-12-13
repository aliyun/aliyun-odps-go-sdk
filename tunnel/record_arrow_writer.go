package tunnel

import (
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/aliyun/aliyun-odps-go-sdk/rest_client"
	"github.com/pkg/errors"
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
		return errors.WithStack(rOrE.err)
	}

	res := rOrE.res
	if res.StatusCode/100 != 2 {
		return errors.WithStack(rest_client.NewHttpNotOk(res))
	}

	return nil
}

func (writer *RecordArrowWriter) Close() error {
	err1 := writer.recordBatchWriter.Close()
	err2 := writer.httpWriter.Close()

	if err1 != nil {
		return errors.WithStack(err1)
	}

	if err2 != nil {
		return errors.WithStack(err2)
	}

	return errors.WithStack(writer.GetHttpError())
}
