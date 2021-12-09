package sqldriver

import (
	"database/sql/driver"
	"github.com/aliyun/aliyun-odps-go-sdk/tunnel"
	"github.com/pkg/errors"
	"io"
)

type rowsReader struct {
	columns []string
	inner   *tunnel.RecordProtocReader
}

func (rr *rowsReader) Columns() []string {
	return rr.columns
}

func (rr *rowsReader) Close() error {
	return errors.WithStack(rr.inner.Close())
}

func (rr *rowsReader) Next(dst []driver.Value) error {
	record, err := rr.inner.Read()

	if err == io.EOF {
		return err
	}

	if err != nil {
		return errors.WithStack(err)
	}

	if record.Len() != len(dst) {
		return errors.Errorf("expect %d columns, but get %d", len(dst), record.Len())
	}

	for i := range dst {
		dst[i] = record.Get(i)
	}

	return nil
}
