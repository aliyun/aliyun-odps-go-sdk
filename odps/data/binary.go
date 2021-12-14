package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type Binary []byte

func (b Binary) Type() datatype.DataType {
	return datatype.BinaryType
}

func (b Binary) String() string {
	return fmt.Sprintf("unhex('%X')", []byte(b))
}

func (b Binary) Sql() string {
	return b.String()
}

func (b *Binary) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, b))
}
