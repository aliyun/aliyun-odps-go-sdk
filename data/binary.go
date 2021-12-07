package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
)

type Binary []byte

func (b Binary) Type() datatype.DataType {
	return datatype.BinaryType
}

func (b Binary) String() string {
	return fmt.Sprintf("unhex('%X')", []byte(b))
}