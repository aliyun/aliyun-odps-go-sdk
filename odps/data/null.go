package data

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

type NullData struct{}

var Null = NullData{}

func (n NullData) Type() datatype.DataType {
	return datatype.NullType
}

func (n NullData) String() string {
	return "NULL"
}

func (n NullData) Sql() string {
	return "NULL"
}

func (n NullData) Scan(interface{}) error {
	return nil
}
