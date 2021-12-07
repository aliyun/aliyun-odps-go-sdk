package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
)

type Data interface {
	Type() datatype.DataType
	fmt.Stringer
}

