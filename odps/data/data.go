package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"reflect"
)

type Data interface {
	Type() datatype.DataType
	fmt.Stringer
	Sql() string
}

func IsDataEqual(d1 Data, d2 Data) bool {
	if reflect.DeepEqual(d1, d2) {
		return true
	}

	return isMapEqual(d1, d2)
}

func isMapEqual(d1 Data, d2 Data) bool {
	m1, ok := d1.(*Map)
	if !ok {
		return false
	}

	m2, ok := d2.(*Map)
	if !ok {
		return false
	}

	if len(m1.data) != len(m2.data) {
		return false
	}

	for key, value1 := range m1.data {
		value2, ok := m2.data[key]
		if !ok {
			return false
		}

		if !IsDataEqual(value1, value2) {
			return false
		}
	}

	return true
}
