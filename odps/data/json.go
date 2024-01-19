package data

import (
	"encoding/json"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type Json struct {
	data  string
	Valid bool
}

func NewJson(value interface{}) *Json {
	byteArr, err := json.Marshal(value)
	if err != nil {
		return &Json{}
	}
	d := string(byteArr)

	return &Json{
		data:  d,
		Valid: true,
	}
}

func (j Json) Type() datatype.DataType {
	return datatype.JsonType{}
}

func (j Json) String() string {
	var sb strings.Builder
	sb.WriteString("\"")
	sb.WriteString(j.data)
	sb.WriteString("\"")

	return sb.String()
}

func (j Json) Sql() string {
	var sb strings.Builder
	sb.WriteString("JSON '")
	sb.WriteString(j.data)
	sb.WriteString("'")

	return sb.String()
}

func (j *Json) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, j))
}

func (j *Json) GetData() string {
	return j.data
}
