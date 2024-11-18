package data

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

type Json struct {
	Data  string
	Valid bool
}

func NewJson(value interface{}) (*Json, error) {
	byteArr, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	d := string(byteArr)

	return &Json{
		Data:  d,
		Valid: true,
	}, nil
}

func (j Json) Type() datatype.DataType {
	return datatype.JsonType{}
}

func (j Json) String() string {
	var sb strings.Builder
	sb.WriteString(j.Data)

	return sb.String()
}

func (j Json) Sql() string {
	var sb strings.Builder
	sb.WriteString("JSON'")
	sb.WriteString(j.Data)
	sb.WriteString("'")

	return sb.String()
}

func (j *Json) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, j))
}

func (j *Json) GetData() string {
	return j.Data
}
