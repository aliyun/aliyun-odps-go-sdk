package data

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"strings"
)


type CharOverflowError struct{
	maxLen int
	realLen int
}

type Char struct {
	length int
	data  string
}

type VarChar struct {
	length int
	data   string
}

func NewCharOverflowError(maxLen, realLen int) CharOverflowError {
	return CharOverflowError{maxLen, realLen}
}

func (c CharOverflowError) Error() string {
	return fmt.Sprintf("string length is %d, bigger than the max length %d", c.realLen, c.maxLen)
}

func (c Char) Type() datatype.DataType {
	return datatype.NewCharType(c.length)
}

func NewChar(length int, data string) (*Char, error) {
	if length > 255 {
		return nil, errors.New(fmt.Sprintf("max length of char is 255, not %d is given", length))
	}

	if len(data) > length {
		return nil, NewCharOverflowError(length, len(data))
	}

	return &Char{length: length, data: data}, nil
}

func (c *Char) Value() string {
	return c.data
}

func (c *Char) String() string {
	return strings.TrimSpace(c.data)
}

func NewVarChar(length int, data string) (*VarChar, error) {
	if length > 65536 {
		return nil, errors.New(fmt.Sprintf("max length of char is 65536, not %d is given", length))
	}

	if len(data) > length {
		return nil, NewCharOverflowError(length, len(data))
	}

	return &VarChar{length: length, data: data}, nil
}

func (v *VarChar) Type() datatype.DataType {
	return datatype.NewVarcharType(v.length)
}

func (v *VarChar) Value() string {
	return v.data
}

func (v *VarChar) String() string {
	return v.data
}
