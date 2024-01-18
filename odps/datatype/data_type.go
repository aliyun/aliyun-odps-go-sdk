// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datatype

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type TypeID int

const (
	NULL TypeID = iota
	BIGINT
	DOUBLE
	BOOLEAN
	DATETIME
	STRING
	DECIMAL
	MAP
	ARRAY
	VOID
	TINYINT
	SMALLINT
	INT
	FLOAT
	CHAR
	VARCHAR
	DATE
	TIMESTAMP
	BINARY
	IntervalDayTime
	IntervalYearMonth
	STRUCT
	JSON
	OBJECT
	SLICE
	TypeUnknown
)

func TypeCodeFromStr(s string) TypeID {
	switch strings.ToUpper(s) {
	case "BIGINT":
		return BIGINT
	case "DOUBLE":
		return DOUBLE
	case "BOOLEAN":
		return BOOLEAN
	case "DATETIME":
		return DATETIME
	case "STRING":
		return STRING
	case "DECIMAL":
		return DECIMAL
	case "MAP":
		return MAP
	case "ARRAY":
		return ARRAY
	case "VOID":
		return VOID
	case "TINYINT":
		return TINYINT
	case "SMALLINT":
		return SMALLINT
	case "INT":
		return INT
	case "FLOAT":
		return FLOAT
	case "CHAR":
		return CHAR
	case "VARCHAR":
		return VARCHAR
	case "DATE":
		return DATE
	case "TIMESTAMP":
		return TIMESTAMP
	case "BINARY":
		return BINARY
	case "INTERVAL_DAY_TIME":
		return IntervalDayTime
	case "INTERVAL_YEAR_MONTH":
		return IntervalYearMonth
	case "STRUCT":
		return STRUCT
	case "JSON":
		return JSON
	case "OBJECT":
		return OBJECT
	case "SLICE":
		return SLICE
	default:
		return TypeUnknown
	}
}

func (t *TypeID) UnmarshalJSON(b []byte) error {
	unquoted, err := strconv.Unquote(string(b))
	if err != nil {
		return errors.WithStack(err)
	}

	*t = TypeCodeFromStr(unquoted)

	return nil
}

func (t TypeID) String() string {
	switch t {
	case BIGINT:
		return "BIGINT"
	case DOUBLE:
		return "DOUBLE"
	case BOOLEAN:
		return "BOOLEAN"
	case DATETIME:
		return "DATETIME"
	case STRING:
		return "STRING"
	case DECIMAL:
		return "DECIMAL"
	case MAP:
		return "MAP"
	case ARRAY:
		return "ARRAY"
	case VOID:
		return "VOID"
	case TINYINT:
		return "TINYINT"
	case SMALLINT:
		return "SMALLINT"
	case INT:
		return "INT"
	case FLOAT:
		return "FLOAT"
	case CHAR:
		return "CHAR"
	case VARCHAR:
		return "VARCHAR"
	case DATE:
		return "DATE"
	case TIMESTAMP:
		return "TIMESTAMP"
	case BINARY:
		return "BINARY"
	case IntervalDayTime:
		return "INTERVAL_DAY_TIME"
	case IntervalYearMonth:
		return "INTERVAL_YEAR_MONTH"
	case STRUCT:
		return "STRUCT"
	case JSON:
		return "JSON"
	case OBJECT:
		return "OBJECT"
	case SLICE:
		return "SLICE"
	default:
		return "TYPE_UNKNOWN"
	}
}

type ColumnDataType struct {
	DataType
}

type DataType interface {
	ID() TypeID
	Name() string
}

type PrimitiveType struct {
	TypeCode TypeID
}

func NewPrimitiveType(code TypeID) PrimitiveType {
	return PrimitiveType{
		TypeCode: code,
	}
}

func (p PrimitiveType) ID() TypeID {
	return p.TypeCode
}

func (p PrimitiveType) Name() string {
	return p.TypeCode.String()
}

func (p PrimitiveType) String() string {
	return p.Name()
}

type CharType struct {
	Length int
}

func NewCharType(length int) CharType {
	return CharType{length}
}

func (c CharType) ID() TypeID {
	return CHAR
}

func (c CharType) Name() string {
	return fmt.Sprintf("%s(%d)", CHAR, c.Length)
}

func (c CharType) String() string {
	return c.Name()
}

type VarcharType struct {
	Length int
}

func NewVarcharType(length int) VarcharType {
	return VarcharType{length}
}

func (c VarcharType) ID() TypeID {
	return VARCHAR
}

func (c VarcharType) Name() string {
	return fmt.Sprintf("%s(%d)", VARCHAR, c.Length)
}

func (c VarcharType) String() string {
	return c.Name()
}

type DecimalType struct {
	Precision int32
	Scale     int32
}

func NewDecimalType(precision, scale int32) DecimalType {
	return DecimalType{precision, scale}
}

func (d DecimalType) ID() TypeID {
	return DECIMAL
}

func (d DecimalType) Name() string {
	return fmt.Sprintf("%s(%d,%d)", DECIMAL, d.Precision, d.Scale)
}

func (d DecimalType) String() string {
	return d.Name()
}

type ArrayType struct {
	ElementType DataType
}

func NewArrayType(elementType DataType) ArrayType {
	return ArrayType{elementType}
}

func (a ArrayType) ID() TypeID {
	return ARRAY
}

func (a ArrayType) Name() string {
	return fmt.Sprintf("%s<%s>", ARRAY, a.ElementType.Name())
}

func (a ArrayType) String() string {
	return a.Name()
}

type MapType struct {
	KeyType   DataType
	ValueType DataType
}

func NewMapType(keyType, valueType DataType) MapType {
	return MapType{keyType, valueType}
}

func (m MapType) ID() TypeID {
	return MAP
}

func (m MapType) Name() string {
	return fmt.Sprintf("%s<%s,%s>", MAP, m.KeyType.Name(), m.ValueType.Name())
}

func (m MapType) String() string {
	return m.Name()
}

type StructType struct {
	Fields []StructFieldType
}

func NewStructType(fields ...StructFieldType) StructType {
	return StructType{fields}
}

func (s StructType) ID() TypeID {
	return STRUCT
}

func (s StructType) Name() string {
	var sb strings.Builder
	var n = len(s.Fields) - 1

	sb.WriteString(STRUCT.String())
	sb.WriteString("<")

	for i, field := range s.Fields {
		sb.WriteString(field.Name)
		sb.WriteString(":")
		sb.WriteString(field.Type.Name())

		if i < n {
			sb.WriteString(",")
		}
	}

	sb.WriteString(">")
	return sb.String()
}

func (s StructType) String() string {
	return s.Name()
}

func (s StructType) FieldType(fileName string) DataType {
	for _, f := range s.Fields {
		if f.Name == fileName {
			return f.Type
		}
	}

	return nil
}

type StructFieldType struct {
	Name string
	Type DataType
}

func NewStructFieldType(name string, _type DataType) StructFieldType {
	return StructFieldType{
		Name: name,
		Type: _type,
	}
}

type StructFields []StructFieldType

func (s StructFields) Len() int {
	return len(s)
}

func (s StructFields) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s StructFields) Less(i, j int) bool {
	return strings.Compare(s[i].Name, s[j].Name) < 0
}

type JsonType struct {
	elementType DataType
}

func NewJsonType(elementType DataType) JsonType {
	switch elementType {
	case BooleanType, StringType, NullType, IntType, DoubleType, ObjectType, SliceType:
		return JsonType{
			elementType: elementType,
		}
	}

	return JsonType{}
}

func (j JsonType) ID() TypeID {
	return JSON
}

func (j JsonType) Name() string {
	return JSON.String()
}

func (j JsonType) GetElementType() DataType {
	return j.elementType
}

func IsTypeEqual(t1, t2 DataType) bool {
	if t1 == nil || t2 == nil {
		return false
	}

	switch t1.ID() {
	case ARRAY, MAP:
		if IsNullType(t2) {
			return true
		}
	case NULL:
		t2Id := t2.ID()
		return t2Id == ARRAY || t2Id == MAP || t2Id == NULL
	}

	if t1.ID() != t2.ID() {
		return false
	}

	switch r1 := t1.(type) {
	case StructType:
		r2, _ := t2.(StructType)
		if len(r1.Fields) != len(r2.Fields) {
			return false
		}

		fields1 := make(StructFields, len(r1.Fields))
		fields2 := make(StructFields, len(r1.Fields))
		copy(fields1, r1.Fields)
		copy(fields2, r2.Fields)

		sort.Sort(fields1)
		sort.Sort(fields2)

		for i := range fields1 {
			f1, f2 := fields1[i], fields2[i]

			if f1.Name != f2.Name {
				return false
			}

			return IsTypeEqual(f1.Type, f2.Type)
		}
	case MapType:
		r2, _ := t2.(MapType)
		return IsTypeEqual(r1.KeyType, r2.KeyType) &&
			IsTypeEqual(r1.ValueType, r2.ValueType)
	case ArrayType:
		r2, _ := t2.(ArrayType)
		return IsTypeEqual(r1.ElementType, r2.ElementType)
	case CharType:
		r2, _ := t2.(CharType)
		return r1.Length == r2.Length
	case VarcharType:
		r2, _ := t2.(VarcharType)
		return r1.Length == r2.Length
	case JsonType:
		j1, _ := t1.(JsonType)
		j2, _ := t2.(JsonType)
		return IsTypeEqual(j1.elementType, j2.elementType)
	}

	return true
}

var TinyIntType = PrimitiveType{TINYINT}
var SmallIntType = PrimitiveType{SMALLINT}
var IntType = PrimitiveType{INT}
var BigIntType = PrimitiveType{BIGINT}
var DoubleType = PrimitiveType{DOUBLE}
var BooleanType = PrimitiveType{BOOLEAN}
var DateType = PrimitiveType{DATE}
var DateTimeType = PrimitiveType{DATETIME}
var TimestampType = PrimitiveType{TIMESTAMP}
var StringType = PrimitiveType{STRING}
var FloatType = PrimitiveType{FLOAT}
var BinaryType = PrimitiveType{BINARY}
var IntervalDayTimeType = PrimitiveType{IntervalDayTime}
var IntervalYearMonthType = PrimitiveType{IntervalYearMonth}
var NullType = PrimitiveType{NULL}
var ObjectType = PrimitiveType{OBJECT}
var SliceType = PrimitiveType{SLICE}

func IsNullType(t DataType) bool {
	return t.ID() == NULL
}
