package datatype

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type TypeCode int

const (
	_ TypeCode = iota
	// BIGINT 8字节有符号整形
	BIGINT

	// DOUBLE 双精度浮点
	DOUBLE

	// BOOLEAN 布尔型
	BOOLEAN

	// DATETIME 日期类型
	DATETIME

	// STRING 字符串类型
	STRING

	// DECIMAL 精确小数类型
	DECIMAL

	// MAP Map类型
	MAP

	// ARRAY Array类型
	ARRAY

	// VOID 空类型
	VOID

	// TINYINT 1字节有符号整型
	TINYINT

	// SMALLINT 2字节有符号整型
	SMALLINT

	// INT 4字节有符号整型
	INT

	// FLOAT 单精度浮点
	FLOAT

	// CHAR 固定长度字符串
	CHAR

	// VARCHAR 可变长度字符串
	VARCHAR

	// DATE 时间类型
	DATE

	// TIMESTAMP 时间戳
	TIMESTAMP

	// BINARY 字节数组
	BINARY

	// IntervalDayTime 日期间隔
	IntervalDayTime

	// IntervalYearMonth 年份间隔
	IntervalYearMonth

	// STRUCT 结构体
	STRUCT

	// TypeUnknown 未知类型
	TypeUnknown
)

func TypeCodeFromStr(s string) TypeCode {
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
	default:
		return TypeUnknown
	}
}

func (t *TypeCode) UnmarshalJSON(b []byte) error {
	unquoted, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}

	*t = TypeCodeFromStr(unquoted)

	return nil
}

func (t TypeCode) String() string {
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
	default:
		return "TYPE_UNKNOWN"
	}
}

type ColumnDataType struct {
	DataType
}

type DataType interface {
	Code() TypeCode
	Name() string
}

type PrimitiveType struct {
	TypeCode TypeCode
}

func NewPrimitiveType(code TypeCode) PrimitiveType  {
	return PrimitiveType{
		TypeCode: code,
	}
}

func (p PrimitiveType) Code() TypeCode {
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

func (c CharType) Code() TypeCode {
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

func (c VarcharType) Code() TypeCode {
	return CHAR
}

func (c VarcharType) Name() string {
	return fmt.Sprintf("%s(%d)", VARCHAR, c.Length)
}

func (c VarcharType) String() string {
	return c.Name()
}

type DecimalType struct {
	Precision int
	Scale     int
}

func NewDecimalType(precision, scale int) DecimalType  {
	return DecimalType{precision, scale}
}

func (d DecimalType) Code() TypeCode {
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

func NewArrayType(elementType DataType) ArrayType  {
	return ArrayType{elementType}
}

func (a ArrayType) Code() TypeCode {
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

func (m MapType) Code() TypeCode {
	return MAP
}

func (m MapType) Name() string {
	return fmt.Sprintf("%s<%s,%s>", MAP, m.KeyType.Name(), m.ValueType.Name())
}

func (m MapType) String() string {
	return m.Name()
}

type StructType struct {
	Fields []StructField
}

func NewStructType(fields ...StructField) StructType {
	return StructType{fields}
}

func (s StructType) Code() TypeCode {
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

type StructField struct {
	Name string
	Type DataType
}

func NewStructField(name string, _type DataType) StructField  {
	return StructField{
		Name: name,
		Type: _type,
	}
}

type StructFields []StructField

func (s StructFields) Len() int {
	return len(s)
}

func (s StructFields) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s StructFields) Less(i, j int) bool {
	return strings.Compare(s[i].Name, s[j].Name) < 0
}

func IsTypeEqual(t1, t2 DataType) bool  {
	if t1.Code() != t2.Code() {
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
	}

	return true
}

func NewBigInt() PrimitiveType  {
	return PrimitiveType{BIGINT}
}

func NewDouble() PrimitiveType  {
	return PrimitiveType{DOUBLE}
}

func NewBoolean() PrimitiveType  {
	return PrimitiveType{BOOLEAN}
}

func NewDateTime() PrimitiveType  {
	return PrimitiveType{DATETIME}
}

func NewString() PrimitiveType  {
	return PrimitiveType{STRING}
}

func NewTinyint() PrimitiveType  {
	return PrimitiveType{TINYINT}
}

func NewSmallint() PrimitiveType  {
	return PrimitiveType{SMALLINT}
}

func NewInt() PrimitiveType  {
	return PrimitiveType{INT}
}

func NewFloat() PrimitiveType  {
	return PrimitiveType{FLOAT}
}

func NewDate() PrimitiveType  {
	return PrimitiveType{DATE}
}

func NewTimestamp() PrimitiveType  {
	return PrimitiveType{TIMESTAMP}
}

func NewBinary() PrimitiveType  {
	return PrimitiveType{BINARY}
}

