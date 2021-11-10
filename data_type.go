package odps

import (
	"strconv"
	"strings"
)

type DataType int

const (
	_ DataType = iota
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

	// DataTypeUnknown 未知类型
	DataTypeUnknown
)

func DataTypeFromStr(s string) DataType {
	switch strings.ToLower(s) {
	case  "bigint":
		return BIGINT
	case  "double":
		return DOUBLE
	case  "boolean":
		return BOOLEAN
	case  "datetime":
		return DATETIME
	case  "string":
		return STRING
	case  "decimal":
		return DECIMAL
	case  "map":
		return MAP
	case  "array":
		return ARRAY
	case  "void":
		return VOID
	case  "tinyint":
		return TINYINT
	case  "smallint":
		return SMALLINT
	case  "int":
		return INT
	case  "float":
		return FLOAT
	case  "char":
		return CHAR
	case  "varchar":
		return VARCHAR
	case  "date":
		return DATE
	case  "timestamp":
		return TIMESTAMP
	case  "binary":
		return BINARY
	case  "interval_day_time":
		return IntervalDayTime
	case  "interval_year_month":
		return IntervalYearMonth
	case  "struct":
		return STRUCT
	default:
		return DataTypeUnknown
	}
}

func (t *DataType) UnmarshalJSON(b []byte) error  {
	unquoted, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}

	*t = DataTypeFromStr(unquoted)

	return nil
}

func (t DataType) String() string  {
	switch t {
	case BIGINT:
		return  "bigint"
	case DOUBLE:
		return  "double"
	case BOOLEAN:
		return  "boolean"
	case DATETIME:
		return  "datetime"
	case STRING:
		return  "string"
	case DECIMAL:
		return  "decimal"
	case MAP:
		return  "map"
	case ARRAY:
		return  "array"
	case VOID:
		return  "void"
	case TINYINT:
		return  "tinyint"
	case SMALLINT:
		return  "smallint"
	case INT:
		return  "int"
	case FLOAT:
		return  "float"
	case CHAR:
		return  "char"
	case VARCHAR:
		return  "varchar"
	case DATE:
		return  "date"
	case TIMESTAMP:
		return  "timestamp"
	case BINARY:
		return  "binary"
	case IntervalDayTime:
		return  "interval_day_time"
	case IntervalYearMonth:
		return  "interval_year_month"
	case STRUCT:
		return  "struct"
	default:
		return  "datatype_unknown"
	}	
}
