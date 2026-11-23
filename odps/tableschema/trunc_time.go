package tableschema

import (
	"fmt"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
)

// TruncTime Implementation class of GenerateExpression, corresponding to SQL trunc_time('dateColumnName', 'datePart') syntax
type TruncTime struct {
	dateColumnName string
	datePart       DatePart
}

// DatePart Enumeration value, used to represent the unit of time type trunc
type DatePart int

const (
	_     DatePart = iota
	YEAR           // YEAR trunc time to string like YYYY
	MONTH          // MONTH trunc time to string like YYYY-MM
	DAY            // DAY trunc time to string like YYYY-MM-DD
	HOUR           // HOUR trunc time to string like YYYY-MM-DD HH:00:00
)

func (p DatePart) String() string {
	switch p {
	case YEAR:
		return "year"
	case MONTH:
		return "month"
	case DAY:
		return "day"
	case HOUR:
		return "hour"
	default:
		return "unknown"
	}
}

const (
	name           = "trunc_time"
	nullValue      = "__NULL__"
	minEpochMillis = -315619200000 // MinEpochMillis Timestamp of '1960-01-01'
)

// NewTruncTime Create a TruncTime instance based on column name and date part
func NewTruncTime(dateColumnName string, datePart DatePart) *TruncTime {
	return &TruncTime{dateColumnName: dateColumnName, datePart: datePart}
}

func newTruncTimeWithConstant(dateColumnName string, constant string) (*TruncTime, error) {
	datePart, err := stringToDatePart(constant)
	if err != nil {
		return nil, err
	}
	if dateColumnName == "" {
		return nil, fmt.Errorf("TruncTime dateColumnName cannot be empty")
	}
	return NewTruncTime(dateColumnName, datePart), nil
}

func stringToDatePart(constant string) (DatePart, error) {
	constant = strings.ToLower(constant)
	switch constant {
	case "year":
		return YEAR, nil
	case "month":
		return MONTH, nil
	case "day":
		return DAY, nil
	case "hour":
		return HOUR, nil
	default:
		return -1, fmt.Errorf("unknown date part: %s", constant)
	}
}

func (t *TruncTime) generate(record *data.Record, schema *TableSchema) (string, error) {
	if record == nil {
		return nullValue, fmt.Errorf("missing record for trunc_time")
	}
	for i, column := range schema.Columns {
		if column.Name == t.dateColumnName {
			if i >= len(*record) {
				return nullValue, fmt.Errorf("record is missing column %q for trunc_time", column.Name)
			}
			return t.generateValue(record.Get(i))
		}
	}
	return nullValue, nil
}

func (t *TruncTime) generateValue(value data.Data) (string, error) {
	switch v := value.(type) {
	case nil, data.NullData:
		return nullValue, nil
	case interface{ Time() time.Time }:
		return t.truncEpochMillis(v.Time().UnixMilli()), nil
	default:
		return nullValue, fmt.Errorf("unknown data type: %T", v)
	}
}

func (t *TruncTime) truncEpochMillis(epochMillis int64) string {
	if epochMillis < minEpochMillis {
		return t.minGenerateValue()
	}
	localDateTime := time.UnixMilli(epochMillis).UTC()
	switch t.datePart {
	case YEAR:
		return localDateTime.Format("2006")
	case MONTH:
		return localDateTime.Format("2006-01")
	case DAY:
		return localDateTime.Format("2006-01-02")
	case HOUR:
		return localDateTime.Format("2006-01-02 15:00:00")
	default:
		return ""
	}
}

func (t *TruncTime) minGenerateValue() string {
	switch t.datePart {
	case YEAR:
		return "1959"
	case MONTH:
		return "1959-12"
	case DAY:
		return "1959-12-31"
	case HOUR:
		return "1959-12-31 23:00:00"
	default:
		return ""
	}
}

// String Returns a string representation like "trunc_time(dateColumnName, 'datePart')"
func (t *TruncTime) String() string {
	return fmt.Sprintf("%s(`%s`, '%s')", name, t.dateColumnName, t.datePart)
}
