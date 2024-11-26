package tableschema

import (
	"fmt"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
)

// TruncTime Implementation class of GenerateExpression, corresponding to SQL trunc_time('dateColumnName', 'datePart') syntax
type TruncTime struct {
	dateColumnName        string
	cachedDataColumnIndex int
	datePart              DatePart
	formatter             *time.Time
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
	truncTime := &TruncTime{
		dateColumnName:        dateColumnName,
		datePart:              datePart,
		cachedDataColumnIndex: -1,
	}
	return truncTime
}

func newTruncTimeWithConstant(dateColumnName string, constant string) (*TruncTime, error) {
	datePart, err := stringToDatePart(constant)
	if err != nil {
		return nil, err
	}
	if dateColumnName == "" {
		return nil, fmt.Errorf("TruncTime dateColumnName cannot be empty")
	}
	truncTime := &TruncTime{
		dateColumnName:        dateColumnName,
		datePart:              datePart,
		cachedDataColumnIndex: -1,
	}
	return truncTime, nil
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
	var dataValue data.Data
	if t.cachedDataColumnIndex != -1 {
		dataValue = record.Get(t.cachedDataColumnIndex)
	} else {
		for i, column := range schema.Columns {
			if column.Name == t.dateColumnName {
				dataValue = record.Get(i)
				t.cachedDataColumnIndex = i
				break
			}
		}
	}
	if dataValue == nil {
		return nullValue, nil
	}
	var epochMillis int64

	switch v := dataValue.(type) {
	case data.Date:
		t := v.Time()
		epochMillis = t.UnixNano() / int64(time.Millisecond)
	case data.DateTime:
		t := v.Time()
		epochMillis = t.UnixNano() / int64(time.Millisecond)
	case data.Timestamp:
		t := v.Time()
		epochMillis = t.UnixNano() / int64(time.Millisecond)
	case data.TimestampNtz:
		t := v.Time()
		epochMillis = t.UnixNano() / int64(time.Millisecond)
	default:
		return nullValue, fmt.Errorf("unknown data type: %T", v)
	}
	return t.truncEpochMillis(epochMillis), nil
}

func (t *TruncTime) truncEpochMillis(epochMillis int64) string {
	if epochMillis < minEpochMillis {
		return t.minGenerateValue()
	}
	localDateTime := time.Unix(epochMillis/1000, 0).UTC()
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
	return fmt.Sprintf("%s(%s, '%s')", name, t.dateColumnName, t.datePart)
}
