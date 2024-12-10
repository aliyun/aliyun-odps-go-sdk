package tableschema

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

var (
	record      data.Record
	tableSchema TableSchema
)

func setup() {
	tableSchema = NewSchemaBuilder().
		Column(Column{Name: "timestamp", Type: datatype.TimestampType}).
		Column(Column{Name: "datetime", Type: datatype.DateTimeType}).
		Column(Column{Name: "date", Type: datatype.DateType}).
		Column(Column{Name: "timestamp_ntz", Type: datatype.TimestampNtzType}).
		Build()
	record = data.NewRecord(4)
}

func TestGeneratePartitionSpec(t *testing.T) {
	setup()
	truncTime := NewTruncTime("timestamp", HOUR)
	newSchema := NewSchemaBuilder().
		Column(Column{Name: "timestamp", Type: datatype.TimestampType}).
		PartitionColumn(Column{Name: "auto_pt", GenerateExpression: truncTime}).
		Build()
	record[0] = data.Timestamp(time.Date(2023, 12, 25, 9, 9, 35, 0, time.UTC))
	fmt.Println(record.Get(0))

	partitionSpec, err := newSchema.GeneratePartitionSpec(&record)
	assert.NoError(t, err)
	fmt.Println(partitionSpec)
}

func TestTruncTimestamp(t *testing.T) {
	setup()
	timestamp := data.Timestamp(time.Date(2023, 12, 25, 9, 9, 35, 0, time.UTC))
	record[0] = timestamp

	truncYear := NewTruncTime("timestamp", YEAR)
	truncMonth := NewTruncTime("timestamp", MONTH)
	truncDay := NewTruncTime("timestamp", DAY)
	truncHour := NewTruncTime("timestamp", HOUR)

	actual, _ := truncYear.generate(&record, &tableSchema)
	assert.Equal(t, "2023", actual)
	actual, _ = truncMonth.generate(&record, &tableSchema)
	assert.Equal(t, "2023-12", actual)
	actual, _ = truncDay.generate(&record, &tableSchema)
	assert.Equal(t, "2023-12-25", actual)
	actual, _ = truncHour.generate(&record, &tableSchema)
	assert.Equal(t, "2023-12-25 09:00:00", actual)
}

func TestTruncDatetime(t *testing.T) {
	setup()
	datetime := data.DateTime(time.Date(2023, 12, 25, 9, 9, 35, 0, time.UTC))
	record[1] = datetime

	truncYear := NewTruncTime("datetime", YEAR)
	truncMonth := NewTruncTime("datetime", MONTH)
	truncDay := NewTruncTime("datetime", DAY)
	truncHour := NewTruncTime("datetime", HOUR)

	actual, _ := truncYear.generate(&record, &tableSchema)
	assert.Equal(t, "2023", actual)
	actual, _ = truncMonth.generate(&record, &tableSchema)
	assert.Equal(t, "2023-12", actual)
	actual, _ = truncDay.generate(&record, &tableSchema)
	assert.Equal(t, "2023-12-25", actual)
	actual, _ = truncHour.generate(&record, &tableSchema)
	assert.Equal(t, "2023-12-25 09:00:00", actual)
}

func TestNullValue(t *testing.T) {
	setup()
	record[0] = nil
	truncYear := NewTruncTime("timestamp", YEAR)
	actual, _ := truncYear.generate(&record, &tableSchema)
	assert.Equal(t, "__NULL__", actual)
}

func TestTimeBefore1960(t *testing.T) {
	setup()
	oldTime := data.DateTime(time.Date(1900, 12, 25, 9, 9, 35, 0, time.UTC))
	record[1] = oldTime

	truncYear := NewTruncTime("datetime", YEAR)
	truncMonth := NewTruncTime("datetime", MONTH)
	truncDay := NewTruncTime("datetime", DAY)
	truncHour := NewTruncTime("datetime", HOUR)

	actual, _ := truncYear.generate(&record, &tableSchema)
	assert.Equal(t, "1959", actual)
	actual, _ = truncMonth.generate(&record, &tableSchema)
	assert.Equal(t, "1959-12", actual)
	actual, _ = truncDay.generate(&record, &tableSchema)
	assert.Equal(t, "1959-12-31", actual)
	actual, _ = truncHour.generate(&record, &tableSchema)
	assert.Equal(t, "1959-12-31 23:00:00", actual)
}
