package tunnel_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/decimal128"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func TestArrowStreamWriter_WriteBasicType(t *testing.T) {
	tableName := "ArrowTest_basic_type"

	allTypeSchema := tableschema.NewSchemaBuilder().Name(tableName).
		Column(tableschema.Column{
			Name: "c1",
			Type: datatype.TinyIntType,
		}).Column(tableschema.Column{
		Name: "c2",
		Type: datatype.SmallIntType,
	}).Column(tableschema.Column{
		Name: "c3",
		Type: datatype.IntType,
	}).Column(tableschema.Column{
		Name: "c4",
		Type: datatype.BigIntType,
	}).Column(tableschema.Column{
		Name: "c5",
		Type: datatype.FloatType,
	}).Column(tableschema.Column{
		Name: "c6",
		Type: datatype.DoubleType,
	}).Column(tableschema.Column{
		Name: "c7",
		Type: datatype.StringType,
	}).Column(tableschema.Column{
		Name: "c8",
		Type: datatype.BooleanType,
	}).Column(tableschema.Column{
		Name: "c9",
		Type: datatype.DateTimeType,
	}).Column(tableschema.Column{
		Name: "c10",
		Type: datatype.NewDecimalType(38, 18),
	}).Column(tableschema.Column{
		Name: "c11",
		Type: datatype.NewCharType(10),
	}).Column(tableschema.Column{
		Name: "c12",
		Type: datatype.NewVarcharType(10),
	}).Column(tableschema.Column{
		Name: "c13",
		Type: datatype.TimestampType,
	}).Column(tableschema.Column{
		Name: "c14",
		Type: datatype.TimestampNtzType,
	}).Column(tableschema.Column{
		Name: "c15",
		Type: datatype.DateType,
	}).Column(tableschema.Column{
		Name: "c16",
		Type: datatype.BinaryType,
	}).Build()

	tables := odpsIns.Schema("tunnel").Tables()
	err := tables.Delete(tableName, true)
	if err != nil {
		t.Fatal(err)
	}
	err = tables.Create(allTypeSchema, true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	session, err := tunnelIns.CreateUploadSession(ProjectName, tableName, tunnel.SessionCfg.WithSchemaName(SchemaName))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := session.OpenRecordArrowWriter(0)
	if err != nil {
		t.Fatal(err)
	}

	arrowSchema := session.ArrowSchema()
	pool := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(pool, arrowSchema)
	defer recordBuilder.Release()
	now := time.Now()

	for i, field := range arrowSchema.Fields() {
		fieldBuilder := recordBuilder.Field(i)
		switch field.Name {
		case "c1":
			builder := fieldBuilder.(*array.Int8Builder)
			builder.Append(1)
		case "c2":
			builder := fieldBuilder.(*array.Int16Builder)
			builder.Append(2)
		case "c3":
			builder := fieldBuilder.(*array.Int32Builder)
			builder.Append(3)
		case "c4":
			builder := fieldBuilder.(*array.Int64Builder)
			builder.Append(4)
		case "c5":
			builder := fieldBuilder.(*array.Float32Builder)
			builder.Append(5.0)
		case "c6":
			builder := fieldBuilder.(*array.Float64Builder)
			builder.Append(6.0)
		case "c7":
			builder := fieldBuilder.(*array.StringBuilder)
			builder.Append("7")
		case "c8":
			builder := fieldBuilder.(*array.BooleanBuilder)
			builder.Append(true)
		case "c9":
			builder := fieldBuilder.(*array.TimestampBuilder)
			t := now
			builder.Append(arrow.Timestamp(t.UnixMilli()))
		case "c10":
			builder := fieldBuilder.(*array.Decimal128Builder)
			// 0.00000000000000001
			builder.Append(decimal128.FromBigInt(big.NewInt(10)))
		case "c11":
			builder := fieldBuilder.(*array.StringBuilder)
			builder.Append("11")
		case "c12":
			builder := fieldBuilder.(*array.StringBuilder)
			builder.Append("12")
		case "c13":
			builder := fieldBuilder.(*array.TimestampBuilder)
			t := now
			builder.Append(arrow.Timestamp(t.UnixNano()))
		case "c14":
			builder := fieldBuilder.(*array.TimestampBuilder)
			t := now
			builder.Append(arrow.Timestamp(t.UnixNano()))
		case "c15":
			builder := fieldBuilder.(*array.Date32Builder)
			epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			epochDay := now.Sub(epoch).Hours() / 24
			builder.Append(arrow.Date32(epochDay))
		case "c16":
			builder := fieldBuilder.(*array.BinaryBuilder)
			builder.Append([]byte("16"))
		}
	}
	record := recordBuilder.NewRecord()
	defer record.Release()

	err = writer.WriteArrowRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()
	err = session.Commit([]int{0})
	if err != nil {
		t.Fatal(err)
	}

	table := tables.Get(tableName)
	data, err := tunnelIns.Preview(table, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range data {
		println(record.String())
	}
}

func TestArrowStreamReader_ReadBasicType(t *testing.T) {
	tableName := "ArrowTest_basic_type"

	session, err := tunnelIns.CreateDownloadSession(ProjectName, tableName, tunnel.SessionCfg.WithSchemaName(SchemaName))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := session.OpenRecordArrowReader(0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	arrowRecord, err := reader.Read()
	columns := session.Schema().Columns
	records, err := tableschema.ToMaxComputeRecords(arrowRecord, columns)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		println(record.String())
	}
}
