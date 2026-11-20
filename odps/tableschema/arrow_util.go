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

package tableschema

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// TypeToArrowType maps a MaxCompute column type to its Storage API V2 Arrow type.
func TypeToArrowType(odpsType datatype.DataType) (arrow.DataType, error) {
	if odpsType == nil {
		return nil, fmt.Errorf("missing MaxCompute type")
	}
	switch typ := odpsType.(type) {
	case datatype.DecimalType:
		if typ.Precision < 1 || typ.Precision > 38 || typ.Scale < 0 || typ.Scale > typ.Precision {
			return nil, fmt.Errorf("invalid MaxCompute type %s", typ.Name())
		}
		return &arrow.Decimal128Type{Precision: typ.Precision, Scale: typ.Scale}, nil
	case datatype.StructType:
		fields := make([]arrow.Field, len(typ.Fields))
		for i, field := range typ.Fields {
			fieldType, err := TypeToArrowType(field.Type)
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", field.Name, err)
			}
			fields[i] = arrow.Field{Name: field.Name, Type: fieldType, Nullable: true}
		}
		return arrow.StructOf(fields...), nil
	case datatype.ArrayType:
		itemType, err := TypeToArrowType(typ.ElementType)
		if err != nil {
			return nil, fmt.Errorf("array element: %w", err)
		}
		return arrow.ListOf(itemType), nil
	case datatype.MapType:
		key, err := TypeToArrowType(typ.KeyType)
		if err != nil {
			return nil, fmt.Errorf("map key: %w", err)
		}
		value, err := TypeToArrowType(typ.ValueType)
		if err != nil {
			return nil, fmt.Errorf("map value: %w", err)
		}
		return arrow.MapOf(key, value), nil
	}

	switch odpsType.ID() {
	case datatype.BOOLEAN:
		return arrow.FixedWidthTypes.Boolean, nil
	case datatype.TINYINT:
		return arrow.PrimitiveTypes.Int8, nil
	case datatype.SMALLINT:
		return arrow.PrimitiveTypes.Int16, nil
	case datatype.INT:
		return arrow.PrimitiveTypes.Int32, nil
	case datatype.BIGINT:
		return arrow.PrimitiveTypes.Int64, nil
	case datatype.FLOAT:
		return arrow.PrimitiveTypes.Float32, nil
	case datatype.DOUBLE:
		return arrow.PrimitiveTypes.Float64, nil
	case datatype.CHAR, datatype.VARCHAR, datatype.STRING, datatype.JSON:
		return arrow.BinaryTypes.String, nil
	case datatype.BINARY:
		return arrow.BinaryTypes.Binary, nil
	case datatype.DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case datatype.DATETIME:
		return arrow.FixedWidthTypes.Timestamp_ms, nil
	case datatype.TIMESTAMP:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, nil
	case datatype.TIMESTAMP_NTZ:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
	case datatype.IntervalDayTime:
		return arrow.FixedWidthTypes.DayTimeInterval, nil
	case datatype.IntervalYearMonth:
		return arrow.FixedWidthTypes.MonthInterval, nil
	}
	return nil, fmt.Errorf("unsupported MaxCompute type: %s", odpsType.Name())
}

func toMaxComputeData(vector arrow.Array, index int, typeInfo datatype.DataType) (data.Data, error) {
	if vector.IsNull(index) {
		return data.Null, nil
	}
	switch typeInfo.ID() {
	case datatype.BOOLEAN:
		value := vector.(*array.Boolean).Value(index)
		return data.Bool(value), nil
	case datatype.TINYINT:
		value := vector.(*array.Int8).Value(index)
		return data.TinyInt(value), nil
	case datatype.SMALLINT:
		value := vector.(*array.Int16).Value(index)
		return data.SmallInt(value), nil
	case datatype.INT:
		value := vector.(*array.Int32).Value(index)
		return data.Int(value), nil
	case datatype.BIGINT:
		value := vector.(*array.Int64).Value(index)
		return data.BigInt(value), nil
	case datatype.FLOAT:
		value := vector.(*array.Float32).Value(index)
		return data.Float(value), nil
	case datatype.DOUBLE:
		value := vector.(*array.Float64).Value(index)
		return data.Double(value), nil
	case datatype.CHAR, datatype.VARCHAR, datatype.STRING, datatype.JSON:
		value := vector.(*array.String).Value(index)
		return data.String(strings.Clone(value)), nil
	case datatype.BINARY:
		value := vector.(*array.Binary).Value(index)
		return data.Binary(bytes.Clone(value)), nil
	case datatype.DATE:
		return data.Date(vector.(*array.Date32).Value(index).ToTime()), nil
	case datatype.DATETIME, datatype.TIMESTAMP, datatype.TIMESTAMP_NTZ:
		timestamps := vector.(*array.Timestamp)
		value := timestamps.Value(index).ToTime(timestamps.DataType().(*arrow.TimestampType).Unit)
		switch typeInfo.ID() {
		case datatype.DATETIME:
			return data.DateTime(value), nil
		case datatype.TIMESTAMP:
			return data.Timestamp(value), nil
		default:
			return data.TimestampNtz(value), nil
		}
	case datatype.DECIMAL:
		decimal := vector.(*array.Decimal128)
		physicalType := decimal.DataType().(*arrow.Decimal128Type)
		expectedType := typeInfo.(datatype.DecimalType)
		if physicalType.Scale != expectedType.Scale {
			return nil, fmt.Errorf("incompatible Arrow decimal scale %d for MaxCompute scale %d", physicalType.Scale, expectedType.Scale)
		}
		value := decimal.Value(index).ToString(expectedType.Scale)
		return data.NewDecimal(int(expectedType.Precision), int(expectedType.Scale), value), nil
	case datatype.IntervalDayTime:
		value := vector.(*array.DayTimeInterval).Value(index)
		total := int64(value.Days)*24*60*60*1000 + int64(value.Milliseconds)
		return data.NewIntervalDayTime(total/1000, int32(total%1000)*int32(time.Millisecond)), nil
	case datatype.IntervalYearMonth:
		return data.IntervalYearMonth(vector.(*array.MonthInterval).Value(index)), nil
	case datatype.ARRAY:
		typ := typeInfo.(datatype.ArrayType)
		list := vector.(*array.List)
		start, end := list.ValueOffsets(index)
		result := data.NewArrayWithType(typ)
		for i := start; i < end; i++ {
			value, err := toMaxComputeData(list.ListValues(), int(i), typ.ElementType)
			if err != nil {
				return nil, err
			}
			result.UnSafeAppend(value)
		}
		return result, nil
	case datatype.MAP:
		typ := typeInfo.(datatype.MapType)
		column := vector.(*array.Map)
		start, end := column.ValueOffsets(index)
		result := data.NewMapWithType(typ)
		for i := start; i < end; i++ {
			key, err := toMaxComputeData(column.Keys(), int(i), typ.KeyType)
			if err != nil {
				return nil, err
			}
			value, err := toMaxComputeData(column.Items(), int(i), typ.ValueType)
			if err != nil {
				return nil, err
			}
			if err := result.Set(key, value); err != nil {
				return nil, err
			}
		}
		return result, nil
	case datatype.STRUCT:
		typ := typeInfo.(datatype.StructType)
		column := vector.(*array.Struct)
		if column.NumField() != len(typ.Fields) {
			return nil, fmt.Errorf("incompatible Arrow struct field count for %s", typ.Name())
		}
		result := data.NewStructWithTyp(typ)
		for i, field := range typ.Fields {
			value, err := toMaxComputeData(column.Field(i), index, field.Type)
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", field.Name, err)
			}
			if err := result.SetField(field.Name, value); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("unsupported ODPS type: %v", typeInfo.Name())
}

// ToMaxComputeRecords converts a Storage API V2 batch to independent MaxCompute
// records. columns must match the batch column order.
func ToMaxComputeRecords(batch arrow.RecordBatch, columns []Column) (records []data.Record, err error) {
	if batch == nil || int64(len(columns)) != batch.NumCols() {
		return nil, fmt.Errorf("columns in the Arrow batch and MaxCompute schema do not match")
	}
	// A stream may carry a different physical Arrow type from the requested table.
	defer func() {
		if value := recover(); value != nil {
			records = nil
			err = fmt.Errorf("incompatible Arrow batch: %v", value)
		}
	}()
	records = make([]data.Record, 0, batch.NumRows())
	for row := range int(batch.NumRows()) {
		record := data.NewRecord(len(columns))
		for index, column := range columns {
			value, err := toMaxComputeData(batch.Column(index), row, column.Type)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.Name, err)
			}
			record[index] = value
		}
		records = append(records, record)
	}
	return records, nil
}
