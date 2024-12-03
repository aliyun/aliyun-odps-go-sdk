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
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

type arrowOptions struct {
	ExtendedMode  bool
	TimestampUnit TimeUnit
	DatetimeUnit  TimeUnit
}

type TimeUnit string

const (
	Second TimeUnit = "second"
	Milli  TimeUnit = "milli"
	Micro  TimeUnit = "micro"
	Nano   TimeUnit = "nano"
)

// ArrowOptions can not be used directly, it can be created by ArrowOptionConfig.XXX
type ArrowOptions func(cfg *arrowOptions)

func newTypeConvertConfig(opts ...ArrowOptions) *arrowOptions {
	cfg := &arrowOptions{
		ExtendedMode:  false,
		TimestampUnit: Nano,
		DatetimeUnit:  Milli,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

func withExtendedMode() ArrowOptions {
	return func(cfg *arrowOptions) {
		cfg.ExtendedMode = true
	}
}

func withTimestampUnit(unit TimeUnit) ArrowOptions {
	return func(cfg *arrowOptions) {
		cfg.TimestampUnit = unit
	}
}

func withDatetimeUnit(unit TimeUnit) ArrowOptions {
	return func(cfg *arrowOptions) {
		cfg.DatetimeUnit = unit
	}
}

var ArrowOptionConfig = struct {
	WithExtendedMode  func() ArrowOptions
	WithTimestampUnit func(unit TimeUnit) ArrowOptions
	WithDatetimeUnit  func(unit TimeUnit) ArrowOptions
}{
	WithExtendedMode:  withExtendedMode,
	WithTimestampUnit: withTimestampUnit,
	WithDatetimeUnit:  withDatetimeUnit,
}

// TypeToArrowType convert odps field type to arrow field type
// *        Storage Type      |  Arrow Type
// *    ----------------------+---------------------
// *      boolean             |  boolean
// *      tinyint             |  int8
// *      smallint            |  int16
// *      int                 |  int32
// *      bigint              |  int64
// *      float               |  float32
// *      double              |  float64
// *      char                |  utf8
// *      varchar             |  utf8
// *      string              |  utf8
// *      binary              |  binary
// *      date                |  date32
// *      datetime            |  timestamp(nano)
// *      timestamp           |  timestamp(nano) 【注：精度选择功能开发中】
// *      interval_day_time   |  day_time_interval
// *      interval_year_month |  month_interval
// *      decimal             |  decimal
// *      struct              |  struct
// *      array               |  list
// *      map                 |  map
func TypeToArrowType(odpsType datatype.DataType) (arrow.DataType, error) {
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
		return arrow.FixedWidthTypes.Timestamp_ns, nil
		// return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, nil
	case datatype.TIMESTAMP:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	case datatype.TIMESTAMP_NTZ:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
		// return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, nil
	case datatype.IntervalDayTime:
		return arrow.FixedWidthTypes.DayTimeInterval, nil
	case datatype.IntervalYearMonth:
		return arrow.FixedWidthTypes.MonthInterval, nil
	case datatype.DECIMAL:
		decimal, _ := odpsType.(datatype.DecimalType)
		return &arrow.Decimal128Type{
			Precision: decimal.Precision,
			Scale:     decimal.Scale,
		}, nil
	case datatype.STRUCT:
		structType, _ := odpsType.(datatype.StructType)
		arrowFields := make([]arrow.Field, len(structType.Fields))
		for i, field := range structType.Fields {
			arrowType, err := TypeToArrowType(field.Type)
			if err != nil {
				return arrow.Null, err
			}

			arrowFields[i] = arrow.Field{
				Name: field.Name,
				Type: arrowType,
			}
		}
		return arrow.StructOf(arrowFields...), nil
	case datatype.ARRAY:
		arrayType, _ := odpsType.(datatype.ArrayType)
		itemType, err := TypeToArrowType(arrayType.ElementType)
		if err != nil {
			return arrow.Null, err
		}

		return arrow.ListOf(itemType), nil
		// case datatype.MAP:
		//	mapType, _ := odpsType.(datatype.MapType)
		//	keyType, err := TypeToArrowType(mapType.KeyType)
		//	if err != nil {
		//		return arrow.Null, err
		//	}
		//	valueType, err := TypeToArrowType(mapType.ValueType)
		//	if err != nil {
		//		return arrow.Null, err
		//	}
		//	return arrow.MapOf(keyType, valueType), nil
	}

	return arrow.Null, errors.Errorf("unknown odps data type: %s", odpsType.Name())
}

func toMaxComputeData(vector arrow.Array, index int, typeInfo datatype.DataType, cfg *arrowOptions) (data.Data, error) {
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
		return data.String(value), nil
	case datatype.BINARY:
		value := vector.(*array.Binary).Value(index)
		return data.Binary(value), nil
	case datatype.DATE:
		value := vector.(*array.Date32).Value(index)
		// Date32 从 epoch（1970-01-01）起的天数
		days := int64(value)
		// 将天数转换为 time.Duration，并加到 epoch 时间上
		return data.Date(time.Unix(0, 0).AddDate(0, 0, int(days))), nil
	case datatype.DATETIME:
		value := vector.(*array.Timestamp).Value(index)
		epochTime := int64(value)
		switch cfg.DatetimeUnit {
		case Second:
			return data.DateTime(time.Unix(epochTime, 0)), nil
		case Milli:
			return data.DateTime(time.Unix(epochTime/1e3, (epochTime%1e3)*1e6)), nil
		case Micro:
			return data.DateTime(time.Unix(epochTime/1e6, (epochTime%1e6)*1e3)), nil
		case Nano:
			return data.DateTime(time.Unix(0, epochTime)), nil
		}
	case datatype.TIMESTAMP:
		if cfg.ExtendedMode {
			sec := vector.(*array.Struct).Field(0).(*array.Int64).Value(index)
			nano := vector.(*array.Struct).Field(1).(*array.Int32).Value(index)
			return data.Timestamp(time.Unix(sec, int64(nano))), nil
		} else {
			value := vector.(*array.Timestamp).Value(index)
			epochTime := int64(value)
			switch cfg.TimestampUnit {
			case Second:
				return data.Timestamp(time.Unix(epochTime, 0)), nil
			case Milli:
				return data.Timestamp(time.Unix(epochTime/1e3, (epochTime%1e3)*1e6)), nil
			case Micro:
				return data.Timestamp(time.Unix(epochTime/1e6, (epochTime%1e6)*1e3)), nil
			case Nano:
				return data.Timestamp(time.Unix(0, epochTime)), nil
			}
		}
	case datatype.TIMESTAMP_NTZ:
		if cfg.ExtendedMode {
			sec := vector.(*array.Struct).Field(0).(*array.Int64).Value(index)
			nano := vector.(*array.Struct).Field(1).(*array.Int32).Value(index)
			return data.TimestampNtz(time.Unix(sec, int64(nano))), nil
		} else {
			value := vector.(*array.Timestamp).Value(index)
			epochTime := int64(value)
			switch cfg.TimestampUnit {
			case Second:
				return data.TimestampNtz(time.Unix(epochTime, 0)), nil
			case Milli:
				return data.TimestampNtz(time.Unix(epochTime/1e3, (epochTime%1e3)*1e6)), nil
			case Micro:
				return data.TimestampNtz(time.Unix(epochTime/1e6, (epochTime%1e6)*1e3)), nil
			case Nano:
				return data.TimestampNtz(time.Unix(0, epochTime)), nil
			}
		}
	case datatype.DECIMAL:
		decimalType := typeInfo.(datatype.DecimalType)
		if cfg.ExtendedMode {
			fixedSizeBinaryVector, extendedMode := vector.(*array.FixedSizeBinary)
			if extendedMode {
				val := fixedSizeBinaryVector.Value(index)
				if len(val) < 8 {
					return nil, errors.Errorf("Unrecognized Decimal type, val len %d", len(val))
				}

				mSign := val[1]
				mIntg := val[2]
				mFrac := val[3]

				var decimalBuilder strings.Builder

				if mSign > 0 {
					decimalBuilder.WriteString("-")
				}

				for j := int(mIntg); j > 0; j-- {
					num := int(binary.LittleEndian.Uint32(val[8+j*4 : 12+j*4]))

					if j == int(mIntg) {
						decimalBuilder.WriteString(fmt.Sprintf("%d", num))
					} else {
						decimalBuilder.WriteString(fmt.Sprintf("%09d", num))
					}
				}

				decimalBuilder.WriteString(".")
				for j := 0; j < int(mFrac); j++ {
					num := int(binary.LittleEndian.Uint32(val[8-4*j : 8-4*j+4]))
					decimalBuilder.WriteString(fmt.Sprintf("%09d", num))
				}

				// trim trailing zeros
				result := decimalBuilder.String()
				result = strings.TrimRight(result, "0")
				if strings.HasSuffix(result, ".") {
					result = result[:len(result)-1] // 移除多余的小数点
				}
				decimal := data.NewDecimal(int(decimalType.Precision), int(decimalType.Scale), result)
				return decimal, nil
			}
		}
		value := vector.(*array.Decimal128).Value(index)
		decimal := data.NewDecimalFromValue(int(decimalType.Precision), int(decimalType.Scale), value.BigInt())
		return decimal, nil
	case datatype.ARRAY:
		arrayType := typeInfo.(datatype.ArrayType)

		// 处理 LIST 类型
		listCol := vector.(*array.List)

		// 获取偏移值，包括第 i 个列表对应的偏移量
		offsets := listCol.Offsets()
		start := offsets[index] // 当前列表的起始位置
		end := offsets[index+1] // 下一个列表的起始位置

		numElements := end - start                      // 当前列表的元素数量
		listData := make([]interface{}, 0, numElements) // 创建容量为当前列表长度的切片

		// 获取当前列表的值
		childArray := listCol.ListValues() // 获取子列表，这里通常是一个 Array 接口

		// 遍历子列表中的实际元素
		for j := start; j < end; j++ {
			elementData, err := toMaxComputeData(childArray, int(j-start), arrayType.ElementType, cfg)
			if err != nil {
				return nil, err
			}
			listData = append(listData, elementData)
		}
		return data.ArrayFromSlice(listData...)

	case datatype.MAP:
		mapType := typeInfo.(datatype.MapType)
		// 处理 MAP 类型
		mapCol := vector.(*array.Map)

		offsets := mapCol.Offsets()
		start := offsets[index]
		end := offsets[index+1]

		mapData := make(map[interface{}]interface{})
		keys := mapCol.Keys()
		values := mapCol.Items()

		for j := start; j < end; j++ {
			keyData, err := toMaxComputeData(keys, int(j-start), mapType.KeyType, cfg)
			if err != nil {
				return nil, err
			}
			valueData, err := toMaxComputeData(values, int(j-start), mapType.ValueType, cfg)
			if err != nil {
				return nil, err
			}
			mapData[keyData] = valueData
		}
		return data.MapFromGoMap(mapData)

	case datatype.STRUCT:
		structType := typeInfo.(datatype.StructType)
		// 处理 STRUCT 类型
		structCol := vector.(*array.Struct)

		structData := data.NewStructWithTyp(structType)

		for fieldIndex := 0; fieldIndex < structCol.NumField(); fieldIndex++ {
			fieldType := structType.Fields[fieldIndex]
			fieldName := fieldType.Name

			fieldArray := structCol.Field(fieldIndex)
			fieldValue, err := toMaxComputeData(fieldArray, index, fieldType.Type, cfg)
			if err != nil {
				return nil, err
			}
			err = structData.SetField(fieldName, fieldValue)
			if err != nil {
				return nil, err
			}
		}
		return structData, nil
	}
	return nil, fmt.Errorf("unsupported ODPS type: %v", typeInfo.Name())
}

// ToMaxComputeRecords 将 Arrow Record Batch 转换为 ODPS Record 列表
func ToMaxComputeRecords(arrowBatch arrow.Record, columns []Column, opt ...ArrowOptions) ([]data.Record, error) {
	cfg := newTypeConvertConfig(opt...)
	odpsRecords := make([]data.Record, 0, int(arrowBatch.NumRows()))
	// 迭代每一行
	for i := 0; i < int(arrowBatch.NumRows()); i++ {
		// 创建 ODPS Record
		odpsRecord := make([]data.Data, 0, int(arrowBatch.NumCols()))

		// 遍历 Arrow Record 中的所有列
		for j := 0; j < int(arrowBatch.NumCols()); j++ {
			col := arrowBatch.Column(j)

			if col.IsValid(i) {
				odpsData, err := toMaxComputeData(col, i, columns[j].Type, cfg)
				if err != nil {
					return nil, err
				}
				odpsRecord = append(odpsRecord, odpsData)
			} else {
				// 处理空值
				odpsRecord = append(odpsRecord, data.Null)
			}
		}
		odpsRecords = append(odpsRecords, odpsRecord)
	}
	return odpsRecords, nil
}
