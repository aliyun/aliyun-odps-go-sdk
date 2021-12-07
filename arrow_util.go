package odps

import (
	"errors"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"github.com/fetchadd/arrow"
)

// TypeToArrowType odps数据类型转换为arrow数据类型
//*        Storage Type      |  Arrow Type
//*    ----------------------+---------------------
//*      boolean             |  boolean
//*      tinyint             |  int8
//*      smallint            |  int16
//*      int                 |  int32
//*      bigint              |  int64
//*      float               |  float32
//*      double              |  float64
//*      char                |  utf8
//*      varchar             |  utf8
//*      string              |  utf8
//*      binary              |  binary
//*      date                |  date32
//*      datetime            |  timestamp(nano)
//*      timestamp           |  timezstamp(nano) 【注：精度选择功能开发中】
//*      interval_day_time   |  day_time_interval
//*      interval_year_month |  month_interval
//*      decimal             |  decimal
//*      struct              |  struct
//*      array               |  list
//*      map                 |  map
func TypeToArrowType(odpsType datatype.DataType) (arrow.DataType, error)  {
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
	case datatype.CHAR, datatype.VARCHAR, datatype.STRING:
		return arrow.BinaryTypes.String, nil
	case datatype.BINARY:
		return arrow.BinaryTypes.Binary, nil
	case datatype.DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case datatype.DATETIME:
		return arrow.FixedWidthTypes.Date64, nil
	case datatype.TIMESTAMP:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	case datatype.IntervalDayTime:
		return arrow.FixedWidthTypes.DayTimeInterval, nil
	case datatype.IntervalYearMonth:
		return arrow.FixedWidthTypes.MonthInterval, nil
	case datatype.DECIMAL:
		decimal, _ := odpsType.(datatype.DecimalType)
		return &arrow.Decimal128Type{
			Precision: decimal.Precision,
			Scale: decimal.Scale,
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
	case datatype.MAP:
		mapType, _ := odpsType.(datatype.MapType)
		keyType, err := TypeToArrowType(mapType.KeyType)
		if err != nil {
			return arrow.Null, err
		}
		valueType, err := TypeToArrowType(mapType.ValueType)
		if err != nil {
			return arrow.Null, err
		}
		return arrow.MapOf(keyType, valueType), nil
	}

	return arrow.Null, errors.New("unknown odps data type: " + odpsType.Name())
}