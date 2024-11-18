package sqldriver

import (
	"database/sql"
	"reflect"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
)

type (
	NullBool         sql.NullBool
	NullFloat64      sql.NullFloat64
	NullInt32        sql.NullInt32
	NullInt64        sql.NullInt64
	NullString       sql.NullString
	NullDate         sql.NullTime
	NullDateTime     sql.NullTime
	NullTimeStamp    sql.NullTime
	NullTimeStampNtz sql.NullTime
	Binary           sql.RawBytes
	Decimal          data.Decimal
	Map              data.Map
	Array            data.Array
	Struct           data.Struct
	Json             data.Json
)

type NullInt8 struct {
	Int8  int8
	Valid bool // Valid is true if Int16 is not NULL
}

type NullInt16 struct {
	Int16 int16
	Valid bool // Valid is true if Int16 is not NULL
}

func (n *NullBool) Scan(value interface{}) error {
	return (*sql.NullBool)(n).Scan(value)
}

func (n *NullFloat64) Scan(value interface{}) error {
	return (*sql.NullFloat64)(n).Scan(value)
}

func (n *NullInt32) Scan(value interface{}) error {
	return (*sql.NullInt32)(n).Scan(value)
}

func (n *NullInt64) Scan(value interface{}) error {
	return (*sql.NullInt64)(n).Scan(value)
}

func (n *NullString) Scan(value interface{}) error {
	return (*sql.NullString)(n).Scan(value)
}

func (n *NullDate) Scan(value interface{}) error {
	return (*sql.NullTime)(n).Scan(value)
}

func (n *NullDateTime) Scan(value interface{}) error {
	return (*sql.NullTime)(n).Scan(value)
}

func (n *NullTimeStamp) Scan(value interface{}) error {
	return (*sql.NullTime)(n).Scan(value)
}

func (n *NullTimeStampNtz) Scan(value interface{}) error {
	return (*sql.NullTime)(n).Scan(value)
}

func (n *Decimal) Scan(value interface{}) error {
	return (*data.Decimal)(n).Scan(value)
}

func (n *Map) Scan(value interface{}) error {
	return (*data.Map)(n).Scan(value)
}

func (n *Array) Scan(value interface{}) error {
	return (*data.Array)(n).Scan(value)
}

func (n *Struct) Scan(value interface{}) error {
	return (*data.Struct)(n).Scan(value)
}

func (n *Json) Scan(value interface{}) error {
	return (*data.Json)(n).Scan(value)
}

func (n *NullInt16) Scan(value interface{}) error {
	if value == nil {
		n.Int16, n.Valid = 0, false
		return nil
	}

	n.Valid = true

	v, ok := value.(int16)
	if !ok {
		return newConvertError(value, *n)
	}

	n.Int16 = v
	return nil
}

func (n *NullInt8) Scan(value interface{}) error {
	if value == nil {
		n.Int8, n.Valid = 0, false
		return nil
	}

	n.Valid = true

	v, ok := value.(int8)
	if !ok {
		return newConvertError(value, *n)
	}

	n.Int8 = v
	return nil
}

type NullFloat32 struct {
	Float32 float32
	Valid   bool
}

func (n *NullFloat32) Scan(value interface{}) error {
	if value == nil {
		n.Float32, n.Valid = 0, false
		return nil
	}

	n.Valid = true

	v, ok := value.(float32)
	if !ok {
		return newConvertError(value, *n)
	}

	n.Float32 = v
	return nil
}

func (n *Binary) Scan(value interface{}) error {
	if value == nil {
		*n = nil
		return nil
	}

	d, ok := value.([]byte)
	if !ok {
		srcT := reflect.TypeOf(value)
		return errors.Errorf("cannt convert %s to sqldriver.Binary", srcT.Name())
	}

	*n = d
	return nil
}

type NullAble interface {
	IsNull() bool
}

func (n NullInt8) IsNull() bool {
	return !n.Valid
}

func (n NullInt16) IsNull() bool {
	return !n.Valid
}

func (n NullInt32) IsNull() bool {
	return !n.Valid
}

func (n NullInt64) IsNull() bool {
	return !n.Valid
}

func (n NullFloat32) IsNull() bool {
	return !n.Valid
}

func (n NullFloat64) IsNull() bool {
	return !n.Valid
}

func (n NullDate) IsNull() bool {
	return !n.Valid
}

func (n NullDateTime) IsNull() bool {
	return !n.Valid
}

func (n NullTimeStamp) IsNull() bool {
	return !n.Valid
}

func (n NullTimeStampNtz) IsNull() bool {
	return !n.Valid
}

func (n NullBool) IsNull() bool {
	return !n.Valid
}

func (n Binary) IsNull() bool {
	return []byte(n) == nil
}

func (n NullString) IsNull() bool {
	return !n.Valid
}

func (n Decimal) IsNull() bool {
	return !n.Valid
}

func (n Map) IsNull() bool {
	return !n.Valid
}

func (n Array) IsNull() bool {
	return !n.Valid
}

func (n Struct) IsNull() bool {
	return !n.Valid
}

func (n Json) IsNull() bool {
	return !n.Valid
}

func (n NullDate) String() string {
	return data.Date(n.Time).String()
}

func (n NullDateTime) String() string {
	return data.DateTime(n.Time).String()
}

func (n NullTimeStamp) String() string {
	return data.Timestamp(n.Time).String()
}

func (n NullTimeStampNtz) String() string {
	return data.TimestampNtz(n.Time).String()
}

func (n Decimal) String() string {
	return data.Decimal(n).String()
}

func (n Map) String() string {
	return data.Map(n).String()
}

func (n Array) String() string {
	return data.Array(n).String()
}

func (n Struct) String() string {
	return data.Struct(n).String()
}

func (n Json) String() string {
	return data.Json(n).String()
}

func (n Binary) String() string {
	return data.Binary(n).String()
}

func newConvertError(src interface{}, dst interface{}) error {
	srcT := reflect.TypeOf(src)
	dstT := reflect.TypeOf(dst)
	return errors.Errorf("cannot convert %s to %s", srcT.Name(), dstT.Name())
}
