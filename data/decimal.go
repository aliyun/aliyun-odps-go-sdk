package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

var InvalidDecimalErr = errors.New("invalid decimal")

type Decimal struct {
	precision int
	scale     int
	value     string
}

func (d *Decimal) Precision() int {
	return d.precision
}

func (d *Decimal) Scale() int {
	return d.scale
}

func (d *Decimal) Value() string {
	return d.value
}

func NewDecimal(precision, scale int, value string) *Decimal {
	return &Decimal{
		precision: precision,
		scale:     scale,
		value:     value,
	}
}

func DecimalFromStr(value string) (*Decimal, error) {
	parts := strings.Split(value, ".")
	if len(parts) > 3 {
		return nil, InvalidDecimalErr
	}

	_, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return nil, InvalidDecimalErr
	}

	d := Decimal{}
	d.precision = len(parts[0])

	if d.precision > 38 {
		return nil, errors.New("integer is too big, the most numbers of the integer is 38")
	}

	if len(parts) == 2 {
		_, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return nil, InvalidDecimalErr
		}

		d.scale = len(parts[1])
	}

	if d.scale > 18 {
		return nil, errors.New("fractional is too long, which is longer than 18")
	}

	d.value = value
	return &d, nil
}

func (d *Decimal) Type() datatype.DataType {
	return datatype.NewDecimalType(int32(d.precision), int32(d.scale))
}

func (d *Decimal) String() string {
	return fmt.Sprintf("%s", d.value)
}

func (d *Decimal) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, d))
}
