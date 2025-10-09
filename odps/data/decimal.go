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

package data

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

var InvalidDecimalErr = errors.New("invalid decimal")

const parseFromValue = "__ParseFromValue__"

type Decimal struct {
	precision int
	scale     int
	value     string
	Valid     bool
	intValue  *big.Int
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

// NewDecimal value means readable string of the decimal, because MaxCompute uses string as the serialization method of decimal
func NewDecimal(precision, scale int, value string) *Decimal {
	return &Decimal{
		precision: precision,
		scale:     scale,
		value:     value,
		Valid:     true,
	}
}

// NewDecimalFromValue typical way to construct decimal, use bigint to represent decimal values
func NewDecimalFromValue(precision, scale int, intValue *big.Int) *Decimal {
	return &Decimal{
		precision: precision,
		scale:     scale,
		intValue:  intValue,
		Valid:     true,
		value:     parseFromValue,
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

	if d.scale > 38 {
		return nil, errors.New("fractional is too long, which is longer than 38")
	}

	d.value = value
	d.Valid = true
	return &d, nil
}

func (d Decimal) Type() datatype.DataType {
	return datatype.NewDecimalType(int32(d.precision), int32(d.scale))
}

func (d Decimal) String() string {
	if d.value == parseFromValue {
		valueStr := d.intValue.String()
		if d.scale > 0 {
			if len(valueStr) <= d.scale {
				valueStr = "0." + fmt.Sprintf("%0*s", d.scale, valueStr)
			} else {
				integerPart := valueStr[:len(valueStr)-d.scale]
				fractionalPart := valueStr[len(valueStr)-d.scale:]
				valueStr = integerPart + "." + fractionalPart
			}
		}
		return fmt.Sprintf("%s", valueStr)
	}
	return fmt.Sprintf("%s", d.value)
}

func (d Decimal) Sql() string {
	return fmt.Sprintf("%sBD", d.String())
}

func (d *Decimal) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, d))
}
