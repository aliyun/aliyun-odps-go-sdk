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
	return d.String()
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

// DecimalFromStr parses a fixed-point value using up to 38 digits of precision.
func DecimalFromStr(value string) (*Decimal, error) {
	return parseDecimal(value, 38)
}

func parseDecimal(value string, maxPrecision int) (*Decimal, error) {
	unsigned := strings.TrimPrefix(value, "+")
	if unsigned == value {
		unsigned = strings.TrimPrefix(value, "-")
	}
	integer, fraction, hasPoint := strings.Cut(unsigned, ".")
	if !decimalDigits(integer) || (hasPoint && !decimalDigits(fraction)) {
		return nil, InvalidDecimalErr
	}
	scale := len(fraction)
	precision := max(1, len(strings.TrimLeft(integer, "0"))+scale)
	if precision > maxPrecision {
		return nil, fmt.Errorf("%w: precision %d exceeds %d", InvalidDecimalErr, precision, maxPrecision)
	}
	return NewDecimal(precision, scale, value), nil
}

func decimalDigits(value string) bool {
	return value != "" && strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) == -1
}

func (d Decimal) Type() datatype.DataType {
	return datatype.NewDecimalType(int32(d.precision), int32(d.scale))
}

func (d Decimal) String() string {
	if d.value != parseFromValue {
		return d.value
	}
	value := d.intValue.String()
	if d.scale <= 0 {
		return value
	}

	sign := ""
	if unsigned, negative := strings.CutPrefix(value, "-"); negative {
		sign = "-"
		value = unsigned
	}
	if len(value) <= d.scale {
		value = strings.Repeat("0", d.scale-len(value)+1) + value
	}
	split := len(value) - d.scale
	return sign + value[:split] + "." + value[split:]
}

func (d Decimal) Sql() string {
	return fmt.Sprintf("%sBD", d.String())
}

// Scan accepts SDK Decimal values and exact decimal text from database/sql.
func (d *Decimal) Scan(value interface{}) error {
	if value == nil {
		*d = Decimal{}
		return nil
	}
	if text, ok := value.(string); ok {
		decimal, err := parseDecimal(text, 76)
		if err != nil {
			return err
		}
		*d = *decimal
		return nil
	}
	return errors.WithStack(tryConvertType(value, d))
}
