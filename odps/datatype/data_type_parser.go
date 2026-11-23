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

package datatype

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

type typeParser struct {
	tokens []string
	index  int
}

func ParseDataType(name string) (DataType, error) {
	parser := typeParser{
		tokens: tokenize(name),
		index:  0,
	}

	dataType, err := parser.parse()
	if err != nil {
		return nil, errors.Wrapf(err, "parse datatype %q", name)
	}

	if parser.hasTokenLeft() {
		return nil, errors.Errorf(
			"parse datatype error: %s, unexpected token: %s at: %d",
			name, parser.peekToken(), parser.index)
	}

	return dataType, nil
}

func tokenize(name string) []string {
	tokens := make([]string, 0)
	var tokenBuilder strings.Builder
	flush := func() {
		if tokenBuilder.Len() > 0 {
			tokens = append(tokens, tokenBuilder.String())
			tokenBuilder.Reset()
		}
	}

	quoted := false
	for _, c := range name {
		switch {
		case c == '`':
			if !quoted {
				flush()
			}
			tokenBuilder.WriteRune(c)
			quoted = !quoted
			if !quoted {
				flush()
			}
		case quoted || isIdentifierChar(c):
			tokenBuilder.WriteRune(c)
		default:
			flush()
			if !unicode.IsSpace(c) {
				tokens = append(tokens, string(c))
			}
		}
	}
	flush()
	return tokens
}

func isIdentifierChar(c rune) bool {
	return unicode.IsDigit(c) || unicode.IsLetter(c) || c == '_' || c == '.'
}

func (parser *typeParser) parse() (DataType, error) {
	token := parser.consumeToken()
	typeCode := TypeCodeFromStr(token)

	switch typeCode {
	case ARRAY:
		return parser.parseArray()
	case MAP:
		return parser.parseMap()
	case STRUCT:
		return parser.parseStruct()
	case CHAR:
		return parser.parserChar()
	case VARCHAR:
		return parser.parserVarchar()
	case DECIMAL:
		return parser.parseDecimal()
	case JSON:
		return parser.parseJson()
	case TypeUnknown:
		return nil, errors.Errorf("unknown data type: %s", token)
	default:
		return parser.newPrimitive(typeCode)
	}
}

func (parser *typeParser) consumeToken() string {
	if !parser.hasTokenLeft() {
		return ""
	}
	token := parser.tokens[parser.index]
	parser.index += 1

	return token
}

func (parser *typeParser) peekToken() string {
	if parser.index < len(parser.tokens) {
		return parser.tokens[parser.index]
	}

	return ""
}

func (parser *typeParser) expect(expected string) error {
	nextToken := parser.consumeToken()

	if nextToken != expected {
		return errors.Errorf("expect %s, but got %s at %d", expected, nextToken, parser.index)
	}

	return nil
}

func (parser *typeParser) newPrimitive(typeCode TypeID) (PrimitiveType, error) {
	return PrimitiveType{typeCode}, nil
}

func (parser *typeParser) parserChar() (CharType, error) {
	err := parser.expect("(")
	if err != nil {
		return CharType{}, err
	}

	token := parser.consumeToken()
	charLength, err := strconv.Atoi(token)
	if err != nil {
		return CharType{}, err
	}

	if charLength > 255 || charLength < 1 {
		return CharType{}, errors.Errorf("length of char is 1~255, get %d", charLength)
	}

	err = parser.expect(")")
	if err != nil {
		return CharType{}, err
	}

	return CharType{Length: charLength}, nil
}

func (parser *typeParser) parserVarchar() (VarcharType, error) {
	err := parser.expect("(")
	if err != nil {
		return VarcharType{}, err
	}

	token := parser.consumeToken()
	charLength, err := strconv.Atoi(token)
	if err != nil {
		return VarcharType{}, err
	}

	if charLength > 65535 || charLength < 1 {
		return VarcharType{}, errors.Errorf("length of varchar is 1~255, get %d", charLength)
	}

	err = parser.expect(")")
	if err != nil {
		return VarcharType{}, err
	}

	return VarcharType{Length: charLength}, nil
}

func (parser *typeParser) parseDecimal() (DecimalType, error) {
	if parser.peekToken() == "" {
		return NewDecimalType(38, 18), nil
	}

	err := parser.expect("(")
	if err != nil {
		return DecimalType{}, err
	}

	token := parser.consumeToken()
	precision, err := strconv.ParseInt(token, 10, 32)
	if err != nil {
		return DecimalType{}, err
	}

	err = parser.expect(",")
	if err != nil {
		return DecimalType{}, err
	}

	token = parser.consumeToken()
	scale, err := strconv.ParseInt(token, 10, 32)
	if err != nil {
		return DecimalType{}, err
	}

	err = parser.expect(")")
	if err != nil {
		return DecimalType{}, err
	}

	decimal := DecimalType{
		Precision: int32(precision),
		Scale:     int32(scale),
	}

	return decimal, nil
}

func (parser *typeParser) parseArray() (ArrayType, error) {
	err := parser.expect("<")
	if err != nil {
		return ArrayType{}, err
	}

	dataType, err := parser.parse()
	if err != nil {
		return ArrayType{}, err
	}

	err = parser.expect(">")
	if err != nil {
		return ArrayType{}, err
	}

	arrayType := ArrayType{
		ElementType: dataType,
	}

	return arrayType, nil
}

func (parser *typeParser) parseMap() (MapType, error) {
	err := parser.expect("<")
	if err != nil {
		return MapType{}, err
	}

	keyType, err := parser.parse()
	if err != nil {
		return MapType{}, err
	}

	err = parser.expect(",")
	if err != nil {
		return MapType{}, err
	}

	valueType, err := parser.parse()
	if err != nil {
		return MapType{}, err
	}

	err = parser.expect(">")
	if err != nil {
		return MapType{}, err
	}

	mapType := MapType{
		KeyType:   keyType,
		ValueType: valueType,
	}

	return mapType, nil
}

func (parser *typeParser) parseStruct() (StructType, error) {
	err := parser.expect("<")
	if err != nil {
		return StructType{}, err
	}

	var fields []StructFieldType

	for {
		fieldName := parser.consumeToken()
		if quoted, ok := strings.CutPrefix(fieldName, "`"); ok {
			name, closed := strings.CutSuffix(quoted, "`")
			if !closed || name == "" {
				return StructType{}, errors.New("invalid quoted struct field name")
			}
			fieldName = name
		}
		err = parser.expect(":")
		if err != nil {
			return StructType{}, err
		}
		fieldType, err := parser.parse()
		if err != nil {
			return StructType{}, err
		}

		fields = append(fields, NewStructFieldType(fieldName, fieldType))
		nextToken := parser.peekToken()

		switch nextToken {
		case ",":
			_ = parser.consumeToken()
		case ">":
			_ = parser.consumeToken()
			return NewStructType(fields...), nil
		default:
			return StructType{}, errors.Errorf("unexpected token %s at %d", nextToken, parser.index)
		}
	}
}

func (parser *typeParser) parseJson() (JsonType, error) {
	return JsonType{}, nil
}

func (parser *typeParser) hasTokenLeft() bool {
	return parser.index < len(parser.tokens)
}
