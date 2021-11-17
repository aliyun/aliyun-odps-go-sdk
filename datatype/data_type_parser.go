package datatype

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type typeParser struct {
	tokens []string
	index  int
}

func ParseDataType(name string) (DataType, error) {
	parser := typeParser{
		tokens: tokenize(name),
		index: 0,
	}

	dataType, err := parser.parse()

	if parser.hasTokenLeft() {
		return nil, errors.New(fmt.Sprintf(
			"parse datatype error: %s, unexpected token: %s at: %d",
			name, parser.peekToken(), parser.index))
	}

	if err != nil {
		err = errors.New(fmt.Sprintf("parse datatype error: %s, %s", name, err.Error()))
	}

	return dataType, err
}

func tokenize(name string) []string {
	name = strings.TrimSpace(name)
	tokens := make([]string, 0)
	var tokenBuilder strings.Builder

	for _, c := range name {
		if unicode.IsSpace(c) {
			continue
		}

		if isIdentifierChar(c) {
			tokenBuilder.WriteRune(c)
		} else {
			if tokenBuilder.Len() > 0 {
				tokens = append(tokens, tokenBuilder.String())
				tokenBuilder.Reset()
			}

			tokens = append(tokens, string(c))
		}
	}

	if len(tokens) == 0 && len(name) > 0 {
		tokens = append(tokens, strings.Fields(name)...)
	}

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
	case TypeUnknown:
		return nil, errors.New("unknown data type: " + token)
	default:
		return parser.newPrimitive(typeCode)
	}
}

func (parser *typeParser)consumeToken() string {
	token := parser.tokens[parser.index]
	parser.index += 1

	return token
}

func (parser *typeParser) peekToken() string {
	return parser.tokens[parser.index]
}

func (parser *typeParser) expect(expected string) error  {
	nextToken := parser.consumeToken()

	if nextToken != expected {
		return errors.New(fmt.Sprintf("expect %s, but got %s at %d", expected, nextToken, parser.index))
	}

	return nil
}

func (parser *typeParser) newPrimitive(typeCode TypeCode) (PrimitiveType, error) {
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
		return CharType{}, errors.New(fmt.Sprintf("length of char is 1~255, get %d", charLength))
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
		return VarcharType{}, errors.New(fmt.Sprintf("length of varchar is 1~255, get %d", charLength))
	}

	err = parser.expect(")")
	if err != nil {
		return VarcharType{}, err
	}

	return VarcharType{Length: charLength}, nil
}

func (parser *typeParser) parseDecimal() (DecimalType, error)  {
	err := parser.expect("(")
	if err != nil {
		return DecimalType{}, err
	}

	token := parser.consumeToken()
	precision, err := strconv.Atoi(token)
	if err != nil {
		return DecimalType{}, err
	}

	err = parser.expect(",")
	if err != nil {
		return DecimalType{}, err
	}

	token = parser.consumeToken()
	scale, err := strconv.Atoi(token)
	if err != nil {
		return DecimalType{}, err
	}

	err = parser.expect(")")
	if err != nil {
		return DecimalType{}, err
	}

	decimal := DecimalType{
		Precision: precision,
		Scale: scale,
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

	arrayType := ArrayType {
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
		KeyType: keyType,
		ValueType: valueType,
	}

	return mapType, nil
}

func (parser *typeParser) parseStruct() (StructType, error) {
	err := parser.expect("<")
	if err != nil {
		return StructType{}, err
	}

	var fields []StructField

LOOP:
	for {
		filedName := parser.consumeToken()
		err = parser.expect(":")
		if err != nil {
			return StructType{}, err
		}
		filedType, err := parser.parse()
		if err != nil {
			return StructType{}, err
		}

		structFiled := StructField{
			Name: filedName,
			Type: filedType,
		}

		fields = append(fields, structFiled)
		nextToken := parser.peekToken()

		switch nextToken {
		case ",":
			var _ = parser.consumeToken()
		case ">":
			break LOOP
		default:
			return StructType{}, errors.New(fmt.Sprintf("unexpected token %s at %d", nextToken, parser.index))
		}
	}

	err = parser.expect(">")
	if err != nil {
		return StructType{}, err
	}

	structType := StructType{
		fields,
	}

	return structType, nil
}

func (parser *typeParser) hasTokenLeft() bool  {
	return parser.index < len(parser.tokens)
}