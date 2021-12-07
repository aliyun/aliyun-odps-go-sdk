package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"strings"
)

type MapKeyTypeErr struct {
	mapType datatype.MapType
	keyType datatype.DataType
}

func (m MapKeyTypeErr) Error() string {
	return fmt.Sprintf("try to set key of %s type to %s", m.keyType, m.mapType)
}

type MapValueTypeErr struct {
	mapType datatype.MapType
	valueType datatype.DataType
}

func (m MapValueTypeErr) Error() string {
	return fmt.Sprintf("try to set value of %s type to %s", m.valueType, m.mapType)
}

type Map struct {
	_type datatype.MapType
	data  map[Data]Data
}

func (m *Map) SetValue(data map[Data]Data) {
	m.data = data
}

func NewMap(t datatype.MapType) *Map  {
	return &Map {
		_type: t,
		data: make(map[Data]Data),
	}
}

func (m *Map) Type() datatype.DataType  {
	return m._type
}

func (m *Map) String() string {
	i, n := 0, len(m.data)
	if n == 0 {
		return "map()"
	}

	sb := strings.Builder{}

	for key, value := range m.data {
		sb.WriteString(key.String())
		sb.WriteString(", ")
		sb.WriteString(value.String())

		i += 1
		if i < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")
	return sb.String()
}

func (m *Map) Value() map[Data]Data {
	return m.data
}

func (m *Map) Set(key Data, value Data) error {
	if ! datatype.IsTypeEqual(key.Type(), m._type.KeyType) {
		return MapKeyTypeErr{keyType: key.Type(), mapType: m._type}
	}

	if ! datatype.IsTypeEqual(value.Type(), m._type.ValueType) {
		return MapValueTypeErr{valueType: key.Type(), mapType: m._type}
	}

	m.data[key] = value
	return nil
}
