package data

import (
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"testing"
)

func TestTypeConvert(t *testing.T) {
	decimal, _ := DecimalFromStr("100.23")
	array := NewArray(datatype.NewArrayType(datatype.IntType))
	_ = array.AddValue(Int(1), Int(2))
	m := NewMap(datatype.NewMapType(datatype.StringType, datatype.StringType))
	sa := String("a")
	sb := String("b")
	_ = m.Set(&sa, &sb)
	st := NewStruct(datatype.NewStructType(
		datatype.NewStructFieldType("a", datatype.FloatType),
		datatype.NewStructFieldType("b", datatype.DateType),
	))

	d, _ := NewDate("2021-12-08")
	st.SetField("a", Float(10.23))
	st.SetField("b", d)

	values := []struct {
		src Data
		dst Data
	}{
		{decimal, new(Decimal)},
		{TinyInt(10), new(TinyInt)},
		{array, new(Array)},
		{m, new(Map)},
		{st, new(Struct)},
		{d, new(Date)},
		{Binary([]byte{1, 2, 3}), new(Binary)},
	}

	for _, value := range values {
		src, dst := value.src, value.dst
		err := tryConvertType(src, dst)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if src.String() != dst.String() {
			t.Fatalf("expected %s, but get %s", src, dst)
		}
	}

	var dd Date
	err := dd.Scan(d)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var bb Binary
	err = bb.Scan(Binary([]byte{1, 2, 3}))
	if err != nil {
		t.Fatalf("%+v", err)
	}
}
