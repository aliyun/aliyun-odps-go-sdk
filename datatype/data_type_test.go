package datatype

import (
	"testing"
)

func TestTokenize(t *testing.T)  {
	name := []string{
		"struct<x:int,y:varchar(256),z:struct<a:tinyint,b:date>>",
		"Map<char(10), int>",
		"datetime",
	}
	tokens := [][]string {
		tokenize(name[0]),
		tokenize(name[1]),
		tokenize(name[2]),
	}

	expected := [][]string {
		{
			"struct", "<", "x", ":", "int", ",", "y", ":", "varchar", "(", "256", ")", ",",
			"z", ":", "struct", "<", "a", ":", "tinyint", ",", "b", ":", "date", ">", ">",
		},
		{
			"Map", "<", "char", "(", "10", ")", ",", "int", ">",
		},
		{
			"datetime",
		},
	}

	for i := range expected {
		e, g := expected[i], tokens[i]
		if len(e) != len(g) {
			t.Fatalf("fail to tokenize for %s, got %v", name[i], g)
		}

		for j := range e {
			et, gt := e[j], g[j]

			if et != gt {
				t.Fatalf("fail to tokenize for %s, %dnth token should be %s, real is %s", name, j, et, gt)
			}
		}
	}
}

func TestTypeParser(t *testing.T)  {
	name := "struct<x:int,y:varchar(256),z:struct<a:tinyint,b:date>>"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(StructType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewStructType([]StructField {
		NewStructField("x", NewInt()),
		NewStructField("y", NewVarcharType(256)),
		NewStructField(
			"z",
			NewStructType(
				NewStructField("a", NewTinyint()),
				NewStructField("b", NewDate()),
			)),
	}...)

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}

func TestParserDecimal(t *testing.T)  {
	name := "Decimal(10,2)"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(DecimalType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewDecimalType(10, 2)

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}

func TestParserVarchar(t *testing.T)  {
	name := "Varchar(32768)"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(VarcharType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewVarcharType(32768)

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}


func TestParserChar(t *testing.T)  {
	name := "Char(23)"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(CharType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewCharType(23)

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}

func TestParserMap(t *testing.T)  {
	name := "Map<char(10),int>"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(MapType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewMapType(NewCharType(10), NewInt())

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}

func TestParserArray(t *testing.T)  {
	name := "Array<Map<char(10),int>>"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(ArrayType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewArrayType(NewMapType(NewCharType(10), NewInt()))

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}

func TestParserPrimitive(t *testing.T)  {
	name := "datetime"
	dataType, err := ParseDataType(name)
	if err != nil {
		t.Fatal(err.Error())
	}

	got, ok := dataType.(PrimitiveType)

	if !ok {
		t.Fatalf("failed to parse data type %s", name)
	}

	expected := NewPrimitiveType(DATETIME)

	if ! IsTypeEqual(expected, got) {
		t.Fatalf("failed to parse type: %s, got %s", name, got.Name())
	}
}

func TestParseFailed(t *testing.T)  {
	names := []string{"datetime,", "int tinyint"}
	for _, name := range names {
		_, err := ParseDataType(name)

		if err == nil {
			t.Fatalf("%s should not be parsed, it is a invlaid data type", names)
		}
	}
}

func TestDataTyeName(t *testing.T)  {
	types := []DataType{
		NewMapType(NewCharType(10), NewInt()),
		NewStructType([]StructField{
			NewStructField("x", NewInt()),
			NewStructField("y", NewVarcharType(256)),
			NewStructField(
				"z",
				NewStructType(
					NewStructField("a", NewTinyint()),
					NewStructField("b", NewDate()),
				)),
		}...),
		NewCharType(10),
		NewVarcharType(10),
		NewDecimalType(20, 3),
		NewArrayType(NewArrayType(NewInt())),
	}

	names := []string {
		"MAP<CHAR(10),INT>",
		"STRUCT<x:INT,y:VARCHAR(256),z:STRUCT<a:TINYINT,b:DATE>>",
		"CHAR(10)",
		"VARCHAR(10)",
		"DECIMAL(20,3)",
		"ARRAY<ARRAY<INT>>",
	}

	for i := range types {
		dt, name := types[i], names[i]

		if dt.Name() != name {
			t.Fatalf("name for type %s is error, got %s", name, dt.Name())
		}
	}
}