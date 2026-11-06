package data

import (
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// A null element and a string whose text is "NULL" are different values. Before this
// rendering rule they printed identically, which is the exact case where a NULL bug in
// user code or in the SDK looks like correct output.
func TestDiagnosticRendersNullAndTheLiteralNULLStringDifferently(t *testing.T) {
	stringArray := datatype.NewArrayType(datatype.StringType)

	nullElement := NewArrayWithType(stringArray)
	if err := nullElement.Append(nil); err != nil {
		t.Fatalf("append null: %v", err)
	}

	literalElement := NewArrayWithType(stringArray)
	if err := literalElement.Append(String("NULL")); err != nil {
		t.Fatalf("append the string NULL: %v", err)
	}

	mixed := NewArrayWithType(stringArray)
	if err := mixed.Append(nil); err != nil {
		t.Fatalf("append null: %v", err)
	}
	if err := mixed.Append(String("NULL")); err != nil {
		t.Fatalf("append the string NULL: %v", err)
	}

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"null element", nullElement.String(), "array(" + NULLDiagnostic + ")"},
		{"string that says NULL", literalElement.String(), "array('NULL')"},
		{"both in one array", mixed.String(), "array(NULL, 'NULL')"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: want %q, got %q", c.name, c.want, c.got)
		}
	}

	if nullElement.String() == literalElement.String() {
		t.Fatal("a null element and a string equal to NULL render the same")
	}
	// Sql() already distinguished them; the diagnostic form now matches it on the point
	// that matters, without becoming SQL.
	if !strings.Contains(literalElement.Sql(), "'NULL'") {
		t.Errorf("Sql() no longer quotes the literal: %s", literalElement.Sql())
	}
}

// The rule is "quote what would collide", not "quote strings": ordinary values keep the
// rendering existing tests and logs depend on.
func TestDiagnosticLeavesNonCollidingRenderingsAlone(t *testing.T) {
	stringArray := datatype.NewArrayType(datatype.StringType)
	arr := NewArrayWithType(stringArray)
	if err := arr.Append(String("hello")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := arr.Append(String("")); err != nil {
		t.Fatalf("append empty: %v", err)
	}
	if got, want := arr.String(), "array(hello, )"; got != want {
		t.Errorf("ordinary strings changed rendering: want %q, got %q", want, got)
	}

	intArray := datatype.NewArrayType(datatype.BigIntType)
	nums := NewArrayWithType(intArray)
	if err := nums.Append(int64(11)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got, want := nums.String(), "array(11)"; got != want {
		t.Errorf("numbers changed rendering: want %q, got %q", want, got)
	}
}

func TestDiagnosticForStructFieldsAndRecordColumns(t *testing.T) {
	structType, err := datatype.ParseDataType("struct<a:string>")
	if err != nil {
		t.Fatalf("parse struct type: %v", err)
	}

	nullField := NewStructWithTyp(structType.(datatype.StructType))
	if err := nullField.SetField("a", nil); err != nil {
		t.Fatalf("set null field: %v", err)
	}
	literalField := NewStructWithTyp(structType.(datatype.StructType))
	if err := literalField.SetField("a", String("NULL")); err != nil {
		t.Fatalf("set literal field: %v", err)
	}
	if nullField.String() == literalField.String() {
		t.Fatalf("struct renders a null field and the string NULL identically: %s", nullField.String())
	}
	if got, want := literalField.String(), "struct<a:'NULL'>"; got != want {
		t.Errorf("struct literal field: want %q, got %q", want, got)
	}

	nullColumn := Record{nil}
	literalColumn := Record{String("NULL")}
	if got, want := nullColumn.String(), "[NULL]"; got != want {
		t.Errorf("record null column: want %q, got %q", want, got)
	}
	if nullColumn.String() == literalColumn.String() {
		t.Fatalf("record renders a null column and the string NULL identically: %s", nullColumn.String())
	}
	if got, want := literalColumn.String(), "['NULL']"; got != want {
		t.Errorf("record literal column: want %q, got %q", want, got)
	}

	// A map keeps its Sql() rendering, which already quotes: pin that it distinguishes the
	// two as well, so the three containers cannot drift apart silently.
	mapType, err := datatype.ParseDataType("map<string,string>")
	if err != nil {
		t.Fatalf("parse map type: %v", err)
	}
	m := NewMapWithType(mapType.(datatype.MapType))
	if err := m.Set("k", nil); err != nil {
		t.Fatalf("set null value: %v", err)
	}
	literal := NewMapWithType(mapType.(datatype.MapType))
	if err := literal.Set("k", String("NULL")); err != nil {
		t.Fatalf("set literal value: %v", err)
	}
	if m.String() == literal.String() {
		t.Fatalf("map renders a null value and the string NULL identically: %s", m.String())
	}
}
