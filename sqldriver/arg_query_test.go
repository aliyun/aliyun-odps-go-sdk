package sqldriver

import (
	"testing"
)

func TestNamedArgQuery(t *testing.T) {
	queries := []string{
		"select * from student where name=@name and student.age<@age or name like '@name' or name='cat'",
		"select * from student where name=@name",
		"update user set name=@name, age=@age, address=@address where id=1",
	}

	expected := []struct {
		value string
		isErr bool
	}{
		{"select * from student where name=tom and student.age<10 or name like 'tom' or name='cat'", false},
		{"select * from student where name=tom", false},
		{"", true},
	}

	for i, q := range queries {
		nq := NewNamedArgQuery(q)
		nq.SetArg("name", "tom")
		nq.SetArg("age", 10)

		sql, err := nq.toSql()

		if expected[i].isErr && err == nil {
			t.Fatalf("some args are not set for %s, there should be an error, but not", queries[i])
		}

		if expected[i].isErr && err != nil {
			continue
		}

		if err != nil {
			t.Fatal(err)
		}

		if !expected[i].isErr && sql != expected[i].value {
			t.Fatalf("expect %s, but get %s", expected[i].value, sql)
		}
	}
}

func TestPositionArgQuery(t *testing.T) {
	queries := []string{
		"select * from student where name=? and student.age<? or name like '?' or name='cat'",
		"select * from student where name=?",
		"update user set name=?, age=?, address=? where id=1",
	}

	expected := []struct {
		value string
		isErr bool
	}{
		{"select * from student where name=tom and student.age<10 or name like 'tom' or name='cat'", false},
		{"select * from student where name=tom", false},
		{"", true},
	}

	args := [][]interface{}{
		{"tom", 10, "tom"},
		{"tom"},
		{"tom", 10},
	}

	for i, q := range queries {
		pq := NewPositionArgQuery(q)
		pq.SetArgs(args[i]...)

		sql, err := pq.toSql()

		if expected[i].isErr && err == nil {
			t.Fatalf("some args are not set for %s, there should be an error, but not", queries[i])
		}

		if expected[i].isErr && err != nil {
			continue
		}

		if err != nil {
			t.Fatal(err)
		}

		if !expected[i].isErr && sql != expected[i].value {
			t.Fatalf("expect %s, but get %s", expected[i].value, sql)
		}
	}
}
