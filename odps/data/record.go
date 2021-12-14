package data

import "strings"

type Record []Data

func NewRecord(columnNums int) Record {
	return make([]Data, 0, columnNums)
}

func (r *Record) Append(d Data) {
	s := []Data(*r)
	*r = append(s, d)
}

func (r *Record) Len() int {
	return len(*r)
}

func (r *Record) Get(i int) Data {
	return (*r)[i]
}

func (r *Record) String() string {
	var sb strings.Builder
	sb.WriteString("[")

	n := len(*r) - 1
	for i, f := range *r {
		sb.WriteString(f.String())

		if i < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("]")

	return sb.String()
}
