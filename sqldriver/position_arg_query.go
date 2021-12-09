package sqldriver

import (
	"fmt"
	"regexp"
	"strings"
)

type PositionArgQuery struct {
	query        string
	queryPieces  []string
	argPositions []int
	lastArgSet   int
}

func NewPositionArgQuery(query string) PositionArgQuery {
	rgx := regexp.MustCompile("\\?")
	indexes := rgx.FindAllStringIndex(query, -1)
	queryPieces := make([]string, 0, 2*len(indexes)+1)
	argPositions := make([]int, 0, len(indexes))

	s, e := 0, 0

	for i, index := range indexes {
		e = index[0]

		queryPieces = append(queryPieces, query[s:e])
		queryPieces = append(queryPieces, "")
		argPositions = append(argPositions, 2*i+1)

		s = index[1]
	}

	if s < len(query)-1 {
		queryPieces = append(queryPieces, query[s:])
	}

	return PositionArgQuery{
		query:        query,
		queryPieces:  queryPieces,
		argPositions: argPositions,
	}
}

func (p *PositionArgQuery) SetArgs(args ...interface{}) {
	n := min(len(args), len(p.argPositions))

	for i := 0; i < n; i++ {
		arg := args[i]
		argPosition := p.argPositions[i]
		p.queryPieces[argPosition] = fmt.Sprintf("%v", arg)
	}

	p.lastArgSet = n
}

func (p *PositionArgQuery) toSql() (string, error) {
	if p.lastArgSet < len(p.argPositions) {
		return "", fmt.Errorf("%dth arg is not set for \"%s\"", p.lastArgSet+1, p.query)
	}

	return strings.Join(p.queryPieces, ""), nil
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}
