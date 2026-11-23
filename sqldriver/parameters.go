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

package sqldriver

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
)

type placeholder struct {
	start, end int
	name       string
}

func bindSQL(query string, args []driver.NamedValue) (string, error) {
	named := make(map[string]string)
	positional := make([]string, 0, len(args))
	for i, arg := range args {
		literal, err := sqlLiteral(arg.Value)
		if err != nil {
			return "", fmt.Errorf("SQL argument %d: %w", i+1, err)
		}
		if arg.Name == "" {
			positional = append(positional, literal)
			continue
		}
		if _, exists := named[arg.Name]; exists {
			return "", fmt.Errorf("duplicate SQL argument @%s", arg.Name)
		}
		named[arg.Name] = literal
	}
	if len(named) != 0 && len(positional) != 0 {
		return "", fmt.Errorf("cannot mix named and positional SQL arguments")
	}

	placeholders, err := scanPlaceholders(query, len(named) != 0)
	if err != nil {
		return "", err
	}
	if len(named) == 0 && len(placeholders) != len(positional) {
		return "", fmt.Errorf("expected %d positional SQL arguments, got %d", len(placeholders), len(positional))
	}
	if len(placeholders) == 0 && len(args) == 0 {
		return query, nil
	}

	var sql strings.Builder
	sql.Grow(len(query))
	used := make(map[string]bool, len(named))
	start := 0
	for i, p := range placeholders {
		sql.WriteString(query[start:p.start])
		if p.name == "" {
			if len(named) != 0 {
				return "", fmt.Errorf("cannot bind '?' with named SQL arguments")
			}
			sql.WriteString(positional[i])
		} else {
			literal, ok := named[p.name]
			if !ok {
				return "", fmt.Errorf("missing SQL argument @%s", p.name)
			}
			sql.WriteString(literal)
			used[p.name] = true
		}
		start = p.end
	}
	for _, arg := range args {
		if arg.Name != "" && !used[arg.Name] {
			return "", fmt.Errorf("unused SQL argument @%s", arg.Name)
		}
	}
	sql.WriteString(query[start:])
	return sql.String(), nil
}

func scanPlaceholders(query string, bindNamed bool) ([]placeholder, error) {
	var found []placeholder
	for i := 0; i < len(query); {
		c := query[i]
		tail := query[i:]
		switch {
		case strings.HasPrefix(tail, "--"):
			// CR and LF both end a line comment.
			end := strings.IndexAny(tail[2:], "\r\n")
			if end == -1 {
				return found, nil
			}
			i += 2 + end
		case strings.HasPrefix(tail, "/*"):
			end := strings.Index(tail[2:], "*/")
			if end == -1 {
				return nil, fmt.Errorf("unterminated SQL block comment")
			}
			i += 4 + end
		case (c == 'r' || c == 'R') && (i == 0 || !identifierByte(query[i-1])) &&
			len(tail) >= 3 && (tail[1] == '\'' || tail[1] == '"') && tail[2] == '(':
			end := strings.Index(tail[3:], ")"+string(tail[1]))
			if end == -1 {
				return nil, fmt.Errorf("unterminated SQL raw string")
			}
			i += 5 + end
		case c == '\'' || c == '"' || c == '\x60':
			end, err := skipSQLQuote(query, i)
			if err != nil {
				return nil, err
			}
			i = end
		case c == '?':
			found = append(found, placeholder{start: i, end: i + 1})
			i++
		case c == '@' && bindNamed && len(tail) > 1 && letter(tail[1]) &&
			(i == 0 || (!identifierByte(query[i-1]) && query[i-1] != '@')):
			end := i + 2
			for end < len(query) && identifierByte(query[end]) {
				end++
			}
			found = append(found, placeholder{start: i, end: end, name: query[i+1 : end]})
			i = end
		default:
			i++
		}
	}
	return found, nil
}

func identifierByte(c byte) bool {
	return letter(c) || c == '_' || c >= '0' && c <= '9'
}

func letter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func skipSQLQuote(query string, start int) (int, error) {
	quote := query[start]
	for i := start + 1; i < len(query); i++ {
		if query[i] == '\\' && quote != '\x60' {
			i++
			continue
		}
		if query[i] == quote {
			if i+1 < len(query) && query[i+1] == quote {
				i++
				continue
			}
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("unterminated SQL quote")
}

func sqlLiteral(v driver.Value) (string, error) {
	switch value := v.(type) {
	case nil:
		return "NULL", nil
	case int64:
		s := strconv.FormatInt(value, 10)
		if value < 0 {
			return "(" + s + ")", nil
		}
		return s, nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return "", fmt.Errorf("non-finite floating-point value")
		}
		s := strconv.FormatFloat(value, 'g', -1, 64)
		if !strings.ContainsRune(s, 'e') {
			s += "D"
		}
		return "(" + s + ")", nil
	case bool:
		return strconv.FormatBool(value), nil
	case string:
		return quoteSQLString(value)
	case []byte:
		if value == nil {
			return "NULL", nil
		}
		return "UNHEX('" + hex.EncodeToString(value) + "')", nil
	case time.Time:
		instant := value.UTC()
		if instant.Year() < 1 || instant.Year() > 9999 {
			return "", fmt.Errorf("timestamp year is outside 1–9999")
		}
		return "TIMESTAMP '" + instant.Format("2006-01-02 15:04:05.999999999Z") + "'", nil
	case data.Decimal:
		return decimalSQLLiteral(&value)
	case *data.Decimal:
		return decimalSQLLiteral(value)
	default:
		return "", fmt.Errorf("unsupported parameter type %T", v)
	}
}

func decimalSQLLiteral(value *data.Decimal) (string, error) {
	if value == nil || !value.Valid {
		return "NULL", nil
	}
	decimal, err := data.DecimalFromStr(value.String())
	if err != nil {
		return "", fmt.Errorf("invalid DECIMAL value")
	}
	return "(" + decimal.Sql() + ")", nil
}

func quoteSQLString(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("invalid UTF-8 string; bind binary data as []byte")
	}
	var literal strings.Builder
	literal.Grow(len(value) + 2)
	literal.WriteByte('\'')
	previousOctal := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c == '\\' || c == '\'':
			literal.WriteByte('\\')
			literal.WriteByte(c)
			previousOctal = false
		case c < ' ' || c == 0x7f || previousOctal && c >= '0' && c <= '9':
			// MaxCompute rejects digits immediately following an octal escape.
			fmt.Fprintf(&literal, "\\%03o", c)
			previousOctal = true
		default:
			literal.WriteByte(c)
			previousOctal = false
		}
	}
	literal.WriteByte('\'')
	return literal.String(), nil
}
