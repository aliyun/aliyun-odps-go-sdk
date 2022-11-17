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
	"fmt"
	"regexp"
	"strings"
)

type NamedArgQuery struct {
	query        string
	queryPieces  []string
	argPositions map[string][]int
	argSetRecord map[string]bool
}

func NewNamedArgQuery(query string) NamedArgQuery {
	rgx := regexp.MustCompile("@[a-zA-Z]\\w*")
	indexes := rgx.FindAllStringIndex(query, -1)
	argNames := rgx.FindAllString(query, -1)

	queryPieces := make([]string, 0, 2*len(indexes)+1)
	argPositions := make(map[string][]int, len(indexes))
	argSetRecord := make(map[string]bool)

	s, e := 0, 0

	for i, index := range indexes {
		argName := argNames[i][1:]
		e = index[0]

		queryPieces = append(queryPieces, query[s:e])
		queryPieces = append(queryPieces, "")
		argPositions[argName] = append(argPositions[argName], 2*i+1)
		argSetRecord[argName] = false

		s = index[1]
	}

	if s <= len(query)-1 {
		queryPieces = append(queryPieces, query[s:])
	}

	return NamedArgQuery{
		query:        query,
		queryPieces:  queryPieces,
		argPositions: argPositions,
		argSetRecord: argSetRecord,
	}
}

func (n *NamedArgQuery) SetArg(name string, value interface{}) {
	positions := n.argPositions[name]
	if positions == nil {
		return
	}

	for _, i := range positions {
		n.queryPieces[i] = fmt.Sprintf("%v", value)
	}

	n.argSetRecord[name] = true
}

func (n *NamedArgQuery) toSql() (string, error) {
	var unSetArgs []string
	for arg, set := range n.argSetRecord {
		if !set {
			unSetArgs = append(unSetArgs, arg)
		}
	}

	if len(unSetArgs) > 1 {
		return "", fmt.Errorf("value of [%s] are not set for \"%s\"", strings.Join(unSetArgs, ", "), n.query)
	}

	if len(unSetArgs) == 1 {
		return "", fmt.Errorf("value of %s is not set for \"%s\"", unSetArgs[0], n.query)
	}

	return strings.Join(n.queryPieces, ""), nil
}
