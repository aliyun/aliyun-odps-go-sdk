package common

import "strings"

func QuoteString(str string) string {
	str = strings.ReplaceAll(str, "'", "\\'")
	return "'" + str + "'"
}

func QuoteRef(ref string) string {
	return "`" + ref + "`"
}
