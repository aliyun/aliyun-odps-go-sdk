package common

import (
	"strings"
)

func QuoteString(str string) string {
	str = strings.ReplaceAll(str, "'", "\\'")
	return "'" + str + "'"
}

func QuoteRef(ref string) string {
	return "`" + ref + "`"
}

// StringToBool 将字符串转换为布尔值，不合法时返回 false
func StringToBool(s string) bool {
	return strings.ToLower(s) == "true"
}
