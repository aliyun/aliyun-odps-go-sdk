package utils

import (
	"fmt"
	"testing"
)

func TestToString(t *testing.T) {
	value := "hello"
	var ptr *string
	ptr = &value

	t.Log(fmt.Sprintf("%v", ptr)) // 0x000
	t.Log(*ToString(ptr))         // hello
	t.Log(*ToString(value))       // hello
}
