package common

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"testing"
	"time"
)

// 辅助函数，检查错误是否包含指定消息
func assertErrorContains(t *testing.T, err error, msg string) {
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !errors.Is(err, errors.New(msg)) {
		t.Errorf("Expected error containing '%s', got '%v'", msg, err)
	}
}

// TestParseRFC1123Date_Valid 测试有效RFC1123日期字符串的解析
func TestParseRFC1123Date_Valid(t *testing.T) {
	testCases := []struct {
		input    string
		expected time.Time
	}{
		{
			input:    "Wed, 01 Jan 2020 00:00:00 GMT",
			expected: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := ParseRFC1123Date(tc.input)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !result.Equal(tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
			if result.Location() != GMT {
				t.Errorf("Expected location GMT, got %v", result.Location())
			}
		})
	}
}

// TestGMTTime_UnmarshalXML 测试XML解码
func TestGMTTime_UnmarshalXML(t *testing.T) {
	testCases := []struct {
		xmlData  string
		expected GMTTime
		errMsg   string
	}{
		{
			xmlData:  `<root><time>Wed, 01 Jan 2020 00:00:00 GMT</time></root>`,
			expected: GMTTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			xmlData:  `<root><time></time></root>`,
			expected: GMTTime{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.xmlData, func(t *testing.T) {
			// 关键修改：使用指针接收者触发自定义 UnmarshalXML
			var data struct {
				Time *GMTTime `xml:"time"`
			}
			err := xml.Unmarshal([]byte(tc.xmlData), &data)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// 处理空指针情况
			if data.Time == nil {
				if tc.expected != (GMTTime{}) {
					t.Errorf("Expected %v, got nil", tc.expected)
				}
				return
			}

			if !time.Time(*data.Time).Equal(time.Time(tc.expected)) {
				t.Errorf("Expected %v, got %v", tc.expected, *data.Time)
			}
		})
	}
}

func TestGMTTime_JSON(t *testing.T) {
	testTime := GMTTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	testCases := []struct {
		name     string
		jsonData string
		expected *GMTTime // 改用指针更准确
		errMsg   string
	}{
		{
			name:     "ValidTimestamp",
			jsonData: `{"time":1577836800}`,
			expected: &testTime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var data struct {
				Time *GMTTime `json:"time"` // 使用指针触发自定义方法
			}

			// 解码测试
			err := json.Unmarshal([]byte(tc.jsonData), &data)

			if tc.errMsg != "" {
				assertErrorContains(t, err, tc.errMsg)
				return
			}

			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			if data.Time == nil {
				t.Fatal("Time should not be nil")
			}

			if !time.Time(*data.Time).Equal(time.Time(*tc.expected)) {
				t.Errorf("Decode expected %v, got %v",
					tc.expected, data.Time)
			}

			// 编码测试
			bytes, err := json.Marshal(data)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			var reconstructed struct {
				Time int64 `json:"time"`
			}
			err = json.Unmarshal(bytes, &reconstructed)
			if err != nil {
				t.Fatalf("Re-decode failed: %v", err)
			}
		})
	}
}

// TestGMTTime_Format_String 测试格式化方法
func TestGMTTime_Format_String(t *testing.T) {
	gmt := GMTTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	expectedString := "2020-01-01 00:00:00 +0000 UTC"
	expectedFormat := "2020-01-01"

	if gmt.String() != expectedString {
		t.Errorf("String() expected %s, got %s", expectedString, gmt.String())
	}
	if gmt.Format("2006-01-02") != expectedFormat {
		t.Errorf("Format() expected %s, got %s", expectedFormat, gmt.Format("2006-01-02"))
	}
}
