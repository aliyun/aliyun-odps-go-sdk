package utils

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ContentMD5  = "Content-MD5"
	ContentType = "Content-Type"
	Date        = "Date"
	PREFIX      = "x-odps-"
)

var GMT, _ = time.LoadLocation("GMT")

// GetApiTimestamp 获取格式为 'Fri, 13 Dec 2024 02:57:00 GMT' 的时间戳
func GetApiTimestamp() (result *string) {
	timestamp := time.Now().In(GMT).Format(time.RFC1123)
	return &timestamp
}

// BuildCanonicalString 构建规范字符串
func BuildCanonicalString(method *string, resource *string, params map[string]*string, headers map[string]*string) (result *string) {
	var builder strings.Builder
	builder.WriteString(*method + "\n")

	headersToSign := make(map[string]string)

	// 筛选 headers
	for key, value := range headers {
		if key != "" {
			lowerKey := strings.ToLower(key)
			if lowerKey == strings.ToLower(ContentMD5) || lowerKey == strings.ToLower(ContentType) || lowerKey == strings.ToLower(Date) || strings.HasPrefix(lowerKey, PREFIX) {
				if value != nil {
					headersToSign[lowerKey] = *value
				} else {
					headersToSign[lowerKey] = ""
				}
			}
		}
	}

	// 确保 Content-Type 和 Content-MD5 存在
	if _, exists := headersToSign[strings.ToLower(ContentType)]; !exists {
		headersToSign[strings.ToLower(ContentType)] = ""
	}
	if _, exists := headersToSign[strings.ToLower(ContentMD5)]; !exists {
		headersToSign[strings.ToLower(ContentMD5)] = ""
	}

	// 添加 params
	for key, value := range params {
		if strings.HasPrefix(key, PREFIX) {
			if value != nil {
				headersToSign[key] = *value
			} else {
				headersToSign[key] = ""
			}
		}
	}

	// 按键排序并加入 builder
	keys := make([]string, 0, len(headersToSign))
	for key := range headersToSign {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if strings.HasPrefix(key, PREFIX) {
			builder.WriteString(key + ":" + headersToSign[key])
		} else {
			builder.WriteString(headersToSign[key])
		}
		builder.WriteString("\n")
	}

	// 添加资源部分
	builder.WriteString(buildCanonicalResource(resource, params))
	res := builder.String()
	return &res
}

// buildCanonicalResource 构建规范资源字符串
func buildCanonicalResource(resource *string, params map[string]*string) string {
	var builder strings.Builder
	builder.WriteString(*resource)

	if params != nil && len(params) > 0 {
		var keys []string
		for key := range params {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		builder.WriteString("?")
		for i, key := range keys {
			if i > 0 {
				builder.WriteString("&")
			}
			builder.WriteString(url.QueryEscape(key))
			if value, exists := params[key]; exists && *value != "" {
				builder.WriteString("=" + url.QueryEscape(*value))
			}
		}
	}
	return builder.String()
}

// GetSignature 获取签名
func GetSignature(strToSign *string, accessKeyId *string, accessKeySecret *string) (result *string) {
	secretKey := []byte(*accessKeySecret)
	h := hmac.New(sha1.New, secretKey)
	h.Write([]byte(*strToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	res := "ODPS " + *accessKeyId + ":" + signature
	return &res
}

// ToString 转换为字符串，能够兼容指针类型与非指针类型
func ToString(obj interface{}) *string {
	if obj == nil {
		return nil
	}
	// 使用 reflect 获取值
	v := reflect.ValueOf(obj)

	// 如果是指针，获取其指向的值
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		// 获取指针指向的值
		v = v.Elem()
	}

	var res string
	// 根据类型转换为字符串
	switch v.Kind() {
	case reflect.String:
		res = v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		res = strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		res = strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		res = strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		res = strconv.FormatBool(v.Bool())
	default:
		res = fmt.Sprintf("%v", obj)
	}
	return &res
}
