package tunnel

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// TestProtocStreamReaderLargeArray 测试读取大量数据时的性能
// 模拟 PR 中提到的场景：1000、10000 甚至 10 万长度的 arraymap
func TestProtocStreamReaderLargeArray(t *testing.T) {
	// 创建一个包含大量 varint 的数据流
	// 模拟 arraymap 的场景：每个元素都有一个长度和值
	size := 10000
	buf := make([]byte, 0, size*5) // 每个 varint 最多 5 字节

	// 创建 10000 个 varint 值
	for i := 0; i < size; i++ {
		buf = protowire.AppendVarint(buf, uint64(i))
	}

	reader := NewProtocStreamReader(bytes.NewReader(buf))

	// 读取所有 varint
	for i := 0; i < size; i++ {
		val, err := reader.ReadVarint()
		if err != nil {
			t.Fatalf("读取第 %d 个 varint 失败: %v", i, err)
		}
		if val != uint64(i) {
			t.Errorf("第 %d 个 varint 值错误: 期望 %d, 得到 %d", i, i, val)
		}
	}

	// 再次读取应该返回 EOF
	_, err := reader.ReadVarint()
	if err == nil {
		t.Error("期望返回 EOF，但没有返回错误")
	}
}

// TestProtocStreamReaderMixedReads 测试混合读取场景
// 模拟实际的数据流：包含 varint、fixed32、fixed64、bytes 等
func TestProtocStreamReaderMixedReads(t *testing.T) {
	// 构建一个混合数据流
	buf := make([]byte, 0, 1024)

	// 写入各种类型的数据
	buf = protowire.AppendVarint(buf, 12345)        // varint
	buf = protowire.AppendFixed32(buf, 0x12345678)  // fixed32
	buf = protowire.AppendFixed64(buf, 0x1234567890ABCDEF) // fixed64
	buf = protowire.AppendVarint(buf, uint64(len("hello")))
	buf = append(buf, []byte("hello")...)          // bytes
	buf = protowire.AppendVarint(buf, 0)           // bool: false
	buf = protowire.AppendVarint(buf, 1)           // bool: true

	reader := NewProtocStreamReader(bytes.NewReader(buf))

	// 读取 varint
	val, err := reader.ReadVarint()
	if err != nil {
		t.Fatalf("读取 varint 失败: %v", err)
	}
	if val != 12345 {
		t.Errorf("varint 值错误: 期望 12345, 得到 %d", val)
	}

	// 读取 fixed32
	val32, err := reader.ReadFixed32()
	if err != nil {
		t.Fatalf("读取 fixed32 失败: %v", err)
	}
	if val32 != 0x12345678 {
		t.Errorf("fixed32 值错误: 期望 0x12345678, 得到 0x%08x", val32)
	}

	// 读取 fixed64
	val64, err := reader.ReadFixed64()
	if err != nil {
		t.Fatalf("读取 fixed64 失败: %v", err)
	}
	if val64 != 0x1234567890ABCDEF {
		t.Errorf("fixed64 值错误: 期望 0x1234567890ABCDEF, 得到 0x%016x", val64)
	}

	// 读取 bytes
	bytesData, err := reader.ReadBytes()
	if err != nil {
		t.Fatalf("读取 bytes 失败: %v", err)
	}
	if string(bytesData) != "hello" {
		t.Errorf("bytes 值错误: 期望 'hello', 得到 '%s'", string(bytesData))
	}

	// 读取 bool: false
	boolVal, err := reader.ReadBool()
	if err != nil {
		t.Fatalf("读取 bool 失败: %v", err)
	}
	if boolVal != false {
		t.Errorf("bool 值错误: 期望 false, 得到 %v", boolVal)
	}

	// 读取 bool: true
	boolVal, err = reader.ReadBool()
	if err != nil {
		t.Fatalf("读取 bool 失败: %v", err)
	}
	if boolVal != true {
		t.Errorf("bool 值错误: 期望 true, 得到 %v", boolVal)
	}

	// 再次读取应该返回 EOF
	_, err = reader.ReadVarint()
	if err == nil {
		t.Error("期望返回 EOF，但没有返回错误")
	}
}

// TestProtocStreamReaderBoundary 测试边界情况
func TestProtocStreamReaderBoundary(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
	}{
		{"最小值", 0},
		{"1字节最大值", 0x7F},
		{"2字节最大值", 0x3FFF},
		{"3字节最大值", 0x1FFFFF},
		{"4字节最大值", 0xFFFFFFF},
		{"5字节最大值", 0x7FFFFFFFF},
		{"最大64位值", 0xFFFFFFFFFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := protowire.AppendVarint(nil, tt.value)
			reader := NewProtocStreamReader(bytes.NewReader(buf))

			val, err := reader.ReadVarint()
			if err != nil {
				t.Fatalf("读取失败: %v", err)
			}
			if val != tt.value {
				t.Errorf("值错误: 期望 %d, 得到 %d", tt.value, val)
			}
		})
	}
}

// BenchmarkProtocStreamReaderVarint 基准测试：varint 读取性能
func BenchmarkProtocStreamReaderVarint(b *testing.B) {
	// 创建测试数据
	data := make([]byte, 0, b.N*5)
	for i := 0; i < b.N; i++ {
		data = protowire.AppendVarint(data, uint64(i))
	}

	b.ResetTimer()
	reader := NewProtocStreamReader(bytes.NewReader(data))
	for i := 0; i < b.N; i++ {
		_, err := reader.ReadVarint()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProtocStreamReaderBytes 基准测试：bytes 读取性能
func BenchmarkProtocStreamReaderBytes(b *testing.B) {
	// 创建测试数据
	testData := []byte("hello world")
	data := make([]byte, 0, b.N*(len(testData)+5))
	for i := 0; i < b.N; i++ {
		data = protowire.AppendVarint(data, uint64(len(testData)))
		data = append(data, testData...)
	}

	b.ResetTimer()
	reader := NewProtocStreamReader(bytes.NewReader(data))
	for i := 0; i < b.N; i++ {
		_, err := reader.ReadBytes()
		if err != nil {
			b.Fatal(err)
		}
	}
}