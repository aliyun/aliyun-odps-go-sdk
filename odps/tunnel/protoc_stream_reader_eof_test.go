package tunnel

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestProtocStreamReaderEOF 测试流结束时的 EOF 检测行为
// 这个测试验证使用 bufio.Reader 后，EOF 检测是否仍然正确
func TestProtocStreamReaderEOF(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		readCount      int
		expectEOF      bool
	}{
		{
			name:           "正常数据读取",
			data:           []byte{0x01}, // varint: 1
			readCount:      1,
			expectEOF:      false,
		},
		{
			name:           "空数据流",
			data:           []byte{},
			readCount:      1,
			expectEOF:      true, // 应该返回 EOF
		},
		{
			name:           "刚好读完数据后读取",
			data:           []byte{0x01}, // 读取完后没有更多数据
			readCount:      2, // 读取两次
			expectEOF:      true, // 第二次读取应该返回 EOF
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewProtocStreamReader(bytes.NewReader(tt.data))

			// 尝试读取 varint
			for i := 0; i < tt.readCount; i++ {
				_, err := reader.ReadVarint()

				if i < tt.readCount-1 {
					// 前几次读取应该成功
					if err != nil {
						t.Errorf("第 %d 次读取期望成功，但得到错误: %v", i+1, err)
					}
				} else {
					// 最后一次读取
					if tt.expectEOF {
						if err == nil {
							t.Errorf("期望返回 EOF，但没有返回错误")
						} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
							t.Errorf("期望返回 EOF 或 ErrUnexpectedEOF，但得到: %v", err)
						}
					} else {
						if err != nil {
							t.Errorf("期望成功读取，但得到错误: %v", err)
						}
					}
				}
			}
		})
	}
}

// TestProtocStreamReaderEOFWithBuffer 测试缓冲区对 EOF 检测的影响
// 这是关键的测试：验证当缓冲区有数据时，EOF 检测是否正确
func TestProtocStreamReaderEOFWithBuffer(t *testing.T) {
	// 创建一个读取器，返回正好 3 字节数据，然后 EOF
	// 使用正确的 varint 编码
	data := []byte{0x01, 0x02, 0x03} // 三个简单的 varint 值
	reader := NewProtocStreamReader(bytes.NewReader(data))

	// 读取第一个 varint
	val1, err := reader.ReadVarint()
	if err != nil {
		t.Fatalf("第一次读取失败: %v", err)
	}
	if val1 != 1 {
		t.Errorf("第一次读取值错误: 期望 1, 得到 %d", val1)
	}

	// 读取第二个 varint
	val2, err := reader.ReadVarint()
	if err != nil {
		t.Fatalf("第二次读取失败: %v", err)
	}
	if val2 != 2 {
		t.Errorf("第二次读取值错误: 期望 2, 得到 %d", val2)
	}

	// 读取第三个 varint
	val3, err := reader.ReadVarint()
	if err != nil {
		t.Fatalf("第三次读取失败: %v", err)
	}
	if val3 != 3 {
		t.Errorf("第三次读取值错误: 期望 3, 得到 %d", val3)
	}

	// 再次读取，应该返回 EOF
	_, err = reader.ReadVarint()
	if err == nil {
		t.Errorf("期望返回 EOF，但没有返回错误")
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("期望返回 EOF 或 ErrUnexpectedEOF，但得到: %v", err)
	}
}

// TestProtocStreamReaderDirectReadEOF 模拟 record_protoc_reader.go:101 的场景
func TestProtocStreamReaderDirectReadEOF(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		readFirstBytes int
		expectEOF      bool
	}{
		{
			name:           "数据已读完，应该返回 EOF",
			data:           []byte{0x01, 0x02},
			readFirstBytes: 2,
			expectEOF:      true,
		},
		{
			name:           "还有数据，不应该返回 EOF",
			data:           []byte{0x01, 0x02, 0x03},
			readFirstBytes: 2,
			expectEOF:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewProtocStreamReader(bytes.NewReader(tt.data))

			// 先读取一些数据，模拟前面的操作
			buf := make([]byte, tt.readFirstBytes)
			n, err := reader.inner.Read(buf)
			if err != nil {
				t.Fatalf("读取失败: %v", err)
			}
			if n != tt.readFirstBytes {
				t.Fatalf("读取字节数错误: 期望 %d, 得到 %d", tt.readFirstBytes, n)
			}

			// 模拟 record_protoc_reader.go:101 的 EOF 检查
			_, err = reader.inner.Read([]byte{'0'})

			if tt.expectEOF {
				if err == nil {
					t.Errorf("期望返回 EOF，但没有返回错误（这表明缓冲区有数据）")
				} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("期望返回 EOF 或 ErrUnexpectedEOF，但得到: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("期望成功读取，但得到错误: %v", err)
				}
			}
		})
	}
}

// TestProtocStreamReaderBufferEdgeCase 测试缓冲区边界情况
func TestProtocStreamReaderBufferEdgeCase(t *testing.T) {
	// 创建略小于默认缓冲区大小（4KB）的数据
	data := make([]byte, 4000)
	for i := 0; i < len(data); i++ {
		data[i] = 0x01 // 简单的 varint 值: 1
	}

	reader := NewProtocStreamReader(bytes.NewReader(data))

	// 读取一些值
	for i := 0; i < 10; i++ {
		val, err := reader.ReadVarint()
		if err != nil {
			t.Fatalf("读取失败: %v", err)
		}
		if val != 1 {
			t.Errorf("值错误: 期望 1, 得到 %d", val)
		}
	}
	t.Logf("成功读取了 10 个 varint 值")
}