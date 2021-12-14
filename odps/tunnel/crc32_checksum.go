package tunnel

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type Crc32CheckSum struct {
	table  *crc32.Table
	value  uint32
	buffer *bytes.Buffer
}

func NewCrc32CheckSum() Crc32CheckSum {
	return Crc32CheckSum{
		table:  crc32.MakeTable(crc32.Castagnoli),
		buffer: bytes.NewBuffer(make([]byte, 0, 8)),
	}
}

// Update 注意不要用int类型的data，因为int类型在32和64位平台上的size不一样
// java中的int为32, 所以同样的int数据，用java和go计算出的crc32值可能不一样
func (crc *Crc32CheckSum) Update(data interface{}) {
	var _ = binary.Write(crc.buffer, binary.LittleEndian, data)
	crc.value = crc32.Update(crc.value, crc.table, crc.buffer.Bytes())
	crc.buffer.Reset()
}

func (crc *Crc32CheckSum) Value() uint32 {
	return crc.value
}

func (crc *Crc32CheckSum) Reset() {
	crc.value = 0
}
