package tunnel

import (
	"fmt"
	"io"
)

const DefaultChunkSize = 65536

// ArrowChunkWriter 将数据构建成固定大小的chunk，以chunk为单位计算crc，并将crc写入数据流
type ArrowChunkWriter struct {
	inner       *io.PipeWriter
	chunkCrc    Crc32CheckSum
	globalCrc   Crc32CheckSum
	chunkLength int
	firstWrite  bool
}

func NewArrowChunkWriter(w *io.PipeWriter) *ArrowChunkWriter {
	return &ArrowChunkWriter{
		inner:       w,
		chunkCrc:    NewCrc32CheckSum(),
		globalCrc:   NewCrc32CheckSum(),
		chunkLength: 0,
		firstWrite:  true,
	}
}

func (a *ArrowChunkWriter) Write(data []byte) (int, error) {
	if a.firstWrite {
		err := a.writeUint32(DefaultChunkSize)
		if err != nil {
			return 0, err
		}

		a.firstWrite = false
	}

	totalWrite := 0

	for dataLength := len(data); totalWrite < dataLength; {

		toWriteN := min(a.leftChunkLength(), dataLength-totalWrite)
		bytesToWrite := data[totalWrite:toWriteN]

		totalWrite += toWriteN
		a.chunkLength += toWriteN

		a.chunkCrc.Update(bytesToWrite)
		a.globalCrc.Update(bytesToWrite)

		_, err := a.writeAll(bytesToWrite)
		if err != nil {
			return 0, err
		}

		if a.chunkIsFull() {
			crc := a.chunkCrc.Value()
			println(fmt.Sprintf("*****%d", crc))
			err = a.writeUint32(crc)
			a.chunkCrc.Reset()
			if err != nil {
				return 0, err
			}
			a.chunkLength = 0
		}
	}

	return totalWrite, nil
}

func (a *ArrowChunkWriter) Close() error {
	crc := a.globalCrc.Value()
	err1 := a.writeUint32(crc)
	a.globalCrc.Reset()
	err2 := a.inner.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *ArrowChunkWriter) chunkIsFull() bool {
	return a.chunkLength >= DefaultChunkSize
}

func (a *ArrowChunkWriter) leftChunkLength() int {
	return DefaultChunkSize - a.chunkLength
}

func (a *ArrowChunkWriter) writeUint32(crcValue uint32) error {
	b := uint32ToBytes(crcValue)
	var _, err = a.writeAll(b)

	return err
}

func (a *ArrowChunkWriter) writeAll(b []byte) (int, error) {
	total := len(b)

	for hasWrite := 0; hasWrite < total; {
		n, err := a.inner.Write(b)
		hasWrite += n

		if err != nil {
			return hasWrite, err
		}
	}

	return total, nil
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(n >> 24 & 0x000000FF)
	b[1] = byte(n >> 16 & 0x000000FF)
	b[2] = byte(n >> 8 & 0x000000FF)
	b[3] = byte(n & 0x000000FF)

	return b
}
