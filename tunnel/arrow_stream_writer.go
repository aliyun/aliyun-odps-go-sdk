package tunnel

import (
	"io"
)

const DefaultChunkSize = 65536

// ArrowStreamWriter 将数据构建成固定大小的chunk，以chunk为单位计算crc，并将crc写入数据流
type ArrowStreamWriter struct {
	inner              io.WriteCloser
	chunkCrc           Crc32CheckSum
	globalCrc          Crc32CheckSum
	currentChunkLength int
	firstWrite         bool
}

func NewArrowStreamWriter(w io.WriteCloser) *ArrowStreamWriter {
	return &ArrowStreamWriter{
		inner:              w,
		chunkCrc:           NewCrc32CheckSum(),
		globalCrc:          NewCrc32CheckSum(),
		currentChunkLength: 0,
		firstWrite:         true,
	}
}

func (aw *ArrowStreamWriter) Write(data []byte) (int, error) {
	if aw.firstWrite {
		err := aw.writeUint32(DefaultChunkSize)
		if err != nil {
			return 0, err
		}

		aw.firstWrite = false
	}

	totalWrite := 0

	for dataLength := len(data); totalWrite < dataLength; {
		toWriteN := min(aw.leftChunkLength(), dataLength-totalWrite)
		bytesToWrite := data[totalWrite : totalWrite+toWriteN]

		totalWrite += toWriteN
		aw.currentChunkLength += toWriteN

		aw.chunkCrc.Update(bytesToWrite)
		aw.globalCrc.Update(bytesToWrite)

		_, err := aw.writeAll(bytesToWrite)
		if err != nil {
			return 0, err
		}

		if aw.chunkIsFull() {
			crc := aw.chunkCrc.Value()
			err = aw.writeUint32(crc)
			aw.chunkCrc.Reset()
			if err != nil {
				return 0, err
			}
			aw.currentChunkLength = 0
		}
	}

	return totalWrite, nil
}

func (aw *ArrowStreamWriter) Close() error {
	crc := aw.globalCrc.Value()
	err1 := aw.writeUint32(crc)
	aw.globalCrc.Reset()

	err2 := aw.inner.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (aw *ArrowStreamWriter) chunkIsFull() bool {
	return aw.currentChunkLength >= DefaultChunkSize
}

func (aw *ArrowStreamWriter) leftChunkLength() int {
	return DefaultChunkSize - aw.currentChunkLength
}

func (aw *ArrowStreamWriter) writeUint32(crcValue uint32) error {
	b := uint32ToBytes(crcValue)
	var _, err = aw.writeAll(b)

	return err
}

func (aw *ArrowStreamWriter) writeAll(b []byte) (int, error) {
	total := len(b)

	for hasWrite := 0; hasWrite < total; {
		n, err := aw.inner.Write(b)
		hasWrite += n

		if err != nil {
			return hasWrite, err
		}
	}

	return total, nil
}

func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(n >> 24 & 0x000000FF)
	b[1] = byte(n >> 16 & 0x000000FF)
	b[2] = byte(n >> 8 & 0x000000FF)
	b[3] = byte(n & 0x000000FF)

	return b
}
