package tunnel

import (
	"errors"
	"io"
)

var CrcErr = errors.New("crc value error when get a tunnel arrow stream")

type arrowHttpReaderStatus int

const (
	readToChunkStatus = iota
	readFromChunkStatus
)

type ArrowHttpReader struct {
	inner     io.ReadCloser
	chunkCrc  Crc32CheckSum
	globalCrc Crc32CheckSum
	chunkSize int
	chunk     *chunk
	firstRead bool
	eof       bool
	status    arrowHttpReaderStatus
}

func NewArrowHttpReader(rc io.ReadCloser) *ArrowHttpReader {
	return &ArrowHttpReader{
		inner:     rc,
		chunkCrc:  NewCrc32CheckSum(),
		globalCrc: NewCrc32CheckSum(),
		firstRead: true,
		status:    readToChunkStatus,
	}
}

func (ar *ArrowHttpReader) ReadChunk() error {
	ar.status = readFromChunkStatus

	// read chunk size from the first 4 bytes
	if ar.firstRead {
		chunkSize, err := ar.readUint32()
		if err != nil {
			return err
		}

		ar.chunkSize = int(chunkSize) + 4
		ar.chunk = newChunk(ar.chunkSize)
		ar.firstRead = false
	}

	// read chunkSize bytes or read to end of inner reader
	ar.chunk.reset()
	n, err := io.ReadFull(ar.inner, ar.chunk.buf)

	switch err {
	case nil:
		ar.chunk.setLength(ar.chunkSize)
		dataBytes := ar.chunk.bytes()[0 : ar.chunkSize-4]
		crcBytes := ar.chunk.bytes()[ar.chunkSize-4:]
		ar.chunk.truncate(ar.chunkSize - 4)

		ar.chunkCrc.Update(dataBytes)
		ar.globalCrc.Update(dataBytes)

		chunkCrcValue := bytesToUint32(crcBytes)
		crcExpected := ar.chunkCrc.Value()

		if chunkCrcValue != crcExpected {
			return CrcErr
		}
	case io.EOF, io.ErrUnexpectedEOF:
		ar.eof = true
		ar.chunk.setLength(n)

		if ar.chunk.length() < 4 {
			return errors.New("not enough bytes, at least 4 bytes for crc value")
		}

		dataBytes := ar.chunk.bytes()[0 : n-4]
		crcBytes := ar.chunk.bytes()[n-4:]
		ar.chunk.truncate(n - 4)

		ar.globalCrc.Update(dataBytes)
		globalCrcValue := bytesToUint32(crcBytes)
		crcExpected := ar.globalCrc.Value()
		ar.globalCrc.Reset()

		if globalCrcValue != crcExpected {
			return CrcErr
		}

		return nil
	}

	// err != nil and err != io.EOF
	return err
}

func (ar *ArrowHttpReader) Read(dst []byte) (int, error) {
	// read chunkSize bytes or read to end of inner reader
	if ar.status == readToChunkStatus {
		err := ar.ReadChunk()
		if err != nil {
			return 0, err
		}
	}

	n, err := io.ReadFull(ar.chunk, dst)
	if err == io.EOF {
		ar.status = readToChunkStatus
	} else if err != nil {
		return n, err
	}

	if ar.chunk.length() == 0 && ar.eof {
		return n, io.EOF
	}

	return n, nil
}

func (ar *ArrowHttpReader) Close() error {
	return ar.inner.Close()
}

func (ar *ArrowHttpReader) readUint32() (uint32, error) {
	uint32Bytes := make([]byte, 4)
	_, err := io.ReadFull(ar.inner, uint32Bytes)
	if err != nil {
		return 0, err
	}

	return bytesToUint32(uint32Bytes), nil
}

type chunk struct {
	buf    []byte
	end    int
	offset int
}

func newChunk(chunkSize int) *chunk {
	return &chunk{
		buf:    make([]byte, chunkSize),
		offset: 0,
		end:    0,
	}
}

func (c *chunk) setLength(n int) {
	c.end = n
}

func (c *chunk) length() int {
	return c.end - c.offset
}

func (c *chunk) bytes() []byte {
	return c.buf[c.offset:c.end]
}

func (c *chunk) truncate(n int) {
	c.end = n
}

func (c *chunk) reset() {
	c.offset = 0
	c.end = 0
}

func (c *chunk) Read(p []byte) (n int, err error) {
	if c.offset >= c.end {
		return 0, io.EOF
	}

	n = copy(p, c.bytes())
	c.offset += n

	return n, nil
}

func bytesToUint32(b []byte) uint32 {
	return (uint32(b[0])&0xFF)<<24 +
		(uint32(b[1])&0xFF)<<16 +
		(uint32(b[2])&0xFF)<<8 +
		(uint32(b[3]) & 0xFF)
}
