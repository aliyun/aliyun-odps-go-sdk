package tunnel

import (
	"bytes"
	"errors"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
	"math"
)

type ProtoStreamReader struct {
	inner io.Reader
	buf   *bytes.Buffer
}

func NewProtoStreamReader(r io.Reader) *ProtoStreamReader {
	return &ProtoStreamReader{
		inner: r,
		buf:   bytes.NewBuffer(nil),
	}
}

func (r *ProtoStreamReader) ReadVarint() (uint64, error) {
	b := []byte{'0'}
	r.buf.Reset()

	for {
		n, err := r.inner.Read(b)
		if n <= 0 {
			return 0, err
		}

		r.buf.Write(b)
		if r.buf.Len() > 10 {
			return 0, errors.New("invalid bytes for varint")
		}

		v, n := protowire.ConsumeVarint(r.buf.Bytes())
		if n > 0 {
			return v, nil
		}
	}
}

func (r *ProtoStreamReader) ReadFixed32() (uint32, error) {
	b := []byte{'0', '0', '0', '0'}

	for {
		n, err := io.ReadFull(r.inner, b)
		if n != 4 {
			return 0, err
		}

		v, _ := protowire.ConsumeFixed32(b)
		return v, nil
	}
}

func (r *ProtoStreamReader) ReadFixed64() (uint64, error) {
	b := []byte{'0', '0', '0', '0', '0', '0', '0', '0'}

	for {
		n, err := io.ReadFull(r.inner, b)
		if n != 8 {
			return 0, err
		}

		v, _ := protowire.ConsumeFixed64(b)
		return v, nil
	}
}

func (r *ProtoStreamReader) ReadBytes() ([]byte, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	b := make([]byte, m)
	_, err = io.ReadFull(r.inner, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (r *ProtoStreamReader) ReadTag() (protowire.Number, protowire.Type, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, 0, err
	}

	num, typ := protowire.DecodeTag(m)
	if num < protowire.MinValidNumber {
		return 0, 0, errors.New("failed to read a tag from the proto stream")
	}

	return num, typ, nil
}

func (r *ProtoStreamReader) ReadBool() (bool, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return false, err
	}

	return protowire.DecodeBool(m), nil
}

func (r *ProtoStreamReader) ReadInt32() (int32, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}

	return int32(m), nil
}

func (r *ProtoStreamReader) ReadSInt32() (int32, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}

	return int32(protowire.DecodeZigZag(m & math.MaxUint32)), nil
}

func (r *ProtoStreamReader) ReadUInt32() (uint32, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}

	return uint32(m), nil
}

func (r *ProtoStreamReader) ReadInt64() (int64, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}

	return int64(m), nil
}

func (r *ProtoStreamReader) ReadSInt64() (int64, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}

	return protowire.DecodeZigZag(m), nil
}

func (r *ProtoStreamReader) ReadUInt64() (uint64, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}

	return uint64(m), nil
}

func (r *ProtoStreamReader) ReadSFixed32() (int32, error) {
	v, err := r.ReadFixed32()
	if err != nil {
		return 0, err
	}

	return int32(v), nil
}

func (r *ProtoStreamReader) ReadFloat32() (float32, error) {
	v, err := r.ReadFixed32()
	if err != nil {
		return 0, err
	}

	return math.Float32frombits(v), nil
}

func (r *ProtoStreamReader) ReadSFixed64() (int64, error) {
	v, err := r.ReadFixed64()
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *ProtoStreamReader) ReadFloat64() (float64, error) {
	v, err := r.ReadFixed64()
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(v), nil
}

func (r *ProtoStreamReader) ReadString() (string, error) {
	v, err := r.ReadBytes()
	if err != nil {
		return "", err
	}

	return string(v), nil
}
