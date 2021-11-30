package tunnel

import (
	"compress/flate"
	"github.com/golang/snappy"
	"io"
)

type Compressor interface {
	Name() string
	NewReader(readCloser io.ReadCloser) io.ReadCloser
	NewWriter(writeCloser io.WriteCloser) io.WriteCloser
}

var Compressors = struct {
	SnappyFramed func() SnappyFramed
	Deflate      func(int) Deflate
}{
	SnappyFramed: newSnappyFramed,
	Deflate:      newDeflate,
}

type SnappyFramed int

func newSnappyFramed() SnappyFramed {
	return SnappyFramed(0)
}

const SnappyFramedName = "x-snappy-framed"

func (s SnappyFramed) Name() string {
	return SnappyFramedName
}

func (s SnappyFramed) NewReader(rc io.ReadCloser) io.ReadCloser {
	return readCloser {
		reader: snappy.NewReader(rc),
		closer: rc,
	}
}

func (s SnappyFramed) NewWriter(wc io.WriteCloser) io.WriteCloser {
	return writeCloser {
		writer: snappy.NewBufferedWriter(wc),
		closer: wc,
	}
}

type Deflate struct {
	level int
}

var DeflateLevel = struct {
	NoCompression      int
	BestSpeed          int
	BestCompression    int
	DefaultCompression int
	HuffmanOnly        int
}{
	NoCompression:      flate.NoCompression,
	BestSpeed:          flate.BestSpeed,
	BestCompression:    flate.BestCompression,
	DefaultCompression: flate.DefaultCompression,
	HuffmanOnly:        flate.HuffmanOnly,
}

const DeflateName = "deflate"

func (d Deflate) Name() string {
	return DeflateName
}

func newDeflate(level int) Deflate {
	return Deflate{level: level}
}


func (d Deflate) NewReader(rc io.ReadCloser) io.ReadCloser {
	return readCloser {
		reader: flate.NewReader(rc),
		closer: rc,
	}
}

func (d Deflate) NewWriter(wc io.WriteCloser) io.WriteCloser {
	w, _ := flate.NewWriter(wc, d.level)
	return writeCloser {
		writer: w,
		closer: wc,
	}
}

type readCloser struct {
	reader io.Reader
	closer io.Closer
}

func (r readCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r readCloser) Close() error {
	return r.closer.Close()
}

type writeCloser struct {
	writer io.Writer
	closer io.Closer
}

func (w writeCloser) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w writeCloser) Close() error {
	return w.closer.Close()
}
