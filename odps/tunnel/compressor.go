// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel

import (
	"compress/flate"
	"compress/zlib"
	"io"
	"strings"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

type Compressor interface {
	Name() string
	NewReader(readCloser io.ReadCloser) io.ReadCloser
	NewWriter(writeCloser io.WriteCloser) io.WriteCloser
}

type SnappyFramed int

func newSnappyFramed() SnappyFramed {
	return SnappyFramed(0)
}

const SnappyFramedName = "x-snappy-framed"

func (s SnappyFramed) Name() string {
	return SnappyFramedName
}

type snappyWrapper struct {
	reader *snappy.Reader
}

func (s snappyWrapper) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s snappyWrapper) Close() error {
	return nil
}

func (s SnappyFramed) NewReader(rc io.ReadCloser) io.ReadCloser {
	return readCloser{
		readCloser: snappyWrapper{snappy.NewReader(rc)},
		closer:     rc,
	}
}

func (s SnappyFramed) NewWriter(wc io.WriteCloser) io.WriteCloser {
	return writeCloser{
		writeCloser: snappy.NewBufferedWriter(wc),
		closer:      wc,
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

func defaultDeflate() Deflate {
	return Deflate{level: DeflateLevel.BestSpeed}
}

func (d Deflate) NewReader(rc io.ReadCloser) io.ReadCloser {
	r, _ := zlib.NewReader(rc)

	return readCloser{
		readCloser: r,
		closer:     rc,
	}
}

func (d Deflate) NewWriter(wc io.WriteCloser) io.WriteCloser {
	w, _ := zlib.NewWriterLevel(wc, flate.DefaultCompression)
	return writeCloser{
		writeCloser: w,
		closer:      wc,
	}
}

type readCloser struct {
	readCloser io.ReadCloser
	closer     io.Closer
}

func (r readCloser) Read(p []byte) (int, error) {
	return r.readCloser.Read(p)
}

func (r readCloser) Close() error {
	err1 := r.readCloser.Close()
	err2 := r.closer.Close()

	if err1 != nil {
		return errors.WithStack(err1)
	}

	return errors.WithStack(err2)
}

type writeCloser struct {
	writeCloser io.WriteCloser
	closer      io.Closer
}

func (w writeCloser) Write(p []byte) (int, error) {
	return w.writeCloser.Write(p)
}

func (w writeCloser) Close() error {
	err1 := w.writeCloser.Close()
	err2 := w.closer.Close()

	if err1 != nil {
		return errors.WithStack(err1)
	}

	return errors.WithStack(err2)
}

func WrapByCompressor(rc io.ReadCloser, contentEncoding string) io.ReadCloser {
	contentEncoding = strings.ToLower(contentEncoding)
	switch {
	case strings.Contains(contentEncoding, DeflateName):
		return defaultDeflate().NewReader(rc)
	case strings.Contains(contentEncoding, SnappyFramedName):
		return newSnappyFramed().NewReader(rc)
	}

	return rc
}
