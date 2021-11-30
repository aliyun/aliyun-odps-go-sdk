package tunnel

import (
	"io"
	"net/http"
)

type httpConnection struct {
	Writer  io.WriteCloser
	resChan <-chan resOrErr
}

type resOrErr struct {
	err error
	res *http.Response
}
