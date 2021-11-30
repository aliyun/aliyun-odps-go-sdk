package tunnel

import (
	"io"
	"net/http"
)

type HttpOutStream struct {
	writer io.Writer
	response *http.Response
}







