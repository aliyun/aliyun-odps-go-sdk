package odps

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type HttpNotOk struct {
	Status     string
	StatusCode int
	Body       []byte
}

func (e HttpNotOk) Error() string {
	return fmt.Sprintf("%s\n%s", e.Status, e.Body)
}

func NewHttpNotOk(res *http.Response) HttpNotOk  {
	var body []byte

	if res.Body != nil {
		body, _ = ioutil.ReadAll(res.Body)
	}

	return HttpNotOk {
		Status: res.Status,
		StatusCode: res.StatusCode,
		Body: body,
	}
}