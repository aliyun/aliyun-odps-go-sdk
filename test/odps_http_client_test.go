package testu

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"io"
	"io/ioutil"
	"testing"
	"unsafe"
)

func TestOdpsHttpClient(t *testing.T)  {
	httpClient := DefaultOdpsHttpClient

	rb := odps.ResourceBuilder{}
	rb.SetProject("project_1")

	req, err := httpClient.NewRequest(odps.HttpMethod.GetMethod, rb.Project(), nil)

	if err != nil {
		panic(err)
	}

	res, err := httpClient.Do(req)

	if err != nil {
		panic(err)
	} else {
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				panic(err)
			}
		}(res.Body)

		body, err := ioutil.ReadAll(res.Body)

		if err != nil {
			panic(err)
		}

		bodyStr := *(*string) (unsafe.Pointer(&body))
		fmt.Println(bodyStr)
	}
}

