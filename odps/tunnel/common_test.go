package tunnel_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/pkg/errors"
)

func TestRetry(t *testing.T) {
	var err error = nil

	err = tunnel.Retry(func() error {
		_, err := Mock_commit()
		return errors.WithStack(err)
	})

	if err == nil {
		t.Error("should be error")
	}
}

func Mock_commit() (string, error) {
	return "", errors.New("commit fail")
}
