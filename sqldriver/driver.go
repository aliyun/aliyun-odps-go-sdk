package sqldriver

import (
	"database/sql"
	"database/sql/driver"
	"github.com/pkg/errors"
)

func init() {
	sql.Register("odps", &OdpsDriver{})
}

type OdpsDriver struct{}

func (d OdpsDriver) Open(name string) (driver.Conn, error) {
	config, err := ParseDSN(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newConnection(config.GenOdps()), nil
}
