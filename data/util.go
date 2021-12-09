package data

import (
	"github.com/pkg/errors"
	"reflect"
)

func tryConvertType(src interface{}, dst interface{}) error {
	srcT := reflect.TypeOf(src)
	srcV := reflect.ValueOf(src)

	dstT := reflect.TypeOf(dst)
	dstV := reflect.ValueOf(dst)

	if srcV.Kind() == reflect.Ptr {
		srcT = srcT.Elem()
		srcV = srcV.Elem()
	}

	if dstV.Kind() == reflect.Ptr {
		dstT = dstT.Elem()
		dstV = dstV.Elem()
	}

	if srcT.AssignableTo(dstT) {
		dstV.Set(srcV)
		return nil
	}

	return errors.Errorf("cannot convert %s to %s", srcT.Name(), dstT.Name())
}
