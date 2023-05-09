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

package data

import (
	"github.com/pkg/errors"
	"reflect"
)

func tryConvertType(src interface{}, dst interface{}) error {
	if src == nil {
		dst = nil
		return nil
	}

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
