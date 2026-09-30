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

package sqldriver

import (
	"github.com/pkg/errors"
)

// CanceledError 是 ctx 取消/超时打断等待时返回的错误。它同时满足两个要求：
//
//  1. errors.Is(err, context.Canceled) / errors.Is(err, context.DeadlineExceeded)
//     仍然成立（原因在 Unwrap 链里），不改变调用方原有的判断写法；
//  2. errors.As(err, &canceledErr) 能直接拿到 InstanceID，不需要去解析错误文本，
//     这样调用方想终止这个任务时有可用的句柄。
//
// 取消本身不会终止 MaxCompute instance，终止是显式动作：
//
//	var canceledErr *sqldriver.CanceledError
//	if errors.As(err, &canceledErr) {
//		_ = odpsIns.Instance(canceledErr.InstanceID).Terminate()
//	}
type CanceledError struct {
	// InstanceID 是被打断的那个 MaxCompute instance。
	InstanceID string
	// Err 是取消原因，通常是 context.Canceled 或 context.DeadlineExceeded。
	Err error
}

func (e *CanceledError) Error() string {
	return errors.Wrapf(e.Err,
		"canceled while waiting for MaxCompute instance %s; the instance keeps running on the server, "+
			"terminate it with odps.Instance.Terminate if it is no longer needed",
		e.InstanceID).Error()
}

// Unwrap keeps the context error in the chain, so errors.Is still matches it.
func (e *CanceledError) Unwrap() error {
	return e.Err
}

// NoResult 用于 ExecContext：MaxCompute 的 instance 不向 database/sql 报告受影响
// 行数与自增 id，所以两个方法都明确回答"取不到"。
//
// 这里以前返回的是 nil driver.Result，database/sql 会原样把它交给调用方，于是
// res.RowsAffected() panic 在 nil interface 上。返回一个明确的 Result 既不改变
// "执行成功"的语义，也不会让下游炸开。它导出，便于测试与调用方判型。
type NoResult struct{}

// ErrNoRowsAffectedInfo 说明为什么拿不到受影响行数。
var ErrNoRowsAffectedInfo = errors.New("no RowsAffected available: MaxCompute SQL instances do not report affected rows")

// ErrNoLastInsertIdInfo 说明为什么拿不到自增 id。
var ErrNoLastInsertIdInfo = errors.New("no LastInsertId available: MaxCompute has no auto-increment id")

func (NoResult) LastInsertId() (int64, error) {
	return 0, ErrNoLastInsertIdInfo
}

func (NoResult) RowsAffected() (int64, error) {
	return 0, ErrNoRowsAffectedInfo
}
