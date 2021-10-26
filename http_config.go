package odps

import "time"

type HttpConfig struct {
	// http超时时间, 从tcp连接开始计时
	timeout time.Duration
}
