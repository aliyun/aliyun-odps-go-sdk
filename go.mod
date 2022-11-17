module github.com/aliyun/aliyun-odps-go-sdk

go 1.17

require (
	github.com/google/flatbuffers v2.0.0+incompatible // indirect
	github.com/google/uuid v1.3.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)

replace github.com/aliyun/aliyun-odps-go-sdk/arrow => ./arrow

require (
	github.com/aliyun/aliyun-odps-go-sdk/arrow v0.0.0-00010101000000-000000000000
	github.com/golang/snappy v0.0.3
	github.com/pkg/errors v0.9.1
	golang.org/x/exp v0.0.0-20211123021643-48cbe7f80d7c
	google.golang.org/protobuf v1.27.1
	gopkg.in/ini.v1 v1.66.2
)

require (
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.11 // indirect
)
