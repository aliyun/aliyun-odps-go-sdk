module odpsexample

go 1.17

//replace github.com/aliyun/aliyun-odps-go-sdk/arrow => ../arrow

replace github.com/aliyun/aliyun-odps-go-sdk => ../

require (
	github.com/aliyun/aliyun-odps-go-sdk v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v23.3.3+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/klauspost/compress v1.16.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	golang.org/x/exp v0.0.0-20230420155640-133eef4313cb // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
)
