module github.com/aliyun/aliyun-odps-go-sdk

go 1.15

require (
	github.com/aliyun/credentials-go v1.3.10
	github.com/golang/protobuf v1.5.3
	github.com/golang/snappy v0.0.4
	github.com/google/flatbuffers v23.5.26+incompatible
	github.com/google/uuid v1.3.0
	github.com/klauspost/compress v1.15.9
	github.com/pierrec/lz4/v4 v4.1.18
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.2
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
	gonum.org/v1/gonum v0.13.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.31.0
	gopkg.in/ini.v1 v1.67.0
)

replace (
	golang.org/x/net => golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/sys => golang.org/x/sys v0.0.0-20220412211240-33da011f77ad
)
