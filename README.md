# Install
```shell
go get github.com/aliyun/aliyun-odps-go-sdk
```

# 目录说明
## odps
odps sdk的主体目录

## sqldriver
go sql/driver 接口的odps实现

## arrow
https://github.com/apache/arrow/tree/master/go 的修改版本，ipc package添加了RecordBatch message的reader和writer方法。
在使用odps sdk操作arrow时，需使用"github.com/aliyun/aliyun-odps-go-sdk/arrow"作为arrow的import path。

# 使用注意事项
1. Project, Table, Instance, Partition等需要从odps后台加载数据的对象，在除使用Name, Id等基本属性的Getter外, 需要先调用Load方法。
2. 所有GetXX方法除了返回正常值，也会返回error, 要注意error值的检查。


# odps model实现列表
[x] project
[x] table
[x] instance
[x] tunnel
    [x] table arrow tunnel
    [x] instance tunnel
    [ ] table protoc tunnel
[ ] resource
[ ] function

# Examples
## 创建表 
### sdk示例
#### 直接使用sql语句创建table
1. 创建odps实例，并设置默认的project
2. 调用odps执行sql语句, 返回Instance实例
3. 使用instance等待sql语句创建成功或失败

```go
package main

import (
   "github.com/aliyun/aliyun-odps-go-sdk/odps"
   "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
   "log"
)

func main() {
    accessId := ""	
    accessKey := ""
    endpoint := ""
    projectName := ""
   
    aliAccount := account.NewAliyunAccount(accessId, accessKey)
	odpsIns := odps.NewOdps(aliAccount, endpoint)
	odpsIns.SetDefaultProjectName(projectName)
	
	sql := "create table if not exists user_test (" +
		"name string,score int,birthday date,addresses array<string>" +
		") " +
		"partitioned by (age int,hometown string) " +
		"lifecycle 2;"
	ins, err := odpsIns.RunSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
```

#### 先创建table schema, 根据table schema创建表
1. 创建odps实例，并设置默认的project
2. 使用SchemaBuilder创建table schema
3. 调用odps.Tables()获取Tables实例
4. 使用Tables实例根据schema创建表
```go
package main

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"log"
)

func main() {
	accessId := ""
    accessKey := ""
    endpoint := ""
	projectName := ""
	
	aliAccount := account.NewAliyunAccount(accessId, accessKey)
    odpsIns := odps.NewOdps(aliAccount, endpoint)
    odpsIns.SetDefaultProjectName(projectName)
	
	c1 := tableschema.Column {
		Name: "name",
        Type: datatype.StringType,
	}

   c2 := tableschema.Column {
	   Name: "score",
	   Type: datatype.IntType,
   }

   arrayType := datatype.NewArrayType(datatype.StringType)
   //or arrayType, _ := datatype.ParseDataType("array<string>")

   c3 := tableschema.Column {
	   Name: "birthday",
       Type: datatype.DateType,
   }

   c4 := tableschema.Column {
	   Name: "addresses", 
	   Type: arrayType,
   }

   c5 := tableschema.Column {
	   Name: "age",
       Type: datatype.IntType,
   }
   
   c6 := tableschema.Column {
	   Name: "hometown", 
	   Type: datatype.StringType,
   }
   
   schemaBuilder := tableschema.NewSchemaBuilder()
   schemaBuilder.Name("user_test").
	   Columns(c1, c2, c3, c4).
	   PartitionColumns(c5, c6).
	   Lifecycle(2)
   
   schema := schemaBuilder.Build()
   tablesIns := odpsIns.Tables()
   err := tablesIns.CreateAndWait(schema, true, nil, nil)
   if err != nil {
	   log.Fatalf("%+v", err)
   }
}
```

### go sql示例



# Scratch
目前不支持
project schema table三级结构

Example代码中的输出都用了println, 这是因为大多示例虽然在没有配置的情况下当做测试用例运行时会报错，
但在本地开发的时候希望可以直接运行代码，并让运行结果不报错，所以使用了println, 将字符输出到标准错误。