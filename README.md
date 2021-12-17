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
3. 打印error值时，请使用"%+v"格式化error，这样可以看到错误堆栈


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

## sql语句执行
执行sql语句的方法有
1. 使用Odps.ExecSql
2. 使用instance执行sql task, 这种方法比较底层，一般用不到。
3. 创建table实例，使用Table.ExecSql
4. 使用go sql

### sdk示例
#### 直接使用sql语句创建table
1. 创建odps实例，并设置默认的project
2. 调用odps执行sql语句, 返回Instance实例
3. 使用instance等待sql语句创建成功或失败

[示例代码](./examples/sdk/create_table_use_sql/main.go)

#### 先创建table schema, 根据table schema创建表
1. 创建odps实例，并设置默认的project
2. 使用SchemaBuilder创建table schema
3. 调用odps.Tables()获取Tables实例
4. 使用Tables实例根据schema创建表

[示例代码](./examples/sdk/create_table_use_table_schema/main.go)

### go sql示例
1  创建dsn(data source name，形如http://user:pass@host:port?param=x)，可以直接使用字符串或通过sqldriver.Config构建
2. 调用sql.Open获取db
3. 使用db执行sql

[示例代码](./examples/sql/create_table/main.go)

## 添加分区
### 使用table实例添加分区
1. 使用odps实例创建table实例
2. 使用table添加分区

[示例代码](./examples/sdk/add_parition/main.go)

### 使用sql添加分区
使用任何一种方法可以执行sql的方法执行sql语句即可，sql语句示例如下
```sql
alter table user_test add partition (age=20, hometown='hangzhou');
```

## 插入数据
### 通过执行sql语句插入数据
插入数据用任何一种执行sql语句的方法都可以，但是建议使用go sql, go sql支持named args, 使构造sql语句更简单。

此外建议使用data.Array，data.Struct, data.Datetime等类型表示要插入的值，这些类型可以直接将值表示为相应类型的常量、或构造方法。
如：
datetime 类型的 "2021-12-18 11:11:00"可以表示为"datetime'2021-12-18 11:11:00'"。
array<string> 类型的["a", "b"]可以表示为"array('a', 'b')"

[示例代码](./examples/sql/insert_data/main.go)

## 查询数据

## 下载数据

## 操作project, table, instance

# Scratch
目前不支持
project schema table三级结构

Example代码中的输出都用了println, 这是因为大多示例虽然在没有配置的情况下当做测试用例运行时会报错，
但在本地开发的时候希望可以直接运行代码，并让运行结果不报错，所以使用了println, 将字符输出到标准错误。