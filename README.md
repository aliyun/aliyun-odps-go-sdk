# Note
Please note that this is far from production ready, it is still under active development

# Install
required >= go1.15
```shell
go get github.com/aliyun/aliyun-odps-go-sdk
```

# 目录说明
## odps
 - odps sdk的主体目录, 包括子目录
 - tunnel: tunnel相关实现
 - account
 - common
 - data: odps数据值的go表示
 - datatype: odps数据类型的go表示
 - restclient: 用于与odps服务交互的restful client
 - security: project相关安全配置
 - tableschema: table schema的go表示
 
## sqldriver
go sql/driver 接口的odps实现


# 使用注意事项
1. Project, Table, Instance, Partition等需要从odps后台加载数据的对象，在除使用Name, Id等基本属性的Getter外, 需要先调用Load方法。
2. 所有GetXX方法除了返回正常值，也会返回error, 要注意error值的检查。
3. 打印error值时，请使用"%+v"格式化error，这样可以看到错误堆栈。go sdk中使用了"github.com/pkg/errors"库代替标准errors库, 建议sdk使用者
也使用errors.WithStack等方法对获取进行封装，这样可以保留完整的error stack context.


# odps model实现列表
<p>[x] project</p>
<p>[x] table</p>
<p>[x] instance</p>
<p>[x] tunnel</p>
<p>[x] table tunnel</p>
<p>    [x] table protoc tunnel</p>
<p>    [x] table stream tunnel</p>
<p>[ ] resource</p>
<p>[ ] function</p>

# Examples

## sql语句执行
执行sql语句的方法有
1. 使用Odps.ExecSql、Odps.ExecSqlWithHints
2. 使用instance执行sql task, 这种方法比较底层，一般用不到。
3. 创建table实例，使用Table.ExecSql, Table.ExecSqlWithHints
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
1. 创建dsn(data source name，形如"http://user:pass@host:port?param=x")，可以直接使用字符串或通过sqldriver.Config构建
2. 调用sql.Open获取db
3. 使用db执行sql

[示例代码](./examples/sql/create_table/main.go)

## 添加分区
### 使用table实例添加分区
1. 使用odps实例创建table实例
2. 使用table添加分区

[示例代码](examples/sdk/partition/add_partitions/main.go)

### 使用sql添加分区
使用任何一种方法可以执行sql的方法执行sql语句即可，sql语句示例如下
```sql
alter table user_test add partition (age=20, hometown='hangzhou');
```

## 插入数据
插入数据时，如果只是插入很少量数据，可以使用insert sql语句，如果插入的数量较大，则需使用tunnel上传数据。

### 通过执行sql语句插入数据(不建议使用)
插入数据用任何一种执行sql语句的方法都可以，但是建议使用go sql, go sql支持named args, 使构造sql语句更简单。

此外建议使用data.Array，data.Struct, data.Datetime等类型表示要插入的值，这些类型可以直接使用Sql()方法获取相应类型的常量或构造方法。
如：
datetime 类型的"2021-12-18 11:11:00" 返回"datetime'2021-12-18 11:11:00'"。
array<string> 类型的["a", "b"] 返回"array('a', 'b')"
struct<address:array<string>, hobby<string>>类型的{"address":["apsaras","efc"],"hobby":"swimming"} 返回 named_struct('address', array('apsaras', 'efc'), 'hobby', 'swimming')

[示例代码](./examples/sql/insert_data/main.go)

### 通过tunnel上传数据(建议使用)
#### 通过protoc协议上传数据
简单的上传过程为
1. 构建Tunnel
2. 使用Tunnel创建UploadSession, 创建Session时，可以指定Partition Key, 压缩方法等
3. 通过UploadSession获取schema
4. 使用schema创建record
5. 使用UploadSession创建record writer， 注意创建record writer的时候要指定block id。服务端这时会将写入的数据存入block id相关的文件
6. 使用UploadSession的commit方法向tunnel server指示可以将写入的数据写进table。commit时要指定block id
   
在上传数据量较大时，可以用一批block ids创建多个record writer, 用这些writer并行写入数据。这时，再commit的时候，要传入这批block ids。
[示例代码](./examples/sdk/tunnel/upload_data_use_protoc/main.go)


## 查询数据
odps sdk中的data package定义了与odps数据类型对应的数据结构，两者之间的对应关系为

| odps go sdk | odps        |
| ----------- | ----------- |
| Bool        | boolean     |
| TinyInt     | tinyint     |
| Int         | int         |
| SmallInt    | smallint    |
| BigInt      | bigint      |
| Float       | float       |
| Double      | double      |
| String      | string      |
| Binary      | binary      |
| Char        | char        |
| VarChar     | varchar     |
| Decimal     | decimal     |
| Date        | date        |
| DateTime    | datetime    |
| TimeStamp   | timestamp   |
| Array       | array       |
| Map         | map         |
| Struct      | struct      |

通过sql driver获取到的数据类型与odps数据类型的对应关系如下
| odps  列类型 | not nullable         | nullable                     |
|-----------|------------------------|------------------------------|
| bigint    | int64                  | odps/sqldriver.NullInt64     |
| int       | int                    | odps/sqldriver.NullInt32     |
| smallint  | int16                  | odps/sqldriver.NullInt16     |
| tinyint   | int8                   | odps/sqldriver.NullInt8      |
| double    | float64                | odps/sqldriver.NullFloat64  |
| float     | float32                | odps/sqldriver.NullFloat32   |
| string    | string                 | odps/sqldriver.NullString    |
| boolean   | bool                   | odps/sqldriver.NullBool      |
| char      | string                 | odps/sqldriver.NullString    |
| varchar   | string                 | odps/sqldriver.NullString    |
| datetime  | time.Time              | odps/sqldriver.NullDateTime  |
| date      | time.Time              | odps/sqldriver.NullDate      |
| timestamp | time.Time              | odps/sqldriver.NullTimestamp |
| binary    | odps/sqldriver.Binary  | odps/sqldriver.Binary        |
| decimal   | odps/sqldriver.Decimal | odps/sqldriver.Decimal       |
| map       | odps/sqldriver.Map     | odps/sqldriver.Map           |
| array     | odps/sqldriver.Array   | odps/sqldriver.Array         |
| struct    | odps/sqldriver.Struct  | odps/sqldriver.Struct        |


### 使用instance执行select语句，获取select结果
使用instance执行select语句后, 可以
1. 使用获取instance结果的接口获取instance的结果，这个结果就是select的返回值。instance结果接口返回的select数据有数量限制， 默认为1W条，而且返回的record的每个字段都是string类型
   [示例代码](./examples/sdk/select_data/main.go)
2. 使用instance tunnel获取select的返回值。在执行select语句获取instance后，用instance创建tunnel instance result download session, 然后用session创建record reader。这种返回获取的record各个字段为与odps对应的go sdk类型
   [示例代码](./examples/sdk/select_data_instance_tunnel/main.go)

### 使用go sql执行select语句并获取结果
**这种方式是建议使用的执行select并获取结果的方法**。go sql使用instance tunnel获取select结果。需要注意的是，Query方法返回Rows，Row需要调用scan方法提取各个字段。
[示例代码1](./examples/sql/select_data/main.go)
[示例代码2](./examples/sql/select_data_1/main.go)

## 通过tunnel下载table数据
odps tunnel支持以protoc(自定义)格式下载数据.

简单的下载过程如下:
1. 构建Tunnel
2. 使用Tunnel创建Download Session
3. 使用Download Session创建Record Reader
4. 使用Record Reader读取数据

使用protoc格式下载数据时， 获取的record中的字段类型为odps go sdk数据类型
[示例代码](./examples/sdk/tunnel/download_data_use_protoc/main.go)


## 操作project, table, instance, partition
相应的操作可以参照文档或示例代码</br>
[Project(s) 示例代码](./odps/example_project_test.go)</br>
[Tables 示例代码](./odps/example_tables_test.go)</br>
[Table 示例代码](./odps/example_table_test.go)</br>
[Partition 示例代码](./odps/example_partition_test.go)</br>
[Instance(s) 示例代码](./odps/example_instance_test.go)</br>
[LogView 示例代码](./examples/sdk/select_data_instance_tunnel)</br>

# 其他
Example代码中的输出都用了println, 这是因为大多示例虽然在没有配置的情况下当做测试用例运行时会报错，
但在本地开发的时候希望可以直接运行代码，并让运行结果不报错，所以使用了println, 将字符输出到标准错误。


# TODO
[comment]: <> (<p>[ ]tunnel buffered writer</p>)
<p>[ ]request id打印开关</p>
<p>[ ]sqldirver的dsn支持sql flag</p>

# License
licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)