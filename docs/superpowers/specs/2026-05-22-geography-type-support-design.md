# Geography Type Support — Design

**Date**: 2026-05-22
**Status**: Approved (pending implementation plan)

## Background

Java SDK (`com.aliyun.odps.data.GeographyObject`) 已支持 MaxCompute 的 `GEOGRAPHY` 类型。Go SDK 的 `odps/datatype` / `odps/data` / `odps/tunnel` 体系尚无该类型，导致：

- `datatype.TypeCodeFromStr("GEOGRAPHY")` 会返回 `TypeUnknown`，schema 解析直接失败
- protobuf tunnel 在 reader/writer 中没有 GEOGRAPHY 分支，遇到该列会以 `Invalid data type` 报错

本设计在 Go SDK 中加入与 Java 对齐、但**最小化**的 Geography 支持。

## Scope

**In scope**

- 新增 `datatype.GEOGRAPHY` TypeID 与 `GeographyType` 单例
- 新增 `data.Geography`（基于 `[]byte`，承载 WKB）实现 `data.Data` 接口
- protobuf tunnel reader / writer 支持读写 GEOGRAPHY 列
- 单元测试：`data` 包内 round-trip、tunnel protobuf round-trip

**Out of scope**

- WKT ↔ WKB 互转（不引入 `paulmach/orb` / `twpayne/go-geom` 等几何库）
- Arrow tunnel 路径的 Geography 适配（首版只覆盖 protobuf tunnel）
- SQL driver / DDL / Result Set 层的额外封装

未来若需 WKT 能力，可在独立子包中加入，不影响本设计。

## Java SDK 参考映射

| Java | Go |
|---|---|
| `OdpsType.GEOGRAPHY(-2)` | `datatype.GEOGRAPHY`（iota 序号，传输只用名称） |
| `TypeInfoFactory.GEOGRAPHY` | `datatype.GeographyType` |
| `GeographyObject` 接口 + `RawGeographyObject` | `data.Geography []byte` |
| `JtsGeographyObject` | 不实现（依赖第三方库，超出 scope） |
| `ProtobufRecordStreamReader` `case GEOGRAPHY: readBytes()` | `record_protoc_reader.go` `case datatype.GEOGRAPHY: ReadBytes()` |
| `OdpsTypeTransformer` `OdpsType.GEOGRAPHY → GeographyObject.class` | 不需要（Go 用具体类型） |

## 设计

### 1. `datatype` 包

`odps/datatype/data_type.go`：

- 在 `JSON` 之后、`TypeUnknown` 之前加入 `GEOGRAPHY`（不影响线协议，因为 TypeID 仅在内存中使用，序列化采用字符串名）
- `TypeCodeFromStr` / `String()` 加入 `"GEOGRAPHY"` 双向映射
- 新增导出变量 `GeographyType = PrimitiveType{GEOGRAPHY}`

`odps/datatype/data_type_parser.go`：

- `parse()` 中 `GEOGRAPHY` 走 `default` 分支调用 `newPrimitive`，**无需新增 case**（无参数类型与 BINARY/STRING 同模式）

### 2. `data` 包

新增文件 `odps/data/geography.go`：

```go
package data

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// Geography 承载 MaxCompute GEOGRAPHY 列的 WKB (Well-Known Binary) 字节。
// 不内置 WKT/WKB 互转；如需 WKT 请自行使用 paulmach/orb 或 twpayne/go-geom 解析。
type Geography []byte

func (g Geography) Type() datatype.DataType { return datatype.GeographyType }

// AsBinary 返回 WKB 字节的副本视图（直接返回底层切片，与 data.Binary 行为一致）。
func (g Geography) AsBinary() []byte { return []byte(g) }

func (g Geography) String() string {
	return fmt.Sprintf("unhex('%X')", []byte(g))
}

// Sql 返回可直接插入 SQL 字面量的表达式。
func (g Geography) Sql() string {
	return fmt.Sprintf("ST_GEOGFROMWKB(unhex('%X'))", []byte(g))
}

func (g *Geography) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, g))
}
```

设计取舍：

- **没有 `AsText()`**：对齐 Java `RawGeographyObject.asText() → null`。用户拿到 `data.Geography` 即得 raw WKB。
- **`String()` 用 `unhex('...')`**：调试输出与 `Binary` 一致，便于直接复制到 SQL。
- **`Sql()` 用 `ST_GEOGFROMWKB`**：让 Geography 可作为参数拼入 `INSERT INTO ... VALUES (..., {geo}.Sql(), ...)`，与 Java SDK 测试中的 `ST_GEOGFROMTEXT` 思路一致。

**类型转换**：`tryConvertType` 在 `odps/data/util.go`（不是 `data_conversion.go`），基于 `reflect.AssignableTo` 通用实现。`Geography` 与 `[]byte` 底层类型相同，`Geography.Scan([]byte{...})` 走通用路径即可，**无需新增分支**（与 `Binary.Scan` 同模式）。

`odps/data/data_conversion.go` 中 `TryConvertGoToOdpsData` 已把 `[]byte` 默认映射为 `data.Binary`，本设计**不动**该映射；用户若希望 `[]byte` 写为 GEOGRAPHY 列，需自行构造 `data.Geography{...}`。这与 Java SDK 的行为对等（Java 用户也要显式构造 `JtsGeographyObject` / `RawGeographyObject`）。

`Binary ↔ Geography` 互转（两者都是 named type，`AssignableTo` 不成立）不在本设计范围；用户可显式 `data.Geography(b)` / `data.Binary(g)` 完成。

### 3. `tunnel` 包

`odps/tunnel/record_protoc_reader.go`：在已有的 `switch dt.ID()` 中加入：

```go
case datatype.GEOGRAPHY:
    v, err := r.protocReader.ReadBytes()
    if err != nil {
        return nil, errors.WithStack(err)
    }
    r.recordCrc.Update(v)
    fieldValue = data.Geography(v)
```

`odps/tunnel/record_protoc_writer.go`：

- `wireType` 选择中将 `datatype.GEOGRAPHY` 加入 `BytesType` 列表（与 BINARY/JSON 同组）
- `writeField` 中加入：
  ```go
  case data.Geography:
      r.recordCrc.Update(val)
      return errors.WithStack(r.protocWriter.WriteBytes(val))
  ```

### 4. 数据流

```
[Server WKB bytes]
    ↓ tunnel protobuf
record_protoc_reader.ReadBytes() → data.Geography(v)
    ↓
user code: geo.AsBinary() / geo.Sql()
    ↓
record_protoc_writer.WriteBytes(geo)
    ↓ tunnel protobuf
[Server WKB bytes]
```

## 错误处理

- **未知类型字符串**：`TypeCodeFromStr("GEOGRAPHY")` 命中后返回 `GEOGRAPHY`，不再退化到 `TypeUnknown`
- **类型转换失败**：`Geography.Scan` 复用现有 `tryConvertType`，遇到不兼容类型返回带 stack 的错误（与 Binary/Json 一致）
- **空值**：tunnel 层的 null 处理由上层 `readField` 已统一处理，本设计不需特殊代码

## 测试计划

### 单元测试（`data` 包）

`odps/data/data_test.go` 或新增 `geography_test.go`：

1. `Geography.Type()` 返回 `GeographyType`
2. `Geography.Sql()` 输出形如 `ST_GEOGFROMWKB(unhex('...'))`
3. `Geography.AsBinary()` 返回原始字节
4. `Geography.Scan(Geography{...})` round-trip
5. `Geography.Scan([]byte{...})` 兼容 `[]byte` 输入

### 单元测试（`tunnel` 包）

`odps/tunnel/protoc_writer_test.go` / `protoc_reader_test.go`：

1. 构造一行 Geography 列 → 写入 buffer → 读回 → 断言字节相等
2. schema 包含 `STRING + GEOGRAPHY` 两列时正常工作（覆盖 wireType 选择）

### 手动验证（可选）

参考 Java SDK 的 `GeographyTest.java` 在真实表上跑一遍：建表带 `geo GEOGRAPHY`，用 `ST_GEOGFROMTEXT('POINT(10 20)')` 写入，通过 tunnel 读出后断言 `AsBinary()` 非空。

## 风险与注意事项

- **TypeID iota 顺序变化**：`TypeUnknown` 的数值会从 24 变为 25。代码内若有依赖具体数字的逻辑会出问题。设计前已 grep 确认 SDK 内仅以名字（字符串）做 TypeID 的对外表达，数值不参与序列化。实施时需在 `data_type_test.go` 再确认一次。
- **CRC 兼容性**：tunnel 协议的 record CRC 已包含字节内容（`r.recordCrc.Update(v)`），与 Java SDK 行为一致，无需特殊处理。
- **向后兼容**：新增类型不影响既有列的读写。已有用户代码无变化。

## 不做的事（反范围）

- 不引入第三方几何库
- 不修改 sqldriver 层
- 不在 schema parser 之外做任何 SQL DDL 关键字识别
- 不实现 `AsText()` / WKT 输出
- 不动 Arrow 路径
