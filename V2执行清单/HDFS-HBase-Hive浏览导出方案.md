# HDFS / HBase / Hive 浏览与导出方案

## 背景

当前项目的数据库栏已经支持 MySQL、PostgreSQL、SQLite、Oracle、SQLServer，以及 Hadoop 相关的 Hive、HDFS、HBase 连接测试。

其中，MySQL 类数据库适合保持现有“连接测试 + 列表 + 导出”模式不变；而 HDFS、HBase、Hive 更适合扩展为“浏览资源”的交互模式。

为了兼顾现有 MySQL 功能和 Hadoop 生态的实际使用场景，建议在不破坏现有 MySQL 栏结构的前提下，为 HDFS、HBase、Hive 增加浏览目录 / 文件 / 表 / 分区等能力。

---

## 目标

1. 保持现有 MySQL 相关功能基本不变。
2. 为 HDFS 增加目录树与文件列表浏览能力。
3. 为 HBase 增加 table / row 浏览能力。
4. 为 Hive 增加 database / table / partition 浏览能力。
5. 统一前后端接口抽象，便于后续扩展其他类似数据源。

---

## 总体设计

建议将“数据库栏”逐步演进为“数据源浏览与导出”模块，按数据源类型分为三类：

- **表型**：MySQL / PostgreSQL / Oracle / SQLServer / Hive
- **文件型**：HDFS
- **宽表 / NoSQL 型**：HBase

前端根据 `db_type` 切换不同的浏览面板，后端按类型路由到不同的浏览逻辑。

---

## 一、HDFS 方案

### 1. 功能目标

HDFS 需要支持：

- 浏览根目录与多级子目录
- 查看目录下的文件
- 点击目录继续展开
- 点击文件进行预览 / 下载 / 导出

### 2. 前端表现

建议在选择 `HDFS` 时显示：

- 主机
- 端口
- 用户名
- 路径输入框
- 刷新按钮
- 左侧目录树
- 右侧文件列表
- 面包屑路径
- 预览 / 下载 / 导出按钮

### 3. 后端接口建议

新增统一浏览接口，例如：

```text
POST /api/v1/browser/list
```

请求示例：

```json
{
  "db_type": "hdfs",
  "host": "localhost",
  "port": 9870,
  "username": "hadoop",
  "password": "",
  "database": "/",
  "path": "/data"
}
```

返回示例：

```json
{
  "code": 0,
  "message": "OK",
  "data": [
    {
      "name": "raw",
      "path": "/data/raw",
      "type": "DIRECTORY"
    },
    {
      "name": "a.csv",
      "path": "/data/a.csv",
      "type": "FILE",
      "size": 1234
    }
  ]
}
```

### 4. 交互流程

1. 选择 HDFS。
2. 输入连接参数。
3. 连接成功后默认加载 `/`。
4. 左侧展示目录树。
5. 点击目录继续加载子项。
6. 点击文件执行预览 / 下载 / 导出。

---

## 二、Hive 方案

### 1. 功能目标

Hive 适合做：

- database 浏览
- table 浏览
- partition 浏览
- preview / query / export

### 2. 前端表现

建议在选择 `Hive` 时显示：

- 主机
- 端口
- 用户名
- 数据库名
- 列出库 / 表按钮
- 左侧库或表树
- 右侧分区与数据预览区
- 导出按钮

### 3. 后端接口建议

继续支持 `SHOW TABLES`，并逐步补充：

- database 列表
- table 分区列表
- table 预览
- 查询结果导出

### 4. 交互流程

1. 选择 Hive。
2. 输入连接参数。
3. 连接成功后加载库 / 表。
4. 点击表查看分区。
5. 点击分区查看样本数据。
6. 支持导出查询结果。

---

## 三、HBase 方案

### 1. 功能目标

HBase 适合做：

- table 列表
- row 浏览
- scan 分页
- row 详情预览
- 导出扫描结果

### 2. 前端表现

建议在选择 `HBase` 时显示：

- 主机
- 端口
- 用户名
- table 列表
- row 浏览区
- rowkey 搜索框
- 扫描分页区
- 预览 / 导出按钮

### 3. 后端接口建议

建议拆分为：

- table 列表
- row 扫描
- row 详情
- 导出扫描结果

### 4. 交互流程

1. 选择 HBase。
2. 输入连接参数。
3. 加载 table 列表。
4. 点击 table 后扫描 row。
5. 点击 row 查看明细。
6. 导出当前扫描结果。

---

## 四、统一接口抽象建议

建议统一为“浏览节点”模型，而不是继续把所有服务强行映射成“表列表”。

### 统一节点结构

```json
{
  "name": "a.csv",
  "path": "/data/a.csv",
  "kind": "FILE",
  "childrenCount": 0,
  "size": 1234,
  "modifiedTime": "2026-05-21T00:00:00Z"
}
```

`kind` 可取：

- `DIRECTORY`
- `FILE`
- `TABLE`
- `ROW`
- `PARTITION`
- `DATABASE`

这样前端可以按类型渲染，而不是为每种服务写完全不同的页面逻辑。

---

## 五、推荐实施顺序

### 第一阶段：先做 HDFS

优先级最高，因为：

- 和文件浏览最接近
- 场景最明确
- 最容易快速验证

交付内容：

- 根目录浏览
- 多级目录展开
- 文件列表
- 文件预览
- 文件下载

### 第二阶段：Hive

交付内容：

- database / table 浏览
- partition 浏览
- 样本数据预览
- 导出查询结果

### 第三阶段：HBase

交付内容：

- table 浏览
- row 扫描
- row 详情
- 导出扫描结果

---

## 六、与现有 MySQL 栏的关系

现有 MySQL 栏建议基本不变，继续保留：

- 连接配置
- 测试连接
- 列表表
- 导出

对 MySQL 类数据库保持原有体验即可，不需要因为 HDFS / HBase / Hive 的扩展而大改。

---

## 七、当前项目里的定位建议

建议把这个能力逐步升级为：

**数据源浏览与导出**

其中：

- MySQL / PostgreSQL / Oracle / SQLServer / Hive：表型
- HDFS：文件型
- HBase：宽表 / NoSQL 型

---

## 八、当前建议的最小可行版本（MVP）

建议先落地 HDFS：

- 目录树
- 文件列表
- 预览
- 下载

确认前端交互和后端路径浏览都通畅后，再继续做 Hive 和 HBase。

---

## 九、备注

本方案保留为后续开发清单使用，当前阶段可先继续测试其他数据库服务，HDFS / HBase / Hive 的浏览功能后续再逐步实现。
