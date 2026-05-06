# V2-Hadoop 最小生态本机 Docker 真实可测方案

> 适用范围：`v1-frontend`、`v1-backend`、`v1-backend/app/db_connect.py`、`v1-backend/app/engine_factory.py`
>
> 目标：在本机通过 Docker 容器起一套最小 Hadoop 生态，使前端的 `hive / hbase / hdfs` 入口可以真实连通测试，并能完成连接校验、表/目录查询、失败定位与后续扩展验证。

---

## 一、方案目标

本方案不是做概念验证，而是做“真正可测”的本机环境。

需要达到的结果：

1. 前端可选择 `hive / hbase / hdfs`
2. 后端可真实连到对应服务
3. `test-connection` 能真实判断连通性
4. `list-tables` 能真实返回 Hive/HBase 的表信息
5. HDFS 能真实返回根目录或指定目录内容
6. 能区分客户端依赖、网络不可达、认证失败、超时等问题

---

## 二、当前代码现状

### 1. 已经具备的能力

后端已有以下基础实现：

- `v1-backend/app/engine_factory.py`
  - 支持 `hive`
  - 支持 `hbase`
  - 支持 `hdfs`
- `v1-backend/app/db_connect.py`
  - `/api/v1/db/test-connection`
  - `/api/v1/db/list-tables`
- 前端 `v1-frontend/app.js`
  - 已有 `dbType` 的多数据库渲染逻辑
  - 已支持 `hive / hbase / hdfs` 的默认端口和字段切换

### 2. 还需要补齐的点

- Docker 最小生态环境本身还没搭建
- 依赖安装与版本确认还没统一
- HDFS 语义需要更清晰的目录展示
- 错误提示还需要增强
- 需要一套稳定的启动、验证、回归流程

---

## 三、整体实施路线

建议按以下顺序执行：

1. 准备 Docker 与资源环境
2. 选择最小可运行的 Hadoop 生态镜像方案
3. 启动 HDFS / Hive / HBase 容器
4. 补齐后端 Python 依赖
5. 配置前端测试参数
6. 验证后端接口真实连通
7. 在前端逐个按钮测试
8. 根据失败结果修正配置或代码
9. 固化为可重复执行的本机测试流程

---

## 四、环境准备

### 1. 本机资源建议

最低建议：

- CPU：4 核以上
- 内存：8 GB 起步，建议 16 GB
- 磁盘：30 GB 以上可用空间
- 系统：Linux / WSL2 / Linux 虚拟机均可

### 2. 必备工具

- Docker
- Docker Compose
- Python 虚拟环境
- 你当前项目的前端与后端代码

### 3. 后端依赖安装

后端需要的客户端库至少包括：

```bash
pip install pyhive happybase hdfs thrift
```

如 Hive 认证方式更复杂，可能还要：

```bash
pip install thrift_sasl sasl
```

如果集群启用 Kerberos，还需要系统级 Kerberos 配置，不是单纯 pip 能解决的。

---

## 五、Docker 最小生态建议

### 方案选择建议

本方案推荐 Docker 容器方式，而不是直接本机安装。

原因：

- 容易重建
- 便于清理
- 端口与配置更可控
- 适合本机开发验证

### 最小服务集

为了让你的前端三个入口能真实可测，最少需要这些服务：

#### HDFS
- NameNode
- DataNode
- WebHDFS

#### Hive
- Hive Metastore
- HiveServer2

#### HBase
- HBase Master
- HBase RegionServer
- HBase Thrift Server

---

## 六、推荐部署结构

建议把 Docker 配置单独放在当前工作区 `iot` 下的一个目录，例如：

- `/home/yhz/iot/docker/hadoop/`

目录内建议包含：

- `docker-compose.yml`
- `.env`
- `README.md` 或启动说明
- 必要的初始化脚本

如果你要进一步规范，还可以拆成：

- `docker/hadoop/hdfs/`
- `docker/hadoop/hive/`
- `docker/hadoop/hbase/`

但第一版没必要过度拆分，先以能跑通为目标。

---

## 七、实施步骤

### 第 1 步，拉起基础 Hadoop HDFS

#### 目标
先把底座跑起来，确保 HDFS 可访问。

#### 需要验证
- NameNode 能启动
- DataNode 能启动
- WebHDFS 能访问
- 浏览器能打开 NameNode Web UI

#### 默认验证点
- `http://localhost:9870`

#### 通过标准
- 能看到 HDFS 管理页面
- 能访问 WebHDFS
- 能列出 `/` 下内容

---

### 第 2 步，拉起 Hive

#### 目标
在 HDFS 基础上起 Hive，确保能连接并执行简单 SQL。

#### 需要验证
- Hive Metastore 正常
- HiveServer2 正常
- 端口可访问
- `SELECT 1` 能执行
- `SHOW TABLES` 能执行

#### 默认验证点
- `localhost:10000`

#### 通过标准
- `pyhive` 能连接
- `SHOW TABLES` 能返回结果

---

### 第 3 步，拉起 HBase

#### 目标
在 HDFS 基础上起 HBase，并开启 Thrift Server。

#### 需要验证
- HBase Master 正常
- HBase RegionServer 正常
- Thrift Server 正常
- `tables()` 能返回表

#### 默认验证点
- `localhost:9090`

#### 通过标准
- `happybase.Connection(...)` 能连接
- `conn.tables()` 能返回结果

---

## 八、端口与连接约定

建议先按以下默认约定来跑：

- HDFS WebHDFS：`9870`
- HiveServer2：`10000`
- HBase Thrift：`9090`

如果容器镜像本身使用不同端口，也可以改，但前端和后端测试参数要同步一致。

---

## 九、后端需要做什么

### 1. 补齐依赖安装验证

在后端虚拟环境中确认这些包存在：

- `pyhive`
- `happybase`
- `hdfs`
- `thrift`

### 2. 确认连接测试逻辑真实可用

当前代码里：

- Hive 连接测试：`SELECT 1`
- Hive 表列表：`SHOW TABLES`
- HBase 连接测试：`tables()`
- HBase 表列表：`tables()`
- HDFS 连接测试：`list('/')`

这部分逻辑原则上可直接用于真实测试。

### 3. 增强错误分类

建议把异常提示分成几类：

- 依赖缺失
- 端口不可达
- 服务未启动
- 认证失败
- 权限不足
- 超时
- 其他未知错误

这样真实测试时，排查会快很多。

---

## 十、前端需要做什么

### 1. 继续使用现有 `dbType` 切换逻辑

前端已经支持：

- `hive`
- `hbase`
- `hdfs`

所以第一版不需要重写 UI。

### 2. 配置默认测试参数

建议前端在测试页准备这些默认值：

- `hive` → `localhost:10000`
- `hbase` → `localhost:9090`
- `hdfs` → `localhost:9870`

### 3. HDFS 文案建议单独处理

建议把 HDFS 相关文案改成“文件系统”或“目录浏览”，避免用户误以为它是数据库表。

---

## 十一、真实测试流程

### 流程 A：先用命令行验证容器

启动后，先不要点前端。

先确认：

1. 容器都已启动
2. 端口映射正常
3. 本机能访问对应端口
4. HDFS/Hive/HBase 服务状态正常

---

### 流程 B：先测 HDFS

#### 步骤
1. 打开前端
2. 选择 `hdfs`
3. 填 `localhost:9870`
4. 点“连接测试”

#### 期望结果
- 返回连接成功
- 能列根目录

#### 若失败
- 看是否端口不通
- 看 WebHDFS 是否起了
- 看容器日志

---

### 流程 C：再测 Hive

#### 步骤
1. 选择 `hive`
2. 填 `localhost:10000`
3. 点“连接测试”
4. 点“列出表”

#### 期望结果
- 连接成功
- 能返回表列表

#### 若失败
- 看 HiveServer2 是否正常
- 看 Metastore 是否正常
- 看 `pyhive` 是否安装完整

---

### 流程 D：最后测 HBase

#### 步骤
1. 选择 `hbase`
2. 填 `localhost:9090`
3. 点“连接测试”
4. 点“列出表”

#### 期望结果
- 连接成功
- 返回表名列表

#### 若失败
- 看 Thrift Server 是否启动
- 看 HBase Master/RegionServer 是否正常
- 看 `happybase` 是否可用

---

## 十二、建议的排查顺序

当测试失败时，按这个顺序排查：

1. 容器是否启动
2. 容器日志是否有报错
3. 端口是否开放
4. Python 依赖是否安装
5. 前端参数是否填写正确
6. 后端接口是否命中正确分支
7. 服务认证方式是否匹配
8. 服务是否允许当前用户访问

---

## 十三、建议补充的脚本

为了让后续反复测试更方便，建议加三个脚本：

### 1. 启动脚本
- 启动 Docker 生态
- 打印端口与健康检查结果

### 2. 依赖检查脚本
- 检查 `pyhive / happybase / hdfs / thrift`
- 打印缺失项

### 3. 连通性测试脚本
- 检查 `9870 / 10000 / 9090`
- 尝试执行最小操作
- 输出成功/失败原因

---

## 十四、验收标准

### HDFS
- 能访问 WebHDFS
- 能列 `/`
- 前端连接测试成功

### Hive
- 能 `SELECT 1`
- 能 `SHOW TABLES`
- 前端连接测试成功
- 前端列表成功

### HBase
- 能连接 Thrift
- 能 `tables()`
- 前端连接测试成功
- 前端列表成功

### 综合
- 前端三个类型都可测试
- 后端返回结果清晰
- 失败原因可定位
- 流程可重复执行

---

## 十五、实施顺序建议

推荐你按下面顺序推进：

1. 写 Docker Compose
2. 起 HDFS
3. 起 Hive
4. 起 HBase
5. 装后端依赖
6. 用命令行验证端口
7. 用前端按钮逐项验证
8. 优化错误提示
9. 固化为可重复流程

---

## 十六、后续还需要做什么

当容器方案跑通后，后续建议再做这些事：

1. 把测试参数沉淀成前端默认配置
2. 把错误提示做成更友好的中文文案
3. 把 HDFS 语义从“数据库”里拆出来
4. 把测试脚本纳入项目维护
5. 把容器方案固化为一键启动方案

---

## 十七、文件关系说明

本文件是本机 Hadoop 最小生态真实可测方案，和现有方案的关系如下：

- `V2执行清单/V1-NiFi统一实施总方案.md`
  - 负责总方向和统一原则
- `V2执行清单/V1-NiFi统一实施可执行方案.md`
  - 负责 NiFi 统一实施落地
- `V2执行清单/V2-Hadoop最小生态本机Docker真实可测方案.md`
  - 负责本机 Hadoop 最小生态真实测试落地

---

## 十八、最终说明

这份方案的核心是：

- 先用 Docker 容器把最小 Hadoop 生态搭起来
- 再用当前前端按钮做真实连通测试
- 最后把测试结果、错误定位和环境准备标准化

只要这套链路跑通，你就拥有一套可重复、可验证、可回归的本机真实测试环境。
