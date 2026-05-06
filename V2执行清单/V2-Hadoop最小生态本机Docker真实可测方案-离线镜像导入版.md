# V2-Hadoop 最小生态本机 Docker 真实可测方案（离线镜像导入版）

> 适用范围：`v1-frontend`、`v1-backend`、`v1-backend/app/db_connect.py`、`v1-backend/app/engine_factory.py`、`docker/hadoop/`
>
> 目标：在当前网络环境下，通过“离线导入镜像 + 本机 Docker 容器”方式，搭建最小 Hadoop 生态，使前端 `hive / hbase / hdfs` 入口可以真实连通测试，并能稳定验证连接、表/目录查询与错误定位。

---

## 一、为什么改成离线镜像导入版

当前在线拉取方案已经验证存在两个问题：

1. 镜像虽然能走 Docker 镜像加速，但仍可能跳转到外部依赖链路。
2. 当前网络环境对部分镜像源、`gcr.io` 等地址访问不稳定，在线拉取成功率低。

因此，本方案改为：

- 先在一台可稳定联网的机器上拉取镜像
- 再用 `docker save` 导出成 `tar`
- 在当前本机使用 `docker load` 导入
- 然后启动容器并进行真实测试

这样做的好处是：

- 不再依赖实时外网拉取
- 结果更可控
- 失败原因更集中于配置本身，而不是网络波动

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

- 离线镜像的获取、导出、导入流程还没执行
- 需要一套稳定的容器启动与验证流程
- HDFS 语义需要更清晰的目录展示
- 错误提示还需要增强
- 需要把测试流程固定成可重复执行的清单

---

## 三、整体实施路线

建议按以下顺序执行：

1. 确定需要的镜像清单
2. 在可联网机器上预拉取并导出镜像
3. 将镜像 `tar` 文件带到当前本机
4. 在当前本机执行 `docker load`
5. 启动 HDFS / Hive / HBase 容器
6. 补齐后端 Python 依赖
7. 验证后端接口真实连通
8. 在前端逐个按钮测试
9. 固化为可重复执行的本机测试流程

---

## 四、离线导入方案的目录约定

建议继续使用当前工作区下的目录：

- `/home/yhz/iot/docker/hadoop/`

目录内建议包含：

- `docker-compose.yml`
- `.env`
- `README.md`
- `images/`
- `scripts/`

### `images/` 目录用途

用于存放离线导出的镜像包，例如：

- `hadoop-namenode.tar`
- `hadoop-datanode.tar`
- `hive-metastore.tar`
- `hive-server2.tar`
- `hbase.tar`
- `hbase-thrift.tar`

### `scripts/` 目录用途

用于存放辅助脚本，例如：

- `load-images.sh`
- `check-ports.sh`
- `verify-hadoop.sh`

---

## 五、需要的镜像清单

离线导入前，先确认要用哪些镜像。

### 最小镜像集

#### HDFS
- NameNode
- DataNode

#### Hive
- Hive Metastore
- HiveServer2

#### HBase
- HBase 主服务镜像
- HBase Thrift Server

### 说明

如果某些镜像在当前选择的仓库里不可得，可以替换为同等功能镜像，但要保证：

- 端口一致或可映射
- 启动命令可用
- 能被后端 `pyhive / happybase / hdfs` 正确连接

---

## 六、离线导入的执行步骤

### 第 1 步，在可联网机器上拉取镜像

你需要在一台能正常访问镜像仓库的机器上执行：

```bash
docker pull <image:tag>
```

把需要的镜像全部拉下来。

### 第 2 步，导出镜像为 tar

在联网机器上执行：

```bash
docker save -o hadoop-namenode.tar <image:tag>
```

每个镜像都导出一个 `tar`，或合并导出成一个总包。

### 第 3 步，把 tar 文件拷贝到当前本机

拷贝到：

- `/home/yhz/iot/docker/hadoop/images/`

### 第 4 步，在当前本机导入镜像

在当前本机执行：

```bash
docker load -i /home/yhz/iot/docker/hadoop/images/hadoop-namenode.tar
```

对所有镜像重复导入。

### 第 5 步，确认镜像已经存在

执行：

```bash
docker images
```

检查镜像名和 tag 是否已出现。

### 第 6 步，启动容器

执行：

```bash
cd /home/yhz/iot/docker/hadoop
docker compose up -d
```

如果 compose 中引用的镜像已经本地存在，Docker 就不会再去联网拉取。

---

## 七、启动后的验证顺序

### 1. 先看容器状态

执行：

```bash
docker compose ps
```

确认容器都处于 `Up` 或健康状态。

### 2. 再看容器日志

如果有异常，执行：

```bash
docker compose logs -f --tail=200
```

重点看：

- HDFS 是否初始化成功
- Hive Metastore 是否连接成功
- HiveServer2 是否启动成功
- HBase Thrift 是否监听成功

### 3. 再检查端口

确认以下端口可访问：

- `9870` → HDFS Web UI
- `10000` → HiveServer2
- `9090` → HBase Thrift

### 4. 最后回到前端测试

按顺序测试：

1. `hdfs`
2. `hive`
3. `hbase`

---

## 八、后端需要做什么

### 1. 补齐依赖安装验证

后端虚拟环境中至少应存在：

```bash
pip install pyhive happybase hdfs thrift
```

如果 Hive 环境更复杂，再补：

```bash
pip install thrift_sasl sasl
```

### 2. 确认连接测试逻辑真实可用

当前代码里：

- Hive 连接测试：`SELECT 1`
- Hive 表列表：`SHOW TABLES`
- HBase 连接测试：`tables()`
- HBase 表列表：`tables()`
- HDFS 连接测试：`list('/')`

这部分逻辑可直接用于真实测试。

### 3. 增强错误分类

建议后续把异常提示分成几类：

- 依赖缺失
- 端口不可达
- 服务未启动
- 认证失败
- 权限不足
- 超时
- 其他未知错误

---

## 九、前端需要做什么

### 1. 继续使用现有 `dbType` 切换逻辑

前端已支持：

- `hive`
- `hbase`
- `hdfs`

第一版不需要重写 UI。

### 2. 配置默认测试参数

建议默认填写：

- `hive` → `localhost:10000`
- `hbase` → `localhost:9090`
- `hdfs` → `localhost:9870`

### 3. HDFS 文案建议单独处理

建议把 HDFS 文案改成“文件系统”或“目录浏览”，避免误导成数据库表。

---

## 十、真实测试流程

### 流程 A：先导入镜像

1. 把镜像 `tar` 放进 `/home/yhz/iot/docker/hadoop/images/`
2. 执行 `docker load -i ...`
3. 确认 `docker images` 中能看到对应镜像

### 流程 B：再启动容器

1. 进入 `/home/yhz/iot/docker/hadoop/`
2. 执行 `docker compose up -d`
3. 查看容器状态和日志

### 流程 C：先测 HDFS

1. 打开前端
2. 选择 `hdfs`
3. 填 `localhost:9870`
4. 点“连接测试”

### 流程 D：再测 Hive

1. 选择 `hive`
2. 填 `localhost:10000`
3. 点“连接测试”
4. 点“列出表”

### 流程 E：最后测 HBase

1. 选择 `hbase`
2. 填 `localhost:9090`
3. 点“连接测试”
4. 点“列出表”

---

## 十一、建议的排查顺序

当测试失败时，按这个顺序排查：

1. 镜像是否已导入
2. 容器是否启动
3. 容器日志是否有报错
4. 端口是否开放
5. Python 依赖是否安装
6. 前端参数是否填写正确
7. 后端接口是否命中正确分支
8. 服务认证方式是否匹配
9. 服务是否允许当前用户访问

---

## 十二、建议补充的脚本

为了让后续反复测试更方便，建议加三个脚本：

### 1. 镜像加载脚本

- 批量 `docker load`
- 输出已导入镜像清单

### 2. 端口检查脚本

- 检查 `9870 / 10000 / 9090`
- 输出每个端口的连通情况

### 3. 连通性验证脚本

- 尝试执行最小操作
- 输出成功/失败原因

---

## 十三、验收标准

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

## 十四、实施顺序建议

推荐按下面顺序推进：

1. 准备离线镜像
2. 导入镜像
3. 启动 HDFS
4. 启动 Hive
5. 启动 HBase
6. 装后端依赖
7. 用命令行验证端口
8. 用前端按钮逐项验证
9. 优化错误提示
10. 固化为可重复流程

---

## 十五、后续还需要做什么

当离线镜像方案跑通后，建议继续做这些事：

1. 把测试参数沉淀成前端默认配置
2. 把错误提示做成更友好的中文文案
3. 把 HDFS 语义从“数据库”里拆出来
4. 把测试脚本纳入项目维护
5. 把离线导入流程固化为一键执行方案

---

## 十六、文件关系说明

本文件是本机 Hadoop 最小生态真实可测方案的离线版，和现有方案的关系如下：

- `V2执行清单/V1-NiFi统一实施总方案.md`
  - 负责总方向和统一原则
- `V2执行清单/V1-NiFi统一实施可执行方案.md`
  - 负责 NiFi 统一实施落地
- `V2执行清单/V2-Hadoop最小生态本机Docker真实可测方案.md`
  - 负责在线镜像方案
- `V2执行清单/V2-Hadoop最小生态本机Docker真实可测方案-离线镜像导入版.md`
  - 负责离线镜像导入方案

---

## 十七、最终说明

这份方案的核心是：

- 先用离线镜像导入绕过当前不稳定的在线拉取
- 再用本机 Docker 容器起最小 Hadoop 生态
- 然后用当前前端按钮做真实连通测试
- 最后把测试结果、错误定位和环境准备标准化

只要这套链路跑通，你就拥有一套可重复、可验证、可回归的本机真实测试环境。
