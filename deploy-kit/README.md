# IoT 智慧平台 部署工具包（deploy-kit）

一套**可复用**的部署脚本，用于把平台装到任意一台机器上 —— 无论是「忽米平台」（管理员 + 多工厂租户）
还是「工厂独立站点」，流程完全一致，区别只在 `config.env` 的参数和执行次数。

> 本目录为**纯新增**，不修改 `v1-backend` / `v1-frontend` 任何现有代码。
> 会改动代码的动作只有三类「补丁」，全部自动备份 `.bak` 且支持 `--revert` 还原。

---

## 0. 可以直接交给别人（或另一个 agent）装吗？

**接近可以，但有三件事必须先确认，否则会卡住。** 脚本本身已做语法校验、补丁已在副本上实测。

### 交接前必须确认的三件事

| # | 事项 | 说明 | 不确认的后果 |
|---|------|------|-------------|
| 1 | **代码怎么到目标机器** | `GIT_REPO` 用 SSH 地址（`git@github-yhz:...`），新机器**必须配好对应的 SSH key**，否则 clone 失败。若不方便配 key，改用离线包：先在能联网的机器 `git clone` + `tar` 打包，拷到目标机解压到 `CODE_DIR`，然后 `GIT_REPO` 留空 | 第 4 步直接失败 |
| 2 | **MySQL root 能否登录** | 默认走免密（Ubuntu auth_socket）。`setup.sh` 会**自动探测**：若 root 设了密码，会当场提示你输入（也可事后填 `MYSQL_ROOT_PASS`） | 第 7 步 die 退出 |
| 3 | **基础软件是否已装** | python3 / mysql / docker / nginx。全新机器把 `INSTALL_SYSTEM_DEPS=true` 打开，脚本会自动装（优先包内离线 deb）；已装过的机器保持 `false`，避免动到现有环境 | 缺哪个，对应步骤失败或跳过 |
| 4 | **Python 是否 ≥3.9** | 应用依赖 pydantic 2.11，硬性要求 ≥3.9。Ubuntu 22.04 自带 3.10、24.04 自带 3.12 都满足；**Ubuntu 20.04 自带 3.8，不能用** | 虚拟环境里装不上依赖，后端起不来 |

### 覆盖边界（哪些情况装不了，预先说清楚）

| 维度 | 覆盖范围 | 超出范围时 |
|---|---|---|
| 发行版 | Ubuntu 20.04/22.04/24.04 有离线系统 deb | 其他发行版走 apt，需要网络 |
| CPU 架构 | 仅 x86_64 | aarch64 无 wheel/deb |
| Python | 3.10 / 3.11 / 3.12 / 3.13（应用另要求 ≥3.9） | 无匹配 wheel 时回退联网 |
| init 系统 | 需要 systemd | 非 systemd（如 WSL1）服务托管不了 |
| 资源 | 内存 ≥8G、/home ≥20G、docker 分区 ≥10G | `check_env.sh` 会报出来 |
| 端口 | 5174/8081/8080/3306 | 冲突则起不来，`check_env.sh` 会提示 |
| 网络 | 全离线可装（上述范围内）；能连网时自动切 HTTPS 源 | **只能走代理**上网的机器需先配 `http_proxy` |

### 已验证到什么程度（诚实告知）

- ✅ **应用链路已实测**：`test_e2e.sh` 在一次性容器里把「离线 deb → 离线 wheel → 建库 →
  三个补丁 → 建账号 → 起后端 → 登录/权限/单账号 → 前端代理 → 文件上传」跑通，10/10 通过。
  靠它抓到过两个致命问题（缺 `python-multipart`、缺 `tzdata`）。
- ⚠️ **尚未覆盖**：systemd 服务托管、NiFi 容器（容器里没有 systemd/docker），
  以及真机上的 Nginx 端口、防火墙、开机自启 —— 这些仍需在目标机上用 `verify.sh` / `smoke_test.sh` 验证。
- **首次部署建议**：挑一台不重要的机器先跑，用 `verify.sh` 定位问题。

---

## 1. 目录结构

```
deploy-kit/
├── setup.sh               # ★ 交互式向导：4 个问题 + 自动检测（给非技术人员）
├── config.env.example     # 参数样例 -> 复制为 config.env
├── deploy.sh              # 主部署脚本（一键，12 步）
├── install_sysdeps.sh     # 系统依赖安装：离线 deb 优先，其次自动把 apt 源切 HTTPS
├── install_odbc.sh        # 离线装 SQL Server ODBC 驱动（可选）
├── check_env.sh           # 部署前五项体检（只检测、不改动）
├── make_offline_package.sh    # 打离线交付包（给无外网的工厂机器）
├── prepare_offline_assets.sh  # 预编译多版本 Python wheel + 导出 NiFi 镜像
├── prepare_sysdeps.sh     # 预下载系统依赖 deb 闭包（供完全离线部署）
├── make_selfextract.sh    # 生成单文件自解压安装器 iot-install.run
├── add_factory.sh         # 开户（多租户模式用；单账号模式不需要）
├── verify.sh              # 部署自检：服务/端口/登录是否成功
├── smoke_test.sh          # ★ 功能冒烟：上传/转换/权限是否可用（走前端）
└── testdata/sample.csv    # 冒烟测试用的样例数据
├── templates/
│   ├── iot-backend.service              # 后端 unit
│   ├── iot-backend.service.override.conf# 后端环境变量（密钥/DB/开关）
│   ├── iot-frontend.service             # 前端 unit ← 原项目缺失，本包补齐
│   ├── docker-compose.yml               # NiFi 容器
│   ├── nginx.conf                        # 反向代理
│   ├── init.sql                          # 建库建表
│   └── requirements-runtime.txt          # 完整运行时依赖 ← 原 requirements.txt 有缺口
├── tools/
│   ├── userctl.py                      # 用户/租户管理（开户、列表），已实测连库 OK
│   ├── render.py                       # 模板渲染（Python 替换，比 sed 安全）
│   ├── patch_disable_self_register.py  # 自助注册开关补丁（方案一必需，默认开）
│   ├── disable_register_page.py        # 下线前端注册入口（方案一必需，默认开）
│   └── patch_private_storage.py        # 私有化补丁（让 ceph_endpoint 真正生效）
└── .deploy-secrets                # 自动生成（权限 600）：DB 密码、管理员密码、密钥
```

---

## 2. 快速开始（推荐：交互式向导）

**给非技术人员**：解压后只跑一条命令，回答 4 个问题就装完。

```bash
cd /home/yhz/iot/deploy-kit
sudo bash setup.sh
```

向导会问：

| # | 问题 | 说明 |
|---|---|---|
| 1 | 这台机器给忽米还是工厂？ | 决定账号性质和默认值 |
| 2 | 账号名叫什么？ | 忽米一般 `humi`；工厂用代号如 `factory_a` |
| 3 | 这个账号能进「内部管理页」吗？ | 见下方"角色区分" |
| 4 | 对外访问地址？ | 自动检测本机 IP，回车即用；需要域名才改 |

> **基础软件是否缺失由脚本自动检测**，不再问用户。缺了就在部署阶段自动安装，
> 齐全就跳过（部署更快）。

**给技术人员**（想手工控制）：跳过向导，直接

```bash
cp config.env.example config.env && vi config.env
sudo ./deploy.sh
```

### 部署后验收（两步，都跑）

```bash
sudo ./verify.sh       # 部署是否成功：服务/端口/登录/用户数
sudo ./smoke_test.sh   # 功能是否可用：上传/文件落盘/权限/目录结构
```

两者的区别：

| 脚本 | 回答的问题 | 关键检查项 |
|---|---|---|
| `verify.sh` | **装上了吗？** | 服务 active、端口监听、登录成功、用户数=1 |
| `smoke_test.sh` | **能用吗？** | 走前端上传 CSV、文件落盘、4 个数据根齐全、权限正确 |

> `smoke_test.sh` **所有请求都走前端 5174**（经 serve.py 代理），
> 不直接打后端 —— 这样连前端代理链路也一起验了。

**建议**：先在**当前已跑通的开发机**上跑一次 `smoke_test.sh` 留作基准，
再到新机器跑，对比结果，就能确认"新环境功能是否和现在一致"。

### 打包前自测（强烈建议，能提前发现"装上去起不来"）

在**打包机上**跑一次端到端测试 —— 它会在一次性容器里把整条链路真装一遍，
不碰宿主机的生产环境：

```bash
cd /home/yhz/iot/deploy-kit
docker run --rm --name iot-e2e \
  -v /home/yhz/iot:/src:ro \
  -v "$PWD/test_e2e.sh":/e2e.sh:ro \
  ubuntu:22.04 bash /e2e.sh
```

覆盖 10 步：离线 deb → 离线 wheel → `init.sql` → 三个代码补丁 → 建账号 →
起后端 → 登录/权限/单账号 → 前端代理 → 文件上传（multipart）→ 其他接口冒烟。
末尾打印「全部通过」即 OK。

> 这个测试已经真实抓到过两个**致命且只有装一遍才会暴露**的问题：
> · `requirements-runtime.txt` 漏了 **python-multipart** → 后端在注册路由阶段直接崩，起不来
> · 缺 **tzdata** → `ZoneInfo("Asia/Shanghai")` 抛异常，每个请求都 500
>
> 它**不覆盖** systemd 服务与 NiFi 容器（容器里没有 systemd/docker），这两部分仍需真机验证。

### 角色区分：管理员 vs 普通用户

由账号的 `is_admin` 标志决定，**部署时设一次即可，之后全自动**：

| | 管理员（忽米） | 普通用户（工厂） |
|---|---|---|
| 内部管理页入口 | 首页显示"打开内部管理页" | **自动隐藏**（`app.js` 按 is_admin 控制） |
| 直接访问 `/internal.html` | 正常 | 后端返回 403 |
| 数据可见范围 | 全部 | 仅自己 |

设置方式：向导第 3 问，或 `config.env` 里 `SITE_IS_ADMIN=true/false`。

> 工厂场景建议填 `true`：那本就是它自己的数据，且忽米用同一账号可远程查看。
> 填 `false` 则忽米无法远程查看该机器。

---

## 2.1 离线交付（工厂无外网 / 不给 SSH key）

### 第一步：在能联网的机器上准备离线资源

```bash
cd /home/yhz/iot/deploy-kit
sudo ./prepare_offline_assets.sh
```

产出到 `deploy-kit/offline-assets/`：

| 产物 | 大小 | 作用 |
|---|---|---|
| `wheels/py310 ~ py313/` | ~112MB | Python 依赖**预编译 wheel**（四个版本各一套），工厂端零下载、零编译、零版本猜测 |
| `iot-nifi-python-<日期>.tar` | ~1.6GB | NiFi 容器镜像，工厂端 `docker load` 即可 |
| `deb/` | ~3MB | SQL Server ODBC 驱动（各 Ubuntu 版本各一份） |
| `ARCH` | - | 记录 CPU 架构，供部署时比对 |

**可选：把系统依赖也离线化**（想让目标机完全断网也能装时做，见下）

```bash
./prepare_sysdeps.sh 20.04 22.04 24.04     # 生成 offline-assets/deb/sysdeps/ubuntu-<版本>/
```

会把 `python3 / python3-venv / python3-pip / nginx / mysql-server / docker.io / curl /
ca-certificates / docker-compose-v2` 的**完整依赖闭包**（含 postinst 所需的一切）下载下来，
部署时 `install_sysdeps.sh` 按本机发行版 `dpkg -i` 离线安装。

**可选：把 SQL Server ODBC 也离线化**（连 SQL Server 才需要）

```bash
./prepare_odbc_assets.sh 20.04 22.04 24.04   # 生成 offline-assets/deb/odbc/ubuntu-<版本>/
```

在 `ubuntu:<版本>` 容器里从微软源抓「msodbcsql17 + unixODBC」的**递归依赖闭包**
（不依赖打包机自身的 ODBC 环境，也不会碰打包机装的那套 unixODBC）。
部署时 `install_odbc.sh` 按本机发行版挑目录、`dpkg -i` 离线安装。
⚠️ unixODBC 的包名在 20.04 与 22.04+ 是两套互斥体系（`libodbc1` vs `libodbc2`，抢同一个
`libodbc.so.2.0.0`），所以**每个发行版都必须各抓一套**，不能互用。

### 第二步：打包（一个文件搞定）

```bash
./make_offline_package.sh
```

产物：**一个** `iot-offline-<日期>.tgz`（约 1.7G），里面已含：

- 项目代码 + 全部部署工具
- **Python 依赖 wheel**（112M，3.10/3.11/3.12/3.13 各一套）→ 部署时零下载、零编译
- **NiFi 镜像**（1.6G）→ 部署时零下载
- **系统软件离线 deb**（约 420M，Ubuntu 20.04/22.04/24.04 各一套）→ 系统软件也零下载
- NiFi flow 配置（`real_nifi_conf/`，含 6 个已启动的 processor）

只打包**白名单**的 5 个顶层目录：`v1-backend` / `v1-frontend` / `docker` /
`real_nifi_conf` / `deploy-kit`（从根本上杜绝把 `.codebuddy`、开发文档等误打进去）。
另外排除 `.venv` / `__pycache__` / 日志 /
`v1-backend/data/app.db`（含本机用户账号，**绝不能外带**）/
`docker/hadoop/images/*.tar`（2.7G，仅 HDFS 数据源需要，加 `--with-hadoop` 可含）。

> 想拆开传（如 U 盘装不下）：
> `--no-sysdeps` 去掉系统 deb（省 420M）、`--no-nifi-image` 去掉镜像（省 1.6G，包缩到 ~150M）。

### 第三步：交付（只给一个文件）

```bash
iot-offline-<日期>.tgz
```

工厂机器上，**一行命令**完成解压+部署：

```bash
sudo tar xzf iot-offline-<日期>.tgz -C /home/yhz \
  && cd /home/yhz/iot/deploy-kit && sudo ./setup.sh
```

### ⚠️ 三个必须提前确认的约束

| 约束 | 说明 |
|---|---|
| **Python 版本** | 包内已为 **3.10 / 3.11 / 3.12 / 3.13** 各备一套 wheel，部署时自动挑匹配的，无需预先知道目标版本 |
| **系统包** | 优先用包内离线 deb（`prepare_sysdeps.sh` 生成，覆盖 Ubuntu 20.04/22.04/24.04）；没有对应 deb 时**自动把 apt 源 `http://` 换成 `https://`** 再装 |
| **MySQL root** | Ubuntu 默认免密；有密码则填 `MYSQL_ROOT_PASS` |

工厂机器上（脚本执行完会打印完整步骤，这里摘要）：

```bash
sudo tar xzf iot-offline-<日期>.tgz -C /home/yhz     # 必须落到 /home/yhz
cd /home/yhz/iot/deploy-kit
cp config.env.example config.env
vi config.env          # GIT_REPO 留空；改 DOMAIN / NIFI_PROXY_HOST / INSTALL_SYSTEM_DEPS=true
sudo ./deploy.sh && sudo ./verify.sh
sudo ./add_factory.sh <工厂名>
```

> **路径提醒**：离线包解压出来是 `iot/`，必须落在 `/home/yhz` 下变成 `/home/yhz/iot`。
> 若工厂机器的用户名不同，需同步改 `config.env` 的 `APP_USER` / `APP_HOME` / `CODE_DIR`
> （但建议直接在工厂机器上也建 `yhz` 用户，可零改动复用）。

---

## 3. 部署形态：一套实例 = 一个账号

采用**最简模型**：每台机器部署一套，只建**一个**账号，不多不少。

```
忽米服务器（一套）          工厂A服务器（一套）        工厂B服务器（一套）
└── humi（管理员）          └── factory_a             └── factory_b
```

| 机器归属 | `SITE_USER` | `SITE_IS_ADMIN` |
|---|---|---|
| 忽米 | `humi` | `true` |
| 工厂 A | `factory_a` | `true`（推荐） |
| 工厂 B | `factory_b` | `true`（推荐） |

**这么设计的好处**
- 不会再出现"内部管理页冒出一堆用户"的混乱（每套就 1 个）
- 数据天然物理隔离 —— 不同机器、不同数据库，A 厂碰不到 B 厂
- 交付关系清晰：一台机器 = 一个客户 = 一个账号

**关于 `SITE_IS_ADMIN`**

| 值 | 效果 |
|---|---|
| `true`（推荐） | 该账号能进内部管理页。工厂场景下那本就是它自己的数据；**忽米持有密码即可远程查看**，在"只有一个账号"的前提下保住了可见性 |
| `false` | 纯普通用户，进不了内部管理页；忽米也就无法远程查看该站点 |

### 部署命令（忽米和工厂完全一样）

```bash
# 该机器的 config.env
SITE_USER=humi                # 工厂机器就填 factory_a
SITE_IS_ADMIN=true
DOMAIN=iot.example.com
NIFI_PROXY_HOST=iot.example.com:8080   # 必须按本站点改，别留上一台的 IP

sudo ./deploy.sh        # 自动建上面这一个账号
sudo ./verify.sh        # 会校验：登录成功 + 用户总数=1
```

> 部署完**不需要**再跑 `add_factory.sh`（那个是给"一套多租户"用的；
> 当前单账号模式下，账号已由 `deploy.sh` 建好）。

> **注意**：`NIFI_PROXY_HOST` 在原始 `docker-compose.yml` 里默认带着 `202.113.76.55:9443`
> （开发机 IP）。模板已把它参数化，部署时务必按当前站点填写，否则 NiFi 界面会出现
> "请求主机头不被允许" 的错误。

---

## 4. 私有化用户（数据写到指定本地盘）

默认**不启用**。启用需要两步：

```bash
# 1) config.env 打开补丁开关
APPLY_PRIVATE_STORAGE_PATCH=true

# 2) 部署（会自动备份 main.py 为 main.py.bak）
sudo ./deploy.sh

# 3) 开户时指定落盘路径
sudo ./add_factory.sh factory_c --private /data/factory_c
```

补丁做了什么：改 `main.py::_resolve_user_storage_root`，让
`deployment_mode=private` 且 `ceph_endpoint` 非空的用户，数据根指向该路径；
其余用户仍走 `<APP_HOME>/<用户名>`。用户名强制过 `_sanitize_filename_component`，
顺带堵掉路径穿越。

还原：

```bash
python3 tools/patch_private_storage.py /home/yhz/iot --revert
```

> **如果不打补丁**：注册/开户时填的 `ceph_endpoint` 只会写入数据库，
> **数据仍然落在平台本地盘**（原代码里这里是 TODO）。别拿这个跟工厂谈"数据在你那儿"。

---

## 4.5 多数据库支持情况

后端支持 8 种数据源（用于「数据库导出」功能）。**Python 驱动已全部打进离线 wheel**，
部署后开箱即用情况如下：

| 数据源 | db_type | Python 驱动 | 系统依赖 | 部署后 |
|---|---|---|---|---|
| MySQL / MariaDB | `mysql` | PyMySQL | 无 | ✅ 开箱即用 |
| PostgreSQL | `postgresql` | psycopg2-binary | 无（自带 libpq） | ✅ 开箱即用 |
| SQLite | `sqlite` | 内置 | 无 | ✅ 开箱即用 |
| Oracle | `oracle` | oracledb | 无（thin 模式） | ✅ 开箱即用 |
| Hive | `hive` | pyhive + thrift | 无（纯 Python） | ✅ 开箱即用 |
| HBase | `hbase` | happybase + thriftpy2 | 无 | ✅ 开箱即用 |
| HDFS | `hdfs` | hdfs（WebHDFS） | 无 | ✅ 开箱即用 |
| **SQL Server** | `mssql` | pyodbc | ⚠️ **需 ODBC 驱动** | 需额外装驱动 |

**SQL Server 的额外步骤**（只有要用它才需要）：

```bash
# unixodbc 会由 INSTALL_SYSTEM_DEPS 自动装；ODBC 驱动需单独装：
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
odbcinst -q -d    # 验证：应列出 [ODBC Driver 17 for SQL Server]
```

> 完全离线且需要 SQL Server 时：在有网机器上下载对应 `.deb` 及其依赖，
> 一起拷到目标机 `dpkg -i` 安装。`check_env.sh` 会检测此驱动并提示。

## 5. 已知问题与注意

| # | 问题 | 本包的处理 |
|---|------|-----------|
| 1 | `requirements.txt` 缺 `SQLAlchemy`/`PyJWT`/`bcrypt`/`cryptography`/`APScheduler`，**新机器装完后端起不来** | 提供 `templates/requirements-runtime.txt` 完整清单 |
| 2 | `bcrypt` 缺失时注册会把**密码明文**存库 | 同上，已补齐 |
| 3 | 原项目缺 `v1-frontend/deploy/iot-frontend.service`，导致 `install_autostart_services.sh` 第 3 步必失败 | 本包补齐模板，由 `deploy.sh` 安装 |
| 4 | 原 `override.conf` 把后端绑到 `0.0.0.0:8081`，绕过 Nginx 直暴露 | 模板默认 `127.0.0.1`，可用 `BACKEND_BIND` 调整 |
| 5 | 路径硬编码 `/home/yhz/iot` | 参数化；但**建议保持默认** `APP_USER=yhz` + `CODE_DIR=/home/yhz/iot`，可零改动复用 |
| 6 | `sed` 替换模板时密码含 `/` `&` 会出错 | 用 `tools/render.py`（Python 替换），规避转义问题 |
| 7 | **后端根本不读 `IOT_ALLOW_SELF_REGISTER`** —— 只在配置里写这个变量等于没关注册 | `patch_disable_self_register.py` 打补丁后变量才生效（默认开） |
| 8 | 内部管理页**没有「新建工厂」按钮**（原项目无此接口，且本次不改业务代码） | 开户一律用 `add_factory.sh`（命令行，等价于忽米代开户） |
| 9 | 某些内网**只放行 HTTPS(443)，HTTP(80) 被挡** → `apt` 连 `security.ubuntu.com:80` 全部超时，部署卡在装系统软件 | `install_sysdeps.sh` 检测到 apt 失败会**自动把源 `http://` 换成 `https://`** 后重试（原文件备份为 `*.bak.iot`） |
| 10 | Ubuntu 官方源**没有 `docker-compose-plugin`** 这个包 | 改为依次尝试 `docker-compose-v2` → `docker-compose`，都失败才告警 |

---

## 6. 验收清单

- [ ] `sudo ./verify.sh` 全部 PASS
- [ ] 管理员能登录并进入 `/internal.html`，看到所有工厂
- [ ] `/register.html` 不可访问（方案一）
- [ ] 直接 `curl` 注册接口返回 403
- [ ] 工厂账号登录后只能看到自己的数据
- [ ] 工厂账号访问 `/internal.html` 返回 403
- [ ] 新建工厂后，`<APP_HOME>/<工厂名>/` 下 4 个数据根自动建好
- [ ] 生产环境已配 HTTPS，`.deploy-secrets` 已妥善保管

---

## 7. 常用运维

```bash
sudo systemctl status iot-backend iot-frontend   # 服务状态
sudo journalctl -u iot-backend -f                # 后端日志
sudo ./add_factory.sh <工厂名>                    # 开户
sudo -u yhz env ... python3 tools/userctl.py list # 看用户列表
docker logs -f iot-nifi                           # NiFi 日志
```
