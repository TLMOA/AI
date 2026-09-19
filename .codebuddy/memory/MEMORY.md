# 长期记忆（IoT 智慧平台）

> 精简版（2026-09-16 整理，原文件超限已去重）；细节历史见 `memory/YYYY-MM-DD.md`。

## 1. 架构与运维
- 后端 FastAPI **:8081**（systemd `iot-backend`，绑 127.0.0.1；无热重载，改 `main.py` 必须重启）
- 前端 `python3 v1-frontend/serve.py` **:5174**，内置 `/api/v1/` → 8081 代理。
  **严禁** `python3 -m http.server`（无代理，远程浏览器全 404）；**必须绑 0.0.0.0**
- NiFi 容器 `iot-nifi`（apache/nifi:2.8.0）HTTPS **:8080**；**必须用 `localhost` 访问**（用 IP 报 400 Invalid SNI；
  Nginx 反代要 `proxy_pass https://localhost:...` + `proxy_ssl_server_name on`）
- 认证源：MySQL `nifi` 库 `iot_users`；业务数据 SQLite `v1-backend/data/app.db`
- 双模式：`backend_mode=local`（纯 Python，默认）/ `nifi`（写任务 JSON 给容器，失败回退本地）
- env 文件：`v1-backend/deploy/iot-backend.service.override.conf`

```bash
前端：pkill -f serve.py; cd v1-frontend && nohup python3 serve.py > /tmp/frontend.log 2>&1 &
后端：kill $(pgrep -f uvicorn | head -1)      # systemd ~10 秒自动拉起
NiFi：docker start iot-nifi                    # 重启后 flow 自动恢复（仓库持久化）
更新 worker：docker cp <src> iot-nifi:/opt/nifi/nifi-current/data/iot/bin/
```
- NiFi 2.x API：登录 `POST /access/token`（form-urlencoded）；清队列 `POST /flowfile-queues/{id}/drop-requests`，
  **删连接前必须先清空**（否则 409 Queue not empty）；改 processor 属性前先 GET 刷新 revision
- 测试**必须走前端页面**，不直接打后端 API；admin/admin 登录、普通用户 zzz/zzz

## 2. 交付架构（2026-09-10 定稿）
- **一套实例 = 一个账号**（忽米 `humi`；每厂各一套）。已否决「一套多租户」。
- `deploy.sh` 用 `SITE_USER/SITE_IS_ADMIN/SITE_PASS` 建号；单账号模式不需要 `add_factory.sh`。
- 愿景（未实施）：控制面集中 + 数据面分布；「删除用户」→ 启用/禁用。

## 3. 数据目录（每用户 4 个根）
| 模式 | 无标签 | 有标签 |
|---|---|---|
| local | `/home/yhz/{user}/nifi-data/` | `/home/yhz/{user}/tagged_nifi_data/` |
| nifi | `/home/yhz/{user}/real_nifi_data/` | `/home/yhz/{user}/tagged_real_nifi_data/` |
- 唯一入口 `main.py::_resolve_user_storage_root`（现硬编码 `IN_DATA_BASE_DIR/username`）
- 私有化 Ceph **未实现**（只记字段，数据仍落本地盘）→ 要隔离只能一厂一套

## 4. 多数据库（`engine_factory.py`，8 种）
mysql/mariadb(PyMySQL)、postgresql(psycopg2-binary)、sqlite(内置)、oracle(oracledb thin)、
hive(pyhive+thrift)、hbase(happybase+thriftpy2)、hdfs(WebHDFS) —— 驱动全在离线 wheel 内；
**mssql/sqlserver 另需 ODBC**（msodbcsql17）。`sasl` 需编译 → 已换 `pure-sasl==0.6.2`。

## 5. deploy-kit（离线部署工具包，纯新增不改项目代码）
`/home/yhz/iot/deploy-kit/`：
- 打包：`make_offline_package.sh`（**白名单**：v1-backend/v1-frontend/docker/real_nifi_conf/deploy-kit；
  排除 `*.run`/`*.sha256`/`.venv`/`data/app.db`/`data/generated`/`__pycache__`）→ `make_selfextract.sh <tgz> <out.run>`
  （**不生成 .sha256，要手工补**；默认输出到 `/home/yhz/` = 工作区上一级，必须显式指定路径，否则出现两个同名 .run）
- 资产：`prepare_offline_assets.sh`（py3.10~3.13 wheel 各一套 + NiFi 镜像 tar ×1.6G）；
  `prepare_sysdeps.sh` + `tools/container_fetch_debs.sh`（sysdeps deb：ubuntu-20.04/22.04/24.04 = 135/108/83 个，419M）；
  `prepare_odbc_assets.sh` + `tools/container_fetch_odbc.sh`（ODBC 闭包：20.04/22.04/24.04 = 35/39/23 个 deb，共 35M）
- 安装：`install_sysdeps.sh`、`install_odbc.sh`、`check_env.sh`（部署闸门）、`deploy.sh`（幂等 12+ 步）、
  `setup.sh`、`set_password.sh`、`verify.sh`/`smoke_test.sh`/`test_e2e.sh`、`uninstall_all.sh`、
  `tools/manifest.sh`（归属台账）、`tools/remove_default_admin.py`、`tools/patch_*.py`（去默认 admin、关自助注册）

## 6. 部署踩坑（全部真机/容器实测换来的）
- 🚨 **打包前必须跑 `deploy-kit/test_e2e.sh`**（一次性容器、断网）：
  `docker run --rm --network none -v /home/yhz/iot:/src:ro -v "$PWD/test_e2e.sh":/e2e.sh:ro ubuntu:22.04 bash /e2e.sh`
  现为 **15 步 / 70 项**（含真跑 `deploy.sh` 全流程 + 真跑 `uninstall_all.sh`）；不覆盖 systemd 与 NiFi 容器（真机已实测 ✓）。
  它抓过的真问题：漏 **python-multipart**（注册路由崩）、缺 **tzdata**（`ZoneInfo` 抛异常→每个请求 500）、
  **worker 没进容器**（新机 `real_nifi_data/bin` 不存在 → `docker cp` 静默失败，必须先 `mkdir -p`）
- ⚠️ 依赖闭包**不要手工列**，递归一次抓全：
  `apt-get download $(apt-cache depends --recurse --no-recommends --no-suggests --no-conflicts --no-breaks --no-replaces --no-enhances <包> 2>/dev/null | grep "^\w" | sort -u)`
- 🔴 **`set -o pipefail` + `grep -q` = 随机变卦（2026-09-16 实测 rc=141）**：`cmd | grep -q PAT` 中 grep 命中即退出，
  上游还在写就吃 SIGPIPE → 整条管道被判失败。`apt-cache`/`docker ps`/`ss` 这类「边算边输出」的必中招
  （`odbcinst -q -d` 只输出 1 行、写完即退，相对安全）。**正确写法**：先收进变量再 `grep -q PAT <<<"$var"`。
  ✅ 2026-09-16 已全仓库扫掉 14 处：`verify.sh`×3、`check_env.sh`×4、`deploy.sh`/`uninstall_all.sh`（ufw）、
  `smoke_test.sh`、`test_e2e.sh`×4、`install_odbc.sh`×3；`grep -rnE '\| *grep +-[a-zA-Z]*q' --include='*.sh'`
  现在只应命中注释 —— **新增代码别再写这种管道**。扫完后 e2e 复跑 15 步全过
- ⚠️ **`LC_ALL` 排序坑**：`for f in *.deb` 在 en_US 下把 `perl` 排到 `perl-base` 前 → perl 配置失败 →
  连累 `mysql-server`。解法：`export LC_ALL=C` + **一次性 `dpkg -i` 全部**（让 dpkg 拓扑排序）；
  两段式兜底（先补缺失 → 配不上再整组重装）
- 🔴 **`set -e` 下的脆弱链路**：任何命令失败会静默中断整个部署 → 「补丁/Nginx/可选步骤」一律 `|| note_warn`，末尾汇总待办
- 🤝 **对「机器上本来就有」的原则**：第一轮绝不动已装包（不升级旧 mysql/docker，apt 加 `--no-upgrade`）；
  Nginx 默认站点停用保留（`default.disabled-by-iot`）；`nifi` 库 IF NOT EXISTS 保数据；
  已有 `${DB_USER}` 会被重置密码（部署时告警）
- 🤖 **能自动判断的绝不留给人工**：`check_env.sh --quiet` 输出 `CHECK_ENV_VERDICT=OK|BLOCKED` + 退出码。
  阻塞：内存/磁盘不足、CPU 架构不符、无 3.10~3.13 Python、端口被**别的程序**占用、缺软件且无包管理器且无离线 deb。
  不阻塞：缺 python3/mysql/docker/nginx（自动装）、离线 deb 缺（切 HTTPS 源）。防火墙自动放行 5174/80/8080；云安全组只能人工
- ⚠️ **Python 是硬门槛**：pydantic 2.11 要求 ≥3.9，离线 wheel 只到 3.10~3.13 → 20.04(3.8) 不能直接装。
  `deploy.sh` 主动挑有 wheel 的解释器（3.13→3.10，优先 `/usr/bin/pythonX.Y`）；前端 service 用 `__PYTHON3__` 占位
- 🔴 **密码必须兜底**：`SITE_PASS="${SITE_PASS:-$ADMIN_PASS}"`；候选口令要含 `SITE_PASS`/`ADMIN_PASS`
  （真因：`IOT_ADMIN_PASSWORD=__SITE_PASS__` 曾导致「admin 密码不是 123456」的误判）
- ⚠️ **MySQL root 登录方式要提前探测**（apt 装的是 auth_socket 免密，别人装的可能有密码）→ `setup.sh` 当场探测
- 🚨 **首次真机失败根因：系统软件走 apt 在线装**（目标机只放行 443，80 被挡）。两层保险：包内离线 deb +
  自动把源 `http://` 换 `https://`（备份 `*.bak.iot`）
- ⚠️ Ubuntu 官方源**无 `docker-compose-plugin`** → 依次试 `docker-compose-v2` → `docker-compose`
- 🚨 **断网 / 无 sudo 机器上的两处真隐患（2026-09-15 修，只有真跑容器才暴露）**：
  · `pip install --upgrade pip -q` 会连 PyPI → 断网机器让部署**在第 6 步直接中断** → 已加 `|| warn`
  · `sudo -u "$APP_USER"` 假设机器装了 sudo → 新增 `as_user()`：**runuser（util-linux 自带）→ sudo → su** 三级回退；
    `systemctl` 在非 systemd 环境不存在 → 已加护栏（跳过 + note_warn，不再静默中断）
- 🆕 **旧版本遗留的弱口令 `admin`（2026-09-16 目标机验收发现并修）**：
  `patch_no_default_admin.py` 只阻止**将来**再建，**不删已建出来的那条** → 被多个旧版本部署过的机器会残留
  `admin`（历史默认密码 `123456`）→ 破坏「一套实例=一个账号」，`verify.sh` 报「用户总数为 2（应为 1）」
  （**别再误判成「补丁没打上」**）。修法：`tools/remove_default_admin.py` —— **MySQL 与 SQLite 两个库都查、都清**
  （⚠️ 实测：`userctl.py list` 查 MySQL 只有 1 条，报「2」的是 API `/internal/users` 读 **SQLite `app.db`**
  → 残留只在 SQLite 里；只查 MySQL 会漏，第一版工具就踩了）+ 两条保护条款（站点账号叫 admin 不动 / 密码非默认只告警）
  + `deploy.sh` **11.1 步**（开关 `REMOVE_DEFAULT_ADMIN`，rc=2 进「需要你关注」）
- ⚠️ `v1-backend/requirements.txt` 严重不全 → 完整清单见 `deploy-kit/templates/requirements-runtime.txt`
- ⚠️ 打包曾泄露 `.codebuddy`（含内部 IP/SSH）→ 已改白名单方案
- ⚠️ 跨 Windows 传输丢 `+x`：`sha256sum -c` 成功但执行报「找不到命令」→ `chmod +x` 即可
- ⚠️ **本执行环境怪癖**：`ls` 带 stdout 重定向、`ls -l`、部分 `docker`/`find` 组合会返回 **255**，
  脚本里若有 `set -e` 会静默退出 → 脚本内用 `printf '%s ' dir/*/` 或管道，只以 stdout 为准
- ⚠️ 「红色退出码」有三种：①故意为 1 的负例 ②环境怪癖 255 ③`Execution Rejected` = 审批被拒
- ✅ `[ cond ] && cmd` 在 `set -e` 下**安全**，无需到处加 `|| true`

## 7. ODBC（SQL Server）
- 包名差异：20.04 = `libodbc1`(+`odbcinst1debian2`)；22.04/24.04 = `libodbc2`+`libodbcinst2`+`odbcinst1debian2`
  +`unixodbc-common`+`libreadline8`。EULA 预填 **`msodbcsql/ACCEPT_EULA`**（写错名字会弹窗）或 `ACCEPT_EULA=Y`；
  验收必须 `odbcinst -q -d` 看到 `[ODBC Driver 17 for SQL Server]`（`odbcinst -j` 会假通过）
- 📦 **离线资产现状（2026-09-16 补齐 20.04/24.04 并实测）**：`offline-assets/deb/odbc/` 下**三个发行版目录齐全**
  · `ubuntu-22.04/` 39 deb / 14M（早期手工抓的、端到端实测过；用新脚本重抓一遍得到 40 个，说明可放心重生成）
  · `ubuntu-24.04/` 23 deb / 8.9M、`ubuntu-20.04/` 35 deb / 12M —— 由 `prepare_odbc_assets.sh` 生成（各含 `ARCH` 标记）
  · ✅ **断网 24.04 实测：离线装完后真连 SQL Server 2019 成功**（`select @@version` 有返回、ldd 无 not found）；
    断网 20.04 同样装成功。开发机自身 ODBC 未受影响（仍 2.3.11-1 + 驱动已注册）
  · `deb/` 根目录仍是 `msodbcsql17_{20.04,22.04,24.04}_amd64.deb` + `libodbc1/odbcinst/unixodbc 2.3.11-1`
    （微软 focal 源通用包，**只对 20.04 成立**、被 22.04 误用会抢 `.so`；同时是 `OUR_L1_VER` 版本旁证的来源，**别删**）
  · 🔑 **微软源里 24.04(noble) 也有 msodbcsql17 17.11.1.1-1** → 24.04 能走原生完整闭包（不必借 jammy 那份驱动）
  · 🧩 抓取三要素（缺一个就*静默*降级成「不含驱动的 Ubuntu 闭包」，看着还挺正常）：
    ① 容器要有 **CA 证书**（基础镜像默认没有；打包脚本挂宿主机的 `ca-certificates.crt`，此时**别**再装 ca-certificates
    —— postinst 写不了那个只读文件会让 apt 返回假失败）② 必须**导入微软源 GPG 公钥**（`[trusted=yes]` 并不能绕过
    InRelease 签名校验）③ 抓依赖必须用 `apt-cache depends --recurse … | apt-get download`，**不能用
    `apt-get install --download-only`**（它会跳过容器里已装好的依赖 → 闭包只剩 6~7 个包 → 目标机缺库）
  · 🧪 驱动 `.so` 真链接但 control **没声明**的库：`libgssapi_krb5.so.2`（已用 `RUNTIME_LIBS=libgssapi-krb5-2 openssl`
    显式带上）。这类缺失 `odbcinst -q -d` 照样"通过"、只有 ldd 或真连才暴露 → 容器脚本里已内置 **ldd 自检**
  · 20.04 上的护栏：微软源不可用时**不回退**抓 Ubuntu focal 的 unixODBC（2.3.6 比包内通用包 2.3.11-1 更旧，
    会变成降级），宁可失败让打包机重试
- 🚨 **「从半成品升级」的连环失败（2026-09-15 真机 202.113.76.54 实测，已闭环）**：早期脚本在 22.04 上走过「通用包」
  回退路径，装下 focal 风格 **`libodbc1 2.3.11-1`**；与 22.04 专属 **`libodbc2 2.3.9`** 抢
  `/usr/lib/x86_64-linux-gnu/libodbc.so.2.0.0` → `dpkg: 正试图覆盖…它同时被包含于 libodbc1` → libodbc2 装不上 →
  `unixodbc`/`msodbcsql17` 一路「尚未配置」→ 看不到驱动。
  **修法（已进包并实机验证）**：现代发行版装闭包前，**台账优先**（`is_ours_pkg libodbc1`）+ **版本兜底**
  （`dpkg-deb -f` 读包内通用包版本，兜底 `2.3.11-1`）双轨判定 → 命中才 `dpkg --purge --force-depends libodbc1`；
  都不命中 = 别人的东西 → 只告警；闭包装完 `_closure_pending` 有残留则 `dpkg -i --force-overwrite` 整组重试；
  **现代发行版上「通用包回退」已禁用**（会抢 .so 装坏 unixODBC）。
  **手修**：`sudo dpkg --purge --force-depends libodbc1 && cd /home/yhz/iot/deploy-kit && sudo bash install_odbc.sh`
- 🔑 **`msodbcsql17` 的 EULA 检查在 `preinst`（解包阶段）就执行** ⇒ **「预接受 EULA」必须排在「装 39 个闭包之前」**；
  放后面会冒出「msodbcsql17 未装好 / 依赖仍未就绪」的误导告警。另：`dpkg --configure -a` 要多跑几轮
  （闭包里 unixodbc 排在 msodbcsql17 之后）。**已前置并实测**
- 🧪 **开发机验证手段（别再答「无法验证」）**：本机有 `ubuntu:22.04`/`ubuntu:24.04` 镜像 → 一次性容器复现，不碰宿主：
  ```bash
  docker run --rm --network none -v /home/yhz/iot/deploy-kit:/kit:ro ubuntu:22.04 bash -c '
    dpkg -i --force-depends /kit/offline-assets/deb/libodbc1_2.3.11-1_amd64.deb >/dev/null 2>&1 || true
    bash /kit/install_odbc.sh; odbcinst -q -d'
  ```
  · 造「旧状态」= 装包内 `libodbc1_2.3.11-1`；造「台账轨道」= 先写 `/var/lib/iot-platform/pkg-new.list` 含 libodbc1；
    造「半成品」= 再 `dpkg -i` 一遍 22.04 闭包 · ⚠️ 容器里 dpkg 说**英文** → 抓 `trying to overwrite`/`also in package`
  · 四个场景（故障复现 / 台账轨道 / 版本旁证 / 半成品修复）全过；但**干净 `ubuntu:22.04` 覆盖不到** libodbc1 冲突场景
  ⚠️ 开发机是 **Ubuntu 20.04.6**（`/etc/os-release`），系统 Python = **3.8.10**（生产跑的是 miniconda 3.13）→
  本机 `MODERN_ODBC=false`，这段清理逻辑**不会也不该**执行。

## 8. 归属台账（用户 2026-09-15 提出，已实现进包）
「在机器上记一份 —— 哪些是本平台装的、哪些是机器原本就有的；以后遇到旧 run 残留就按它处理」。
- `deploy-kit/tools/manifest.sh`（`bash` 调用，不需 +x）：`/var/lib/iot-platform/` 下 `pkg-new.list`（**累加语义**）+
  `files-rendered-by-iot.txt` + `dirs-created-by-iot.txt` + `services-enabled-by-iot.txt` + `containers-by-iot.txt` +
  `users-created-by-iot.txt`（`pre-existing.txt`/`firewall-ports.txt`/`nifi-image.loaded` 是原有的）
- `deploy.sh` 各步调 `record`（一律 `|| true`，台账写不进去绝不影响部署）；`install_odbc.sh` 用台账判定归属
- ✅ `uninstall_all.sh` 已接台账：文件需「台账登记 + 路径在白名单」**双满足**；服务/容器只认 `iot-` 开头；
  目录**只删空的**；白名单 `PREFIX_OK` 是**精确路径表**（不是通配）；默认保留 `/home/yhz` 与用户 `yhz`；
  参数 `--yes/--keep-docker/--keep-mysql/--del-user`。实测 13 用例：平台路径全允许，`/etc/passwd`、别人的家目录、
  `sshd.service`、`/home/yhzabc` 全拒绝
- 🐞 已修的真隐患：`install_sysdeps.sh::finalize_manifest` 原用 `comm -13` **重写** `pkg-new.list`（冲掉 ODBC 记录）→ 改为只累加
- ✅ `deploy.sh` 有**多版本遗留探测**：`iot-nifi` 无 compose 标签（旧版 `docker run` 建的）→ 打印处置办法 + 进「需要你关注」
  （`docker inspect --format '{{if index .Config.Labels "com.docker.compose.project"}}y{{else}}n{{end}}'`）

## 9. 输出规范与交付原则
### 输出规范（2026-09-15 定稿，用户明确要求「不要杂乱」）
- 🎨 **颜色变量必须用 `$'...'` 定义**（真 ESC）：`deploy.sh` 末尾汇总块是 `cat <<EOF`，字面量会打成 `\033[0;32m` 乱码
- 🔇 **第三方原始输出一律落日志**：sysdeps → `/tmp/iot-sysdeps.log`（apt 用 `-qq`）；ODBC → `/tmp/iot-odbc-install.log`；
  失败时只回显「关键几行」（`_tail_key_lines`、`_closure_pending`）
- 🔢 步骤编号统一 `x/14`（0/14 体检 → 14/14 ODBC），别出现 `13/12`
- 🧾 不要把工具原始 JSON 打到屏幕（`log "  $USERCTL_OUT"` 曾让运维以为报错）→ 解析成中文
- ⚠️ **同一文件的多处改动必须串行 `replace_in_file`**（并行批次里后写的会覆盖先写的，本轮真踩过）
### 交付与重跑（2026-09-13 定稿，09-15 强化为严格幂等）
- **只传 3 个文件**：`iot-install.run` + `.sha256` + `部署文档.txt`，统一放 `/home/yhz/iot/`；
  其余脚本都在 `.run` 内（解压到 `/home/yhz/iot/deploy-kit/`），**不单独传**
- 🥇 部署有不足 / 想改配置 → **直接重跑**（重跑优先于卸载）
- 🔁 **重跑 = 严格幂等**：有 `config.env` 就不再问（`--reconfig` 才问）；密码 `--keep-password`；
  NiFi 镜像按 `nifi-image.loaded` 指纹跳过 `docker load`；已装软件/ODBC(早退)/补丁(自检 MARK)/`init.sql` 都不重复。
  **worker 每次 cp 进容器是故意的**（4 个小文件，能修上次残缺）
- 📄 技术文档只有 `deploy-kit/部署文档.txt`，顶层 `/home/yhz/iot/部署文档.txt` 是传输副本，**改完必须同步**；
  文档里**不要硬编码 `.run` 的 sha256**（重打包必然过期）
- 🧩 **`.run` = 头部脚本（69 行，`__IOT_ARCHIVE_BELOW__`）+ tgz 原样拼接**（`tail -n +70 | cmp` 可验）。
  **交付前四步审计**：① `head -n 69 iot-install.run | bash -n` + `awk '/^__IOT_ARCHIVE_BELOW__$/{print NR}'` 应为 69
  ② `stat -c '%n %s'` 三文件 + `sha256sum -c` ③ `tail -n +70 | tar xzOf - <关键文件> | grep -aoE "<本轮特征串>"` 确认包内是新版
  ④ `cmp` 与 tgz 逐字节一致（tgz 删掉后此项不可复做）
  **改包内任何文件都必须重新 `tar czf` 全量重压**（gzip 是跨全流的 DEFLATE 压缩流，改任意字节都要从该处起重压；
  ❌ 不要走「追加第二个 gzip 成员」的取巧路子）。重压约 3 分钟（成本是 gzip CPU，镜像 tar 原样流过）。
  💡 唯一绕开办法：**不重打包，直接把单个小文件覆盖到目标机同一路径**（如 11KB 的 `install_odbc.sh`）
- 🧰 命令审批经验：破坏性命令（`rm`、`chmod`、写宿主机 `/tmp`、覆盖已有交付物）会被拦 → **删文件用 `delete_file` 工具**；
  `docker run` 自 2026-09-15 起已可放行，**别再假设它一定被拦**
- 🆕 **当前交付物（2026-09-17 第十二次打包）**：`.run` = 1,778,671,256 字节、`.sha256` 82 字节、
  `部署文档.txt` = 34,021 字节（sha256 以包旁的 `iot-install.run.sha256` 为准，别手抄）。
  v12 = v11 + **`as_user` 死递归修复 + 收拢成唯一定义**（🆕 `tools/lib.sh`：runuser→sudo→su，EUID 判定；
  以前 4 个脚本各抄一份 → `deploy.sh` 那份的 sudo 分支误写成 `as_user "$@"` 自调用，无 runuser 的机器会**死递归**；
  `verify.sh`/`set_password.sh`/`add_factory.sh` 写死 `sudo -u` → 没装 sudo 的机器失败；e2e 已加静态断言防复发）
  + **改密码三处同步**（`tools/sync_site_password.py`：`config.env(SITE_PASS)` + `.deploy-secrets(ADMIN_PASS)`
  + 已渲染的 `override.conf(IOT_ADMIN_PASSWORD)`）+ pip 升级不再刷屏。
  **e2e 79 项断言全过**；v9 已在目标机实跑全绿（verify PASS=10 FAIL=0、用户总数 1）。
  审计四步全过；⚠️ 包内成员前缀是 `iot/`（`iot/deploy-kit/...`），`tar xzOf - <成员>` 要带前缀。
  📝 四条教训：① **HTML 注释型补丁必须断言"注释闭合后行内无残文"**（只断言"打了 MARK"拦不住半截注释）；
  ② **改了某处状态就要检查还有谁依赖它的旧值**（改库密码不同步自检文件 → `verify.sh` 假报「登录失败」）；
  ③ **同一份逻辑在多个脚本里各抄一遍必然漂移**（`as_user` 据此炸出死递归）→ 公共逻辑一律进 `tools/lib.sh`；
  ④ **新写的 e2e 断言要连夹具一起想**（12.6 忘了容器里运行用户还不存在 → 假失败）
  ✅ **2026-09-17 v12 已在目标机（202.113.76.54 / 22.04）重跑全绿**：`verify.sh` PASS=10 FAIL=0、
  `smoke_test.sh` PASS=10 FAIL=0 WARN=1（仅自动打标 422 可选项）、第 6 步再无 pypi 重试刷屏、
  `set_password.sh` 真机打出「override.conf 已刷新 + 已同步密码到 config.env/.deploy-secrets」→
  「改密码导致自检登录失败」这条链**彻底闭环**。⚠️ 用户当时把密码设成了字面量 `humi`（账号=密码，且是公网机器）
  → 已提示改强密码：`sudo bash deploy-kit/set_password.sh humi '<强密码>'`（v12 会自动同步，不会再不一致）

## 10. 用户偏好 / 约定
- 🎯 **2026-09-15 用户定调：「以当前这份 run 为准」** —— 目标机上凡与本包声明不符的（典型是旧 run 残留），
  **直接打破、按新 run 重建**，不迁就旧状态。
  · 已符合：平台代码/脚本、代码补丁（代码被覆盖后 MARK 消失 → 重打）、systemd/nginx/compose 配置、
    NiFi 镜像（指纹变了重 load）、worker（每次 cp）、ODBC 的 focal 残留 libodbc1、被闭包降级的 unixodbc
  · **边界（同样重要）**：只打破「本平台自己可能留下的、且与本包声明不符的」；**别人的软件与所有数据一律不动**
    —— MySQL 库/表/数据（IF NOT EXISTS）、现场已发的账号密码（不重置）、别人的 mysql/docker/nginx 站点、防火墙原有规则
  · 未实现角落：22.04+ 上 `libodbc1` **版本与包内不一致**时当前只告警不自动清（按原则应一律清掉，但可能弄坏第三方
    focal 风格程序 → 待用户拍板）
- 离线交付：**所有依赖提前备好，部署时零下载零编译**，步骤尽量少
- 用户强调「**一次装不成功 = 失败**」「**有不足就重跑，重跑优先于卸载**」「能自动判断的不要让人判断」
- 每次改完脚本**必须重打包**才会带上；用户会问「要 A（重打）还是 B（只手修）」→ 按他的选择执行
- 远程访问：tailscale `http://100.121.225.98:5174`
- `.codebuddy/memory/` 已按要求提交 GitHub（含内部 IP，用户明确同意）；远程别名 `github-yhz`

## 11. AI 训练网站（solo_ai_iot，独立子系统）
- 后端 :8002（conda env `iot_clone`，ludwig+optuna+pycaret，GPU RTX A5000）+ 前端 :3002（vite）
- 训练目录只认 `{user}/nifi-data` / `real_nifi_data`；预测只认 `tagged_*`；需 `.meta.json` 配套
- 10 模型：xgboost/lightgbm/catboost/hgb/TabNet/TabTransformer/Deep-MLP/CNN/LSTM/GRU
- 重启：`kill <pid>; cd banckend && nohup /home/yhz/miniconda3/envs/iot_clone/bin/python main.py > logs.log 2>&1 &`
