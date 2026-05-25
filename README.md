# IoT AI 模块 V1

本仓库是 IoT 智慧平台 AI 模块 V1 的本机部署与联调工程，当前以 **systemd 常驻服务** 为主要运行方式，不再建议日常手动执行 `uvicorn` 或前端 `serve.py`。

当前可用能力包括：数据导出、上传转换、文件预览编辑、打标、任务流、NiFi 对接与运行日志观测。

**项目优先级：`智慧平台模块详细开发方案.pdf` 为本仓库最高优先级方案。**

## 当前运行方式

日常只需要管理 systemd 服务：

```bash
sudo systemctl restart iot-backend.service
sudo systemctl restart iot-frontend.service
```

常用状态检查：

```bash
systemctl status iot-backend.service --no-pager --full
systemctl status iot-frontend.service --no-pager --full
systemctl status iot-backend-health.timer --no-pager --full
```

查看日志：

```bash
journalctl -u iot-backend.service -f
journalctl -u iot-frontend.service -f
journalctl -u iot-backend-health.service -n 100 --no-pager
```

健康检查：

```bash
curl http://127.0.0.1:8081/health
curl http://127.0.0.1:5174/api/v1/health/databases
```

## 访问地址

- 本机前端：`http://127.0.0.1:5174`
- 局域网前端：`http://202.113.76.55:5174`
- 后端本机地址：`http://127.0.0.1:8081`
- 后端健康检查：`http://127.0.0.1:8081/health`

前端配置位于 `v1-frontend/config.js`，当前默认通过相对路径 `api/v1` 访问后端，适合由前端服务或反向代理统一转发 API 请求。

## 目录说明

- `v1-backend/`：FastAPI 后端，主入口为 `app.main:app`
- `v1-backend/deploy/`：后端 systemd 单元、健康检查脚本与覆盖配置
- `v1-frontend/`：前端静态页面与本地静态服务
- `docker/nifi/`：NiFi Docker 与 `iot-nifi.service` 相关文件
- `docker/hadoop/`：Hadoop 最小生态 Docker 编排文件
- `v2-nifi/`：NiFi 方案、示例、运行手册、K8s/Registry 相关材料
- `V1执行清单/`：V1 需求、验收与执行记录
- `V2执行清单/`：V1 阶段 NiFi 统一实施方案与执行清单
- `test-data/`：测试样例文件
- `scripts/`：本机部署、nginx 上传限制、SSH 隧道等辅助脚本

## 服务清单

### `iot-backend.service`

后端 FastAPI 服务。当前服务文件来源：`v1-backend/deploy/iot-backend.service`。

关键配置：

- 运行用户：`yhz`
- 工作目录：`/home/yhz/iot/v1-backend`
- 启动命令：`/home/yhz/iot/v1-backend/.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8081`
- 失败自动重启：`Restart=always`
- 监听地址：`127.0.0.1:8081`

常用操作：

```bash
sudo systemctl restart iot-backend.service
sudo systemctl stop iot-backend.service
sudo systemctl start iot-backend.service
systemctl status iot-backend.service --no-pager --full
journalctl -u iot-backend.service -f
```

### `iot-frontend.service`

前端静态服务，提供页面访问，并负责或配合反向代理把 `/api/v1` 请求转发到后端。

常用操作：

```bash
sudo systemctl restart iot-frontend.service
sudo systemctl stop iot-frontend.service
sudo systemctl start iot-frontend.service
systemctl status iot-frontend.service --no-pager --full
journalctl -u iot-frontend.service -f
```

### `iot-backend-health.timer`

后端健康检查定时器，配合 `iot-backend-health.service` 和 `/usr/local/bin/iot-backend-health.sh` 使用，用于断电恢复或后端异常后的自动检查。

常用操作：

```bash
systemctl status iot-backend-health.timer --no-pager --full
systemctl list-timers | grep iot-backend-health
journalctl -u iot-backend-health.service -n 100 --no-pager
```

### `api-worker.service`

CursorPool 依赖进程。该服务通过 `start-api-worker.sh` 在 `/home/yhz/iot` 下启动 `api-worker`，日志写入 `api-worker-npx.log`。

常用操作：

```bash
systemctl status api-worker.service --no-pager --full
pgrep -af 'api-worker|cursorpool'
sudo systemctl start api-worker.service
sudo systemctl enable api-worker.service
```

### NiFi / Hadoop

NiFi 和 Hadoop 当前按独立基础设施管理：

- NiFi Docker 编排：`docker/nifi/docker-compose.yml`
- NiFi systemd 相关：`docker/nifi/iot-nifi.service`
- Hadoop Docker 编排：`docker/hadoop/docker-compose.yml`

默认实施口径：NiFi 容器与 Flow 先由运维或脚本预置；后端只负责对接、启停既有 Flow、提交任务和同步结果。自动创建容器或自动部署 Flow 只作为显式开关能力，不作为默认行为。

NiFi V1 主文档：

- `V2执行清单/V1-NiFi统一实施总方案.md`
- `v2-nifi/V1-NiFi统一实施总方案.md`

## 首次安装或重新安装 systemd 服务

如果是新机器、服务文件丢失，或需要把仓库里的部署文件重新安装到系统目录，执行：

```bash
bash scripts/install_autostart_services.sh
```

脚本会安装并启用：

- `iot-backend.service`
- `iot-frontend.service`
- `iot-backend-health.timer`

安装完成后检查：

```bash
systemctl status iot-backend.service iot-frontend.service iot-backend-health.timer --no-pager --full
```

## 数据库服务一键启动

如果你需要把本机数据库测试相关的服务一次性拉起并完成连通性校验，直接执行：

```bash
sudo bash /home/yhz/iot/scripts/start_db_services.sh
```

这个脚本当前会处理以下内容：

- 修正 SQLite 备份库权限，确保后端能打开文件型数据库
- 启动 Hadoop / Hive / HBase 相关 Docker 编排
- 启动 PostgreSQL、SQL Server、Oracle 等独立数据库容器
- 重启 `iot-backend.service` 和 `iot-frontend.service`
- 调用后端接口做 SQLite、PostgreSQL、SQL Server、Oracle、Hive、HBase、HDFS 的测试连接

说明：NiFi 已经改为单独自启动管理，不再由这个脚本处理。

如果修改了 systemd 单元文件，记得重新加载：

```bash
sudo systemctl daemon-reload
sudo systemctl restart iot-backend.service
sudo systemctl restart iot-frontend.service
```

## 后端依赖与虚拟环境

当前 systemd 后端服务使用 `v1-backend/.venv`：

```bash
cd /home/yhz/iot/v1-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

如果运行环境是 Python 3.8，可使用：

```bash
pip install -r requirements-py38.txt
```

依赖更新后重启后端：

```bash
sudo systemctl restart iot-backend.service
```

## 本地开发临时启动方式

日常部署请优先使用 systemd。只有在调试代码时，才建议临时手动启动。

后端调试：

```bash
cd /home/yhz/iot/v1-backend
source .venv/bin/activate
python -m uvicorn app.main:app --host 127.0.0.1 --port 8081 --reload
```

前端调试：

```bash
cd /home/yhz/iot/v1-frontend
python serve.py
```

如果端口已被 systemd 服务占用，请先停止对应服务：

```bash
sudo systemctl stop iot-backend.service
sudo systemctl stop iot-frontend.service
```

调试结束后恢复：

```bash
sudo systemctl start iot-backend.service
sudo systemctl start iot-frontend.service
```

## 常见维护操作

### 上传大文件失败

如果前端通过 nginx 或其他反向代理访问后端，上传较大 CSV 时出现 `TypeError: NetworkError when attempting to fetch resource`，可放开上传限制：

```bash
bash scripts/apply_nginx_upload_limits.sh
```

该脚本会把 nginx 上传体积限制调大到 50M，并延长代理超时，适合本项目 CSV/JSON 上传场景。

### 服务启动失败

按以下顺序排查：

```bash
systemctl status iot-backend.service --no-pager --full
journalctl -u iot-backend.service -n 200 --no-pager
curl http://127.0.0.1:8081/health
```

如果修改过服务文件：

```bash
sudo systemctl daemon-reload
sudo systemctl restart iot-backend.service
```

如果怀疑 Python 依赖缺失：

```bash
cd /home/yhz/iot/v1-backend
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart iot-backend.service
```

### 前端 API 不通

先检查前端配置和后端健康：

```bash
cat /home/yhz/iot/v1-frontend/config.js
curl http://127.0.0.1:8081/health
curl http://127.0.0.1:5174/api/v1/health/databases
```

再查看前端服务日志：

```bash
systemctl status iot-frontend.service --no-pager --full
journalctl -u iot-frontend.service -n 200 --no-pager
```

## 日常推荐命令速查

```bash
# 重启服务
sudo systemctl restart iot-backend.service
sudo systemctl restart iot-frontend.service

# 查看状态
systemctl status iot-backend.service --no-pager --full
systemctl status iot-frontend.service --no-pager --full

# 看日志
journalctl -u iot-backend.service -f
journalctl -u iot-frontend.service -f

# 健康检查
curl http://127.0.0.1:8081/health
curl http://127.0.0.1:5174/api/v1/health/databases
```
