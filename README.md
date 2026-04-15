# AI 模块 V1（IoT）

本仓库用于 AI 模块 V1 的后端 + 前端联调与验收。
当前已形成可演示闭环：导出、上传转换、打标、文件预览编辑、可观测日志。

**项目优先级：`智慧平台模块详细开发方案.pdf` 为本仓库最高优先级方案。**

## 目录说明

- `v1-backend/`：FastAPI 后端
- `v1-frontend/`：前端页面（静态）
- `V1执行清单/`：需求、验收、执行记录
- `V1执行清单nifi/`：V1 阶段 NiFi 统一实施文档与执行清单
- `test-data/`：测试样例文件（含有表头/无表头 TSV）

## NiFi V1 主文档

- `V1执行清单nifi/V1-NiFi统一实施总方案.md`

## 快速启动

### 1) 启动后端

```bash
cd v1-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8081
```

Python 3.8 环境可用：

```bash
pip install -r requirements-py38.txt
```

### 2) 启动前端

```bash
cd v1-frontend
python serve.py
```

打开：`http://127.0.0.1:5174`

同一局域网其他机器可访问：`http://202.113.76.55:5174`

如果前端是通过 nginx 或其他反向代理访问后端，且上传较大 CSV 时出现 `TypeError: NetworkError when attempting to fetch resource`，请先放开上传限制：

```bash
bash scripts/apply_nginx_upload_limits.sh
```

该脚本会把 nginx 上传体积限制调大到 50M，并延长代理超时，适合本项目的 CSV/JSON 上传场景。

## 常用命令

```bash
curl http://127.0.0.1:8081/health
git log --oneline -n 5
```

## 断电后自动恢复（开机自启）

若希望电脑断电/重启后，前后端服务自动恢复：

```bash
bash scripts/install_autostart_services.sh
```

安装后会启用以下 systemd 单元：

- `iot-backend.service`（后端 FastAPI）
- `iot-frontend.service`（前端静态服务 + `/api/v1` 代理）
- `iot-backend-health.timer`（后端健康检查定时器）

可用以下命令查看状态：

```bash
systemctl status iot-backend.service
systemctl status iot-frontend.service
systemctl status iot-backend-health.timer
```
