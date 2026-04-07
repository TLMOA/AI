# AI 模块 V1（IoT）

本仓库用于 AI 模块 V1 的后端 + 前端联调与验收。
当前已形成可演示闭环：导出、上传转换、打标、文件预览编辑、可观测日志。

## 目录说明

- `v1-backend/`：FastAPI 后端
- `v1-frontend/`：前端页面（静态）
- `V1执行清单/`：需求、验收、执行记录
- `test-data/`：测试样例文件（含有表头/无表头 TSV）

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
python -m http.server 5174
```

打开：`http://127.0.0.1:5174`

## 当前验收状态

- 导出链路：CSV / JSON / TSV 通过，where 分流可区分记录数
- 上传转换链路：6 条转换通过
- 打标链路：manual-table 通过，产物落 `tagged_output`
- 可观测：`traceId + STARTED/SUCCEEDED/FAILED + durationMs` 可检索
- 前端上传结果：已支持三行展示（成功信息、上传路径、转换路径）

详见：`V1执行清单/需求规格-NiFi-文件输出.md`

## 常用命令

```bash
# 查看后端健康
curl http://127.0.0.1:8081/health

# 查看 git 近期提交
git log --oneline -n 5
```

## GitHub 远程

- 远程仓库：`https://github.com/TLMOA/AI.git`
- 默认分支：`main`
