# V1 Backend

## Run

```bash
cd v1-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
APP_EXECUTOR_MODE=mock uvicorn app.main:app --reload --port 8081
```

### 运行不了时的推荐方案（本地 Python 3.8）

当前仓库默认 `requirements.txt` 是较新的锁定版本；如果你本机是 Python 3.8 且镜像源缺少对应包版本，建议使用兼容清单：

```bash
cd v1-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-py38.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8081
```

如果本机 Python >= 3.9，优先使用 `requirements.txt`。

Executor mode:

- `APP_EXECUTOR_MODE=mock` (default): local simulation mode.
- `APP_EXECUTOR_MODE=nifi`: NiFi adapter scaffold mode (Day2 placeholder).

NiFi mode environment variables:

- `NIFI_BASE_URL` (default: `https://localhost:8443`)
- `NIFI_USERNAME` (default: `admin`)
- `NIFI_PASSWORD` (default: `YourStrongPassword123`)
- `NIFI_VERIFY_SSL` (`true|false`, default: `false`)
- `NIFI_TIMEOUT` (seconds, default: `15`)
- `NIFI_RETRY_COUNT` (default: `2`)
- `NIFI_RETRY_DELAY_MS` (default: `800`)
- `NIFI_STATUS_POLL_STEPS` (default: `3`)
- `NIFI_STATUS_POLL_INTERVAL_MS` (default: `1000`)
- `NIFI_OUTPUT_DIR` (default: `/home/yhz/nifi-data/output_csv`)

## Quick check

```bash
curl http://127.0.0.1:8081/health
```

## Large upload note

`/api/v1/upload/inbox_csv` 和相关上传接口在后端本身可以处理 3MB+ 文件；如果浏览器里上传 4MB 左右的 CSV 时报 `TypeError: NetworkError when attempting to fetch resource`，通常不是解析代码问题，而是前面的 nginx / 网关没有放开请求体大小或代理超时。

补充说明：当前这台机器实际运行的是 `docker-nginx-1`，其生效配置里已经是 `client_max_body_size 100M`，`proxy_read_timeout 3600s`，`proxy_send_timeout 3600s`。如果你仍然遇到这个错误，问题更可能在前端访问地址、API_BASE 配置，或别的代理/入口层，而不是这份后端上传代码本身。

可直接在部署机执行：

```bash
bash ../scripts/apply_nginx_upload_limits.sh
```

脚本会设置 `client_max_body_size 50M`、`proxy_read_timeout 300s` 和 `proxy_send_timeout 300s`。

## Implemented APIs

- GET /api/v1/system/executor
- POST /api/v1/jobs
- GET /api/v1/jobs
- GET /api/v1/jobs/{jobId}
- POST /api/v1/jobs/{jobId}/cancel
- GET /api/v1/files
- GET /api/v1/jobs/{jobId}/outputs
- GET /api/v1/files/{fileId}/download
- GET /api/v1/files/{fileId}/preview
- POST /api/v1/tags/manual
- POST /api/v1/tags/auto
- GET /api/v1/tags/rules
- POST /api/v1/schedules
- GET /api/v1/schedules
- PATCH /api/v1/schedules/{scheduleId}
- DELETE /api/v1/schedules/{scheduleId}

## Day2 Notes

- File preview supports CSV/JSON/TSV with offset/limit pagination.
- Tag manual API now validates records(rowId, label).
- NiFi flow mapping template is available at `app/nifi_flow_mapping.template.json`.

## Day3 Notes

- Real NiFi REST client is implemented in `app/nifi_client.py`.
- In `nifi` mode, if flow mapping is not filled, jobs fail with `NIFI_FLOW_UNMAPPED`.
- In `nifi` mode, execution errors are classified to `NIFI_AUTH_ERROR`, `NIFI_NETWORK_ERROR`, `NIFI_FLOW_NOT_FOUND`, or fallback `NIFI_EXEC_ERROR`.
- Fill `app/nifi_flow_mapping.template.json` with real process-group IDs before real run.

## Batch smoke script (nifi mode)

Run after backend is started in nifi mode:

```bash
cd v1-backend
python scripts/nifi_batch_smoke.py --base-url http://127.0.0.1:8082/api/v1 --rounds 2 --json-out ./data/generated/day3-batch-report.json
```

This script creates multiple job types and checks final status for each round.
