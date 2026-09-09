# 长期记忆

## 🚨 关键运维规则（绝对不要违反）

### 前端启动
- **前端用 `python3 v1-frontend/serve.py` 启动，监听 5174 端口**
- serve.py 内置 `/api/v1/` → 后端 8081 的代理转发
- **严禁用 `python3 -m http.server` 替代**：无代理功能，远程浏览器无法访问后端 API
- 前端代理链路：浏览器 → 5174（serve.py） → `/api/v1/*` 转发到 8081（uvicorn）

### 前后端重启方式
```
前端：pkill -f "serve.py"; cd v1-frontend && nohup python3 serve.py > /tmp/frontend.log 2>&1 &
后端：kill $(pgrep -f uvicorn | head -1); sleep 10  # systemd 自动拉起
```
- kill 后端 uvicorn 后 systemd 会在 ~10 秒内自动重启，无需手动 nohup
- 后端环境变量从 `v1-backend/deploy/iot-backend.service.override.conf` 读取
- 每次修改 main.py 后必须重启后端才能生效（uvicorn 无热重载）

### NiFi 容器
- 容器名：iot-nifi，镜像：iot-nifi-python:latest（基于 apache/nifi:2.8.0）
- 挂载：`/home/yhz/real_nifi_data` → `/opt/nifi/nifi-current/data/iot`
- 额外挂载：`/home/yhz:/home/yhz:rw`（让容器能访问所有用户目录）
- **重启容器后 flow 会自动恢复**（FlowFile Repository 持久化）
- Worker 脚本在容器内 `/opt/nifi/nifi-current/data/iot/bin/`
- 更新 worker 后必须 `docker cp` 到容器：`docker cp <src> iot-nifi:<dst>`

### 端口说明
| 端口 | 服务 | 启动方式 |
|------|------|----------|
| 5174 | 主站前端（serve.py，含 API 代理） | `python3 serve.py` |
| 8081 | 主站后端 FastAPI | systemd（iot-backend） |
| 8080 | NiFi HTTPS | docker compose |
| 3002 | AI 训练网站前端（Vue vite） | `npm run dev`（solo_ai_iot/frontend） |
| 8002 | AI 训练网站后端 FastAPI | `conda env iot_clone` + `python main.py`（solo_ai_iot/banckend） |

## AI 训练网站（solo_ai_iot）要点
- 后端 8002 用 conda env `iot_clone`（含 ludwig+optuna+pycaret），GPU RTX A5000
- 训练数据文件夹只认 `{user}/nifi-data` 或 `real_nifi_data`；预测文件夹只认 `tagged_nifi_data` 或 `tagged_real_nifi_data`
- 训练文件需配套 `.meta.json`（tagColumn/datesetname/failedTag）；预测文件 meta 的 datesetname 须与已部署模型精确匹配
- 前端训练默认传 `label_column=auto_tag` 占位符，pipeline 须回退到配置 tagColumn（2026-08-08 修复）
- 预测接口 `/api/predict/batch_file` 按 dataset_name 匹配所有已部署模型（多模型诊断）
- 部署：训练完成后 leaderboard「一键部署至 IoT」，每模型一行需逐一部署
- 10 个模型：xgboost/lightgbm/catboost/hgb/TabNet/TabTransformer/Deep-MLP/CNN/LSTM/GRU
- 重启后端：`kill <pid>; cd banckend && nohup /home/yhz/miniconda3/envs/iot_clone/bin/python main.py > logs.log 2>&1 &`

## Playwright 前端自动化要点（AI 网站）
- snapshot 保存到**当前工作目录**（非 .playwright-cli），找最近 yml 读取
- Element Plus 下拉/checkbox/tab：eval `.click()` 不触发 Vue，必须 playwright `click` 点击 generic [cursor=pointer] 容器或 label ref
- eval 返回 stdout 含 JS 源码，判断用 `'"clicked"' in str(r)` 而非 `"disabled" in`
- 预测 tab 内下拉是第 1 个 select（训练 tab 隐藏 select 仍在 DOM），文件下拉是第 2 个

### 远程访问
- 用户可通过 tailscale SSH + 浏览器访问 `http://100.121.225.98:5174`
- serve.py 代理确保远程浏览器也能访问后端 API
- 如果改用纯 HTTP server，远程会因无代理导致 API 404

## 项目核心信息
- IoT 智慧平台 AI 模块，后端 FastAPI (8081端口)，前端 serve.py (5174端口)
- 后端通过 systemd service `iot-backend` 运行
- 认证源：MySQL nifi 数据库，独立于 backend mode 切换
- 双模部署：Local 模式（即时处理）和 NiFi 模式（异步提交给 NiFi 容器）

## NiFi 2.x API 要点
- 登录：POST /access/token，Content-Type: application/x-www-form-urlencoded
- 清空 queue：POST /flowfile-queues/{connectionId}/drop-requests（返回 202 Accepted）
- 删除前必须先清空 queue，否则 409 "Queue not empty"
- processor 创建后 revision 会变，更新属性前必须 GET 刷新 revision

## 数据目录映射（用户根目录下 4 个数据根）
- Local 无标签：`/home/yhz/{username}/nifi-data/`
- NiFi 无标签：`/home/yhz/{username}/real_nifi_data/`
- Local 有标签：`/home/yhz/{username}/tagged_nifi_data/`（独立顶层，2026-08-07 起）
- NiFi 有标签：`/home/yhz/{username}/tagged_real_nifi_data/`（独立顶层）
- 每个根下有 inbox_csv/inbox_json/inbox_tsv/csv_to_json/.../output_csv/ 等 12 子目录
- hasTag=true 时源文件+转换+导出全部入 tagged_*_data 顶层目录（不再用 tagged_output 子目录）
- 全局 NiFi 标签输出：`/home/yhz/tagged_real_nifi_data/`（worker 写容器内 /home/yhz/tagged_real_nifi_data）
- 全局 TAGGED_OUTPUT_DIR 默认值：`/home/yhz/tagged_nifi_data`
- exports/ 是 run_export_job 的临时目录，export_generic 移动文件到 output_csv/ 后应清理

## 关键函数
- `_get_user_upload_dirs(username, tagged=False)` — 获取上传/转换目录，tagged 模式根于 tagged_output
- `_parse_bool_query(value)` — 安全解析 Query 布尔值，避免 bool("false")=True
- `_host_to_container_path(host_path)` — 宿主机路径→容器内路径（NiFi worker 用）
- `_wait_nifi_task_done(...)` — 等待 NiFi 任务完成，同时搜索全局和用户 done 目录
- `_route_nifi_convert_output_to_user(...)` — 将 NiFi worker 产物从全局目录路由到用户目录

## 用户偏好
- 所有测试必须通过前端，不能直接打后端 API 验证功能可用性
- 测试用 admin/admin 登录，普通用户用 zzz/zzz
- Playwright 自动化测试用 JWT Cookie 注入登录
- 用户通过 tailscale 远程访问网站，不是本地 localhost

## Git 约定
- **`.codebuddy/` 目录严禁提交到 GitHub**：含远程 IP、SSH、内部记忆等敏感信息（2026-09-09 提交时确认排除）
- 提交前用 `git add -A && git reset HEAD .codebuddy` 暂存全部并排除该目录
- 远程主机别名 `github-yhz`（`git@github-yhz:TLMOA/AI.git`），main 分支直接 push 即可（fast-forward）
- V1/V2 历史文档已归档至 `docs-archive/`，real_nifi_conf/archive 历史 flow 快照(.gz)已清理出库
