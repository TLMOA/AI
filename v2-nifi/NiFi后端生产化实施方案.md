# DEPRECATED: 本文件已被合并，请参考 `v2-nifi/NiFi_统一实施方案.md`

# NiFi 后端生产化实施方案（V2）

版本：1.0
作者：xy（由 Copilot 协助生成）
日期：2026-05-07

## 1 目标与范围
- 目标：把当前 `v1-backend` 的 NiFi 整合能力，升级为可生产、可观测、可运维、可回滚的 NiFi 后端子系统。
- 范围：受控 NiFi 镜像、NiFi Registry、后端 `nifi_orchestrator` 的 K8s 与 Docker 适配、CI/CD（Flow + 镜像 + manifest）、监控/告警、备份/恢复、Runbooks 与演练。

## 2 核心设计原则
- 最小侵入：后端负责 Flow 管理与任务编排，不直接承担集群调度（若在 K8s 下由 K8s 负责）。
- 可重复/幂等：部署、Flow 导入、任务提交需支持重试与幂等。
- 可观测：采集指标、日志与审计链路（traceId/任务ID）。
- 安全优先：所有凭据使用 Secrets 管理（Vault 或 K8s Secrets）；启用 TLS。 

## 3 架构概览
- 部署平台：优先 Kubernetes（推荐 Helm + GitOps），无法使用时退回到 docker-compose/systemd 模式。
- 核心组件：
  - NiFi（StatefulSet，若需要 HA 则启用集群模式）
  - NiFi Registry（用于 Flow 版本化）
  - 私有镜像仓库（存放受控 NiFi 镜像）
  - 后端服务（现有 `v1-backend` 扩展为 K8s-aware orchestrator）
  - 对象存储（备份 provenance/flow 与 content）
  - Secrets 管理（Vault 或 K8s Secrets）
  - 监控：Prometheus + JMX exporter；日志：Loki 或 ELK

## 4 部署前提与宿主机目录约定
- 主机目录（实验/本地模式）: `/home/yhz/iot/real_nifi_data`（已存在）
- 镜像需包含：python3、pymysql、worker 脚本、必要的 JDBC 驱动。JDBC Jar 可放 `/opt/nifi/nifi-current/lib` 或以 PVC 挂载注入。
- 后端配置统一环境变量：`NIFI_API_BASE`、`NIFI_REGISTRY_URL`、`NIFI_MODE`（k8s/docker/local）等，存放在 `.env` 或 Secrets 中。

## 5 详细实施步骤（建议 6 周路线）

周 0：规划与准备
- 确定目标平台（K8s 或 仅 docker）。
- 准备私有 registry、对象存储（S3/MinIO）、证书策略、测试命名空间。

周 1：受控 NiFi 镜像与单节点环境
- 基于 `docker/nifi/Dockerfile` 构建镜像（包含 python+pymysql 与 worker）。
- 在实验环境运行单节点 NiFi 与 NiFi Registry。
- 验证 ExecuteStreamCommand worker 能正确执行 `v1-backend/scripts/nifi_mysql_export_worker.py` 并写 `done/error` 文件。

示例：构建并运行镜像（宿主机实验）
```bash
# 构建镜像
cd docker/nifi
./build.sh  # 或: docker build -t iot-nifi-python:latest .

# 运行单节点 NiFi（挂载 real_nifi_data）
docker run -d --name iot-nifi \
  -p 8080:8080 \
  -v /home/yhz/iot/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  -v /home/yhz/iot/real_nifi_data/lib:/opt/nifi/nifi-current/lib \
  iot-nifi-python:latest

# 触发后端 ensure 接口
curl -X POST http://localhost:8000/api/v1/internal/nifi/ensure
``` 

周 2：后端改造（orchestrator）
- 将 `v1-backend/app/nifi_orchestrator.py` 扩展为支持 `NIFI_MODE`：`k8s` 与 `docker`。
- 增加幂等锁（基于 DB）与任务退避/重试策略。
- 统一配置名：`NIFI_API_BASE` / `NIFI_REGISTRY_URL`。

周 3：Flow 版本化与 CI
- 将关键 Flow 导入 NiFi Registry，标注版本（`iot_mysql_export_flow_v1`）。
- CI：当 Flow 有变更时自动构建 Registry bundle、在 staging 导入并 run contract-tests。

周 4：监控与日志
- 部署 Prometheus + JMX exporter（NiFi JMX）。
- 日志集中（Loki/ELK），准备 Grafana 面板与告警规则（NiFi down、Flow error rate、export_jobs/error 增长）。

周 5：E2E 验证与演练
- 完整 E2E：提交导出任务 -> NiFi 执行 -> worker 输出 -> 后端扫描并注册结果。
- 并发压力测试与回滚演练（切换到 Local 模式并验证一致性）。

周 6：上生产前准备与交付
- 编写 Runbooks：启动/停止/回滚/常见故障排查清单。
- 签署 SLO、演练报告与交付验收。

## 6 Orchestrator（后端）改造清单（实现要点）
- 接入层：增加 `NIFI_MODE` 支持（`k8s`/`docker`/`local`），接口不变，对前端透明。
- 部署管理：当 `k8s` 模式时，orchestrator 調用 K8s API（或 kubectl）來部署/scale/rollback NiFi；Docker 模式則使用 docker 客戶端。
- 幂等与锁：对 `ensure` 与 `deploy flow` 操作加 DB 事务/锁（防止并发重复部署）。
- 超时与退避：网络/启动失败时用指数退避策略；关键操作应记录审计日志。
- 回退策略：Flow 部署失败自动回滚到上一个 registry 版本，或切换后端到 Local 模式。

## 7 CI/CD 建议
- Flow 管理：将 Flow 的模板/参数化配置放入 Git 仓库，CI 生成 Registry bundle 并自动导入到 Registry（staging），运行 contract-tests，然后 promote 到 prod。
- 镜像/manifest：镜像通过 CI 构建并推到私有 registry；K8s manifest/Helm chart 经 GitOps 同步。

## 8 监控与告警（关键指标）
- NiFi 节点可用性、Flow 成功率/失败率、Process Group 延迟、Queue 长度、磁盘使用、JVM 堆内存。
- 告警示例：NiFi down（15 分钟），Flow failure rate > 1%（10 分钟），export_jobs/error 连续 20 次。

## 9 备份与恢复
- 持久化目录（flow、provenance、content）应使用 PVC（K8s）或宿主机 PV，并定期备份到对象存储（每日增量 + 周全量）。
- 测试恢复流程并纳入 Runbook。

## 10 Runbooks（示例条目）
- 启动 NiFi（K8s）：`kubectl rollout restart statefulset/nifi -n iot`。
- 回滚 Flow：通过 NiFi Registry 回退到上一版本；或在 orchestrator 中调用 `run_process_group` 指令停止并替换 bundle。
- 常见故障：worker 无法连接 MySQL（检查 JDBC、Secrets、network）；NiFi 无法写入 output（检查权限、挂载点）。

## 11 验收标准
- 单节点实验：镜像可构建并运行，worker 完成一次导出并由后端注册结果。
- Staging：Flow 通过 Registry 管理，并在 staging 執行 contract-tests 通過。
- Prod：监控/告警就绪、Runbooks 齐备、演练通过、SLO 被批准。

## 12 交付清单
- K8s manifest / Helm chart（或 docker-compose）
- 受控 NiFi 镜像（registry 地址）和构建脚本
- NiFi Registry 中的版本化 Flow（bundle）
- 更新后的 `nifi_orchestrator`：代码变更与使用说明
- CI/CD pipelines（Flow + 镜像 + manifest）
- 监控面板与告警规则
- Runbooks 与演练报告

---
说明：本文档为实施级别方案，包含实验到生产的全流程要点。后续我可以把关键步骤转为脚本（例如：Helm chart、GitHub Actions CI、orchestrator K8s 客户端实现）并在实验环境做一次 E2E 验证。
