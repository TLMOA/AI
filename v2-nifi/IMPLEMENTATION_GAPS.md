# NiFi 后端生产化 — 实施缺口（概要）

## 已识别缺口
- 高可用部署：缺少标准化的 StatefulSet/HA 部署文档与测试用例。
- 持久化策略：PVC、StorageClass 与备份恢复流程未统一并测试。
- 安全与认证：TLS、证书管理、Registry 认证与 LDAP/OAuth 集成方案未落地。
- 配置管理：NiFi 流程版本化与 Registry 集成操作规范不完整。
- 监控与告警：Prometheus、Grafana 指标、告警规则与日志采集流水线缺失。
- CI/CD：缺少自动化构建与部署流水线（容器镜像 + k8s 资源 + Flow 导入）。
- Orchestrator：缺少 K8s 适配示例、幂等策略、锁与回退的实现样板。
- 测试：缺少 E2E 与契约测试套件以验证 Local/NiFi 路径一致性。

## 建议优先级（高→低）
1. 完成 K8s minimal manifest（StatefulSet + PVC + Service）并在测试集群演练。
2. 实现 Registry 部署与 Flow 版本化流程（导入/回退示例）。
3. 为 `nifi_orchestrator` 提供 K8s 客户端示例，包含幂等与锁机制。
4. 补齐 CI/CD：镜像构建、Flow bundle 导入、manifest 部署、contract-tests。
5. 部署 Prometheus/JMX exporter、Grafana 面板与基础告警规则。
6. 编写 Runbooks 与备份/恢复脚本并演练。
