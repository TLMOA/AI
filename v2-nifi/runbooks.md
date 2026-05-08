# NiFi 运行手册（Runbooks）— 精简版

## 快速命令
- 查看 Pod 状态：`kubectl -n <ns> get pods -l app=nifi`
- 查看 Registry：`kubectl -n <ns> get pods -l app=nifi-registry`
- 日志：`kubectl -n <ns> logs <pod>`

## 启动/停止
- 启动（apply manifests）：`kubectl apply -f v2-nifi/k8s/`
- 停止：`kubectl delete -f v2-nifi/k8s/`（谨慎，生产请先缩容/备份）

## 故障恢复（示例）
1. Pod CrashLoop：`kubectl -n <ns> describe pod <pod>` -> 检查事件与挂载
2. 磁盘问题：确认 PVC 与 PV 状态，若 PV 有问题，联系存储管理员或创建新 PVC 并从备份恢复数据

## 回滚流程（Flow 失败）
1. 在 NiFi Registry 找到上一版本 bundle。
2. 在测试环境先恢复并验证流程。
3. 在 orchestrator 内触发回滚（或通过 UI 手工回退）。

## 升级（镜像）
1. 在非高峰时间更新 StatefulSet image 字段，设置 `partitioned` rolling update 策略（如果需要）。
2. 观察 `/nifi-api/flow/process-groups/{id}/status` 确认流程健康。
