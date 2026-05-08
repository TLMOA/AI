# nifi_orchestrator_k8s 示例

用途：演示如何用后端以幂等方式在 k8s 上创建/更新 NiFi 及 Registry 资源，并提供简单备份与回滚机制。

先决条件：已安装 Python 3.8+，并能访问集群的 kubeconfig（`kubectl` 可用）。

安装依赖：
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

运行示例：
```bash
python nifi_orchestrator_k8s.py
```

注意：该示例为 POC 级别，生产使用时需要替换文件锁为 DB 分布式锁、完善错误处理与审计、并把凭据放入 Secrets。回滚仅基于 /tmp 备份实现，生产应使用更可靠的版本库或直接从 NiFi Registry 恢复。 
