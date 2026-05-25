# NiFi MySQL 导出 Flow MVP

本 MVP 目标：在 `backend_mode=nifi` 时，后端只投递任务 JSON；NiFi 负责触发 MySQL 查询、生成 CSV/JSON/TSV、写回 done/error 状态。

## 共享目录

宿主机目录：

- `/home/yhz/iot/real_nifi_data/export_jobs/inbox`
- `/home/yhz/iot/real_nifi_data/export_jobs/done`
- `/home/yhz/iot/real_nifi_data/export_jobs/error`
- `/home/yhz/iot/real_nifi_data/output_csv`
- `/home/yhz/iot/real_nifi_data/output_json`
- `/home/yhz/iot/real_nifi_data/output_tsv`
- `/home/yhz/iot/real_nifi_data/bin/nifi_mysql_export_worker.py`

NiFi 容器内对应目录：

- `/opt/nifi/nifi-current/data/iot/export_jobs/inbox`
- `/opt/nifi/nifi-current/data/iot/export_jobs/done`
- `/opt/nifi/nifi-current/data/iot/export_jobs/error`
- `/opt/nifi/nifi-current/data/iot/output_csv`
- `/opt/nifi/nifi-current/data/iot/output_json`
- `/opt/nifi/nifi-current/data/iot/output_tsv`
- `/opt/nifi/nifi-current/data/iot/bin/nifi_mysql_export_worker.py`

## Flow 方案 A：ExecuteStreamCommand 调用导出脚本

这是当前最小可运行方案。NiFi 不直接拼接 SQL，而是由 NiFi 调用共享目录里的 Python worker。这样 SQL 查询和 CSV/JSON/TSV 文件生成仍然由 NiFi Flow 触发执行，后端不会本地生成导出文件。

### Processor 1：GetFile

配置：

- Input Directory：`/opt/nifi/nifi-current/data/iot/export_jobs/inbox`
- Keep Source File：`false`
- File Filter：`.*\\.json`
- Batch Size：`1`

### Processor 2：ExecuteStreamCommand

配置：

- Command Path：`python3`
- Command Arguments：`/opt/nifi/nifi-current/data/iot/bin/nifi_mysql_export_worker.py`
- Ignore STDIN：`false`

连接：

- `GetFile/success` -> `ExecuteStreamCommand/original`

说明：

- `GetFile` 读取任务 JSON 后作为 FlowFile 内容传给脚本 stdin。
- 脚本执行 MySQL 查询。
- 脚本按任务中的 `format` 写入 `output_csv/output_json/output_tsv`。
- 脚本成功写 `export_jobs/done/{jobId}.json`。
- 脚本失败写 `export_jobs/error/{jobId}.json`。

### 结果文件

成功状态示例：

```json
{
  "jobId": "export_20260506_0001",
  "status": "SUCCEEDED",
  "filePath": "/home/yhz/iot/real_nifi_data/output_csv/sensor_export_20260506_153000.csv",
  "rows": 1000,
  "message": "export completed by nifi mysql worker",
  "finishedAt": "2026-05-06T15:30:00"
}
```

失败状态示例：

```json
{
  "jobId": "export_20260506_0001",
  "status": "FAILED",
  "filePath": "",
  "rows": 0,
  "message": "mysql connect failed",
  "finishedAt": "2026-05-06T15:30:00"
}
```

## NiFi 容器依赖

NiFi 容器内必须有：

- `python3`
- `pymysql`

如果官方 NiFi 镜像没有 `pymysql`，可以先进入容器安装验证：

```bash
docker exec -it iot-nifi bash
python3 -m pip install pymysql
```

正式化建议构建自定义镜像，把 `pymysql` 固化进去。

## 验收步骤

1. 后端切换到 `nifi` 模式。
2. 点击数据库导出。
3. 后端自动启动/复用 `iot-nifi` 容器。
4. 后端写任务到 `/home/yhz/iot/real_nifi_data/export_jobs/inbox`。
5. NiFi `GetFile` 消费任务。
6. NiFi `ExecuteStreamCommand` 调用 worker。
7. 结果落到 `/home/yhz/iot/real_nifi_data/output_*`。
8. 状态写到 `/home/yhz/iot/real_nifi_data/export_jobs/done`。
9. 后端扫描 done 并注册 fileId。
10. 前端可预览/下载。
