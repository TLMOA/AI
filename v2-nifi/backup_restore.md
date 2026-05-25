# NiFi 备份与恢复（简要）

## 备份要点
- 备份对象：NiFi flow configuration（flow.xml.gz 等）、provenance repository、content repository、NiFi Registry 数据库。
- 备份频率建议：flow 配置每日、provenance/content 每日增量 + 周全量（根据存储能力调整）。
- 备份目标：对象存储（S3/MinIO）或存储类快照。

## 备份示例（PVC 快照 -> 对象存储）
1. 创建 StorageClass 支持快照。
2. 使用 VolumeSnapshot 创建快照：`kubectl create -f volumesnapshot.yaml`。
3. 将快照导出到对象存储（依赖云厂商工具或自定义脚本）。

## 恢复示例（从对象存储）
1. 从对象存储拉取备份到临时目录。
2. 停止 NiFi（或缩容为 0）并替换 PVC 内容。
3. 重建 PVC / 恢复数据后重启 StatefulSet。
4. 验证流程和数据一致性（运行 contract-tests）。

## 注意事项
- 恢复操作风险高，务必在演练环境验证后再对生产执行。
- 推荐每季度演练一次完整恢复流程并记录时间/问题。
