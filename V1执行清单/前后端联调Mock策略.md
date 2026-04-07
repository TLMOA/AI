# 前后端联调Mock策略（V1）

## 1. 目标

在NiFi偶发不可用、JetLinks未联调、大模型后端未接入的情况下，保证前后端开发和演示不停摆。

## 2. Mock分层

## 2.1 后端内Mock（优先）

场景：NiFi触发暂不可用或不稳定。

策略：

1. POST /jobs创建后写入本地任务表。
2. 后台定时任务模拟状态流转：PENDING -> RUNNING -> SUCCEEDED/FAILED。
3. 产物文件可使用样例文件占位。

开关建议：

- APP_MOCK_NIFI=true/false

## 2.2 前端Mock（备用）

场景：后端接口尚未就绪。

策略：

1. api adapter层加mock实现。
2. 使用固定数据集模拟任务列表、详情、文件预览。
3. 后端就绪后切换adapter，不改页面组件。

开关建议：

- VITE_USE_MOCK_API=true/false

## 3. Mock数据规范

## 3.1 任务样例

- jobId: job_20260330_001
- 状态序列：PENDING -> RUNNING -> SUCCEEDED
- progress：0, 30, 70, 100

## 3.2 文件样例

- fileId: file_20260330_001
- fileName: sensor_tagged_demo.csv
- fileFormat: CSV
- previewRows: 前100行模拟

## 4. 演示保底路径

1. 创建任务。
2. 查看任务进度变化。
3. 查看输出文件预览。
4. 下载样例产物。

## 5. 切换到真实链路条件

满足任意一项即可切换：

1. NiFi触发与状态查询连续稳定24小时。
2. 后端真实接口通过联调清单90%以上。

## 6. 禁止事项

1. 页面组件直接写死mock数据（必须通过adapter）。
2. 生产环境开启mock开关。
3. mock与真实返回结构不一致。

## 7. 验收标准

1. mock开关开启时，核心页面全可用。
2. mock开关关闭时，页面可无改造切换真实接口。
3. 同一页面在mock/real模式下渲染结构一致。
