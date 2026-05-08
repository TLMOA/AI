NiFi custom image (python + pymysql)

目的：为 NiFi 提供 Python 运行时与 `pymysql`，用于在容器内运行 `ExecuteStreamCommand` 调用的 `nifi_mysql_export_worker.py`。

构建与运行：

```bash
# 在仓库根目录下运行
cd docker/nifi
chmod +x build.sh
./build.sh

# 启动容器（示例）
docker run -d --name iot-nifi -p 8080:8080 \
  -v /home/yhz/iot/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  iot-nifi-python:latest
```

JDBC 驱动：
- 将 `mysql-connector-java-<version>.jar` 放到宿主机目录 `/home/yhz/iot/real_nifi_data/lib`，并在 NiFi 容器中将该路径挂载到 NiFi 可加载的目录或在 NiFi UI 中通过 Controller Services 引用该路径。

环境变量模板：请参考 `.env.template`。
