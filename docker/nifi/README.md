NiFi custom image (python + pymysql)

目的：为 NiFi 提供 Python 运行时与 `pymysql`，用于在容器内运行 `ExecuteStreamCommand` 调用的 `nifi_mysql_export_worker.py`。

默认使用方式是运维预置 NiFi 容器与 Flow，后端只负责对接、启停既有 Flow、提交任务和同步结果；如果需要演示或联调，再通过显式环境变量打开自动创建容器或自动部署 Flow。

构建与运行：

```bash
# 在仓库根目录下运行
cd docker/nifi
chmod +x build.sh
./build.sh

# 启动容器（示例，预置方式）
docker run -d --name iot-nifi -p 8080:8080 \
  -v /home/yhz/iot/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  iot-nifi-python:latest
```

JDBC 驱动：
- 将 `mysql-connector-java-<version>.jar` 放到宿主机目录 `/home/yhz/iot/real_nifi_data/lib`，并在 NiFi 容器中将该路径挂载到 NiFi 可加载的目录或在 NiFi UI 中通过 Controller Services 引用该路径。

环境变量模板：请参考 `.env.template`。

持久化（包括 `flow` 配置）— 推荐做法
--------------------------------------------------
为了确保 NiFi 的 Flow 配置在主机重启、容器重建或断电后不丢失，推荐同时持久化 NiFi 的 `conf`（包含 `flow.xml.gz`）和运行时数据。仓库里已经建议把运行数据挂载到 `/home/yhz/iot/real_nifi_data`，下面给出完整示例和初始化步骤：

1) 创建宿主目录并设置权限（在主机上运行）：

```bash
mkdir -p /home/yhz/iot/real_nifi_data
mkdir -p /home/yhz/iot/real_nifi_conf
mkdir -p /home/yhz/iot/real_nifi_data/lib
sudo chown -R 1000:1000 /home/yhz/iot/real_nifi_data /home/yhz/iot/real_nifi_conf
```

2) 若你还没有 `conf`（首次使用），先用临时容器生成默认配置并拷贝到宿主：

```bash
# 启动临时容器（不挂载 conf），让 NiFi 生成默认 conf
docker run -d --name temp-nifi -p 8080:8080 iot-nifi-python:latest
sleep 8
docker cp temp-nifi:/opt/nifi/nifi-current/conf /home/yhz/iot/real_nifi_conf
docker stop temp-nifi && docker rm temp-nifi
sudo chown -R 1000:1000 /home/yhz/iot/real_nifi_conf
```

注意：如果你已有 `flow.xml.gz`（或从 NiFi Registry/备份恢复），把它放到 `/home/yhz/iot/real_nifi_conf/flow.xml.gz`。

3) 把 JDBC 驱动放到宿主的 lib 目录：

```bash
# 例如
cp mysql-connector-java-<version>.jar /home/yhz/iot/real_nifi_data/lib/
sudo chown 1000:1000 /home/yhz/iot/real_nifi_data/lib/mysql-connector-java-<version>.jar
```

注意：不要把宿主目录挂载到 `/opt/nifi/nifi-current/lib`。该目录是 NiFi 核心依赖目录，覆盖后会导致 `org.apache.nifi.bootstrap.BootstrapProcess` 类找不到，容器循环重启。JDBC 驱动建议保存在 `/opt/nifi/nifi-current/data/iot/lib`（即宿主 `/home/yhz/iot/real_nifi_data/lib`）并在 Controller Service 的 Driver Location 中引用。

4) 使用 `docker-compose` 启动（仓库已提供示例 `docker-compose.yml`）：

```bash
cd docker/nifi
docker-compose up -d --build
```

5) 验证与备份

```bash
# 查看容器是否已自动重启
docker ps -a | grep iot-nifi

# 查看日志确认 NiFi 已加载 flow
docker logs -f iot-nifi

# 备份 flow（必要时）
docker cp iot-nifi:/opt/nifi/nifi-current/conf/flow.xml.gz /home/yhz/iot/real_nifi_data/backup/flow.xml.gz
```

访问说明：当前证书的 SAN 包含 `localhost` 和容器名，不包含 `127.0.0.1`。如果浏览器提示 `invalid SNI`，请优先使用 `https://localhost:8080` 打开 NiFi；若必须通过 IP 访问，需要重新生成包含该 IP 的证书。

附加说明：
- `--restart unless-stopped` 会在主机重启后自动重启容器，但若你手动 `docker stop` 过则不会自动重启。
- 挂载 `conf` 到宿主意味着你要负责 `conf` 的版本与兼容性（升级镜像时注意备份并检查 `conf` 差异）。
- 更稳定的做法是使用 NiFi Registry 管理版本化的 flows，并把 Registry 存储也配置为外置持久化（此处 README 未覆盖 Registry 部署）。

示例 `docker-compose.yml` 已添加到本目录，包含 `conf` 与 `data` 的挂载示例。

