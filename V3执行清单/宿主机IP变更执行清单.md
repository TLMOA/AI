# 宿主机 IP 变更执行清单

> 把 `<OLD_IP>` 替换为旧 IP，把 `<NEW_IP>` 替换为新 IP。
> 下面表格里 `旧值` 是当前仓库里实际写的内容，`新值` 改成 `<NEW_IP>`。

---

## 一、必须改

| # | 文件 | 行号 | 旧值 | 新值 |
|---|------|------|------|------|
| 1 | [v1-frontend/index.html](../v1-frontend/index.html) | 53 | `value="192.168.1.181"` | `value="<NEW_IP>"` |
| 2 | [v1-frontend/app.js](../v1-frontend/app.js) | 884 | `\|\| "192.168.1.181"` | `\|\| "<NEW_IP>"` |
| 3 | [v1-frontend/app.js](../v1-frontend/app.js) | 900 | `mysql: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 4 | [v1-frontend/app.js](../v1-frontend/app.js) | 901 | `postgres: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 5 | [v1-frontend/app.js](../v1-frontend/app.js) | 902 | `postgresql: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 6 | [v1-frontend/app.js](../v1-frontend/app.js) | 903 | `sqlserver: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 7 | [v1-frontend/app.js](../v1-frontend/app.js) | 904 | `oracle: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 8 | [v1-frontend/app.js](../v1-frontend/app.js) | 906 | `hive: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 9 | [v1-frontend/app.js](../v1-frontend/app.js) | 907 | `hdfs: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 10 | [v1-frontend/app.js](../v1-frontend/app.js) | 908 | `hbase: { host: '192.168.1.181', ...` | `host: '<NEW_IP>', ...` |
| 11 | [v1-frontend/app.js](../v1-frontend/app.js) | 970 | `'192.168.1.181'` | `'<NEW_IP>'` |
| 12 | [v1-frontend/internal.js](../v1-frontend/internal.js) | 50-60 | `getApiBaseCandidates()` 里的 `http://127.0.0.1:8081...` 段 | 在 candidates 数组里追加 `http://<NEW_IP>:8081/api/v1` 和 `http://<NEW_IP>:8081/api` |
| 13 | [docker/nifi/docker-compose.yml](../docker/nifi/docker-compose.yml) | 15 | `202.113.76.55:9443` | `<NEW_IP>:9443` |
| 14 | [real_nifi_conf/nifi.properties](../real_nifi_conf/nifi.properties) | 376 | `202.113.76.55:9443` | `<NEW_IP>:9443` |
| 15 | [README.md](../README.md) | 14 / 43 等 | `http://202.113.76.55:5174`、`http://202.113.76.55:9443`、`192.168.1.181` | 全部替换为 `<NEW_IP>` |
| 16 | [v1-frontend/README.md](../v1-frontend/README.md) | 10 / 12 | `http://127.0.0.1:5174`、`http://202.113.76.55:5174` | 全部替换为 `<NEW_IP>` |

---

## 二、建议改

| # | 文件 | 改动 |
|---|------|------|
| 17 | [scripts/ssh_tunnel.sh](../scripts/ssh_tunnel.sh) | 第 6 行注释里的 `--host 202.113.76.55` 改为 `--host <NEW_IP>` |
| 18 | [/etc/mysql/mysql.conf.d/mysqld.cnf](file:///etc/mysql/mysql.conf.d/mysqld.cnf) | 在 `[mysqld]` 段加 `skip-name-resolve`（如已有则跳过），然后 `sudo systemctl restart mysql` |
| 19 | [/etc/mysql/mysql.conf.d/mysqld.cnf](file:///etc/mysql/mysql.conf.d/mysqld.cnf) | 如果有 `bind-address = <OLD_IP>`，改成 `bind-address = 0.0.0.0` 或 `<NEW_IP>`，再 `sudo systemctl restart mysql` |

---




