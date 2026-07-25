# 07 — NiFi容器策略

> 依赖：02-核心路径解析函数
> 状态：⚠️ 部分实现（扫描逻辑已支持用户隔离，文件搬移待实现）
> 涉及文件：`v1-backend/app/main.py`、`v1-backend/app/nifi_orchestrator.py`

---

## 一、当前实现状态

### 已实现 ✅

1. **`_scan_nifi_export_results_for_user(username)`** — 扫描指定用户的 nifi 输出目录
2. **`_scan_nifi_export_results()`** — 遍历所有 IotUser，逐个调用 `_scan_nifi_export_results_for_user`
3. **`_sync_nifi_files()`** — 遍历所有用户的 storage root 同步文件到注册表
4. 每个用户的 `_nifi_export_job_dirs` 已指向用户专属目录

### 待实现 📋

1. NiFi 全局工作区产物搬移到用户目录（`_route_nifi_output_to_user`）
2. 生产环境独立容器管理（`_ensure_user_nifi_container`）

---

## 二、本地模拟：方案 A — 共享容器 + 后端搬文件

```
┌─────────────────────────────────────────┐
│  共享 NiFi 容器 (iot-nifi)               │
│  Docker volume:                          │
│    /home/yhz/real_nifi_data  ←→ 容器内   │
│    /opt/nifi/.../data/iot                │
│                                          │
│  产出文件写入:                            │
│    /home/yhz/real_nifi_data/export_jobs/ │
│          │                               │
│          ▼ 后端识别 username 后搬文件     │
│  /home/yhz/{username}/real_nifi_data/    │
│  /home/yhz/{username}/real_nifi_data/ │
└─────────────────────────────────────────┘
```

**流程**：
1. NiFi 容器启动，挂载全局 `/home/yhz/real_nifi_data/` 作为工作区
2. 导出任务执行完毕，产物写入全局工作区
3. 后端扫描完成的任务，根据 `username` 把文件搬到用户自己的目录
4. 用户看到的文件树指向自己的目录

**优点**：无需多容器，资源消耗低，适合本地模拟
**缺点**：全局工作区是临时中转站，文件需要搬移

---

## 二、生产环境：方案 B — 每用户独立 NiFi 容器

```
┌──────────────────────────────────────────┐
│  zhangsan (公有化)                        │
│  Docker: iot-nifi-zhangsan               │
│  volume: /home/yhz/zhangsan/real_nifi_data/ │
├──────────────────────────────────────────┤
│  wangwu (私有化)                          │
│  Docker: iot-nifi-wangwu                 │
│  volume: /home/yhz/wangwu/real_nifi_data/ │
└──────────────────────────────────────────┘
```

每用户独立容器，各自挂载自己的目录。端口动态分配，记录在数据库中。

---

## 三、当前实现方案（本地模拟）

### 3.1 共享容器架构

```bash
# 当前 NiFi 容器
docker run -d --name iot-nifi \
  -p 8080:8080 \
  -v /home/yhz/real_nifi_data:/opt/nifi/nifi-current/data/iot \
  apache/nifi:latest
```

所有用户共用同一个 NiFi 容器，产物写入 `/home/yhz/real_nifi_data/export_jobs/`。

### 3.2 后端搬文件逻辑

```python
# nifi_orchestrator.py 或 main.py 中
def _route_nifi_output_to_user(username: str, job_id: str):
    """将 NiFi 全局工作区的产物搬到用户目录"""
    global_export = Path("/home/yhz/real_nifi_data/export_jobs") / job_id
    user_export = _get_user_real_nifi_dir(username) / "export_jobs" / job_id
    
    if global_export.exists():
        shutil.copytree(global_export, user_export, dirs_exist_ok=True)
        # 可选：清理全局工作区
        # shutil.rmtree(global_export)
```

### 3.3 待实现

1. 在 `_sync_nifi_files` 或 `_scan_nifi_export_results` 中加入搬文件逻辑
2. 搬文件时根据 `username` 路由到正确的用户目录
3. 清理全局工作区（避免文件堆积）

---

## 四、未来：方案 B 独立容器

当需要生产环境时，实现以下：

```python
def _ensure_user_nifi_container(username: str) -> str:
    """确保用户有独立的 NiFi 容器，返回容器名"""
    container_name = f"iot-nifi-{username}"
    # 检查容器是否存在
    # 如果不存在，创建并启动
    # 端口动态分配，记录到数据库
    return container_name
```

---

## 五、验证方法

```bash
# 1. 确认共享容器运行
docker ps | grep iot-nifi

# 2. 创建导出任务
# 前端 → 配置数据库 → 创建导出任务 → NiFi 执行

# 3. 检查产物是否搬到用户目录
ls /home/yhz/{username}/real_nifi_data/export_jobs/
ls /home/yhz/{username}/real_nifi_data/export_jobs/
```