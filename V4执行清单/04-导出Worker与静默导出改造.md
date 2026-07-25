# 04 — 导出Worker与静默导出改造

> 依赖：02-核心路径解析函数、03-后端API隔离改造
> 状态：✅ 已实现
> 涉及文件：`v1-backend/app/export_worker.py`、`v1-backend/app/silent_export_worker.py`、`v1-backend/app/admin_routes.py`、`v1-backend/app/meta_json.py`

---

## 一、export_worker.py 改造

### 改动内容

```python
# 1. DEFAULT_NIFI_BASE 标记 deprecated
DEFAULT_NIFI_BASE = Path(os.getenv("NIFI_BASE_DIR", "/home/yhz/nifi-data"))  # deprecated — v4 使用 _get_user_nifi_dir

# 2. 所有 factory_id 变量改为 username
# 3. 路径构建改为使用 _get_user_nifi_dir(username)
```

### 验证

```bash
# 创建导出任务后检查产物路径
ls /home/yhz/{username}/nifi-data/export_jobs/
```

---

## 二、silent_export_worker.py 改造

### 改动内容

```python
# NIFI_SILENT_DIR 改为使用 _get_user_nifi_dir(username, "silent_exports")
# tenant 参数保持（语义已是用户名）
```

### 验证

```bash
# 触发静默导出后检查产物
ls /home/yhz/{username}/nifi-data/silent_exports/
```

---

## 三、admin_routes.py 改造

### 改动内容

```python
# _get_silent_export_dir 改为动态路径
def _get_silent_export_dir(tenant: str) -> Path:
    return _get_user_nifi_dir(_normalize_username(tenant), "silent_exports")
```

### 新增接口

```python
@router.post("/internal/admin/ensure-user-storage")
def ensure_user_storage(request: Request):
    """管理员手动触发，为已存在但缺目录的用户补建 nifi-data / real_nifi_data。"""
    # 遍历所有 IotUser，调用 _create_user_storage_dirs
```

---

## 四、meta_json.py 改造

```python
# _resolve_output_dir() 接受用户路径参数
# 不再依赖全局 NIFI_OUTPUT_DIR 等常量
```

---

## 五、验证方法

```bash
# 1. 补建目录
curl -X POST http://localhost:8000/internal/admin/ensure-user-storage \
  -H "Cookie: access_token=<admin_token>"

# 2. 静默导出
# 在内部管理页触发静默导出，检查产物路径
ls /home/yhz/{username}/nifi-data/silent_exports/
```