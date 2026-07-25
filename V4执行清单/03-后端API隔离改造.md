# 03 — 后端API隔离改造

> 依赖：02-核心路径解析函数
> 状态：✅ 已实现
> 涉及文件：`v1-backend/app/main.py`

---

## 一、已完成 API 清单

| 接口 | 方法 | 说明 | 状态 |
|------|------|------|:---:|
| `/api/v1/auth/register` | POST | 支持 `deployment_mode` + `ceph_endpoint` 参数 | ✅ |
| `/api/v1/auth/me` | GET | 返回 `deployment_mode` + `ceph_endpoint` | ✅ |
| `/api/v1/internal/users` | GET | 用户列表（含部署模式） | ✅ |
| `/api/v1/internal/users/{username}/deployment` | PUT | 修改部署模式 | ✅ |
| `/api/v1/internal/users/{username}` | DELETE | 删除用户（含目录清理） | ✅ |
| `/api/v1/internal/all-users` | GET | 所有用户下拉列表 | ✅ |
| `/api/v1/internal/private-users/{username}/pull` | POST | 拉取私有化用户数据 | ✅ |
| `/api/v1/user/files/tree` | GET | 普通用户文件中心（双树） | ✅ |
| `/api/v1/admin/users/tree` | POST | 管理员查看用户文件树 | ✅ |
| `/api/v1/admin/users/files` | POST | 管理员查看用户文件列表 | ✅ |

---

## 二、已改造的函数

### 2.1 上传文件 → 用户目录

```python
# upload_inbox_csv / upload_inbox_json / upload_inbox_tsv
# 改为使用 _get_user_upload_dirs(username)
dirs = _get_user_upload_dirs(username)
target = dirs[f"inbox_{ext}"] / file.filename
```

### 2.2 文件列表 → 按用户过滤

```python
# GET /api/v1/files 增加 target_username 参数
# 管理员可通过 target_username 查看任意用户的文件
# 普通用户只能看到自己的文件
```

### 2.3 文件下载/预览 → 权限校验

```python
# resolve_nifi_output_file 增加 username 参数
# 优先搜索用户隔离目录
def resolve_nifi_output_file(expected_format: str, username: Optional[str] = None):
    if username:
        user_nifi = _get_user_nifi_dir(_normalize_username(username))
        for sub in ["output_csv", "output_json", "output_tsv", ...]:
            d = user_nifi / sub
            if d.exists() and d.is_dir():
                search_dirs.append(d)
```

### 2.4 purge-missing → 动态路径

```python
# 不再硬编码 /home/yhz/nifi-data/output_*
# 改为通过 _resolve_user_storage_root("admin") 动态解析
```

### 2.5 export_jobs API → factory_id 改为 username

```python
# 所有 export_jobs 相关 API 的参数从 factory_id 改为 username
# 数据库查询优先使用 username 列，fallback 到 factory_id 列
```

### 2.6 管理员删除用户

```python
@app.delete("/api/v1/internal/users/{username}")
def api_delete_user(username: str, request: Request, body: Dict[str, Any] = Body(default={})):
    # 删除 IotUser 记录
    # 删除关联的 export_jobs 记录
    # 删除 /home/yhz/{username}/ 目录（purge_data=true 时）
```

---

## 三、验证方法

```bash
# 1. 上传文件到用户目录
curl -X POST http://localhost:8000/api/v1/files/upload/inbox_csv \
  -H "Cookie: access_token=<user_token>" \
  -F "file=@test.csv"

# 2. 查看用户文件列表
curl http://localhost:8000/api/v1/files?target_username=zhangsan \
  -H "Cookie: access_token=<admin_token>"

# 3. 删除用户
curl -X DELETE http://localhost:8000/api/v1/internal/users/zhangsan \
  -H "Cookie: access_token=<admin_token>" \
  -H "Content-Type: application/json" \
  -d '{"purge_data": true}'
```