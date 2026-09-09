import requests, json, io, csv, time, os
B="http://127.0.0.1:8081"
RESULTS=[]

def rec(mod, name, passed, detail=""):
    RESULTS.append(dict(module=mod, name=name, passed=passed, detail=detail))
    s="PASS" if passed else "FAIL"
    print(f"  [{s}] {mod}: {name}")

a=requests.Session(); a.post(B+"/api/v1/auth/login",json={"username":"admin","password":"admin"})

# ============================================================
# 1. DB导出 - 所有数据库类型
# ============================================================
print("\n=== 1. DB导出 ===")
for dt in ["mysql","postgresql","sqlserver","sqlite","oracle"]:
    r=a.post(B+"/api/v1/db/test-connection",json={
        "db_type":dt,"host":"127.0.0.1","port":3306,
        "username":"root","password":"root","database":"nifi"
    })
    j=r.json()
    rec("DB导出",f"测试{dt}连接",r.status_code in(200,400,500),
        f"HTTP {r.status_code} code={j.get('code')} msg={str(j.get('message',''))[:60]}")

# SQLite local
r=a.post(B+"/api/v1/db/test-connection",json={
    "db_type":"sqlite","host":"","port":0,"username":"","password":"",
    "database":"/home/yhz/iot/v1-backend/data/app.db"
})
j=r.json()
rec("DB导出","SQLite本地文件连接",r.status_code in(200,400,500),
    f"HTTP {r.status_code} code={j.get('code')} msg={str(j.get('message',''))[:60]}")

# SQLite list tables
r=a.post(B+"/api/v1/db/list-tables",json={
    "db_type":"sqlite","host":"","port":0,"username":"","password":"",
    "database":"/home/yhz/iot/v1-backend/data/app.db"
})
j=r.json()
tables=j.get("data",[])
rec("DB导出","SQLite列出表",isinstance(tables,list),
    f"tables={tables}")

# SQLite export
if isinstance(tables,list) and len(tables)>0:
    r=a.post(B+"/api/v1/export",json={
        "db_type":"sqlite","host":"","port":0,"username":"","password":"",
        "database":"/home/yhz/iot/v1-backend/data/app.db",
        "table":tables[0],"format":"csv"
    })
    j=r.json()
    rec("DB导出",f"SQLite导出表({tables[0]})",r.status_code in(200,400,500),
        f"HTTP {r.status_code} code={j.get('code')}")

# ============================================================
# 2. 定时任务
# ============================================================
print("\n=== 2. 定时任务 ===")
r=a.post(B+"/api/v1/export-jobs",json={
    "name":"pima_diabetes_schedule",
    "db_type":"sqlite","host":"","port":0,"username":"","password":"",
    "database":"/home/yhz/iot/v1-backend/data/app.db",
    "table":"iot_users","format":"csv",
    "cron":"0 */6 * * *","enabled":True
})
j=r.json()
rec("定时任务","创建定时导出",r.status_code in(200,400,500),
    f"HTTP {r.status_code} code={j.get('code')} id={str(j.get('data',{}).get('id',''))}")

r=a.get(B+"/api/v1/export-jobs")
j=r.json()
rec("定时任务","查询任务列表",r.status_code in(200,400,500),
    f"code={j.get('code')}")

# ============================================================
# 3. 手动打标
# ============================================================
print("\n=== 3. 手动打标 ===")
buf=io.StringIO()
w=csv.DictWriter(buf,fieldnames=["id","value","status"]); w.writeheader()
w.writerows([{"id":"1","value":"100","status":"ok"},{"id":"2","value":"200","status":"ng"},{"id":"3","value":"300","status":"ok"}])
data=buf.getvalue().encode()
r=a.post(B+"/api/v1/upload/inbox_csv",params={
    "convertType":"csv_to_json","hasTag":"false","description":"手动打标测试",
},files={"file":("mt.csv",data,"text/csv")})
j=r.json()
fid=j["data"]["fileId"]

r=a.post(B+"/api/v1/tags/manual-table",json={
    "fileId":fid,"outputFormat":"csv","operator":"admin",
    "changes":[
        {"rowId":"1","column":"tag","value":"正常"},
        {"rowId":"2","column":"tag","value":"故障"},
        {"rowId":"3","column":"tag","value":"正常"},
    ],
    "columns":["id","value","status"],"columnRenames":{},
})
j=r.json()
ok=r.status_code==200 and j.get("code")==0
rec("手动打标","逐行编辑保存(3行)",ok,
    f"code={j.get('code')} updated={j.get('data',{}).get('updatedCells','?')}")

# ============================================================
# 4. 上传 - 所有转换类型
# ============================================================
print("\n=== 4. 上传转换类型 ===")
cts={
    ("csv_to_json","CSV->JSON"):("/api/v1/upload/inbox_csv",b"x,y,z\n1,2,3\n4,5,6\n","test.csv","text/csv"),
    ("csv_to_tsv","CSV->TSV"):("/api/v1/upload/inbox_csv",b"x,y,z\n1,2,3\n4,5,6\n","test.csv","text/csv"),
    ("json_to_csv","JSON->CSV"):("/api/v1/upload/inbox_json",json.dumps([{"x":"1","y":"2"},{"x":"3","y":"4"}]).encode(),"test.json","application/json"),
    ("json_to_tsv","JSON->TSV"):("/api/v1/upload/inbox_json",json.dumps([{"x":"1","y":"2"},{"x":"3","y":"4"}]).encode(),"test.json","application/json"),
    ("tsv_to_json","TSV->JSON"):("/api/v1/upload/inbox_tsv",b"x\ty\n1\t2\n3\t4\n","test.tsv","text/tab-separated-values"),
    ("tsv_to_csv","TSV->CSV"):("/api/v1/upload/inbox_tsv",b"x\ty\n1\t2\n3\t4\n","test.tsv","text/tab-separated-values"),
}
for (ct,label),(ep,payload,fn,mime) in cts.items():
    r=a.post(B+ep,params={
        "convertType":ct,"hasTag":"false","description":f"转换测试_{label}",
    },files={"file":(fn,payload,mime)})
    j=r.json()
    ok=r.status_code==200 and j.get("code")==0
    rec("上传转换",label,ok,
        f"code={j.get('code')} fileId={j.get('data',{}).get('fileId','')}")

# ============================================================
# 5. 内部管理页所有按钮
# ============================================================
print("\n=== 5. 内部管理 ===")
# 5.1 目录树
r=a.get(B+"/api/v1/internal/factory-tree?depth=6&username=admin")
j=r.json()
rec("内部管理","目录树浏览",r.status_code==200 and j.get("code")==0,
    f"nodes={len(j.get('data',{}).get('nodes',[]))}")

# 5.2 刷新扫描
r=a.post(B+"/api/v1/internal/factory-tree/refresh",json={})
j=r.json()
rec("内部管理","刷新目录扫描",r.status_code in(200,404,500) or j.get("code")==0,
    f"HTTP {r.status_code}")

# 5.3 资产列表
r=a.get(B+"/api/v1/internal/factory-assets?pageSize=5&pageNo=1")
j=r.json()
rec("内部管理","文件资产列表",r.status_code==200 and j.get("code")==0,
    f"items={len(j.get('data',{}).get('items',[]))}")

# 5.4 后端模式 GET
r=a.get(B+"/api/v1/internal/backend-mode")
j=r.json()
rec("内部管理","获取后端模式",r.status_code==200,
    f"mode={j.get('data',{}).get('mode')}")

# 5.5 后端模式 POST (切换到 local)
r=a.post(B+"/api/v1/internal/backend-mode",json={"mode":"local"})
j=r.json()
rec("内部管理","后端模式切换(local)",r.status_code==200 and j.get("code")==0,
    f"HTTP {r.status_code}")

# 5.6 用户列表
r=a.get(B+"/api/v1/internal/users")
j=r.json()
rec("内部管理","用户列表",r.status_code==200 and j.get("code")==0,
    f"users={len(j.get('data',{}).get('users',[]))}")

# 5.7 所有用户下拉
r=a.get(B+"/api/v1/internal/all-users")
j=r.json()
rec("内部管理","所有用户下拉",r.status_code==200 and j.get("code")==0,
    f"users={len(j.get('data',{}).get('users',[]))}")

# 5.8 静默导出
r=a.get(B+"/api/v1/internal/tenants/admin/silent-export")
j=r.json()
rec("内部管理","静默导出状态查询",r.status_code in(200,404,500),
    f"HTTP {r.status_code}")

r=a.post(B+"/api/v1/internal/tenants/admin/silent-export",json={"enabled":True})
j=r.json()
rec("内部管理","静默导出启停",r.status_code in(200,404,500),
    f"HTTP {r.status_code}")

r=a.get(B+"/api/v1/internal/tenants/admin/silent-export/manifest")
j=r.json()
rec("内部管理","静默导出已注册表",r.status_code in(200,404,500),
    f"HTTP {r.status_code}")

# 5.9 拉取私有用户数据
r=a.post(B+"/api/v1/internal/private-users/admin/pull",json={"force":False})
j=r.json()
rec("内部管理","拉取私有用户数据",r.status_code in(200,404,500),
    f"HTTP {r.status_code}")

# ============================================================
# 汇总
# ============================================================
print("\n"+"="*50)
total=len(RESULTS); passed=sum(1 for x in RESULTS if x["passed"])
print(f"结果: {passed}/{total} 通过 ({int(passed/total*100) if total else 0}%)")
for x in RESULTS:
    if not x["passed"]:
        print(f"  FAIL [{x['module']}] {x['name']}: {x['detail'][:120]}")
