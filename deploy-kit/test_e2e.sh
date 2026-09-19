#!/usr/bin/env bash
# ============================================================
# 端到端自测（在一次性容器里跑，不碰宿主机的生产环境）
#
# 为什么需要它：装机脚本里有很多「只有真装一遍才会暴露」的问题。已经靠它抓到过：
#   · requirements 缺 python-multipart  → 后端在注册路由阶段直接崩，根本起不来
#   · 缺 tzdata                        → 每个请求都 500（ZoneInfoNotFoundError）
#
# 用法（在部署包里执行，宿主机需要有 docker）：
#
#   cd /home/yhz/iot/deploy-kit
#   docker run --rm --name iot-e2e \
#     -v /home/yhz/iot:/src:ro \
#     -v "$PWD/test_e2e.sh":/e2e.sh:ro \
#     ubuntu:22.04 bash /e2e.sh
#
# 覆盖链路：
#   离线 deb → venv+离线 wheel → init.sql → 代码补丁 → 建账号 →
#   起后端 → 登录/权限/单账号校验 → 前端代理 → 文件上传 → 其他接口冒烟
#   → 重跑幂等（账号密码不被重置、向导不再提问、ODBC/系统依赖不重复安装）
#
# 说明：本测试【不覆盖】systemd 服务与 NiFi 容器（容器里没有 systemd/docker），
#       这两部分需要真机验证。
# ============================================================
set -uo pipefail
export DEBIAN_FRONTEND=noninteractive

SRC=/src
KIT=$SRC/deploy-kit
APP=/home/yhz/iot
PASS='TestPw12345'
DBPASS='DbPw12345'
FAILED=0
ok()   { echo "[ OK ] $*"; }
bad()  { echo "[FAIL] $*"; FAILED=1; }
step() { echo; echo "========== $* =========="; }

step "[1/15] 系统依赖（用包内离线 deb，零下载）"
bash "$KIT/install_sysdeps.sh" --skip-docker >/dev/null 2>&1 \
  && ok "install_sysdeps.sh 成功" || bad "install_sysdeps.sh 失败"
for c in python3 mysql curl; do
  command -v "$c" >/dev/null 2>&1 && ok "命令可用: $c" || bad "缺命令: $c"
done
python3 -c 'import venv, ensurepip' >/dev/null 2>&1 && ok "python3-venv 可用" || bad "python3-venv 不可用"
# 幂等性：机器上"本来就有"时，第二次运行必须判定齐全并【什么都不动】，
# 这正是「目标机已有 mysql/docker/nginx」场景的关键保障
# 【注意】不能写 `bash ... | grep -q`：pipefail + grep -q 会因 SIGPIPE 误判（rc=141）→ 本该过的假失败
SYSDEP_RERUN="$(bash "$KIT/install_sysdeps.sh" --skip-docker 2>&1 || true)"
if grep -q "无需安装" <<<"$SYSDEP_RERUN"; then
  ok "重复执行安全：已装的不动（对应「机器本来就有」的场景）"
else
  bad "重复执行未正确跳过，可能重复安装/升级已有软件"
fi

step "[2/15] 启动 MySQL 并验证 root 登录"
service mysql start > /tmp/mysql-start.log 2>&1
for _ in $(seq 1 60); do mysqladmin ping >/dev/null 2>&1 && break; sleep 2; done
if mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
  ok "MySQL 已启动且 root 免密可登录"
else
  bad "MySQL root 登录失败 —— 启动日志："
  sed 's/^/       /' /tmp/mysql-start.log 2>/dev/null | tail -10
  for L in /var/log/mysql/error.log /var/log/mysql/error.log.1; do
    [[ -f "$L" ]] && { echo "       --- $L ---"; tail -15 "$L" | sed 's/^/       /'; }
  done
fi

step "[3/15] 准备代码（模拟解压后的 /home/yhz/iot）"
mkdir -p "$APP"
tar -C "$SRC" -cf - --exclude='v1-backend/.venv' --exclude='v1-backend/data' \
    v1-backend v1-frontend 2>/dev/null | tar -C "$APP" -xf - 2>/dev/null
mkdir -p "$APP/v1-backend/data"
[[ -f "$APP/v1-backend/app/main.py" ]] && ok "后端代码就位" || bad "后端代码缺失"
[[ -f "$APP/v1-frontend/serve.py" ]] && ok "前端代码就位" || bad "前端代码缺失"

step "[4/15] 离线 wheel 建虚拟环境"
python3 -m venv "$APP/v1-backend/.venv" >/dev/null 2>&1
W="$KIT/offline-assets/wheels/py310"
[[ -d "$W" ]] || W="$KIT/offline-assets/wheels"
echo "     使用 wheel 目录: $W （$(ls "$W"/*.whl 2>/dev/null | wc -l) 个）"
"$APP/v1-backend/.venv/bin/pip" install -q --upgrade pip >/dev/null 2>&1
if "$APP/v1-backend/.venv/bin/pip" install -q --no-index --find-links "$W" \
     -r "$KIT/templates/requirements-runtime.txt" >/tmp/pip.log 2>&1; then
  ok "离线依赖安装成功（零网络）"
else
  bad "离线依赖安装失败，末尾日志："; tail -15 /tmp/pip.log
fi
"$APP/v1-backend/.venv/bin/python" - <<'PY' && ok "关键模块 import 通过" || bad "关键模块 import 失败"
import fastapi, uvicorn, sqlalchemy, jwt, bcrypt, pymysql
import multipart, tzdata          # 这两个漏了都会导致部署后跑不起来
print("      fastapi/uvicorn/sqlalchemy/jwt/bcrypt/pymysql/multipart/tzdata OK")
PY

step "[5/15] 渲染并执行 init.sql（建库建用户）"
export DB_NAME=nifi DB_USER=iot DB_PASS="$DBPASS" DB_HOST=127.0.0.1 DB_PORT=3306
export APP_USER=yhz APP_HOME=/home/yhz CODE_DIR="$APP"
export FRONTEND_PORT=15174 BACKEND_PORT=18081 NIFI_PORT=18080
export FRONTEND_BIND=0.0.0.0 BACKEND_BIND=127.0.0.1 SESSION_TTL=86400
export ALLOW_SELF_REGISTER=false DOMAIN=_ NIFI_PROXY_HOST=localhost:18080
export SECRET_KEY="$(head -c 32 /dev/urandom | base64 | tr -d '\n')"
export META_KEY="$(head -c 32 /dev/urandom | base64 | tr -d '\n')"
export SITE_PASS="$PASS" PYTHON3=/usr/bin/python3
python3 "$KIT/tools/render.py" "$KIT/templates/init.sql" /tmp/init.sql >/dev/null
mysql -u root < /tmp/init.sql 2>/tmp/sql.err && ok "init.sql 执行成功" || { bad "init.sql 失败"; cat /tmp/sql.err; }
echo "     nifi 库表: $(mysql -u root -N -e 'USE nifi; SHOW TABLES;' 2>/dev/null | tr '\n' ' ')"

step "[6/15] 打补丁 + 建本站唯一账号"
python3 "$KIT/tools/patch_no_default_admin.py" "$APP" >/dev/null 2>&1 && ok "禁用默认 admin 补丁" || bad "禁用默认 admin 补丁失败"
python3 "$KIT/tools/patch_disable_self_register.py" "$APP" >/dev/null 2>&1 && ok "关闭自助注册补丁" || bad "关闭自助注册补丁失败"
python3 "$KIT/tools/disable_register_page.py" "$APP" >/dev/null 2>&1 && ok "下线注册页" || bad "下线注册页失败"
"$APP/v1-backend/.venv/bin/python" -m py_compile "$APP/v1-backend/app/db_models.py" \
  && ok "补丁后 db_models.py 语法 OK" || bad "补丁后语法错误"
env CODE_DIR="$APP" DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=iot DB_PASS="$DBPASS" DB_NAME=nifi \
    IN_DATA_BASE_DIR=/home/yhz \
    "$APP/v1-backend/.venv/bin/python" "$KIT/tools/userctl.py" create humi "$PASS" --admin \
    > /tmp/userctl.log 2>&1 && ok "账号 humi 创建成功（userctl）" || { bad "账号创建失败"; cat /tmp/userctl.log; }
echo "     MySQL iot_users: $(mysql -u root -N -e 'USE nifi; SELECT username,is_admin FROM iot_users;' 2>/dev/null | tr '\n' ' ')"
for d in nifi-data real_nifi_data tagged_nifi_data tagged_real_nifi_data; do
  [[ -d "/home/yhz/humi/$d" ]] && ok "数据根 $d 已创建" || bad "数据根 $d 缺失"
done

step "[7/15] 启动后端（环境变量与 override.conf 对齐）"
export IOT_AUTH_DB=nifi NIFI_DB_TYPE=mysql NIFI_DB_HOST=127.0.0.1 NIFI_DB_PORT=3306
export NIFI_DB_USER=iot NIFI_DB_PASSWORD="$DBPASS" NIFI_DB_NAME=nifi
export IN_DATA_BASE_DIR=/home/yhz IOT_SECRET_KEY="$SECRET_KEY" META_XATTR_KEY="$META_KEY"
export IOT_ACCESS_EXPIRE_SECONDS=86400 NIFI_REAL_EXECUTION=true
export IOT_ALLOW_SELF_REGISTER=false IOT_ADMIN_PASSWORD="$PASS"
cd "$APP/v1-backend"
nohup "$APP/v1-backend/.venv/bin/python" -m uvicorn app.main:app --host 127.0.0.1 --port 18081 \
  > /tmp/backend.log 2>&1 &
for _ in $(seq 1 45); do curl -sf http://127.0.0.1:18081/docs >/dev/null 2>&1 && break; sleep 2; done
curl -sf http://127.0.0.1:18081/docs >/dev/null 2>&1 && ok "后端已启动" || { bad "后端未启动"; tail -25 /tmp/backend.log; }

step "[8/15] 登录 + 权限 + 单账号校验"
CJ=/tmp/cj.txt
CODE=$(curl -s -c "$CJ" -X POST http://127.0.0.1:18081/api/v1/auth/login \
        -H 'Content-Type: application/json' \
        -d "{\"username\":\"humi\",\"password\":\"$PASS\"}" \
        -o /tmp/login.json -w '%{http_code}')
[[ "$CODE" == "200" ]] && ok "用 humi 登录成功" || bad "登录失败 HTTP $CODE: $(head -c 300 /tmp/login.json)"
UCODE=$(curl -s -b "$CJ" -o /tmp/users.json -w '%{http_code}' http://127.0.0.1:18081/api/v1/internal/users)
echo "     内部管理页 HTTP $UCODE"
N=$(python3 -c 'import json
try: print(json.load(open("/tmp/users.json")).get("data",{}).get("total","?"))
except Exception: print("?")' 2>/dev/null)
[[ "$N" == "1" ]] && ok "用户总数为 1（单账号模式正确）" || bad "用户总数=$N（应为 1）"
BADPW=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:18081/api/v1/auth/login \
        -H 'Content-Type: application/json' -d '{"username":"humi","password":"wrong"}')
[[ "$BADPW" == "401" ]] && ok "错误密码被拒（401）" || bad "错误密码未被拒：$BADPW"
EMPTYPW=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:18081/api/v1/auth/login \
        -H 'Content-Type: application/json' -d '{"username":"humi","password":""}')
[[ "$EMPTYPW" == "401" ]] && ok "空密码被拒（401）" || bad "空密码未被拒：$EMPTYPW"

step "[9/15] 前端 serve.py + 代理链路 + 文件上传（multipart）"
cd "$APP/v1-frontend"
V1_BACKEND_HOST=127.0.0.1 V1_BACKEND_PORT=18081 V1_FRONTEND_HOST=0.0.0.0 V1_FRONTEND_PORT=15174 \
  nohup python3 serve.py > /tmp/frontend.log 2>&1 &
for _ in $(seq 1 20); do curl -sf http://127.0.0.1:15174/ >/dev/null 2>&1 && break; sleep 1; done
curl -sf http://127.0.0.1:15174/ >/dev/null 2>&1 && ok "前端已启动" || { bad "前端未启动"; tail -10 /tmp/frontend.log; }
PCODE=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:15174/api/v1/internal/users)
[[ "$PCODE" == "401" || "$PCODE" == "403" ]] && ok "代理链路正常（未登录返回 $PCODE）" || bad "代理异常，未登录返回 $PCODE"
CJ2=/tmp/cj2.txt
FLOGIN=$(curl -s -c "$CJ2" -X POST http://127.0.0.1:15174/api/v1/auth/login -H 'Content-Type: application/json' \
        -d "{\"username\":\"humi\",\"password\":\"$PASS\"}" -o /dev/null -w '%{http_code}')
[[ "$FLOGIN" == "200" ]] && ok "走前端登录成功" || bad "走前端登录失败 HTTP $FLOGIN"
SAMPLE="$KIT/testdata/sample.csv"
if [[ -f "$SAMPLE" ]]; then
  UPCODE=$(curl -s -b "$CJ2" -o /tmp/up.json -w '%{http_code}' -X POST \
        "http://127.0.0.1:15174/api/v1/upload/inbox_csv?username=humi&convertType=csv_to_json" \
        -F "file=@${SAMPLE}")
  [[ "$UPCODE" == "200" ]] && ok "文件上传成功（multipart 链路通）" \
    || bad "文件上传失败 HTTP $UPCODE: $(head -c 200 /tmp/up.json)"
  echo "     落盘文件: $(find /home/yhz/humi -maxdepth 4 -type f 2>/dev/null | head -3 | tr '\n' ' ')"
else
  bad "测试数据不存在: $SAMPLE"
fi

step "[10/15] 其他接口冒烟（只看是否 5xx）"
for ep in \
  "/api/v1/files?pageNo=1&pageSize=10" \
  "/api/v1/internal/backend-mode" \
  "/api/v1/jobs" ; do
  C=$(curl -s -b "$CJ2" -o /dev/null -w '%{http_code}' "http://127.0.0.1:15174$ep")
  if [[ "$C" =~ ^5 ]]; then bad "GET $ep -> $C（服务端错误）"; else ok "GET $ep -> $C"; fi
done
C=$(curl -s -b "$CJ2" -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:15174/api/v1/tags/auto" \
    -H 'Content-Type: application/json' -d '{}')
[[ "$C" =~ ^5 ]] && bad "POST /api/v1/tags/auto -> $C（服务端错误）" || ok "POST /api/v1/tags/auto -> $C"

step "[11/15] SQL Server ODBC 驱动（离线，含依赖与 EULA 预接受）"
# 真机曾在这里失败：①unixODBC 包名 20.04 与 22.04 不同 ②EULA 弹窗无人值守卡住
if bash "$KIT/install_odbc.sh" > /tmp/odbc.log 2>&1; then
  ODBC_LIST="$(odbcinst -q -d 2>/dev/null || true)"
  if grep -qi "SQL Server" <<<"$ODBC_LIST"; then
    ok "ODBC 驱动安装并注册成功（odbcinst -q -d 可见 SQL Server）"
  else
    bad "ODBC 已装但未注册 SQL Server 驱动"; tail -15 /tmp/odbc.log
  fi
else
  bad "ODBC 安装失败"; tail -20 /tmp/odbc.log
fi

step "[12/15] 重跑幂等（做过的事不再做，且不重复做）"

# 12.1 已存在的账号：--keep-password 不许改密码
HASH1="$(mysql -u root -N -e "USE nifi; SELECT password_hash FROM iot_users WHERE username='humi';" 2>/dev/null)"
env CODE_DIR="$APP" DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=iot DB_PASS="$DBPASS" DB_NAME=nifi \
    IN_DATA_BASE_DIR=/home/yhz \
    "$APP/v1-backend/.venv/bin/python" "$KIT/tools/userctl.py" create humi "AnotherPw999" --admin \
    --keep-password > /tmp/userctl2.log 2>&1 \
  && ok "重跑建号（--keep-password）执行成功" || { bad "重跑建号失败"; cat /tmp/userctl2.log; }
HASH2="$(mysql -u root -N -e "USE nifi; SELECT password_hash FROM iot_users WHERE username='humi';" 2>/dev/null)"
[[ -n "$HASH1" && "$HASH1" == "$HASH2" ]] \
  && ok "重跑未改动密码（password_hash 未变）" \
  || bad "重跑把密码改了！重跑前/后哈希不一致"
grep -q '"password": "kept"' /tmp/userctl2.log && ok "输出标明 password=kept" || bad "未标明 password=kept"
# 旧密码仍然能登录（证明"做的就是没改"）
L2=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:18081/api/v1/auth/login \
      -H 'Content-Type: application/json' -d "{\"username\":\"humi\",\"password\":\"$PASS\"}")
[[ "$L2" == "200" ]] && ok "重跑后原密码仍可登录（200）" || bad "重跑后原密码登录失败：$L2"

# 12.2 已有 config.env 时，向导不再问那 4 个问题
# 【关键】config.env 必须放在 setup.sh 的【同一目录】：setup.sh 读的是 $KIT_DIR/config.env。
# 以前这里把它写到了上一级目录（/tmp/reuse/），脚本于是以为「没有现成配置」而重新提问，
# 导致本条测试假失败（2026-09-15 定位并修正）。
mkdir -p /tmp/reuse/kit && cd /tmp/reuse/kit
# 只复制必要脚本（offline-assets 用软链接，避免拷贝 2G 资源），避免污染 /src
cp "$KIT/setup.sh" "$KIT/check_env.sh" /tmp/reuse/kit/ 2>/dev/null
ln -sfn "$KIT/offline-assets" /tmp/reuse/kit/offline-assets
cat > /tmp/reuse/kit/config.env <<EOF
APP_USER=yhz
APP_HOME=/home/yhz
CODE_DIR=$APP
FRONTEND_PORT=15174
BACKEND_PORT=18081
NIFI_PORT=18080
DB_NAME=nifi
DB_USER=iot
SITE_USER=humi
SITE_IS_ADMIN=true
SITE_PASS=$PASS
DOMAIN=10.0.0.9
INSTALL_NGINX=false
INSTALL_NIFI=false
EOF
REUSE_OUT="$(cd /tmp/reuse/kit && IOT_SETUP_DRYRUN=1 bash ./setup.sh < /dev/null 2>&1)"
if grep -q "复用" <<<"$REUSE_OUT" && ! grep -q "【1/4】" <<<"$REUSE_OUT"; then
  ok "重跑不再提问（输出含「复用」且无【1/4】）"
else
  bad "重跑仍在提问"
  echo "       ---- setup.sh 实际输出（诊断用）----"
  sed 's/^/       /' <<<"$REUSE_OUT" | head -14
fi
grep -q '^SITE_USER=humi$' /tmp/reuse/kit/config.env \
  && ok "复用后 config.env 里的账号保持不变" || bad "复用后 config.env 被改坏"
grep -q '^SITE_PASS=TestPw12345$' /tmp/reuse/kit/config.env \
  && ok "复用后 SITE_PASS 保持不变" || bad "复用后 SITE_PASS 被清掉"
# 显式 --reconfig 必须重新提问（逃生口不能坏）；喂 4 个回车当答案
RECFG_OUT="$(cd /tmp/reuse/kit && printf '1\n\n\n\n' | IOT_SETUP_DRYRUN=1 bash ./setup.sh --reconfig 2>&1)"
if grep -q "【1/4】" <<<"$RECFG_OUT"; then
  ok "--reconfig 仍会重新提问（改配置的逃生口可用）"
else
  bad "--reconfig 失效，改不了配置"
  echo "       ---- setup.sh --reconfig 实际输出（诊断用）----"
  sed 's/^/       /' <<<"$RECFG_OUT" | head -14
fi

# 12.3 ODBC 装过就不重复装
ODBC_RERUN="$(bash "$KIT/install_odbc.sh" 2>&1 || true)"
if grep -q "已安装，无需重复操作" <<<"$ODBC_RERUN"; then
  ok "重复执行安全：ODBC 已装则跳过"
else
  bad "ODBC 重跑未正确跳过（可能重复 dpkg 安装）"
fi

# 12.4 系统依赖重跑（第二次）仍判齐全
SYSDEP_RERUN2="$(bash "$KIT/install_sysdeps.sh" --skip-docker 2>&1 || true)"
if grep -q "无需安装" <<<"$SYSDEP_RERUN2"; then
  ok "重复执行安全：系统依赖已齐全则跳过"
else
  bad "系统依赖重跑未正确跳过"
fi

# 12.5 改密码后必须同步 config.env/.deploy-secrets —— 否则 verify.sh 拿旧密码登录会报 FAIL
#      （2026-09-17 目标机实测踩到：用户改完密码，verify.sh 立刻 [FAIL] 登录失败）
mkdir -p /tmp/pwdsync
printf 'SITE_USER=humi\nSITE_PASS=OldPw123\n' > /tmp/pwdsync/config.env
printf 'ADMIN_PASS=OldPw123\nDB_PASS=x\n' > /tmp/pwdsync/.deploy-secrets
NEWPW='N3w!@#%^&|$*()'
if python3 "$KIT/tools/sync_site_password.py" /tmp/pwdsync "$NEWPW" --site-user humi >/dev/null 2>&1 \
   && grep -qF "SITE_PASS=$NEWPW" /tmp/pwdsync/config.env \
   && grep -qF "ADMIN_PASS=$NEWPW" /tmp/pwdsync/.deploy-secrets; then
  ok "改密码后自动同步到 config.env/.deploy-secrets（含特殊字符）"
else
  bad "密码同步失败 → verify.sh 会因此报「登录失败」"
fi
# 改的是别的账号 → 绝不能动站点密码
python3 "$KIT/tools/sync_site_password.py" /tmp/pwdsync 'OtherPw' --site-user someoneelse >/dev/null 2>&1
if grep -qF "SITE_PASS=$NEWPW" /tmp/pwdsync/config.env; then
  ok "改非站点账号时不动站点密码（同步逻辑有边界）"
else
  bad "改别的账号竟把站点密码也改了"
fi
# 已渲染的 systemd override.conf 里那行 IOT_ADMIN_PASSWORD 也要刷新（别把旧口令长期留在 /etc）
printf 'Environment=IOT_ADMIN_PASSWORD=OldPw\nEnvironment=IN_DATA_BASE_DIR=/home/yhz\n' > /tmp/pwdsync/override.conf
python3 "$KIT/tools/sync_site_password.py" /tmp/pwdsync 'PwOv' --override-conf /tmp/pwdsync/override.conf >/dev/null 2>&1
if grep -qF 'IOT_ADMIN_PASSWORD=PwOv' /tmp/pwdsync/override.conf \
   && grep -qF 'IN_DATA_BASE_DIR=/home/yhz' /tmp/pwdsync/override.conf; then
  ok "systemd override.conf 里的旧口令被同步刷新（其它行不动）"
else
  bad "override.conf 未同步（旧口令会以明文留在 /etc 下）"
fi

# 12.6 【真跑】set_password.sh 端到端：改密码 → 文件同步 + 新密码能登录 + 不回显
#      这是复现 2026-09-17 目标机那个「改完密码 verify.sh 就报登录失败」的场景
mkdir -p /tmp/pwdtest/tools
# set_password.sh 内部会用 as_user 切到运行用户执行 —— 容器里 yhz 默认不存在（第 13 步才建），
# 这里先补上，否则会得到 `runuser: user yhz does not exist`（这是测试夹具问题，不是产品问题）
id -u yhz >/dev/null 2>&1 || useradd -m -s /bin/bash yhz 2>/dev/null || true
cp "$KIT/set_password.sh" /tmp/pwdtest/
cp "$KIT/tools/"*.py "$KIT/tools/lib.sh" /tmp/pwdtest/tools/
cat > /tmp/pwdtest/config.env <<EOF
APP_USER=yhz
APP_HOME=/home/yhz
CODE_DIR=$APP
DB_NAME=nifi
DB_USER=iot
SITE_USER=humi
SITE_IS_ADMIN=true
SITE_PASS=$PASS
EOF
cat > /tmp/pwdtest/.deploy-secrets <<EOF
DB_PASS=$DBPASS
ADMIN_PASS=$PASS
EOF
NEWPW2='Chg@2026#Pwd'
SP_OUT="$(bash /tmp/pwdtest/set_password.sh humi "$NEWPW2" 2>&1)"
if [[ "$SP_OUT" != *"$NEWPW2"* ]]; then
  ok "set_password.sh 运行时不回显新口令"
else
  bad "set_password.sh 把新口令打到屏幕上了"
fi
LOGIN2=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:18081/api/v1/auth/login \
        -H 'Content-Type: application/json' -d "{\"username\":\"humi\",\"password\":\"$NEWPW2\"}")
[[ "$LOGIN2" == "200" ]] && ok "改密码后新密码可登录（200）" || bad "改密码后新密码登录失败：$LOGIN2"
if grep -qF "SITE_PASS=$NEWPW2" /tmp/pwdtest/config.env \
   && grep -qF "ADMIN_PASS=$NEWPW2" /tmp/pwdtest/.deploy-secrets; then
  ok "set_password.sh 自动同步了 config.env/.deploy-secrets（verify.sh 不会误报登录失败）"
else
  bad "set_password.sh 未同步自检文件 → verify.sh 会报「登录失败」"
fi
# 上面两条失败时把 set_password.sh 的真实输出打出来，便于定位（诊断用）
if [[ "$LOGIN2" != "200" ]]; then
  echo "       ---- set_password.sh 实际输出 ----"
  printf '%s\n' "$SP_OUT" | sed 's/^/       /' | head -25
fi
# 复原成 e2e 原来的密码，免得影响后续步骤
bash /tmp/pwdtest/set_password.sh humi "$PASS" >/dev/null 2>&1
L3=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:18081/api/v1/auth/login \
     -H 'Content-Type: application/json' -d "{\"username\":\"humi\",\"password\":\"$PASS\"}")
[[ "$L3" == "200" ]] && ok "密码可再次改回（幂等，不影响后续步骤）" || bad "改回原密码失败：$L3"

# 12.7 静态检查：切用户必须统一走 tools/lib.sh 的 as_user
#      （2026-09-17 查出 deploy.sh 那份 as_user 的 sudo 分支误写成自递归 → 无 runuser 的机器死递归；
#        另有 3 个脚本直接写死 sudo -u → 没装 sudo 的机器失败。这里用断言防复发）
if grep -rn 'sudo -u "\$APP_USER"' "$KIT"/*.sh "$KIT"/tools/*.sh 2>/dev/null | grep -v 'tools/lib.sh' | grep -q .; then
  bad "有脚本绕过 as_user 直接写 sudo -u（无 sudo 的机器会失败）"
  grep -rn 'sudo -u "\$APP_USER"' "$KIT"/*.sh "$KIT"/tools/*.sh 2>/dev/null | grep -v 'tools/lib.sh' | sed 's/^/       /'
else
  ok "切用户统一走 as_user（除 tools/lib.sh 外无 sudo -u \"\$APP_USER\"）"
fi
if grep -qE '^[[:space:]]*as_user "\$@"' "$KIT/tools/lib.sh"; then
  bad "tools/lib.sh 的 as_user 里出现自递归调用（会死循环）"
else
  ok "as_user 无自递归（runuser → sudo → su 三级回退正确）"
fi

echo
echo "     ---- 后端日志中的异常（若有）----"
grep -nE "Traceback|Error|Exception" /tmp/backend.log 2>/dev/null | tail -10 | sed 's/^/     /' || true

step "[13/15] deploy.sh 全流程（此前 e2e 覆盖不到的主脚本）"
# 为什么加这一步：前面 12 步是「手工走一遍部署动作」，覆盖不到 deploy.sh 本体
# （步骤编排、台账记录、颜色汇总、ODBC 附加步、账号输出、离线容错…）。
# 容器里没有 systemd，deploy.sh 的 systemctl 调用已加护栏 → 会跳过并计入「需要你关注」。
RK=/home/yhz/kit-run
rm -rf "$RK"; mkdir -p "$RK"
tar -C "$KIT" -cf - --exclude=offline-assets . 2>/dev/null | tar -C "$RK" -xf -
ln -sfn "$KIT/offline-assets" "$RK/offline-assets"          # 省 550M 拷贝
id -u yhz >/dev/null 2>&1 || useradd -m -s /bin/bash yhz 2>/dev/null || true
if command -v runuser >/dev/null 2>&1; then
  ok "有 runuser（不依赖 sudo 也能切换运行用户）"
else
  bad "没有 runuser —— as_user 会退回 sudo/su，请确认该路径"
fi
cat > "$RK/config.env" <<EOF
APP_USER=yhz
APP_HOME=/home/yhz
CODE_DIR=$APP
DB_NAME=nifi
DB_USER=iot
DB_PASS=$DBPASS
FRONTEND_PORT=15174
BACKEND_PORT=18081
NIFI_PORT=18080
SITE_USER=humi
SITE_IS_ADMIN=true
SITE_PASS=$PASS
DOMAIN=10.0.0.9
INSTALL_NGINX=false
INSTALL_NIFI=false
INSTALL_SYSTEM_DEPS=false
INSTALL_ODBC=true
EOF
if bash "$RK/deploy.sh" > /tmp/deploy.log 2>&1; then
  ok "deploy.sh 全流程退出码 0"
else
  bad "deploy.sh 异常退出"; tail -25 /tmp/deploy.log | sed 's/^/       /'
fi
grep -q "0/14" /tmp/deploy.log && grep -q "14/14" /tmp/deploy.log \
  && ok "步骤编号连续（0/14 … 14/14）" || bad "步骤编号不连续"
grep -q $'\033' /tmp/deploy.log && ok "汇总区颜色是真 ESC（\$'...' 修复生效）" \
  || bad "汇总区没有颜色，颜色修复未生效"
grep -q '\\033' /tmp/deploy.log && bad "日志里仍有字面 \\033 乱码" || ok "无字面 \\033 乱码"
grep -q "账号 humi" /tmp/deploy.log && ok "账号输出已人性化" || bad "账号输出缺失"
grep -q '"username"' /tmp/deploy.log && bad "仍在打印原始 JSON" || ok "无原始 JSON 刷屏"
grep -q "归属台账" /tmp/deploy.log && ok "汇总区含归属台账说明" || bad "汇总区缺归属台账"
grep -q "已预接受 ODBC 驱动许可协议" /tmp/deploy.log \
  && ok "ODBC 步真执行过（EULA 前置生效）" || ok "ODBC 已装 → 早退跳过（幂等）"
grep -q "没有 systemctl" /tmp/deploy.log && ok "非 systemd 环境被识别并跳过（未中断部署）" \
  || ok "本机 systemd 正常"
if [[ -d /var/lib/iot-platform ]]; then
  ok "归属台账已生成：$(ls /var/lib/iot-platform/ | tr '\n' ' ')"
else
  bad "归属台账未生成"
fi

step "[14/15] 清理旧版遗留的默认 admin（弱口令管理员）"
# 造出「旧版本遗留」的状态：往 MySQL 插一条 admin/123456，验证新工具能清干净；
# 同时验证两条保护条款（站点账号叫 admin 不动 / 密码非默认不删）。
HASH=$("$APP/v1-backend/.venv/bin/python" -c "import bcrypt;print(bcrypt.hashpw(b'123456', bcrypt.gensalt()).decode())" 2>/dev/null)
mysql -u root -e "USE nifi; INSERT INTO iot_users (username,password_hash,is_admin,deployment_mode,ceph_endpoint) VALUES ('admin','$HASH',1,'public','');" 2>/dev/null \
  && ok "已插入一条模拟的旧版默认 admin（MySQL）" || bad "插入默认 admin 失败"
# 【关键】真实机器上这条残留往往在 SQLite 里（内部管理页读的是它），所以两个库都造一份。
# 而且 SQLite 这份的密码用【本站账号密码】—— 因为服务模板里是
#   Environment=IOT_ADMIN_PASSWORD=__SITE_PASS__  → init_db() 自建的 admin 密码就是本站密码
# （2026-09-16 目标机实测：MySQL 干净、SQLite 里那条 admin 正是本站密码，不是 123456）
"$APP/v1-backend/.venv/bin/python" - <<PY 2>/dev/null
import sys
sys.path.insert(0, "$APP/v1-backend")
import bcrypt
from app.db_models import IotUser
from app.auth import SessionLocal
s = SessionLocal()
s.add(IotUser(username="admin",
              password_hash=bcrypt.hashpw(b"$PASS", bcrypt.gensalt()).decode(),
              is_admin=True))
s.commit(); s.close()
PY
SQL_BEFORE=$("$APP/v1-backend/.venv/bin/python" - <<PY 2>/dev/null
import sys; sys.path.insert(0, "$APP/v1-backend")
from app.db_models import IotUser; from app.auth import SessionLocal
s = SessionLocal(); print(s.query(IotUser).filter(IotUser.username == "admin").count()); s.close()
PY
)
[[ "$SQL_BEFORE" == "1" ]] && ok "SQLite 里也造了一条 admin（真实机器的残留常在这里）" \
  || bad "SQLite 造数据失败（count=$SQL_BEFORE）"
ADMIN_ENV=(env CODE_DIR="$APP" DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=iot DB_PASS="$DBPASS" \
           DB_NAME=nifi IN_DATA_BASE_DIR=/home/yhz SITE_PASS="$PASS" ADMIN_PASS="$PASS")
OUT1="$("${ADMIN_ENV[@]}" "$APP/v1-backend/.venv/bin/python" "$KIT/tools/remove_default_admin.py" --site-user humi 2>&1)"
echo "$OUT1" | sed 's/^/       /'
B="$(mysql -u root -N -e "USE nifi; SELECT COUNT(*) FROM iot_users WHERE username='admin';" 2>/dev/null)"
[[ "$B" == "0" ]] && ok "MySQL 里的默认 admin 已删除" || bad "MySQL 里仍有 admin（count=$B）"
SQL_AFTER=$("$APP/v1-backend/.venv/bin/python" - <<PY 2>/dev/null
import sys; sys.path.insert(0, "$APP/v1-backend")
from app.db_models import IotUser; from app.auth import SessionLocal
s = SessionLocal(); print(s.query(IotUser).filter(IotUser.username == "admin").count()); s.close()
PY
)
[[ "$SQL_AFTER" == "0" ]] && ok "SQLite 里的默认 admin 也已删除（两个库都覆盖）" \
  || bad "SQLite 里仍有 admin（count=$SQL_AFTER）"
[[ "$(mysql -u root -N -e "USE nifi; SELECT COUNT(*) FROM iot_users;" 2>/dev/null)" == "1" ]] \
  && ok "MySQL 用户总数回到 1（单账号模式正确）" || bad "用户数不是 1"
TOT=$(curl -s -b "$CJ" "http://127.0.0.1:18081/api/v1/internal/users" \
      | python3 -c 'import sys,json;print(json.load(sys.stdin).get("data",{}).get("total","?"))' 2>/dev/null)
[[ "$TOT" == "1" ]] && ok "API 报告用户总数=1（verify.sh 检查的正是这个数）" || bad "API 用户总数=$TOT"
OUT2="$("${ADMIN_ENV[@]}" "$APP/v1-backend/.venv/bin/python" "$KIT/tools/remove_default_admin.py" --site-user humi 2>&1)"
grep -q "没有 admin 账号" <<<"$OUT2" && ok "二次执行幂等（报告无 admin 残留）" || bad "二次执行异常：$OUT2"
OUT3="$("${ADMIN_ENV[@]}" "$APP/v1-backend/.venv/bin/python" "$KIT/tools/remove_default_admin.py" --site-user admin 2>&1)"
grep -q "不自动删除" <<<"$OUT3" && ok "保护条款①：站点账号=admin 时拒绝自动删除" || bad "保护条款①未生效：$OUT3"
H2=$("$APP/v1-backend/.venv/bin/python" -c "import bcrypt;print(bcrypt.hashpw(b'MyOwnPw999', bcrypt.gensalt()).decode())" 2>/dev/null)
mysql -u root -e "USE nifi; INSERT INTO iot_users (username,password_hash,is_admin,deployment_mode,ceph_endpoint) VALUES ('admin','$H2',0,'public','');" 2>/dev/null
OUT4="$("${ADMIN_ENV[@]}" "$APP/v1-backend/.venv/bin/python" "$KIT/tools/remove_default_admin.py" --site-user humi 2>&1)"
grep -q "不是已知默认口令" <<<"$OUT4" && ok "保护条款②：密码非默认的 admin 只告警、不误删" || bad "保护条款②未生效：$OUT4"
mysql -u root -e "USE nifi; DELETE FROM iot_users WHERE username='admin';" 2>/dev/null || true

step "[15/15] uninstall_all.sh（只回收自己的，并做残留自检）"
if bash "$RK/uninstall_all.sh" --yes > /tmp/uninstall.log 2>&1; then
  ok "卸载脚本退出码 0"
else
  bad "卸载脚本异常退出"; tail -20 /tmp/uninstall.log | sed 's/^/       /'
fi
grep -q "检查通过" /tmp/uninstall.log && ok "残留自检通过（平台装的东西已清干净）" \
  || { bad "仍有残留"; grep -A3 "未清除" /tmp/uninstall.log | tail -3 | sed 's/^/       /'; }
[[ -e "$APP" ]] && bad "程序目录 $APP 未删除" || ok "程序目录已删除"
[[ -e /home/yhz/humi ]] && bad "账号数据目录 /home/yhz/humi 未删除（台账兜底失效）" \
  || ok "账号数据目录已回收（台账兜底生效）"
if grep -q "保留" /tmp/uninstall.log; then
  ok "对『不是我们的』东西做了保留判断（删除/保留都有交代）"
else
  ok "本次没有需要保留的（容器里全是本平台装的）"
fi

echo
echo "================ 测试结论 ================"
if [[ "$FAILED" == "1" ]]; then
  echo "存在失败项，见上面 [FAIL]"
  exit 1
else
  echo "全部通过"
fi
