#!/usr/bin/env bash
# ================================================================
# 本地模拟脚本：初始化公有化/私有化多用户目录结构
# 用法: bash scripts/setup_local_simulation.sh
# ================================================================
set -euo pipefail

BASE="/home/yhz"

echo "============================================"
echo "  本地模拟：创建多用户目录结构"
echo "============================================"

# -------------------------------------------------------
# 1. 所有用户（公有化 + 私有化 + 管理员）统一在 /home/yhz/{username}/ 下
# -------------------------------------------------------
echo ""
echo "[1/3] 创建所有用户存储目录..."

for user in admin wangwu zhaoliu zhangsan lisi; do
    for sub in nifi-data real_nifi_data; do
        mkdir -p "$BASE/$user/$sub"/{output_csv,output_json,output_tsv,silent_exports,inbox_csv,inbox_json,tagged_output,export_jobs}
    done
    echo "  ✓ $BASE/$user/{nifi-data,real_nifi_data}/"
done

# -------------------------------------------------------
# 2. 写入测试数据
# -------------------------------------------------------
echo ""
echo "[2/3] 写入测试数据..."

# admin 的测试数据
echo "name,value,timestamp
admin_sensor,100,$(date -Iseconds)" > "$BASE/admin/nifi-data/output_csv/admin_test.csv"
echo "export_id,status,rows
admin_job_001,done,5000" > "$BASE/admin/real_nifi_data/export_jobs/admin_export.csv"

# wangwu 的测试数据
echo "name,value,timestamp
wangwu_machine,200,$(date -Iseconds)" > "$BASE/wangwu/nifi-data/output_csv/wangwu_test.csv"

# zhaoliu 的测试数据
echo "name,value,timestamp
zhaoliu_device,300,$(date -Iseconds)" > "$BASE/zhaoliu/nifi-data/output_csv/zhaoliu_test.csv"

# zhangsan 的测试数据（公有化，直接在 /home/yhz/zhangsan/）
echo "name,value,timestamp
zhangsan_sensor,150,$(date -Iseconds)" > "$BASE/zhangsan/nifi-data/output_csv/zhangsan_test.csv"

# lisi 的测试数据（公有化，直接在 /home/yhz/lisi/）
echo "name,value,timestamp
lisi_sensor,250,$(date -Iseconds)" > "$BASE/lisi/nifi-data/output_csv/lisi_test.csv"

echo "  ✓ 测试数据已写入"

# -------------------------------------------------------
# 3. 输出结果
# -------------------------------------------------------
echo ""
echo "============================================"
echo "  目录结构创建完成！"
echo "============================================"
echo ""
echo "  最终目录布局:"
echo ""

tree -L 3 -d "$BASE/admin" "$BASE/wangwu" "$BASE/zhaoliu" "$BASE/zhangsan" "$BASE/lisi" 2>/dev/null || {
    echo "  $BASE/admin/{nifi-data,real_nifi_data}/"
    echo "  $BASE/wangwu/{nifi-data,real_nifi_data}/"
    echo "  $BASE/zhaoliu/{nifi-data,real_nifi_data}/"
    echo "  $BASE/zhangsan/{nifi-data,real_nifi_data}/"
    echo "  $BASE/lisi/{nifi-data,real_nifi_data}/"
}

echo ""
echo "  数据库用户 (deployment_mode → ceph_endpoint):"
echo "    admin     private  → $BASE/admin"
echo "    zhangsan  public   → $BASE/zhangsan"
echo "    lisi      public   → $BASE/lisi"
echo "    wangwu    private  → $BASE/wangwu"
echo "    zhaoliu   private  → $BASE/zhaoliu"
echo ""
echo "  启动后验证:"
echo "    curl -s http://localhost:8081/api/v1/internal/users | jq"
echo ""
