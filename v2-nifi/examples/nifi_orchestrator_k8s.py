#!/usr/bin/env python3
"""
示例：`nifi_orchestrator` 的 K8s 适配样板（简化）
依赖：pip install kubernetes requests pyyaml

功能：
- 读取 k8s manifest 并尝试 apply（示例仅演示结构，生产需处理 create/patch/replace/回退）
- 提供幂等操作模板与简单锁示例（基于文件锁或 DB 可替换）
"""

import os
import subprocess
import shlex
import yaml
from kubernetes import client, config
from contextlib import contextmanager
import time
import fcntl

LOCK_FILE = '/tmp/nifi_orchestrator.lock'

@contextmanager
def file_lock(path=LOCK_FILE):
    fd = open(path, 'w')
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()

def load_manifest(path):
    with open(path, 'r') as f:
        docs = list(yaml.safe_load_all(f))
    return docs

def _ensure_namespace(meta):
    ns = meta.get('namespace') or 'default'
    return ns

def _backup_resource(kind, name, namespace, api_client):
    # 将现有资源以 YAML 形式保存到 namespace 内的 ConfigMap（key: <Kind>-<name>）
    try:
        if kind == 'Deployment':
            api = client.AppsV1Api(api_client)
            res = api.read_namespaced_deployment(name, namespace)
        elif kind == 'StatefulSet':
            api = client.AppsV1Api(api_client)
            res = api.read_namespaced_stateful_set(name, namespace)
        elif kind == 'Service':
            api = client.CoreV1Api(api_client)
            res = api.read_namespaced_service(name, namespace)
        else:
            return
        manifest_yaml = yaml.safe_dump(res.to_dict())
        _store_last_applied(kind, name, namespace, manifest_yaml, api_client)
    except Exception:
        # k8s 备份失败，降级到本地文件备份以防万一
        try:
            os.makedirs('/tmp/nifi_orchestrator_backup', exist_ok=True)
            with open(f'/tmp/nifi_orchestrator_backup/{kind}-{name}-{namespace}.yaml', 'w') as f:
                f.write(yaml.safe_dump(res.to_dict()))
        except Exception:
            print(f"[orchestrator] 无法备份 {kind} {namespace}/{name}")

def _store_last_applied(kind, name, namespace, manifest_yaml, api_client):
    """为每个资源创建独立的 ConfigMap 保存 last-applied manifest，名称包含 namespace/kind/name。"""
    core = client.CoreV1Api(api_client)
    safe_name = name.replace('/', '-').replace('.', '-')
    cm_name = f'nifi-orch-backup-{namespace}-{kind.lower()}-{safe_name}'
    body = {
        'metadata': {
            'name': cm_name,
            'labels': {
                'app': 'nifi-orch-backup',
                'resource': f"{kind}-{name}"
            }
        },
        'data': {'manifest.yaml': manifest_yaml}
    }
    try:
        # 若存在则 patch，否则 create
        existing = core.read_namespaced_config_map(cm_name, namespace)
        data = existing.data or {}
        data['manifest.yaml'] = manifest_yaml
        patch = {'data': data}
        core.patch_namespaced_config_map(cm_name, namespace, patch)
        print(f"[orchestrator] updated ConfigMap backup {namespace}/{cm_name}")
    except client.exceptions.ApiException as e:
        if getattr(e, 'status', None) == 404:
            core.create_namespaced_config_map(namespace, body)
            print(f"[orchestrator] created ConfigMap backup {namespace}/{cm_name}")
        else:
            print(f"[orchestrator] 存储备份到 ConfigMap 失败: {e}")
            raise

def create_or_patch_resource(doc, api_client):
    kind = doc.get('kind')
    meta = doc.get('metadata', {})
    name = meta.get('name')
    namespace = _ensure_namespace(meta)
    apps = client.AppsV1Api(api_client)
    core = client.CoreV1Api(api_client)
    try:
        # 读取现状并决定 create 或 patch
        if kind == 'Deployment':
            try:
                apps.read_namespaced_deployment(name, namespace)
                _backup_resource(kind, name, namespace, api_client)
                apps.patch_namespaced_deployment(name, namespace, doc)
                print(f"[orchestrator] patched Deployment {namespace}/{name}")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    apps.create_namespaced_deployment(namespace, doc)
                    print(f"[orchestrator] created Deployment {namespace}/{name}")
                else:
                    raise
        elif kind == 'StatefulSet':
            try:
                apps.read_namespaced_stateful_set(name, namespace)
                _backup_resource(kind, name, namespace, api_client)
                apps.patch_namespaced_stateful_set(name, namespace, doc)
                print(f"[orchestrator] patched StatefulSet {namespace}/{name}")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    apps.create_namespaced_stateful_set(namespace, doc)
                    print(f"[orchestrator] created StatefulSet {namespace}/{name}")
                else:
                    raise
        elif kind == 'Service':
            try:
                core.read_namespaced_service(name, namespace)
                _backup_resource(kind, name, namespace, api_client)
                core.patch_namespaced_service(name, namespace, doc)
                print(f"[orchestrator] patched Service {namespace}/{name}")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    core.create_namespaced_service(namespace, doc)
                    print(f"[orchestrator] created Service {namespace}/{name}")
                else:
                    raise
        elif kind == 'ConfigMap':
            try:
                core.read_namespaced_config_map(name, namespace)
                _backup_resource(kind, name, namespace, api_client)
                core.patch_namespaced_config_map(name, namespace, doc)
                print(f"[orchestrator] patched ConfigMap {namespace}/{name}")
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    core.create_namespaced_config_map(namespace, doc)
                    print(f"[orchestrator] created ConfigMap {namespace}/{name}")
                else:
                    raise
        elif kind in ('PersistentVolumeClaim', 'PVC'):
            try:
                core.read_namespaced_persistent_volume_claim(name, namespace)
                _backup_resource(kind, name, namespace, api_client)
                core.patch_namespaced_persistent_volume_claim(name, namespace, doc)
                print(f"[orchestrator] patched PVC {namespace}/{name}")
            except client.exceptions.ApiException as e:
                if getattr(e, 'status', None) == 404:
                    core.create_namespaced_persistent_volume_claim(namespace, doc)
                    print(f"[orchestrator] created PVC {namespace}/{name}")
                else:
                    raise
        else:
            print(f"[orchestrator] Unsupported kind: {kind}, skipping")
    except Exception as e:
        print(f"[orchestrator] Error applying {kind} {namespace}/{name}: {e}")
        raise

def apply_manifests(paths):
    # 加载 kubeconfig
    config.load_kube_config()
    api_client = client.ApiClient()
    for p in paths:
        docs = load_manifest(p)
        for doc in docs:
            if not doc:
                continue
            kind = doc.get('kind')
            name = doc.get('metadata', {}).get('name')
            print(f"[orchestrator] 准备应用 {kind} {name}")
            create_or_patch_resource(doc, api_client)

def rollback_from_backup():
    # 优先从 namespace 内的 ConfigMap 恢复备份
    api_client = client.ApiClient()
    core = client.CoreV1Api(api_client)
    # placeholder name (not used directly here)
    # 列出所有命名空间的 ConfigMap（带 label app=nifi-orch-backup）
    try:
        cms = core.list_config_map_for_all_namespaces(label_selector='app=nifi-orch-backup')
        for item in cms.items:
            ns = item.metadata.namespace
            data = item.data or {}
            for key, val in data.items():
                try:
                    # key 格式： <Kind>-<name>
                    doc = yaml.safe_load(val)
                    if not isinstance(doc, dict):
                        # 如果存的是 dict.to_dict() 的 YAML，尝试重建 k8s manifest 的基本结构
                        # 将其作为原始 manifest 直接应用
                        create_or_patch_resource(doc, api_client)
                    else:
                        create_or_patch_resource(doc, api_client)
                    print(f"[orchestrator] rollback applied {ns}/{key}")
                except Exception as e:
                    print(f"[orchestrator] rollback failed for {ns}/{key}: {e}")
    except Exception:
        # 若 k8s API 不可用，再回退到本地 /tmp 文件备份
        backup_dir = '/tmp/nifi_orchestrator_backup'
        if not os.path.isdir(backup_dir):
            print('[orchestrator] No backups to rollback')
            return
        for fn in os.listdir(backup_dir):
            path = os.path.join(backup_dir, fn)
            try:
                with open(path) as f:
                    doc = yaml.safe_load(f)
                create_or_patch_resource(doc, api_client)
                print(f"[orchestrator] rollback applied {fn}")
            except Exception as e:
                print(f"[orchestrator] rollback failed for {fn}: {e}")

def ensure_nifi(k8s_manifests):
    # 幂等+锁
    with file_lock():
        print("获得部署锁，开始应用 manifest")
        mode = os.environ.get('NIFI_MODE', 'k8s')
        if mode == 'docker':
            ensure_nifi_docker()
        else:
            apply_manifests(k8s_manifests)
        print("manifest 已应用（示例）")


def _run(cmd):
    print(f"[orchestrator] run: {cmd}")
    return subprocess.run(shlex.split(cmd), capture_output=True, text=True)


def ensure_nifi_docker(image='iot-nifi-python:latest', name='iot-nifi', ports=None, mounts=None):
    """简易 docker 模式下的 ensure：检查并运行 NiFi 容器（非全面实现，仅作示例）"""
    if ports is None:
        ports = {'8080': '8080'}
    if mounts is None:
        mounts = {os.path.abspath('/home/yhz/iot/real_nifi_data'): '/opt/nifi/nifi-current/data/iot'}

    # 检查是否已有容器存在
    res = _run(f"docker ps -a --filter name={name} --format '{{{{.ID}}}} {{{{.Status}}}}'")
    if res.returncode == 0 and res.stdout.strip():
        # 如果存在且退出则移除
        for line in res.stdout.strip().splitlines():
            cid = line.split()[0]
            status = ' '.join(line.split()[1:])
            if 'Exited' in status or 'Created' in status:
                _run(f"docker rm {cid}")
    # 启动容器
    port_args = ' '.join([f"-p {h}:{c}" for h, c in ports.items()])
    mount_args = ' '.join([f"-v {h}:{c}" for h, c in mounts.items()])
    cmd = f"docker run -d --name {name} {port_args} {mount_args} {image}"
    r = _run(cmd)
    if r.returncode != 0:
        print(f"[orchestrator] 启动 docker 容器失败: {r.stderr}")
        raise RuntimeError(r.stderr)
    # 等待端口就绪的简单策略
    for _ in range(10):
        time.sleep(1)
        try:
            c = _run("curl -s -I http://localhost:8080 || true")
            if c.returncode == 0 and c.stdout:
                return
        except Exception:
            pass
    print('[orchestrator] docker mode: 等待 NiFi 启动超时')

if __name__ == '__main__':
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    manifests = [
        os.path.join(base, 'k8s', 'nifi-statefulset.yaml'),
        os.path.join(base, 'k8s', 'nifi-registry-deployment.yaml')
    ]
    ensure_nifi(manifests)
