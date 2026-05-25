import importlib.util
import os


def load_module():
    path = os.path.join(os.path.dirname(__file__), '..', 'examples', 'nifi_orchestrator_k8s.py')
    path = os.path.abspath(path)
    spec = importlib.util.spec_from_file_location('nifi_orch', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_create_deployment_when_not_exists(tmp_path, monkeypatch):
    module = load_module()

    state = {'created': False}

    class FakeApiException(Exception):
        def __init__(self, status):
            self.status = status

    class FakeAppsApi:
        def __init__(self, api_client=None):
            pass

        def read_namespaced_deployment(self, name, namespace):
            raise FakeApiException(404)

        def create_namespaced_deployment(self, namespace, body):
            state['created'] = True

    # monkeypatch kubernetes client classes used in module
    monkeypatch.setattr(module.client, 'AppsV1Api', FakeAppsApi)
    monkeypatch.setattr(module.client, 'exceptions', type('E', (), {'ApiException': FakeApiException}))
    # 避免真实 ConfigMap 写入，替换 _store_last_applied 为 noop
    monkeypatch.setattr(module, '_store_last_applied', lambda *a, **k: None)

    doc = {'kind': 'Deployment', 'metadata': {'name': 'd1', 'namespace': 'default'}}
    module.create_or_patch_resource(doc, api_client=None)
    assert state['created'] is True


def test_patch_deployment_when_exists(monkeypatch):
    module = load_module()

    state = {'patched': False}

    class FakeApiException(Exception):
        def __init__(self, status):
            self.status = status

    class FakeExisting:
        def to_dict(self):
            return {'mock': True}

    class FakeAppsApi:
        def __init__(self, api_client=None):
            pass

        def read_namespaced_deployment(self, name, namespace):
            return FakeExisting()

        def patch_namespaced_deployment(self, name, namespace, body):
            state['patched'] = True

    monkeypatch.setattr(module.client, 'AppsV1Api', FakeAppsApi)
    monkeypatch.setattr(module.client, 'exceptions', type('E', (), {'ApiException': FakeApiException}))
    monkeypatch.setattr(module, '_store_last_applied', lambda *a, **k: None)

    doc = {'kind': 'Deployment', 'metadata': {'name': 'd2', 'namespace': 'default'}}
    module.create_or_patch_resource(doc, api_client=None)
    assert state['patched'] is True


def test_create_configmap_when_not_exists(monkeypatch):
    module = load_module()
    state = {'created': False}

    class FakeApiException(Exception):
        def __init__(self, status):
            self.status = status

    class FakeCoreApi:
        def __init__(self, api_client=None):
            pass

        def read_namespaced_config_map(self, name, namespace):
            raise FakeApiException(404)

        def create_namespaced_config_map(self, namespace, body):
            state['created'] = True

    monkeypatch.setattr(module.client, 'AppsV1Api', lambda a=None: None)
    monkeypatch.setattr(module.client, 'CoreV1Api', FakeCoreApi)
    monkeypatch.setattr(module.client, 'exceptions', type('E', (), {'ApiException': FakeApiException}))
    monkeypatch.setattr(module, '_store_last_applied', lambda *a, **k: None)

    doc = {'kind': 'ConfigMap', 'metadata': {'name': 'cm1', 'namespace': 'default'}, 'data': {'k': 'v'}}
    module.create_or_patch_resource(doc, api_client=None)
    assert state['created'] is True


def test_patch_configmap_when_exists(monkeypatch):
    module = load_module()
    state = {'patched': False}

    class FakeApiException(Exception):
        def __init__(self, status):
            self.status = status

    class FakeExisting:
        def to_dict(self):
            return {'mock': True}

    class FakeCoreApi:
        def __init__(self, api_client=None):
            pass

        def read_namespaced_config_map(self, name, namespace):
            return FakeExisting()

        def patch_namespaced_config_map(self, name, namespace, body):
            state['patched'] = True

    monkeypatch.setattr(module.client, 'CoreV1Api', FakeCoreApi)
    monkeypatch.setattr(module.client, 'exceptions', type('E', (), {'ApiException': FakeApiException}))
    monkeypatch.setattr(module, '_store_last_applied', lambda *a, **k: None)

    doc = {'kind': 'ConfigMap', 'metadata': {'name': 'cm2', 'namespace': 'default'}, 'data': {'a': 'b'}}
    module.create_or_patch_resource(doc, api_client=None)
    assert state['patched'] is True


def test_ensure_nifi_docker_invokes_docker(monkeypatch):
    module = load_module()
    calls = []

    class FakeCompleted:
        def __init__(self, stdout='', stderr='', returncode=0):
            self.stdout = stdout
            self.stderr = stderr
            self.returncode = returncode

    def fake_run(cmd, capture_output=None, text=None):
        calls.append(cmd)
        # Simulate docker ps -a returning nothing, then docker run success, then curl success
        c = ' '.join(cmd) if isinstance(cmd, (list, tuple)) else cmd
        if 'docker ps' in c:
            return FakeCompleted(stdout='')
        if 'docker run' in c:
            return FakeCompleted(stdout='containerid')
        if 'curl' in c:
            return FakeCompleted(stdout='HTTP/1.1 200 OK')
        return FakeCompleted()

    monkeypatch.setattr(module, '_run', lambda cmd: fake_run(cmd))
    # call ensure_nifi_docker
    module.ensure_nifi_docker(image='iot-nifi-python:latest', name='iot-nifi-test')
    assert any('docker run' in str(x) for x in calls)
