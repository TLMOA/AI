# CI / GitHub Actions 工作流示例

目标
- 在 CI 中复现最小 NiFi 环境以验证 contract-tests、Flow 导入与镜像推/拉。

推荐 Jobs（示例）
1. `lint` / `unit-tests`
2. `build`（构建后端、构建 NiFi 流镜像可选）
3. `spin-nifi-smoke`：启动 ephemeral NiFi 容器并等待就绪
4. `import-flow`：用 `import_flow_to_registry.sh` 或 volume 将测试 Flow 提供给 NiFi
5. `prepare-data`：启动 MySQL 测试容器并导入示例数据（如导出场景需要）
6. `contract-tests`：运行 `newman` 或 pytest-based E2E
7. `cleanup`

示例片段（Actions step）
```yaml
- name: Spin up NiFi
  run: |
    docker run -d --name ci-nifi -p 8080:8080 \
      -v $GITHUB_WORKSPACE/ci-fixtures:/opt/nifi/nifi-current/data/iot apache/nifi:latest
    # poll readiness

- name: Import test flow
  run: |
    ./v2-nifi/scripts/import_flow_to_registry.sh --registry http://localhost:18080 --bucket test

- name: Run contract-tests
  run: |
    npm install -g newman
    newman run contract-tests/collection.json -e contract-tests/postman_env.json --bail

- name: Cleanup
  run: |
    docker rm -f ci-nifi || true
```

注意事项
- 在 CI 中使用 GHCR 镜像推/拉时，确保 `GITHUB_TOKEN` 有 `packages: write` 权限或使用 PAT（如组织策略要求）。
- JDBC 驱动：若 NiFi 需要 MySQL connector，可在 CI 容器中通过 volume 挂载 jar。
- 测试隔离：为 contract-tests 提供独立 DB 实例（docker compose 或服务 containers）。

验证点
- contract-tests 能在 ephemeral NiFi 上通过；流程能正确消费 `inbox` 并生成 `done`。