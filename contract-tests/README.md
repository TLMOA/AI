目的

提供一个最小可运行的契约测试示例（Postman 集合 + 环境），并示范如何在 CI 中通过 Newman 执行。此示例面向 `contract-v1` 的关键接口：export、upload、tags。

本地运行

1. 安装 newman：

```bash
npm install -g newman
```

2. 启动后端（示例假定在 `http://localhost:8081` 运行）。

3. 运行集合：

```bash
newman run contract-tests/postman_collection.json -e contract-tests/postman_env.json
```

GitHub Actions

仓库已添加 `.github/workflows/contract-tests.yml`，在 PR 与主分支 push 时会触发契约测试。请注意：CI 环境需要对接或 mock 后端（比如用 WireMock 或在 workflow 中启动后端服务）。

如何定制

- 编辑 `contract-tests/postman_env.json` 中的 `baseUrl` 指向你的测试服务地址。
- 补充/调整集合 `contract-tests/postman_collection.json` 中的请求与断言以匹配 contract-v1 的细节。
