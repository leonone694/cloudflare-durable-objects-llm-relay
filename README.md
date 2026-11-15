# Cloudflare Durable Objects LLM Relay

自用的 LLM 连接中继：本地 HTTP 桥接器通过 WebSocket 连接 Cloudflare Durable Object，再由云端请求上游模型服务。本地桥接器与 Cloudflare 断线后，可以按已接收的数据位置重连续传。

- `cf-do-project`：部署到 Cloudflare 的 Worker 和 Durable Object。
- `cf-local-ws-bridge`：本地桥接器，默认监听 `127.0.0.1:4000`。

## 环境要求

- Node.js 22.12 或更新版本，及 npm。
- 支持 SQLite Durable Objects 的 Cloudflare 账户；免费计划也可使用，受 Cloudflare 当前配额限制。
- 可用的上游模型服务地址和 API Key。

## 部署云端

```bash
cd cf-do-project
npm ci
npx wrangler login
```

编辑 `wrangler.toml`，填写 `UPSTREAM_BASE_URL`，例如 `https://api.openai.com` 或自己的 NewAPI 地址。地址可包含 `/v1` 等基础路径，请勿在这里写入密码或 API Key。

```bash
npm run deploy
npx wrangler secret put RELAY_SECRET
```

第二条命令会提示输入自定义密码，请设置一个随机的长密码。部署后记下 Worker 域名，例如 `llm-ws-relay.example.workers.dev`。密钥未配置时，Worker 会拒绝转发。

## 启动本地桥接器

从项目根目录执行：

```bash
cd cf-local-ws-bridge
npm ci
cp .env.example .env
```

编辑 `.env`，将 `CF_DOMAIN` 改成 Worker 域名（不带 `https://` 或路径），将 `RELAY_SECRET` 改成刚才设置的相同密码。`LOCAL_PORT` 默认是 `4000`。

```bash
npm start
```

在支持 OpenAI 兼容接口的客户端中，将 API Base URL 设置为 `http://127.0.0.1:4000/v1`，API Key 仍填写上游服务的真实 Key。桥接器会转发客户端的鉴权和其他业务请求头。

`.env`、`.dev.vars`、依赖目录和临时文件已加入 Git 忽略规则，真实密钥不要写进源码或提交记录。

## 检查与本地联调

- 在 `cf-local-ws-bridge` 中执行 `npm run check`，检查桥接脚本语法。
- 在 `cf-do-project` 中执行 `npm run check`，只构建 Worker，不部署。
- 云端本地联调：将 `.dev.vars.example` 复制为 `.dev.vars` 并填写配置，再执行 `npm run dev`。桥接器设置 `CF_DOMAIN=127.0.0.1:8787` 和 `BRIDGE_TEST_HTTP=1`，使用相同密钥即可连接本地 Worker。

## 使用限制

- 续传针对本地桥接器与 Cloudflare 之间的 WebSocket 断线；客户端退出、桥接器进程重启或用户主动停止请求后，需要重新发起请求。
- 流传输过程中主要使用内存缓存，结束后才保存快照。DO 在中途重启时可能无法续传，此时返回 409，需要重试请求。
- 完成的快照会在客户端确认接收后删除，未确认的快照保留约 10 分钟。
- 请求体上限为 24 MiB；上游约 120 秒未返回数据时会被判定为超时。

## 许可证

GPL-3.0-only，见 [LICENSE](LICENSE)。
