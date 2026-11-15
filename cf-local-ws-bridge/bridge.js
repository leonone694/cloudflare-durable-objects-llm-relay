const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

// npm start 会读取本地 .env，也可以直接通过环境变量配置。
const CF_DOMAIN = process.env.CF_DOMAIN || ""; // 不要带 https:// 或斜杠！
const RELAY_SECRET = process.env.RELAY_SECRET || "";
const LOCAL_PORT = Number(process.env.LOCAL_PORT || 4000);
// 仅测试用：本地联调时允许走 http 而不是 https。
const RELAY_ORIGIN = process.env.BRIDGE_TEST_HTTP === "1" ? "http" : "https";
// ==========================

if (!CF_DOMAIN || CF_DOMAIN === "YOUR_WORKER_DOMAIN" || /[/?#\s]/.test(CF_DOMAIN)) {
  console.error("请在 .env 或环境变量中设置 CF_DOMAIN（仅域名，不带协议或路径）。");
  process.exit(1);
}
if (!RELAY_SECRET || RELAY_SECRET === "YOUR_STRONG_PASSWORD") {
  console.error("请设置 RELAY_SECRET，并与 Cloudflare Worker 的密钥保持一致；不能使用示例密码。");
  process.exit(1);
}
if (!Number.isInteger(LOCAL_PORT) || LOCAL_PORT < 1 || LOCAL_PORT > 65535) {
  console.error("LOCAL_PORT 必须是 1 到 65535 之间的整数。");
  process.exit(1);
}

const MAX_CONNECT_ATTEMPTS = 5;           // 同一次请求最多重连几次，避免死循环
const CONNECT_ATTEMPTS_BEFORE_HEADERS = 30; // 还没拿到响应头时，重连额度放宽（正常要等上游响应）
const CONNECT_TIMEOUT_MS = 10_000;        // 握手超时
// 链路活性：pong 丢失超过这个窗口就重连。握手成功后通常毫秒级就回 pong，
// 留一点余量给弱网；原来 60 秒意味着半死连接要卡住用户整整一分钟。
const ACTIVITY_TIMEOUT_MS = 30_000;
// 故意留很长：模型/Agent 正常可能几分钟不吐字，上游自己会在长时间无字时失败，
// bridge 不该比它更早放弃。
const CLIENT_IDLE_TIMEOUT_MS = 15 * 60_000;
// start 帧里的 body 是 base64（4/3 倍）。CF 单条 WS 消息 32 MiB 只写在"接收"方向，
// 32 MiB * 3/4 = 24 MiB。取自 2025-10-31 的 changelog，CF 再上调时需要同步这里。
const MAX_BODY_BYTES = 24 * 1024 * 1024;

const now = () => new Date().toLocaleTimeString();

function appendQuery(rawUrl, paramsObj) {
  const u = new URL(rawUrl, "http://localhost");
  for (const [k, v] of Object.entries(paramsObj)) u.searchParams.set(k, v);
  return u.pathname + u.search;
}

const server = http.createServer(async (req, res) => {
  const tag = crypto.randomUUID().slice(0, 6);
  console.log(`\n[${now()}][${tag}] ➡️ 收到客户端请求: ${req.method} ${req.url}`);

  // 1. GET / HEAD / OPTIONS 请求（比如拉取 /models）
  if (req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") {
    const pathWithSecret = appendQuery(req.url, { __secret: RELAY_SECRET });
    const cfUrl = `${RELAY_ORIGIN}://${CF_DOMAIN}${pathWithSecret}`;
    console.log(`[${now()}][${tag}] 🌐 正在走 HTTPS 转发至 CF: ${RELAY_ORIGIN}://${CF_DOMAIN}${req.url}`);

    try {
      const headers = { "accept-encoding": "identity" };
      for (const [k, v] of Object.entries(req.headers)) {
        if (!["host", "connection", "content-length", "accept-encoding"].includes(k.toLowerCase()) && typeof v === "string") {
          headers[k] = v;
        }
      }

      const resp = await fetch(cfUrl, { method: req.method, headers });
      const buf = Buffer.from(await resp.arrayBuffer());
      const targetHint = resp.headers.get("x-relay-target") || "(未开启回显)";

      console.log(`[${now()}][${tag}] ⬅️ CF返回状态码: ${resp.status} | 上游目标: ${targetHint} | 字节数: ${buf.length}`);
      if (resp.status >= 400) {
        console.error(`[${now()}][${tag}] ❌ 错误响应内容:`, buf.toString("utf8").slice(0, 500));
      }

      const resHeaders = {};
      resp.headers.forEach((v, k) => {
        if (!["content-encoding", "transfer-encoding", "content-length", "x-relay-target"].includes(k.toLowerCase())) {
          resHeaders[k] = v;
        }
      });
      // HEAD 没有响应体，写 0 长度即可，Node 不会尝试发 body。
      res.writeHead(resp.status, resHeaders);
      if (req.method === "HEAD") res.end();
      else res.end(buf);
    } catch (e) {
      console.error(`[${now()}][${tag}] 💥 本地连接 CF 彻底失败:`, e.cause || e);
      if (!res.headersSent) {
        res.writeHead(502, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: { message: `Bridge GET Error: ${e.message}` } }));
      } else {
        res.end();
      }
    }
    return;
  }

  // 2. POST 请求（全部走 WSS 断点续传隧道）
  const bodyChunks = [];
  let bodySize = 0;
  let tooLarge = false;
  try {
    for await (const chunk of req) {
      bodySize += chunk.length;
      // 超限后不再缓存，但仍把流读完，否则客户端可能收到 RST 而不是这个 413。
      if (bodySize > MAX_BODY_BYTES) {
        tooLarge = true;
        bodyChunks.length = 0;
        continue;
      }
      bodyChunks.push(chunk);
    }
  } catch (e) {
    console.error(`[${now()}][${tag}] ❌ 读取请求体失败:`, e.message);
    if (!res.headersSent) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: { message: "Bridge: failed to read request body" } }));
    }
    return;
  }

  if (tooLarge) {
    // 明确拒绝，而不是让 CF 用 WS 1009 神秘关掉连接再无限重连。
    console.error(`[${now()}][${tag}] ❌ 请求体超过 ${MAX_BODY_BYTES / 1024 / 1024} MiB，拒绝转发`);
    if (!res.headersSent) {
      res.writeHead(413, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        error: {
          message: `Bridge: request body exceeds ${MAX_BODY_BYTES / 1024 / 1024} MiB (base64 tunnel limit); send it upstream directly or split the input`,
          type: "request_too_large",
        },
      }));
    }
    return;
  }

  const bodyBuffer = Buffer.concat(bodyChunks);
  const bodyB64 = bodyBuffer.toString("base64");
  console.log(`[${now()}][${tag}] 📦 POST 请求体大小: ${bodyBuffer.length} 字节`);

  const reqId = crypto.randomUUID();
  let cursor = 0;
  let headersSent = false;
  let isDone = false;
  let clientAborted = false;
  let currentWs = null;
  let reconnectTimer = null;
  let connectionGeneration = 0;
  let reconnectAttempts = 0;
  let lastFrameAt = Date.now();
  let clientIdleTimer = null;

  const writeChunk = (buf) => {
    if (res.writableEnded || res.destroyed) return;
    // 保持字节透明：不能在任意上游分块之间插入 SSE 注释，分块可能位于
    // data 行、JSON 或 UTF-8 多字节字符的中间。
    res.write(buf);
  };

  const finishClient = () => {
    clearInterval(clientIdleTimer);
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  };

  // 响应对象本身出错（客户端炸了）时也要收尾，否则会成为未捕获异常。
  res.on("error", (e) => {
    console.error(`[${now()}][${tag}] 💥 本地响应写入失败:`, e.message);
    abortClient();
  });

  const abortClient = () => {
    if (!clientAborted && !isDone) {
      console.log(`[${now()}][${tag}] 🛑 客户端主动断开连接`);
      clientAborted = true;
      finishClient();
      // 通知 DO 立刻掐断上游，避免用户点了"停止"还在继续计费。
      if (currentWs && currentWs.readyState === WebSocket.OPEN) {
        try { currentWs.send(JSON.stringify({ type: "cancel" })); } catch {}
      }
      if (currentWs) currentWs.terminate();
    }
  };

  // 统一收尾：无论哪条路径失败，客户端都不能被挂着等。
  const failClient = (status, message) => {
    if (isDone || clientAborted || res.writableEnded) return;
    isDone = true;
    finishClient();
    if (currentWs) currentWs.terminate();
    console.error(`[${now()}][${tag}] ❌ 放弃本次请求: ${message}`);
    if (!headersSent) {
      res.writeHead(status, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: { message: `Bridge: ${message}` } }));
    } else {
      res.end();
    }
  };

  req.on("aborted", abortClient);
  res.on("close", () => {
    if (!res.writableEnded) abortClient();
  });

  // 兜底：重连一直失败或上游彻底静默时，别让客户端无限期挂着。
  clientIdleTimer = setInterval(() => {
    if (isDone || clientAborted) return;
    if (Date.now() - lastFrameAt > CLIENT_IDLE_TIMEOUT_MS) {
      failClient(504, `超过 ${CLIENT_IDLE_TIMEOUT_MS / 60000} 分钟没有收到上游任何数据`);
    }
  }, 30_000);

  // 重连只从这里发起，避免 close / unexpected-response 两条路径各自调度一次。
  const scheduleReconnect = (why) => {
    if (isDone || clientAborted || reconnectTimer) return;

    // 握上手过之后只容忍少量重连；还没拿到响应头时上游可能确实在排队。
    const maxAttempts = headersSent ? MAX_CONNECT_ATTEMPTS : CONNECT_ATTEMPTS_BEFORE_HEADERS;
    if (++reconnectAttempts > maxAttempts) {
      failClient(502, `WSS 重连 ${maxAttempts} 次仍失败（最后一次: ${why}）`);
      return;
    }

    console.warn(`\n[${now()}][${tag}] ⚠️ ${why}，1秒后从 cursor=${cursor} 重连 (第 ${reconnectAttempts}/${maxAttempts} 次)...`);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connectWSS();
    }, 1000);
  };

  function connectWSS() {
    if (isDone || clientAborted) return;

    const generation = ++connectionGeneration;

    const wsPath = appendQuery(req.url, {
      __reqId: reqId,
      __cursor: String(cursor),
      __secret: RELAY_SECRET,
    });

    console.log(`[${now()}][${tag}] 🔌 正在建立 WSS 连接 (cursor=${cursor}): ${RELAY_ORIGIN}://${CF_DOMAIN}${req.url}`);
    const ws = new WebSocket(`${RELAY_ORIGIN === "http" ? "ws" : "wss"}://${CF_DOMAIN}${wsPath}`, { handshakeTimeout: CONNECT_TIMEOUT_MS });
    currentWs = ws;

    // 只由 pong 刷新：chunk 变多只是说明上游在吐字，不代表这条链路还活着。
    let lastPongAt = Date.now();
    let closeCause = "远端关闭";
    const heartbeat = setInterval(() => {
      // 客户端早就走了，这条连接没有存在意义。
      if (clientAborted) {
        closeCause = "客户端已断开";
        ws.terminate();
        return;
      }
      if (ws.readyState !== WebSocket.OPEN) return;

      // 链路已半死（pong 丢失超过窗口）：立刻重连，别让用户干等。
      if (Date.now() - lastPongAt > ACTIVITY_TIMEOUT_MS) {
        closeCause = `${ACTIVITY_TIMEOUT_MS / 1000}秒未收到心跳响应`;
        console.warn(`\n[${now()}][${tag}] ⏱️ ${closeCause}，准备重连`);
        ws.terminate();
        return;
      }

      try {
        ws.send(JSON.stringify({ type: "ping", ts: Date.now() }));
      } catch {}
    }, 10_000);

    ws.on("open", () => {
      console.log(`[${now()}][${tag}] ✅ WSS 握手成功!`);
      lastPongAt = Date.now();
      reconnectAttempts = 0;
      if (clientAborted) {
        ws.terminate();
        return;
      }

      // 每次重连都发送 start；服务端按同一个 reqId 幂等处理。
      ws.send(JSON.stringify({
        type: "start",
        path: req.url,
        method: req.method,
        headers: req.headers,
        bodyB64,
      }));
    });

    ws.on("unexpected-response", (request, response) => {
      const status = response.statusCode;
      // 401/403 是配置问题，重试多少次都没用；5xx 才值得重连。
      const fatal = status === 401 || status === 403 || status === 404;
      const hint = status === 401
        ? "密码不对"
        : status === 500
          ? "DO 绑定没生效"
          : status === 409
            ? "服务端无法续传（上游流已中断，重试即可）"
            : "非预期状态";
      console.error(`[${now()}][${tag}] ❌ WSS 握手被 CF 拒绝! HTTP ${status}（${hint}）`);
      // ws 在 unexpected-response 之后不会再触发 close，必须自己收尾，否则请求永久挂起。
      // 409 是服务端明确的"无法续传"，直接失败，免得把同一个 DO 撞崩。
      if (status === 409) {
        failClient(409, `服务端无法续传（${hint}）`);
        return;
      }
      if (fatal) {
        failClient(502, `WSS 握手被拒: HTTP ${status}（${hint}）`);
        return;
      }
      scheduleReconnect(`WSS 握手被拒: HTTP ${status}`);
      ws.terminate();
    });

    ws.on("message", (raw) => {
      if (generation !== connectionGeneration || ws !== currentWs) return;

      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        closeCause = "服务端返回了无法解析的消息";
        ws.terminate();
        return;
      }

      // pong 证明链路活着，但不算"上游有数据"，不计入客户端的空闲超时。
      if (msg.type === "pong") {
        lastPongAt = Date.now();
        return;
      }

      lastFrameAt = Date.now();

      if (msg.type === "meta" && !headersSent) {
        headersSent = true;
        const responseHeaders = msg.headers || {};
        const ct = (responseHeaders["content-type"] || "").toLowerCase();
        console.log(`[${now()}][${tag}] ⬅️ 收到上游响应头: HTTP ${msg.status} | Content-Type: ${ct}`);
        res.writeHead(msg.status, responseHeaders);
      } else if (msg.type === "chunk") {
        if (!Number.isInteger(msg.seq)) return;
        if (msg.seq < cursor) return;

        if (msg.seq > cursor) {
          closeCause = `检测到数据缺口，期望 seq=${cursor}，实际 seq=${msg.seq}`;
          console.warn(`\n[${now()}][${tag}] ⚠️ ${closeCause}`);
          ws.terminate();
          return;
        }

        const chunkBuf = Buffer.from(msg.b64, "base64");
        writeChunk(chunkBuf);
        cursor++;
        if (cursor === 1 || cursor % 20 === 0) {
          process.stdout.write(`[${tag}:已收${cursor}块] `);
        }
      } else if (msg.type === "done") {
        if (Number.isInteger(msg.total) && msg.total !== cursor) {
          closeCause = `done 数量不一致，本地=${cursor}，服务端=${msg.total}`;
          console.warn(`\n[${now()}][${tag}] ⚠️ ${closeCause}，将从断点重连`);
          ws.terminate();
          return;
        }

        isDone = true;
        clearInterval(heartbeat);
        finishClient();
        ws.send(JSON.stringify({ type: "ack", outcome: "done", cursor }), () => {
          try { ws.close(1000, "ACK sent"); } catch {}
        });
        // 上游没给响应头就直接结束的情况：补一个合法响应，别让 Node 自己瞎猜。
        if (!headersSent) res.writeHead(200, { "Content-Type": "application/octet-stream" });
        res.end();
        console.log(`\n[${now()}][${tag}] 🎉 流传输完成，共 ${cursor} 块`);
      } else if (msg.type === "error") {
        if (Number.isInteger(msg.total) && msg.total !== cursor) {
          closeCause = `error 数量不一致，本地=${cursor}，服务端=${msg.total}`;
          console.warn(`\n[${now()}][${tag}] ⚠️ ${closeCause}，将从断点重连`);
          ws.terminate();
          return;
        }

        isDone = true;
        clearInterval(heartbeat);
        finishClient();
        console.error(`\n[${now()}][${tag}] ❌ CF 拉取上游出错:`, msg.message);
        ws.send(JSON.stringify({ type: "ack", outcome: "error", cursor }), () => {
          try { ws.close(1000, "Error ACK sent"); } catch {}
        });
        if (!headersSent) {
          res.writeHead(502, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: { message: msg.message } }));
        } else {
          res.end();
        }
      }
    });

    ws.on("close", (code, reasonBuffer) => {
      clearInterval(heartbeat);
      if (generation !== connectionGeneration) return;
      if (currentWs === ws) currentWs = null;
      if (isDone || clientAborted) return;

      const reason = reasonBuffer.toString() || closeCause;
      scheduleReconnect(`WSS 已断开 (code=${code}, reason=${reason})`);
    });

    ws.on("error", (err) => {
      closeCause = err.message;
      console.error(`\n[${now()}][${tag}] 💥 WS 底层报错: ${err.message}`);
    });
  }

  connectWSS();
});

server.on("error", (e) => {
  if (e.code === "EADDRINUSE") {
    console.error(`❌ 端口 ${LOCAL_PORT} 已被占用：是不是已经开着一个 bridge 了？先用 lsof -i :${LOCAL_PORT} 找到并关掉它。`);
  } else {
    console.error("❌ 本地 HTTP 服务启动失败:", e);
  }
  process.exit(1);
});

// 只监听回环地址：服务端会自动带上 secret，绑 0.0.0.0 等于把上游账号借给同网段所有人。
server.listen(LOCAL_PORT, "127.0.0.1", () => {
  console.log(`=================================================`);
  console.log(`🚀 WSS 透明桥接器已启动: http://127.0.0.1:${LOCAL_PORT}`);
  console.log(`🎯 目标 CF 节点: ${CF_DOMAIN}`);
  console.log(`=================================================`);
});
