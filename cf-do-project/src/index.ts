import { DurableObject } from "cloudflare:workers";

export interface Env {
  STREAM_RELAY: DurableObjectNamespace<StreamRelayDO>;
  UPSTREAM_BASE_URL: string; // 例如 https://us-central1-aiplatform.googleapis.com
  RELAY_SECRET: string;
}

// 内部参数只应出现在本机 ↔ CF 之间；万一漏进上游 URL，会落到上游日志里。
const INTERNAL_PARAMS = ["__secret", "__reqId", "__cursor"];

// 剥离内部参数，还原 100% 纯净的上游 URL；保留 baseUrl 的子路径，同时防双写 /v1/v1
function buildUpstreamUrl(reqUrl: string, baseUrl: string, rawPath?: string): string {
  const base = baseUrl.replace(/\/+$/, "");
  let pathAndQuery = "";
  if (rawPath) {
    pathAndQuery = rawPath.startsWith("/") ? rawPath : "/" + rawPath;
  } else {
    const u = new URL(reqUrl);
    pathAndQuery = u.pathname + u.search;
  }
  // rawPath 是客户端传来的，两条分支都要剥。
  const parsed = new URL(pathAndQuery, "http://relay.invalid");
  for (const key of INTERNAL_PARAMS) parsed.searchParams.delete(key);
  pathAndQuery = parsed.pathname + parsed.search;
  // 防止两边都写了 /v1 拼出 /v1/v1
  // 1. 防重复 /v1
  if (
    base.endsWith("/v1") &&
    (pathAndQuery === "/v1" || pathAndQuery.startsWith("/v1/") || pathAndQuery.startsWith("/v1?"))
  ) {
    pathAndQuery = pathAndQuery.slice(3) || "/";
  }
  // 2. 防重复或多余的 /v1beta（兼容普通 Gemini /v1beta 结尾，以及 Vertex 的 /publishers/google 结尾）
  else if (
    (base.endsWith("/v1beta") || base.endsWith("/publishers/google")) &&
    (pathAndQuery === "/v1beta" || pathAndQuery.startsWith("/v1beta/") || pathAndQuery.startsWith("/v1beta?"))
  ) {
    pathAndQuery = pathAndQuery.slice(7) || "/";
  }
  return base + pathAndQuery;
}
// 剔除逐跳 Header，保留所有业务 Header（Authorization, x-goog-*, anthropic-* 等全保留）
function cleanHeaders(headers: Record<string, string> | Headers): Headers {
  const out = new Headers();
  const skip = new Set(["host", "connection", "upgrade", "content-length", "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions", "cf-connecting-ip", "cf-ray", "cf-ipcountry", "cf-visitor"]);
  const entries = headers instanceof Headers ? headers.entries() : Object.entries(headers);
  for (const [k, v] of entries) {
    if (!skip.has(k.toLowerCase()) && v !== undefined) out.set(k, String(v));
  }
  return out;
}
// 响应体在 Workers 里已被解压，这三类头会与实际 body 对不上，直通和隧道两条路径都要剔除。
const RESPONSE_HOP_HEADERS = new Set(["content-encoding", "content-length", "transfer-encoding"]);

function stripResponseHeaders(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((v, k) => {
    if (!RESPONSE_HOP_HEADERS.has(k.toLowerCase())) out[k] = v;
  });
  return out;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.searchParams.get("__secret") !== env.RELAY_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }

    // 非 WebSocket 请求（如 GET 模型列表、普通非流式 POST），原样透传
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      const targetUrl = buildUpstreamUrl(request.url, env.UPSTREAM_BASE_URL);
      try {
        const resp = await fetch(targetUrl, {
          method: request.method,
          headers: cleanHeaders(request.headers),
          body: request.method !== "GET" && request.method !== "HEAD" ? request.body : undefined,
        });
        return new Response(resp.body, { status: resp.status, headers: stripResponseHeaders(resp.headers) });
      } catch (err: any) {
        return new Response(
          JSON.stringify({ error: { message: "Upstream fetch failed: " + (err?.message || String(err)) } }),
          { status: 502, headers: { "Content-Type": "application/json" } },
        );
      }
    }

    const reqId = url.searchParams.get("__reqId");
    if (!reqId) return new Response("Missing __reqId", { status: 400 });

    const id = env.STREAM_RELAY.idFromName(reqId);
    return env.STREAM_RELAY.get(id).fetch(request);
  },
};
interface RelaySnapshot {
  meta: { status: number; headers: Record<string, string> } | null;
  total: number;
  finished: boolean;
  errorMsg: string | null;
}

const SNAPSHOT_KEY = "relay-state-v2";
const SNAPSHOT_TTL_MS = 10 * 60_000;
const STORAGE_BATCH_SIZE = 8;
// 单个 chunk 的上限。CF 文档只明确"接收方向"是 32 MiB，"发出方向"没写；
// 切到 512 KiB 后无论实际上限是多少都安全，顺带压小存储单值。
const MAX_CHUNK_BYTES = 512 * 1024;
// 上游长时间一个字节都不返就判定失败，避免 reader.read() 永久挂起、DO 永远等不到终态。
const UPSTREAM_IDLE_TIMEOUT_MS = 120_000;

function chunkStorageKey(index: number): string {
  return "chunk:" + index.toString().padStart(10, "0");
}

export class StreamRelayDO extends DurableObject<Env> {
  // 传输期间使用内存热缓存；结束时批量持久化，等客户端 ACK 后再删除。
  private meta: { status: number; headers: Record<string, string> } | null = null;
  private chunks: string[] = [];
  private started = false;
  private finished = false;
  private errorMsg: string | null = null;
  private terminalPersisted = false;
  private cleaning = false;
  private upstreamAbort: AbortController | null = null;
  // 取消时自增，让还在跑的 pullUpstream 在收尾阶段主动放弃，不会把状态写回去。
  private session = 0;

  async fetch(request: Request): Promise<Response> {
    if (this.cleaning) {
      return new Response("Relay is being cleaned up", { status: 409 });
    }

    try {
      await this.restoreTerminalSnapshot();
    } catch (err: any) {
      return new Response("Corrupt relay snapshot: " + (err?.message || String(err)), { status: 500 });
    }

    const url = new URL(request.url);
    const parsedCursor = Number.parseInt(url.searchParams.get("__cursor") || "0", 10);
    const cursor = Number.isSafeInteger(parsedCursor) && parsedCursor >= 0 ? parsedCursor : 0;

    // chunks 只在终态落盘，所以本实例的 chunks 永远 >= 客户端 cursor，除非：
    // 1) 有快照：按快照补齐后仍超前 → 客户端的 cursor 不可信；
    // 2) 没快照：说明 DO 在拉流中途被驱逐，状态已丢，重拉会把新流的 seq 0
    //    拼到客户端已有的前缀后面 → 直接失败，不能让响应体静默错乱。
    if (cursor > this.chunks.length) {
      return new Response(
        this.terminalPersisted
          ? "Cursor is ahead of the stored stream"
          : "Relay state was lost after the Durable Object restarted; the partial stream cannot be resumed. Please retry the request.",
        { status: 409 },
      );
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);

    // fetch() 在同一个 DO 中串行执行到下一个 await；下面连续 send 可保证
    // 补发块先于随后到来的实时 broadcast。
    if (this.meta) {
      server.send(JSON.stringify({ type: "meta", ...this.meta }));
    }

    for (let i = cursor; i < this.chunks.length; i++) {
      server.send(JSON.stringify({ type: "chunk", seq: i, b64: this.chunks[i] }));
    }

    if (this.terminalPersisted && this.errorMsg) {
      server.send(JSON.stringify({
        type: "error",
        message: this.errorMsg,
        total: this.chunks.length,
      }));
    } else if (this.terminalPersisted && this.finished) {
      server.send(JSON.stringify({ type: "done", total: this.chunks.length }));
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    // 连接可能跨休眠存活：实例被唤醒后内存是空的，状态只在 storage 里。
    // 注意这条路径不经过 fetch()，所以必须自己恢复一次。
    try {
      await this.restoreTerminalSnapshot();
    } catch (err: any) {
      try {
        ws.send(JSON.stringify({ type: "error", message: "Relay state unreadable: " + (err?.message || String(err)), total: 0 }));
      } catch {}
      return;
    }

    let msg: any;
    try {
      msg = JSON.parse(
        typeof message === "string"
          ? message
          : new TextDecoder().decode(message),
      );
    } catch {
      ws.close(1003, "Invalid JSON");
      return;
    }

    if (msg.type === "ping") {
      ws.send(JSON.stringify({ type: "pong", ts: msg.ts ?? Date.now() }));
      return;
    }

    if (msg.type === "ack") {
      const expectedOutcome = this.errorMsg ? "error" : this.finished ? "done" : null;
      const ackCursor = Number(msg.cursor);

      if (
        this.terminalPersisted &&
        expectedOutcome &&
        msg.outcome === expectedOutcome &&
        Number.isSafeInteger(ackCursor) &&
        ackCursor === this.chunks.length
      ) {
        try {
          ws.send(JSON.stringify({ type: "acknowledged", cursor: ackCursor }));
        } catch {}
        await this.cleanup("Acknowledged");
      }
      return;
    }

    // 客户端主动放弃（用户点"停止"）：立刻掐断上游，不再白白计费。
    if (msg.type === "cancel") {
      this.session++;
      this.abortUpstream("Client cancelled");
      this.ctx.waitUntil(this.cleanup("Client cancelled"));
      return;
    }

    if (msg.type === "start" && !this.started && !this.finished && !this.errorMsg) {
      this.started = true;
      this.upstreamAbort = new AbortController();
      const session = this.session;

      // 相同 reqId 的后续 start 会被 started 拦住，因此重连可以安全重发。
      this.ctx.waitUntil(
        this.pullUpstream(session, this.upstreamAbort.signal, msg.path, msg.method, msg.headers, msg.bodyB64),
      );
    }
  }

  private abortUpstream(reason: string) {
    if (!this.upstreamAbort) return;
    try {
      this.upstreamAbort.abort(reason);
    } catch {}
    this.upstreamAbort = null;
  }

  async webSocketClose(_ws: WebSocket, _code: number, _reason: string) {
    // 断线不清数据。上游继续拉取；结束后保存快照，等待客户端按 cursor 重连。
  }

  async webSocketError(ws: WebSocket) {
    try {
      ws.close(1011, "WebSocket Error");
    } catch {}
  }

  private broadcast(payload: object): number {
    const encoded = JSON.stringify(payload);
    let sentCount = 0;

    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(encoded);
        sentCount++;
      } catch {}
    }

    return sentCount;
  }

  private uint8ToBase64(bytes: Uint8Array): string {
    let binary = "";
    const sliceSize = 0x8000;

    for (let offset = 0; offset < bytes.byteLength; offset += sliceSize) {
      binary += String.fromCharCode(...bytes.subarray(offset, offset + sliceSize));
    }

    return btoa(binary);
  }

  private async pullUpstream(
    session: number,
    signal: AbortSignal,
    path: string,
    method: string,
    headers: Record<string, string>,
    bodyB64: string,
  ) {
    let idleTimer: ReturnType<typeof setTimeout> | null = null;
    const armIdleTimer = () => {
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(() => {
        this.abortUpstream(`Upstream idle for ${UPSTREAM_IDLE_TIMEOUT_MS / 1000}s`);
      }, UPSTREAM_IDLE_TIMEOUT_MS);
    };

    try {
      const targetUrl = buildUpstreamUrl("", this.env.UPSTREAM_BASE_URL, path);
      const bodyBytes = bodyB64
        ? Uint8Array.from(atob(bodyB64), (c) => c.charCodeAt(0))
        : undefined;

      armIdleTimer();
      const resp = await fetch(targetUrl, {
        method,
        headers: cleanHeaders(headers),
        body: bodyBytes,
        signal,
      });

      this.meta = { status: resp.status, headers: stripResponseHeaders(resp.headers) };
      this.broadcast({ type: "meta", ...this.meta });

      if (resp.body) {
        const reader = resp.body.getReader();

        while (true) {
          armIdleTimer();
          const { done, value } = await reader.read();
          if (done) break;

          if (value && value.byteLength > 0) {
            // 一次 read() 可能返回很大的块，按 MAX_CHUNK_BYTES 切开再发。
            // 切出来的子块必须连续、按序广播：这里在循环里不插任何 await，
            // 否则别的 await 可能把后面的帧插到前面。
            const firstSeq = this.chunks.length;
            for (let offset = 0; offset < value.byteLength; offset += MAX_CHUNK_BYTES) {
              const part = value.subarray(offset, Math.min(offset + MAX_CHUNK_BYTES, value.byteLength));
              this.chunks.push(this.uint8ToBase64(part));
            }
            for (let i = firstSeq; i < this.chunks.length; i++) {
              this.broadcast({ type: "chunk", seq: i, b64: this.chunks[i] });
            }
          }
        }
      }

      this.finished = true;
      this.errorMsg = null;
    } catch (err: any) {
      // 被 cancel 掐断时状态已经交给 cleanup 处理，这里不能再写回去。
      if (session !== this.session) return;
      this.finished = false;
      this.errorMsg = err?.message || "CF Fetch Error";
    } finally {
      if (idleTimer) clearTimeout(idleTimer);
    }

    if (session !== this.session) return;

    try {
      // 终态再写一次，确保快照里的 finished/errorMsg/meta 一定存在。
      await this.persistTerminalSnapshot();
      this.terminalPersisted = true;
      await this.ctx.storage.setAlarm(Date.now() + SNAPSHOT_TTL_MS);
    } catch (storageErr: any) {
      this.errorMsg += "; snapshot failed: " + (storageErr?.message || String(storageErr));
    }

    if (this.errorMsg) {
      this.broadcast({
        type: "error",
        message: this.errorMsg,
        total: this.chunks.length,
      });
    } else {
      this.broadcast({ type: "done", total: this.chunks.length });
    }
  }

  private async persistTerminalSnapshot() {
    // 只在终态落盘。传输过程被驱逐时无法续传（客户端会收到 409 并重试），
    // 这是有意的取舍：增量落盘会给每个 chunk 多写一行，逼近免费版的日写入上限。
    await this.putChunks(0, this.chunks.length);

    // 状态最后写：看到 SNAPSHOT_KEY 就表示所有 chunk 已经持久化完成。
    await this.ctx.storage.put(SNAPSHOT_KEY, this.buildSnapshot());
  }

  // 每个 chunk 单独一个 value，避免把整个流塞进单个 Durable Object value
  // 而触发单值大小限制；按 STORAGE_BATCH_SIZE 分批减少往返。
  private async putChunks(from: number, to: number) {
    for (let start = from; start < to; start += STORAGE_BATCH_SIZE) {
      const entries: Record<string, string> = {};
      const end = Math.min(start + STORAGE_BATCH_SIZE, to);

      for (let i = start; i < end; i++) {
        entries[chunkStorageKey(i)] = this.chunks[i];
      }

      await this.ctx.storage.put(entries);
    }
  }

  private buildSnapshot(): RelaySnapshot {
    return {
      meta: this.meta,
      total: this.chunks.length,
      finished: this.finished,
      errorMsg: this.errorMsg,
    };
  }

  private async restoreTerminalSnapshot() {
    if (this.started || this.chunks.length > 0 || this.finished || this.errorMsg) {
      return;
    }

    const snapshot = await this.ctx.storage.get<RelaySnapshot>(SNAPSHOT_KEY);
    if (!snapshot) return;

    if (!Number.isSafeInteger(snapshot.total) || snapshot.total < 0) {
      throw new Error("invalid chunk count");
    }

    const restored: string[] = new Array(snapshot.total);

    for (let start = 0; start < snapshot.total; start += STORAGE_BATCH_SIZE) {
      const end = Math.min(start + STORAGE_BATCH_SIZE, snapshot.total);
      const keys: string[] = [];

      for (let i = start; i < end; i++) {
        keys.push(chunkStorageKey(i));
      }

      const values = await this.ctx.storage.get<string>(keys);
      for (let i = start; i < end; i++) {
        const value = values.get(chunkStorageKey(i));
        if (typeof value !== "string") {
          throw new Error("missing chunk " + i);
        }
        restored[i] = value;
      }
    }

    this.meta = snapshot.meta;
    this.chunks = restored;
    this.finished = snapshot.finished;
    this.errorMsg = snapshot.errorMsg;
    this.terminalPersisted = true;
    this.started = true;

    // 正常情况下不会出现"非终态快照"（只在终态落盘）。这里兜的是旧版本留下的
    // 中途快照：上游连接早已不存在，started 又会拦住重发 start，只能明确报错，
    // 否则客户端会一直重连等 done。
    if (!this.finished && !this.errorMsg) {
      this.errorMsg = "Relay interrupted: the Durable Object restarted mid-stream; the partial stream cannot be resumed. Please retry the request.";
      await this.ctx.storage.put(SNAPSHOT_KEY, this.buildSnapshot());
    }
  }

  private async cleanup(reason: string) {
    if (this.cleaning) return;
    this.cleaning = true;

    for (const socket of this.ctx.getWebSockets()) {
      try {
        socket.close(1000, reason);
      } catch {}
    }

    try {
      await this.ctx.storage.deleteAlarm();
      await this.ctx.storage.deleteAll();

      this.chunks = [];
      this.meta = null;
      this.started = false;
      this.finished = false;
      this.errorMsg = null;
      this.terminalPersisted = false;
      this.upstreamAbort = null;
    } finally {
      this.cleaning = false;
    }
  }

  async alarm() {
    await this.cleanup("Snapshot TTL expired");
  }
}
