import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Bucket } from "./bucket.ts";
import Counter from "./counter.ts";
import Room from "./room.ts";

// WorkerA hosts the Counter DO
export default Cloudflare.Worker(
  "WorkerA",
  { main: import.meta.filename },
  Effect.gen(function* () {
    const bucket = yield* Cloudflare.R2Bucket.bind(Bucket);
    const counters = yield* Counter;
    const rooms = yield* Room;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const parts = request.url.split("/").filter(Boolean);

        if (parts.length === 0) {
          return HttpServerResponse.text(
            [
              "WorkerA (hosts Counter DO)",
              "",
              "── Room (WebSocket) ──",
              "  GET  /room/:name    — join chat room",
              "  POST /room/:name    — broadcast message",
              "",
              "── Counter ──",
              "  POST /counter/:name — increment",
              "  GET  /counter/:name — get",
              "  DELETE /counter/:name — reset",
              "  GET  /tick/:n       — sequential tick",
              "",
              "── R2 ──",
              "  GET  /list?prefix=  — list objects",
              "  GET  /:key          — read",
              "  PUT  /:key          — write",
              "  DELETE /:key         — delete",
              "  GET  /__health       — R2 smoke test",
            ].join("\n"),
            { status: 200 },
          );
        }

        if (parts[0] === "chat") {
          const name = parts[1] || "lobby";
          return HttpServerResponse.text(
            `<!DOCTYPE html>
<meta charset="utf-8">
<title>Chat: ${name}</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{font:14px/1.5 system-ui,sans-serif;max-width:600px;margin:2rem auto;padding:1rem}
  #log{border:1px solid #ccc;height:300px;overflow-y:auto;padding:.5rem;margin-bottom:.5rem;background:#fafafa}
  #log div{margin:2px 0}
  form{display:flex;gap:.5rem}
  input{flex:1;padding:.4rem;border:1px solid #ccc}
  button{padding:.4rem 1rem}
</style>
<h2>Chat: ${name}</h2>
<div id="log"></div>
<form onsubmit="send();return false">
  <input id="msg" autofocus placeholder="Type a message...">
  <button>Send</button>
</form>
<div><small>Try <code>/remind 30 hello</code> for a scheduled message</small></div>
<script>
  const log=document.getElementById("log"),msg=document.getElementById("msg");
  const ws=new WebSocket(\`wss://\${location.host}/room/${name}\`);
  ws.onmessage=e => {const d=document.createElement("div");d.textContent=e.data;log.appendChild(d);log.scrollTop=log.scrollHeight};
  function send(){ws.send(msg.value);msg.value=""}
</script>`, { status: 200, headers: { "content-type": "text/html" } });
        }

        if (parts[0] === "room") {
          const name = parts[1];
          if (!name) return HttpServerResponse.text("Usage: GET /room/:name (WebSocket upgrade)", { status: 400 });
          return yield* rooms.getByName(name).fetch(request);
        }

        if (parts[0] === "counter") {
          const name = parts[1];
          if (!name) return HttpServerResponse.json({ usage: "POST/GET/DELETE /counter/:name" });
          const c = counters.getByName(name);
          if (request.method === "POST") return HttpServerResponse.json({ name, count: yield* c.increment() });
          if (request.method === "GET") return HttpServerResponse.json({ name, count: yield* c.get() });
          if (request.method === "DELETE") { yield* c.reset(); return HttpServerResponse.json({ name, count: 0 }); }
          return HttpServerResponse.text("Method Not Allowed", { status: 405 });
        }

        if (parts[0] === "tick") {
          return HttpServerResponse.json(yield* counters.getByName("tick").tick(Number(parts[1]) || 5));
        }

        if (parts[0] === "list") {
          const prefix = request.url.includes("?prefix=")
            ? request.url.split("?prefix=")[1]?.split("&")[0]
            : undefined;
          const result = yield* bucket.list(prefix ? { prefix } : undefined);
          return HttpServerResponse.json({
            objects: result.objects.map((o: any) => ({ key: o.key, size: o.size, uploaded: o.uploaded })),
            truncated: (result as any).truncated,
          });
        }

        const key = parts[0] ?? "";

        if (key === "__health") {
          const probe = yield* Effect.gen(function* () {
            yield* bucket.put("__hp__", "ok");
            const obj = yield* bucket.get("__hp__");
            if (!obj) return "FAIL";
            const text = yield* obj.text();
            yield* bucket.delete("__hp__");
            return text === "ok" ? "ok" : "FAIL";
          }).pipe(Effect.catch((e) => Effect.succeed(`FAIL: ${String(e)}`)));
          return HttpServerResponse.json(
            { status: probe === "ok" ? "healthy" : "unhealthy", r2: probe },
            { status: probe === "ok" ? 200 : 503 },
          );
        }

        if (request.method === "GET") {
          const obj = yield* bucket.get(key);
          if (!obj) return HttpServerResponse.text("Not Found", { status: 404 });
          return HttpServerResponse.text(yield* obj.text());
        }
        if (request.method === "PUT") {
          yield* bucket.put(key, yield* request.text);
          return HttpServerResponse.text("OK", { status: 201 });
        }
        if (request.method === "DELETE") {
          yield* bucket.delete(key);
          return HttpServerResponse.empty({ status: 204 });
        }
        return HttpServerResponse.text("Method Not Allowed", { status: 405 });
      }),
    };
  }).pipe(Effect.provide(Cloudflare.R2BucketBindingLive)),
);
