import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import Counter from "./counter.ts";

export default Cloudflare.Worker(
  "WorkerC",
  { main: import.meta.filename },
  Effect.gen(function* () {
    const counters = yield* Counter;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const parts = request.url.split("/").filter(Boolean);

        if (parts.length === 0) {
          return HttpServerResponse.text(
            [
              "WorkerC — isolated Counter namespace",
              "",
              "  POST /counter/:name — increment",
              "  GET  /counter/:name — get",
              "  GET  /__health      — ok",
            ].join("\n"),
            { status: 200 },
          );
        }

        if (parts[0] === "__health") {
          return HttpServerResponse.json({ status: "healthy" });
        }

        if (parts[0] === "counter") {
          const name = parts[1];
          if (!name) return HttpServerResponse.json({ usage: "POST/GET /counter/:name" });
          const c = counters.getByName(name);
          if (request.method === "POST") return HttpServerResponse.json({ worker: "C", name, count: yield* c.increment() });
          if (request.method === "GET") return HttpServerResponse.json({ worker: "C", name, count: yield* c.get() });
          return HttpServerResponse.text("Method Not Allowed", { status: 405 });
        }

        return HttpServerResponse.text("Not Found", { status: 404 });
      }),
    };
  }),
);
