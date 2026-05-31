import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import { Bucket } from "./bucket.ts";

export default class Worker extends Cloudflare.Worker<Worker>()(
  "Worker",
  { main: import.meta.filename },
  Effect.gen(function* () {
    const bucket = yield* Cloudflare.R2Bucket.bind(Bucket);

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const key = request.url.split("/").pop()!;

        // Health check — smoke-tests the R2 binding
        if (key === "__health") {
          const probe = yield* Effect.gen(function* () {
            const healthKey = "__health_probe__";
            yield* bucket.put(healthKey, "ok");
            const obj = yield* bucket.get(healthKey);
            if (!obj) return "FAIL: readback null";
            const text = yield* obj.text();
            yield* bucket.delete(healthKey);
            return text === "ok" ? "ok" : `FAIL: readback "${text}"`;
          }).pipe(
            Effect.catch((e) =>
              Effect.succeed(`FAIL: ${e instanceof Error ? e.message : String(e)}`),
            ),
          );

          return HttpServerResponse.json(
            { status: probe === "ok" ? "healthy" : "unhealthy", r2: probe },
            { status: probe === "ok" ? 200 : 503 },
          );
        }

        // Root: show usage
        if (key === "") {
          return HttpServerResponse.text(
            "R2 Object Store\n" +
              "  GET  /__health — health check (smoke-tests R2)\n" +
              "  GET  /:key     — read an object\n" +
              "  PUT  /:key     — write an object\n" +
              "  DELETE /:key   — delete an object\n",
            { status: 200 },
          );
        }

        if (request.method === "GET") {
          const object = yield* bucket.get(key);
          if (object === null) return HttpServerResponse.text("Not Found", { status: 404 });
          const text = yield* object.text();
          return HttpServerResponse.text(text, { status: 200 });
        }

        if (request.method === "PUT") {
          const body = yield* request.text;
          yield* bucket.put(key, body);
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
) {}