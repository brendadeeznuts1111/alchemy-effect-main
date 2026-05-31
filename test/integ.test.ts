import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Vitest";
import { expect } from "vitest";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/unstable/http/HttpBody";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import Stack from "../alchemy.run.ts";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll(destroy(Stack), { timeout: 300_000 });

test(
  "worker URLs",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA, urlC } = yield* stack;
      expect(urlA).toBeTruthy();
      expect(urlC).toBeTruthy();
    }),
  ),
);

test(
  "health endpoint returns healthy",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA } = yield* stack;
      const client = yield* HttpClient.HttpClient;
      const r = yield* client.get(`${urlA}/__health`);
      expect(r.status).toBe(200);
      const body = (yield* r.json) as { status: string };
      expect(body.status).toBe("healthy");
    }),
  ),
);

test(
  "PUT and GET an R2 object",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA } = yield* stack;
      const client = yield* HttpClient.HttpClient;
      const key = `test-${Date.now()}`;
      const value = "alchemy rocks";
      const put = yield* client.execute(
        HttpClientRequest.put(`${urlA}/${key}`).pipe(
          HttpClientRequest.setBody(HttpBody.text(value)),
        ),
      );
      expect(put.status).toBe(201);
      const get = yield* client.get(`${urlA}/${key}`);
      expect(get.status).toBe(200);
      expect(yield* get.text).toBe(value);
    }),
  ),
);

test(
  "GET nonexistent key returns 404",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA } = yield* stack;
      const client = yield* HttpClient.HttpClient;
      const r = yield* client.get(`${urlA}/no-such-key-${Date.now()}`);
      expect(r.status).toBe(404);
    }),
  ),
);

test(
  "Counter persists per key",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA } = yield* stack;
      const client = yield* HttpClient.HttpClient;
      const key = `test-${Date.now()}`;
      yield* client.post(`${urlA}/counter/${key}`);
      yield* client.post(`${urlA}/counter/${key}`);
      const r = yield* client.get(`${urlA}/counter/${key}`);
      const body = (yield* r.json) as { count: number };
      expect(body.count).toBe(2);
    }),
  ),
);

test(
  "tick returns sequential values",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA } = yield* stack;
      const client = yield* HttpClient.HttpClient;
      const r = yield* client.get(`${urlA}/tick/3`);
      expect(yield* r.json).toEqual([0, 1, 2]);
    }),
  ),
  { timeout: 15_000 },
);

test(
  "WorkerA and WorkerC have isolated Counter namespaces",
  Effect.suspend(() =>
    Effect.gen(function* () {
      const { urlA, urlC } = yield* stack;
      const client = yield* HttpClient.HttpClient;
      yield* client.post(`${urlA}/counter/iso`);
      yield* client.post(`${urlA}/counter/iso`);
      const rC = yield* client.get(`${urlC}/counter/iso`);
      const bodyC = (yield* rC.json) as { count: number };
      expect(bodyC.count).toBe(0);
      yield* client.post(`${urlC}/counter/iso`);
      const rA = yield* client.get(`${urlA}/counter/iso`);
      const bodyA = (yield* rA.json) as { count: number };
      expect(bodyA.count).toBe(2);
    }),
  ),
);
