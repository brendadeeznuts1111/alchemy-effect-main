import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/unstable/http/HttpBody";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import Stack from "../alchemy.run.ts";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test("both workers have URLs", () =>
  Effect.gen(function* () {
    const { urlA, urlB } = yield* stack;
    expect(urlA).toBeString();
    expect(urlB).toBeString();
  }),
);

test("health endpoint returns healthy", () =>
  Effect.gen(function* () {
    const { urlA } = yield* stack;
    const response = yield* HttpClient.get(`${urlA}/__health`);
    expect(response.status).toBe(200);
  }),
);

test("PUT and GET an R2 object", () =>
  Effect.gen(function* () {
    const { urlA } = yield* stack;
    const key = `test-${Date.now()}`;
    const value = "alchemy rocks";

    const put = yield* HttpClient.execute(
      HttpClientRequest.put(`${urlA}/${key}`).pipe(
        HttpClientRequest.setBody(HttpBody.text(value)),
      ),
    );
    expect(put.status).toBe(201);

    const get = yield* HttpClient.get(`${urlA}/${key}`);
    expect(get.status).toBe(200);
    expect(yield* get.text).toBe(value);
  }),
);

test("WorkerB reads counter written by WorkerA", () =>
  Effect.gen(function* () {
    const { urlA, urlB } = yield* stack;

    yield* HttpClient.execute(HttpClientRequest.post(`${urlA}/counter/shared`));
    yield* HttpClient.execute(HttpClientRequest.post(`${urlA}/counter/shared`));

    const res = yield* HttpClient.get(`${urlB}/counter/shared`);
    const body = (yield* res.json) as { name: string; count: number };
    expect(body.count).toBe(2);
  }),
);

test("tick from WorkerB", () =>
  Effect.gen(function* () {
    const { urlB } = yield* stack;
    const res = yield* HttpClient.get(`${urlB}/tick/3`);
    const values = (yield* res.json) as number[];
    expect(values).toEqual([0, 1, 2]);
  }),
  { timeout: 15_000 },
);
