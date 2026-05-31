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

afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), {
  timeout: 300_000,
});

test(
  "stack has a worker URL",
  Effect.gen(function* () {
    const { workerUrl } = yield* stack;
    expect(workerUrl).toBeString();
  }),
);

test(
  "health endpoint returns healthy",
  Effect.gen(function* () {
    const { workerUrl } = yield* stack;
    const response = yield* HttpClient.get(`${workerUrl}/__health`);
    expect(response.status).toBe(200);
    const body = (yield* response.json) as { status: string; r2: string };
    expect(body.status).toBe("healthy");
    expect(body.r2).toBe("ok");
  }),
);

test(
  "PUT and GET an object round-trip",
  Effect.gen(function* () {
    const { workerUrl } = yield* stack;
    const key = `test-${Date.now()}`;
    const value = "alchemy rocks";

    const putResponse = yield* HttpClient.execute(
      HttpClientRequest.put(`${workerUrl}/${key}`).pipe(
        HttpClientRequest.setBody(HttpBody.text(value)),
      ),
    );
    expect(putResponse.status).toBe(201);

    const getResponse = yield* HttpClient.get(`${workerUrl}/${key}`);
    expect(getResponse.status).toBe(200);
    const body = yield* getResponse.text;
    expect(body).toBe(value);
  }),
);

test(
  "GET nonexistent key returns 404",
  Effect.gen(function* () {
    const { workerUrl } = yield* stack;
    const response = yield* HttpClient.get(
      `${workerUrl}/no-such-key-${Date.now()}`,
    );
    expect(response.status).toBe(404);
  }),
);
