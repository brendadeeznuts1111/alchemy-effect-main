import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { Bucket } from "./src/bucket.ts";
import { Gateway } from "./src/ai-gateway.ts";
import WorkerA from "./src/workerA.ts";
import WorkerC from "./src/workerC.ts";

export default Alchemy.Stack(
  "CrossWorkerDO",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const bucket = yield* Bucket;
    const gateway = yield* Gateway;
    const a = yield* WorkerA;
    const c = yield* WorkerC;

    return {
      bucketName: bucket.bucketName,
      gatewayId: gateway.gatewayId,
      urlA: a.url,
      urlC: c.url,
    };
  }),
);
