import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as State from "alchemy/State";
import { Bucket } from "./src/bucket.ts";
import WorkerA from "./src/workerA.ts";
import WorkerB from "./src/workerB.ts";

export default Alchemy.Stack(
  "CrossWorkerDO",
  {
    providers: Cloudflare.providers(),
    state: State.localState(),
  },
  Effect.gen(function* () {
    const bucket = yield* Bucket;
    const a = yield* WorkerA;
    const b = yield* WorkerB;

    return {
      bucketName: bucket.bucketName,
      urlA: a.url,
      urlB: b.url,
    };
  }),
);
