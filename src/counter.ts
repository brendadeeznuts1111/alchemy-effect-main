import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";

export class Counter extends Cloudflare.DurableObjectNamespace<Counter>()(
  "Counter",
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      let count = (yield* state.storage.get<number>("count")) ?? 0;

      return {
        increment: () =>
          Effect.gen(function* () {
            count += 1;
            yield* state.storage.put("count", count);
            return count;
          }),
        get: () => Effect.succeed(count),
        reset: () =>
          Effect.gen(function* () {
            count = 0;
            yield* state.storage.put("count", 0);
            return 0;
          }),
        tick: (n: number) =>
          Effect.gen(function* () {
            const result: number[] = [];
            for (let i = 0; i < n; i++) {
              result.push(i);
              yield* Effect.sleep("100 millis");
            }
            return result;
          }),
      };
    });
  }),
) {}

export default Counter;
