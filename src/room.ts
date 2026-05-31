import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";

export default class Room extends Cloudflare.DurableObjectNamespace<Room>()(
  "Rooms",
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      const sessions = new Map<string, Cloudflare.DurableWebSocket>();

      for (const socket of yield* state.getWebSockets()) {
        const a = socket.deserializeAttachment<{ id: string }>();
        if (a) sessions.set(a.id, socket);
      }

      const broadcast = (text: string) =>
        Effect.gen(function* () {
          for (const [, peer] of sessions)
            yield* Effect.sync(() => peer.send(text));
        });

      return {
        fetch: Effect.gen(function* () {
          const [response, socket] = yield* Cloudflare.upgrade();
          const id = crypto.randomUUID();
          socket.serializeAttachment({ id });
          sessions.set(id, socket);
          return response;
        }),
        broadcast,
        alarm: () =>
          Effect.gen(function* () {
            for (const event of yield* Cloudflare.processScheduledEvents)
              yield* Effect.sync(() =>
                broadcast(`[reminder] ${(event.payload as any).message}`),
              );
          }),
        webSocketMessage: Effect.fnUntraced(function* (
          socket: Cloudflare.DurableWebSocket,
          message: string | ArrayBuffer,
        ) {
          const a = socket.deserializeAttachment<{ id: string }>();
          if (!a) return;
          const text =
            typeof message === "string"
              ? message
              : new TextDecoder().decode(message);

          const remind = text.match(/^\/remind\s+(\d+)\s+(.+)$/);
          if (remind) {
            const sec = parseInt(remind[1], 10);
            const msg = remind[2];
            yield* Cloudflare.scheduleEvent(
              crypto.randomUUID(),
              new Date(Date.now() + sec * 1000),
              { message: msg },
            );
            yield* Effect.sync(() =>
              socket.send(`[system] Reminder in ${sec}s: "${msg}"`),
            );
            return;
          }

          for (const [, peer] of sessions)
            yield* Effect.sync(() =>
              peer.send(`[${a.id.slice(0, 8)}] ${text}`),
            );
        }),
        webSocketClose: Effect.fnUntraced(function* (
          ws: Cloudflare.DurableWebSocket,
          code: number,
          reason: string,
        ) {
          const a = ws.deserializeAttachment<{ id: string }>();
          if (a) sessions.delete(a.id);
          yield* ws.close(code, reason);
        }),
      };
    });
  }),
) {}
