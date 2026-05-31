import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";

export default class Room extends Cloudflare.DurableObjectNamespace<Room>()(
  "Rooms",
  Effect.gen(function* () {
    return Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      const sessions = new Map<string, Cloudflare.DurableWebSocket>();

      for (const socket of yield* state.getWebSockets()) {
        const attachment = socket.deserializeAttachment<{ id: string }>();
        if (attachment) sessions.set(attachment.id, socket);
      }

      return {
        fetch: Effect.gen(function* () {
          const [response, socket] = yield* Cloudflare.upgrade();
          const id = crypto.randomUUID();
          socket.serializeAttachment({ id });
          sessions.set(id, socket);
          return response;
        }),
        webSocketMessage: Effect.fnUntraced(function* (
          socket: Cloudflare.DurableWebSocket,
          message: string | ArrayBuffer,
        ) {
          const attachment = socket.deserializeAttachment<{ id: string }>();
          if (!attachment) return;
          const text = typeof message === "string" ? message : new TextDecoder().decode(message);
          for (const peer of sessions.values()) yield* peer.send(`[${attachment.id.slice(0, 8)}] ${text}`);
        }),
        broadcast: (text: string) =>
          Effect.gen(function* () {
            for (const peer of sessions.values()) yield* peer.send(`[system] ${text}`);
          }),
        webSocketClose: Effect.fnUntraced(function* (
          ws: Cloudflare.DurableWebSocket,
        ) {
          const attachment = ws.deserializeAttachment<{ id: string }>();
          if (attachment) sessions.delete(attachment.id);
        }),
      };
    });
  }),
) {}
