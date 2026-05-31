import {
  Ai as e,
  Bt as t,
  Ct as n,
  D as r,
  Di as i,
  En as a,
  Fn as o,
  Ft as s,
  Ia as c,
  L as l,
  Ln as u,
  Lt as d,
  Ma as f,
  Mi as p,
  Na as m,
  Nn as h,
  On as g,
  Yn as _,
  Yt as v,
  Zt as y,
  ar as b,
  fn as x,
  fr as S,
  ht as C,
  in as w,
  jt as T,
  or as E,
  vr as D,
  wn as O,
  xn as k,
  za as A,
} from "./Channel-BjrpyLFT.js";
import {
  _ as j,
  a as M,
  d as N,
  h as P,
  i as F,
  l as I,
  m as L,
  n as R,
  o as z,
  p as B,
  r as V,
  t as H,
} from "./Socket-B5eNQsB_.js";
import * as U from "node:net";
var W = A(
    c((e, t) => {
      t.exports = function () {
        throw Error(
          `ws does not work in the browser. Browser clients must use the native WebSocket object`,
        );
      };
    })(),
    1,
  ),
  NetSocket = class extends i()(
    `@effect/platform-node/NodeSocket/NetSocket`,
  ) {};
const makeNet = (t) =>
    fromDuplex(
      T((r) => {
        let i;
        return d(
          E(
            e(r, b),
            a(() => {
              i &&
                i.closed === !1 &&
                (`destroySoon` in i ? i.destroySoon() : i.destroy());
            }),
          ),
          () =>
            n((e) => {
              ((i = U.createConnection(t)),
                i.once(`connect`, () => {
                  e(O(i));
                }),
                i.on(`error`, (t) => {
                  e(s(new V({ reason: new F({ kind: `Unknown`, cause: t }) })));
                }));
            }),
        );
      }),
      t,
    ),
  fromDuplex = (e, r) =>
    u((i) => {
      let c,
        u = l(!1),
        d = i.context,
        run = (n, i) =>
          k(
            t(function* (t) {
              let l = yield* P().pipe(S(t)),
                d;
              (yield* E(
                t,
                a(() => {
                  d &&
                    (d.off(`data`, onData),
                    d.off(`end`, onEnd),
                    d.off(`error`, onError),
                    d.off(`close`, onClose));
                }),
              ),
                (d = yield* S(e, t).pipe(
                  r?.openTimeout
                    ? g({
                        duration: r.openTimeout,
                        orElse: () =>
                          s(
                            new V({
                              reason: new F({
                                kind: `Timeout`,
                                cause: Error(`Connection timed out`),
                              }),
                            }),
                          ),
                      })
                    : m,
                )),
                d.on(`end`, onEnd),
                d.on(`error`, onError),
                d.on(`close`, onClose));
              let f = yield* x(j(l)(), NetSocket, d);
              return (
                d.on(`data`, onData),
                (c = d),
                u.openUnsafe(),
                i?.onOpen && (yield* i.onOpen),
                yield* L(l)
              );
              function onData(e) {
                let t = n(e);
                v(t) && f(t);
              }
              function onEnd() {
                D(l.deferred, o);
              }
              function onError(e) {
                D(l.deferred, s(new V({ reason: new M({ cause: e }) })));
              }
              function onClose(e) {
                D(
                  l.deferred,
                  s(new V({ reason: new R({ code: e ? 1006 : 1e3 }) })),
                );
              }
            }),
          ).pipe(
            h((e) => p(d, e)),
            w(() =>
              a(() => {
                (u.closeUnsafe(), (c = void 0));
              }),
            ),
          ),
        write = (e) =>
          u.whenOpen(
            n((t) => {
              let n = c;
              if (I(e))
                return (
                  n.destroy(
                    e.code > 1e3 ? Error(`closed with code ${e.code}`) : void 0,
                  ),
                  t(o)
                );
              c.write(e, (e) => {
                t(e ? s(new V({ reason: new z({ cause: e }) })) : o);
              });
            }),
          );
      return O(
        N({
          run,
          runRaw: run,
          writer: C(O(write), () =>
            a(() => {
              !c || c.writableEnded || c.end();
            }),
          ),
        }),
      );
    }),
  makeNetChannel = (e) => r(y(makeNet(e), B())),
  G = f(makeNet, _(H));
export {
  makeNet as a,
  G as i,
  fromDuplex as n,
  makeNetChannel as o,
  W as r,
  NetSocket as t,
};
