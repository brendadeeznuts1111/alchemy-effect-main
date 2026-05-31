import {
  Dn as e,
  Fa as t,
  Ha as n,
  Ia as r,
  In as i,
  It as a,
  Mt as o,
  Ni as s,
  O as c,
  Oi as l,
  Pn as u,
  Qt as d,
  R as f,
  Rn as p,
  Rt as m,
  Sn as h,
  Tn as g,
  Vt as _,
  Xn as v,
  Xt as y,
  an as b,
  gt as x,
  ji as S,
  kn as C,
  or as w,
  pn as T,
  pr as E,
  sr as D,
  wt as O,
  yr as k,
  za as A,
} from "./Channel-DhiT8qYC.js";
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
} from "./Socket-Bdlo5aJp.js";
import * as U from "node:net";
var W = n(
    A((e, t) => {
      t.exports = function () {
        throw Error(
          `ws does not work in the browser. Browser clients must use the native WebSocket object`,
        );
      };
    })(),
    1,
  ),
  NetSocket = class extends l()(
    `@effect/platform-node/NodeSocket/NetSocket`,
  ) {};
const makeNet = (t) =>
    fromDuplex(
      o((n) => {
        let r;
        return m(
          D(
            S(n, w),
            e(() => {
              r &&
                r.closed === !1 &&
                (`destroySoon` in r ? r.destroySoon() : r.destroy());
            }),
          ),
          () =>
            O((e) => {
              ((r = U.createConnection(t)),
                r.once(`connect`, () => {
                  e(g(r));
                }),
                r.on(`error`, (t) => {
                  e(a(new V({ reason: new F({ kind: `Unknown`, cause: t }) })));
                }));
            }),
        );
      }),
      t,
    ),
  fromDuplex = (t, n) =>
    p((o) => {
      let c,
        l = f(!1),
        d = o.context,
        run = (o, f) =>
          h(
            _(function* (s) {
              let u = yield* P().pipe(E(s)),
                d;
              (yield* D(
                s,
                e(() => {
                  d &&
                    (d.off(`data`, onData),
                    d.off(`end`, onEnd),
                    d.off(`error`, onError),
                    d.off(`close`, onClose));
                }),
              ),
                (d = yield* E(t, s).pipe(
                  n?.openTimeout
                    ? C({
                        duration: n.openTimeout,
                        orElse: () =>
                          a(
                            new V({
                              reason: new F({
                                kind: `Timeout`,
                                cause: Error(`Connection timed out`),
                              }),
                            }),
                          ),
                      })
                    : r,
                )),
                d.on(`end`, onEnd),
                d.on(`error`, onError),
                d.on(`close`, onClose));
              let p = yield* T(j(u)(), NetSocket, d);
              return (
                d.on(`data`, onData),
                (c = d),
                l.openUnsafe(),
                f?.onOpen && (yield* f.onOpen),
                yield* L(u)
              );
              function onData(e) {
                let t = o(e);
                y(t) && p(t);
              }
              function onEnd() {
                k(u.deferred, i);
              }
              function onError(e) {
                k(u.deferred, a(new V({ reason: new M({ cause: e }) })));
              }
              function onClose(e) {
                k(
                  u.deferred,
                  a(new V({ reason: new R({ code: e ? 1006 : 1e3 }) })),
                );
              }
            }),
          ).pipe(
            u((e) => s(d, e)),
            b(() =>
              e(() => {
                (l.closeUnsafe(), (c = void 0));
              }),
            ),
          ),
        write = (e) =>
          l.whenOpen(
            O((t) => {
              let n = c;
              if (I(e))
                return (
                  n.destroy(
                    e.code > 1e3 ? Error(`closed with code ${e.code}`) : void 0,
                  ),
                  t(i)
                );
              c.write(e, (e) => {
                t(e ? a(new V({ reason: new z({ cause: e }) })) : i);
              });
            }),
          );
      return g(
        N({
          run,
          runRaw: run,
          writer: x(g(write), () =>
            e(() => {
              !c || c.writableEnded || c.end();
            }),
          ),
        }),
      );
    }),
  makeNetChannel = (e) => c(d(makeNet(e), B())),
  G = t(makeNet, v(H));
export {
  makeNet as a,
  G as i,
  fromDuplex as n,
  makeNetChannel as o,
  W as r,
  NetSocket as t,
};
