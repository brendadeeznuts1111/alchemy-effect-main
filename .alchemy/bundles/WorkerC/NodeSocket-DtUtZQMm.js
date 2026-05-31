import {
  $n as e,
  $t as t,
  Dn as n,
  E as r,
  I as i,
  Jn as a,
  Jt as o,
  Kt as s,
  Ln as c,
  Lt as l,
  Mt as u,
  Ot as d,
  Pt as f,
  Sn as p,
  _a as m,
  ai as h,
  bn as g,
  bt as _,
  ci as v,
  da as y,
  hn as b,
  ir as x,
  jn as S,
  kn as C,
  ma as w,
  on as T,
  pt as E,
  qn as D,
  ua as O,
  ui as k,
  vn as A,
} from "./Channel-C7bUVWpS.js";
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
} from "./Socket-CIOlbnxL.js";
import * as U from "node:net";
var W = m(
    w((e, t) => {
      t.exports = function () {
        throw Error(
          `ws does not work in the browser. Browser clients must use the native WebSocket object`,
        );
      };
    })(),
    1,
  ),
  NetSocket = class extends h()(
    `@effect/platform-node/NodeSocket/NetSocket`,
  ) {};
const makeNet = (e) =>
    fromDuplex(
      d((t) => {
        let n;
        return f(
          a(
            v(t, D),
            g(() => {
              n &&
                n.closed === !1 &&
                (`destroySoon` in n ? n.destroySoon() : n.destroy());
            }),
          ),
          () =>
            _((t) => {
              ((n = U.createConnection(e)),
                n.once(`connect`, () => {
                  t(A(n));
                }),
                n.on(`error`, (e) => {
                  t(u(new V({ reason: new F({ kind: `Unknown`, cause: e }) })));
                }));
            }),
        );
      }),
      e,
    ),
  fromDuplex = (r, o) =>
    S((c) => {
      let d,
        f = i(!1),
        m = c.context,
        run = (i, c) =>
          b(
            l(function* (t) {
              let n = yield* P().pipe(e(t)),
                l;
              (yield* a(
                t,
                g(() => {
                  l &&
                    (l.off(`data`, onData),
                    l.off(`end`, onEnd),
                    l.off(`error`, onError),
                    l.off(`close`, onClose));
                }),
              ),
                (l = yield* e(r, t).pipe(
                  o?.openTimeout
                    ? p({
                        duration: o.openTimeout,
                        orElse: () =>
                          u(
                            new V({
                              reason: new F({
                                kind: `Timeout`,
                                cause: Error(`Connection timed out`),
                              }),
                            }),
                          ),
                      })
                    : y,
                )),
                l.on(`end`, onEnd),
                l.on(`error`, onError),
                l.on(`close`, onClose));
              let m = yield* T(j(n)(), NetSocket, l);
              return (
                l.on(`data`, onData),
                (d = l),
                f.openUnsafe(),
                c?.onOpen && (yield* c.onOpen),
                yield* L(n)
              );
              function onData(e) {
                let t = i(e);
                s(t) && m(t);
              }
              function onEnd() {
                x(n.deferred, C);
              }
              function onError(e) {
                x(n.deferred, u(new V({ reason: new M({ cause: e }) })));
              }
              function onClose(e) {
                x(
                  n.deferred,
                  u(new V({ reason: new R({ code: e ? 1006 : 1e3 }) })),
                );
              }
            }),
          ).pipe(
            n((e) => k(m, e)),
            t(() =>
              g(() => {
                (f.closeUnsafe(), (d = void 0));
              }),
            ),
          ),
        write = (e) =>
          f.whenOpen(
            _((t) => {
              let n = d;
              if (I(e))
                return (
                  n.destroy(
                    e.code > 1e3 ? Error(`closed with code ${e.code}`) : void 0,
                  ),
                  t(C)
                );
              d.write(e, (e) => {
                t(e ? u(new V({ reason: new z({ cause: e }) })) : C);
              });
            }),
          );
      return A(
        N({
          run,
          runRaw: run,
          writer: E(A(write), () =>
            g(() => {
              !d || d.writableEnded || d.end();
            }),
          ),
        }),
      );
    }),
  makeNetChannel = (e) => r(o(makeNet(e), B())),
  G = O(makeNet, c(H));
export {
  makeNet as a,
  G as i,
  fromDuplex as n,
  makeNetChannel as o,
  W as r,
  NetSocket as t,
};
