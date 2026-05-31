import {
  An as e,
  Ca as t,
  Cn as n,
  D as r,
  Ft as i,
  L as a,
  Mn as o,
  Nt as s,
  On as c,
  Qn as l,
  Rt as u,
  Vn as d,
  Yt as f,
  Zn as p,
  _a as m,
  ba as h,
  cr as g,
  en as _,
  fi as v,
  ga as y,
  gn as b,
  kt as x,
  li as S,
  mi as C,
  mt as w,
  qt as T,
  rr as E,
  sn as D,
  xn as O,
  xt as k,
  yn as A,
} from "./Channel-CNB8PN6D.js";
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
} from "./Socket-CLf5YXIx.js";
import * as U from "node:net";
var W = t(
    h((e, t) => {
      t.exports = function () {
        throw Error(
          `ws does not work in the browser. Browser clients must use the native WebSocket object`,
        );
      };
    })(),
    1,
  ),
  NetSocket = class extends S()(
    `@effect/platform-node/NodeSocket/NetSocket`,
  ) {};
const makeNet = (e) =>
    fromDuplex(
      x((t) => {
        let n;
        return i(
          l(
            v(t, p),
            O(() => {
              n &&
                n.closed === !1 &&
                (`destroySoon` in n ? n.destroySoon() : n.destroy());
            }),
          ),
          () =>
            k((t) => {
              ((n = U.createConnection(e)),
                n.once(`connect`, () => {
                  t(A(n));
                }),
                n.on(`error`, (e) => {
                  t(s(new V({ reason: new F({ kind: `Unknown`, cause: e }) })));
                }));
            }),
        );
      }),
      e,
    ),
  fromDuplex = (t, r) =>
    o((i) => {
      let o,
        d = a(!1),
        f = i.context,
        run = (i, a) =>
          b(
            u(function* (c) {
              let u = yield* P().pipe(E(c)),
                f;
              (yield* l(
                c,
                O(() => {
                  f &&
                    (f.off(`data`, onData),
                    f.off(`end`, onEnd),
                    f.off(`error`, onError),
                    f.off(`close`, onClose));
                }),
              ),
                (f = yield* E(t, c).pipe(
                  r?.openTimeout
                    ? n({
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
                f.on(`end`, onEnd),
                f.on(`error`, onError),
                f.on(`close`, onClose));
              let p = yield* D(j(u)(), NetSocket, f);
              return (
                f.on(`data`, onData),
                (o = f),
                d.openUnsafe(),
                a?.onOpen && (yield* a.onOpen),
                yield* L(u)
              );
              function onData(e) {
                let t = i(e);
                T(t) && p(t);
              }
              function onEnd() {
                g(u.deferred, e);
              }
              function onError(e) {
                g(u.deferred, s(new V({ reason: new M({ cause: e }) })));
              }
              function onClose(e) {
                g(
                  u.deferred,
                  s(new V({ reason: new R({ code: e ? 1006 : 1e3 }) })),
                );
              }
            }),
          ).pipe(
            c((e) => C(f, e)),
            _(() =>
              O(() => {
                (d.closeUnsafe(), (o = void 0));
              }),
            ),
          ),
        write = (t) =>
          d.whenOpen(
            k((n) => {
              let r = o;
              if (I(t))
                return (
                  r.destroy(
                    t.code > 1e3 ? Error(`closed with code ${t.code}`) : void 0,
                  ),
                  n(e)
                );
              o.write(t, (t) => {
                n(t ? s(new V({ reason: new z({ cause: t }) })) : e);
              });
            }),
          );
      return A(
        N({
          run,
          runRaw: run,
          writer: w(A(write), () =>
            O(() => {
              !o || o.writableEnded || o.end();
            }),
          ),
        }),
      );
    }),
  makeNetChannel = (e) => r(f(makeNet(e), B())),
  G = y(makeNet, d(H));
export {
  makeNet as a,
  G as i,
  fromDuplex as n,
  makeNetChannel as o,
  W as r,
  NetSocket as t,
};
