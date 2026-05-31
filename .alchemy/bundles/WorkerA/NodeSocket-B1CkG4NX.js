import {
  $n as e,
  An as t,
  Cn as n,
  D as r,
  Ft as i,
  Hn as a,
  L as o,
  Mn as s,
  Nt as c,
  On as l,
  Qn as u,
  Rt as d,
  Yt as f,
  _a as p,
  en as m,
  gn as h,
  hi as g,
  ir as _,
  kt as v,
  lr as y,
  mt as b,
  pi as x,
  qt as S,
  sn as C,
  ui as w,
  va as T,
  wa as E,
  xa as D,
  xn as O,
  xt as k,
  yn as A,
} from "./Channel-Udu1G47X.js";
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
} from "./Socket-BzotPBfG.js";
import * as U from "node:net";
var W = E(
    D((e, t) => {
      t.exports = function () {
        throw Error(
          `ws does not work in the browser. Browser clients must use the native WebSocket object`,
        );
      };
    })(),
    1,
  ),
  NetSocket = class extends w()(
    `@effect/platform-node/NodeSocket/NetSocket`,
  ) {};
const makeNet = (t) =>
    fromDuplex(
      v((n) => {
        let r;
        return i(
          e(
            x(n, u),
            O(() => {
              r &&
                r.closed === !1 &&
                (`destroySoon` in r ? r.destroySoon() : r.destroy());
            }),
          ),
          () =>
            k((e) => {
              ((r = U.createConnection(t)),
                r.once(`connect`, () => {
                  e(A(r));
                }),
                r.on(`error`, (t) => {
                  e(c(new V({ reason: new F({ kind: `Unknown`, cause: t }) })));
                }));
            }),
        );
      }),
      t,
    ),
  fromDuplex = (r, i) =>
    s((a) => {
      let s,
        u = o(!1),
        f = a.context,
        run = (a, o) =>
          h(
            d(function* (l) {
              let d = yield* P().pipe(_(l)),
                f;
              (yield* e(
                l,
                O(() => {
                  f &&
                    (f.off(`data`, onData),
                    f.off(`end`, onEnd),
                    f.off(`error`, onError),
                    f.off(`close`, onClose));
                }),
              ),
                (f = yield* _(r, l).pipe(
                  i?.openTimeout
                    ? n({
                        duration: i.openTimeout,
                        orElse: () =>
                          c(
                            new V({
                              reason: new F({
                                kind: `Timeout`,
                                cause: Error(`Connection timed out`),
                              }),
                            }),
                          ),
                      })
                    : T,
                )),
                f.on(`end`, onEnd),
                f.on(`error`, onError),
                f.on(`close`, onClose));
              let p = yield* C(j(d)(), NetSocket, f);
              return (
                f.on(`data`, onData),
                (s = f),
                u.openUnsafe(),
                o?.onOpen && (yield* o.onOpen),
                yield* L(d)
              );
              function onData(e) {
                let t = a(e);
                S(t) && p(t);
              }
              function onEnd() {
                y(d.deferred, t);
              }
              function onError(e) {
                y(d.deferred, c(new V({ reason: new M({ cause: e }) })));
              }
              function onClose(e) {
                y(
                  d.deferred,
                  c(new V({ reason: new R({ code: e ? 1006 : 1e3 }) })),
                );
              }
            }),
          ).pipe(
            l((e) => g(f, e)),
            m(() =>
              O(() => {
                (u.closeUnsafe(), (s = void 0));
              }),
            ),
          ),
        write = (e) =>
          u.whenOpen(
            k((n) => {
              let r = s;
              if (I(e))
                return (
                  r.destroy(
                    e.code > 1e3 ? Error(`closed with code ${e.code}`) : void 0,
                  ),
                  n(t)
                );
              s.write(e, (e) => {
                n(e ? c(new V({ reason: new z({ cause: e }) })) : t);
              });
            }),
          );
      return A(
        N({
          run,
          runRaw: run,
          writer: b(A(write), () =>
            O(() => {
              !s || s.writableEnded || s.end();
            }),
          ),
        }),
      );
    }),
  makeNetChannel = (e) => r(f(makeNet(e), B())),
  G = p(makeNet, a(H));
export {
  makeNet as a,
  G as i,
  fromDuplex as n,
  makeNetChannel as o,
  W as r,
  NetSocket as t,
};
