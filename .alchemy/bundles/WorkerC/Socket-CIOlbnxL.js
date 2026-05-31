import {
  $n as e,
  $r as t,
  An as n,
  Bt as r,
  Ct as i,
  Dn as a,
  Dt as o,
  F as s,
  G as c,
  Gt as l,
  H as u,
  I as d,
  Ii as f,
  J as p,
  Jt as m,
  Kt as h,
  Lt as g,
  M as ee,
  Mt as _,
  P as te,
  Pn as v,
  Pt as y,
  Qi as b,
  Qr as x,
  R as ne,
  Si as re,
  Sn as ie,
  St as ae,
  Ti as oe,
  U as S,
  V as C,
  X as w,
  Xn as se,
  Zn as T,
  Zr as E,
  ai as D,
  bi as O,
  bn as k,
  cn as ce,
  cr as A,
  d as le,
  dr as ue,
  ga as de,
  hn as j,
  ir as M,
  j as N,
  jn as P,
  jt as F,
  k as I,
  kn as L,
  la as R,
  ln as z,
  nt as B,
  on as V,
  or as H,
  pr as fe,
  pt as U,
  q as pe,
  rn as me,
  rr as W,
  sa as he,
  sn as ge,
  tt as _e,
  ui as ve,
  vn as G,
  wr as ye,
  xr as be,
  yn as xe,
  zt as Se,
} from "./Channel-C7bUVWpS.js";
const K = `~effect/FiberSet`,
  isFiberSet = (e) => b(e, K),
  Ce = {
    [K]: K,
    [Symbol.iterator]() {
      return this.state._tag === `Closed`
        ? O()
        : this.state.backing[Symbol.iterator]();
    },
    ...f,
    toJSON() {
      return { _id: `FiberMap`, state: this.state };
    },
  },
  makeUnsafe = (e, t) => {
    let n = Object.create(Ce);
    return ((n.state = { _tag: `Open`, backing: e }), (n.deferred = t), n);
  },
  q = de(
    () =>
      U(
        k(() => makeUnsafe(new Set(), A())),
        (e) =>
          xe(() => {
            let t = e.state;
            if (t._tag === `Closed`) return L;
            e.state = { _tag: `Closed` };
            let n = t.backing;
            return ne(n).pipe(H(e.deferred));
          }),
      ),
    `make`,
  ),
  we = t(E(be, x(-1))),
  J = R(
    (e) => isFiberSet(e[0]),
    (e, t, n) => {
      if (e.state._tag === `Closed`) {
        t.interruptUnsafe(-1);
        return;
      } else if (e.state.backing.has(t)) return;
      (e.state.backing.add(t),
        t.addObserver((r) => {
          e.state._tag !== `Closed` &&
            (e.state.backing.delete(t),
            ue(r) &&
              (n?.propagateInterruption === !0 ? !we(r.cause) : !ye(r.cause)) &&
              M(e.deferred, r));
        }));
    },
  ),
  Y = (function () {
    let e;
    return () => (e === void 0 && (e = ce(l)), e);
  })(),
  run = function () {
    let e = arguments[0];
    if (!h(arguments[1])) {
      let t = arguments[1];
      return (n) => runImpl(e, n, t);
    }
    return runImpl(e, arguments[1], arguments[2]);
  },
  runImpl = (e, t, n) =>
    P((r) => {
      if (e.state._tag === `Closed`) return k(Y);
      let i = z(r.context)(t);
      return (J(e, i, n), G(i));
    }),
  runtime = (e) => () =>
    m(o(), (t) => {
      let n = z(t);
      return (t, r) => {
        if (e.state._tag === `Closed`) return Y();
        let i = n(t, r);
        return (J(e, i), i);
      };
    }),
  join = (e) => W(e.deferred),
  X = `~effect/socket/Socket`,
  Z = D(`effect/socket/Socket`),
  make = (e) =>
    Z.of({
      [X]: X,
      runRaw: e.runRaw,
      run:
        e.run ??
        ((t, n) =>
          e.runRaw(
            (e) =>
              typeof e == `string`
                ? t(Te.encode(e))
                : e instanceof Uint8Array
                  ? t(e)
                  : t(new Uint8Array(e)),
            n,
          )),
      runString:
        e.runString ??
        (e.run
          ? (t, n) => e.run((e) => t(Q.decode(e)), n)
          : (t, n) =>
              e.runRaw(
                (e) =>
                  typeof e == `string`
                    ? t(e)
                    : e instanceof Uint8Array
                      ? t(Q.decode(e))
                      : t(Q.decode(new Uint8Array(e))),
                n,
              )),
      writer: e.writer,
    }),
  Te = new TextEncoder(),
  Q = new TextDecoder(),
  isCloseEvent = (e) => b(e, `~effect/socket/Socket/CloseEvent`),
  $ = `~effect/socket/Socket/SocketError`,
  isSocketError = (e) => b(e, $);
var SocketReadError = class extends u(`effect/socket/Socket/SocketReadError`)({
    _tag: B(`SocketReadError`),
    cause: C,
  }) {
    message = `An error occurred during Read`;
  },
  SocketWriteError = class extends u(`effect/socket/Socket/SocketWriteError`)({
    _tag: B(`SocketWriteError`),
    cause: C,
  }) {
    message = `An error occurred during Write`;
  },
  SocketOpenError = class extends u(`effect/socket/Socket/SocketOpenError`)({
    _tag: B(`SocketOpenError`),
    kind: S([`Unknown`, `Timeout`]),
    cause: C,
  }) {
    get message() {
      return this.kind === `Timeout`
        ? `timeout waiting for "open"`
        : `An error occurred during Open`;
    }
  },
  SocketCloseError = class extends u(`effect/socket/Socket/SocketCloseError`)({
    _tag: B(`SocketCloseError`),
    code: c,
    closeReason: _e(pe),
  }) {
    static filterClean(e) {
      return function (t) {
        return SocketError.is(t) &&
          t.reason._tag === `SocketCloseError` &&
          e(t.reason.code)
          ? oe(t.reason)
          : re(t);
      };
    }
    get message() {
      return this.closeReason
        ? `${this.code}: ${this.closeReason}`
        : `${this.code}`;
    }
  };
const Ee = w([
  SocketReadError,
  SocketWriteError,
  SocketOpenError,
  SocketCloseError,
]);
var SocketError = class extends p($)(`SocketError`, {
  _tag: B(`SocketError`),
  reason: Ee,
}) {
  constructor(e) {
    `cause` in e.reason ? super({ ...e, cause: e.reason.cause }) : super(e);
  }
  [$] = $;
  static is(e) {
    return isSocketError(e);
  }
  message = this.reason.message;
};
const toChannelMap = (t, i) =>
    le(
      g(function* (a, o) {
        let c = yield* ee(),
          l = yield* T(o),
          u = yield* e(t.writer, l),
          d,
          f = 0,
          p = n({ while: () => f < d.length, body: () => u(d[f++]), step: he });
        return (
          yield* a.pipe(
            y((e) => (e.length === 1 ? u(e[0]) : ((d = e), (f = 0), p))),
            Se({ disableYield: !0 }),
            ae(v, (e) => I(c, e)),
            F(se(l, fe)),
            r(o),
          ),
          yield* t
            .runRaw((e) => {
              te(c, i(e));
            })
            .pipe(N(c), r(o)),
          s(c)
        );
      }),
    ),
  toChannel = (e) => {
    let t = new TextEncoder();
    return toChannelMap(e, (e) => (typeof e == `string` ? t.encode(e) : e));
  },
  toChannelWith = () => (e) => toChannel(e),
  defaultCloseCodeIsError = (e) => !0;
var WebSocket = class extends D()(`~effect/socket/Socket/WebSocket`) {},
  WebSocketConstructor = class extends D()(
    `@effect/platform/Socket/WebSocketConstructor`,
  ) {};
const makeWebSocket = (e, t) =>
    WebSocketConstructor.use((n) =>
      fromWebSocket(
        U(
          (typeof e == `string` ? G(e) : e).pipe(m((e) => n(e, t?.protocols))),
          (e) => k(() => e.close(1e3)),
        ),
        t,
      ),
    ),
  fromWebSocket = (t, n) =>
    P((r) => {
      let o,
        s = d(!1),
        c = r.context,
        l = n?.closeCodeIsError ?? defaultCloseCodeIsError,
        runRaw = (r, u) =>
          j(
            g(function* (a) {
              let c = yield* q().pipe(e(a)),
                d = yield* e(t, a),
                f = yield* V(runtime(c)(), WebSocket, d),
                p = !1;
              function onMessage(e) {
                if (e.data instanceof Blob)
                  return f(
                    y(
                      me(() => e.data.arrayBuffer()),
                      (e) => {
                        let t = r(new Uint8Array(e));
                        return h(t) ? t : L;
                      },
                    ),
                  );
                let t = r(e.data);
                h(t) && f(t);
              }
              function onError(e) {
                (d.removeEventListener(`message`, onMessage),
                  d.removeEventListener(`close`, onClose),
                  M(
                    c.deferred,
                    _(
                      new SocketError({
                        reason: p
                          ? new SocketReadError({ cause: e })
                          : new SocketOpenError({ kind: `Unknown`, cause: e }),
                      }),
                    ),
                  ));
              }
              function onClose(e) {
                let t = typeof e.code == `number` ? e.code : 1001;
                (d.removeEventListener(`message`, onMessage),
                  d.removeEventListener(`error`, onError),
                  M(
                    c.deferred,
                    _(
                      new SocketError({
                        reason: new SocketCloseError({
                          code: t,
                          closeReason: e.reason,
                        }),
                      }),
                    ),
                  ));
              }
              if (
                (d.addEventListener(`close`, onClose, { once: !0 }),
                d.addEventListener(`error`, onError, { once: !0 }),
                d.addEventListener(`message`, onMessage),
                d.readyState !== 1)
              ) {
                let e = A();
                (d.addEventListener(
                  `open`,
                  () => {
                    ((p = !0), M(e, L));
                  },
                  { once: !0 },
                ),
                  yield* W(e).pipe(
                    ie({
                      duration: n?.openTimeout ?? 1e4,
                      orElse: () =>
                        _(
                          new SocketError({
                            reason: new SocketOpenError({
                              kind: `Timeout`,
                              cause: Error(`timeout waiting for "open"`),
                            }),
                          }),
                        ),
                    }),
                    ge(join(c)),
                  ));
              }
              return (
                (p = !0),
                (o = d),
                s.openUnsafe(),
                u?.onOpen && (yield* u.onOpen),
                yield* i(
                  join(c),
                  SocketCloseError.filterClean((e) => !l(e)),
                  () => L,
                )
              );
            }),
          ).pipe(
            a((e) => ve(c, e)),
            F(
              k(() => {
                (s.closeUnsafe(), (o = void 0));
              }),
            ),
          ),
        write = (e) =>
          s.whenOpen(
            k(() => {
              let t = o;
              isCloseEvent(e) ? t.close(e.code, e.reason) : t.send(e);
            }),
          );
      return G(make({ runRaw, writer: G(write) }));
    });
export {
  runtime as _,
  SocketReadError as a,
  WebSocketConstructor as c,
  make as d,
  makeWebSocket as f,
  run as g,
  q as h,
  SocketOpenError as i,
  isCloseEvent as l,
  join as m,
  SocketCloseError as n,
  SocketWriteError as o,
  toChannelWith as p,
  SocketError as r,
  X as s,
  Z as t,
  isSocketError as u,
};
