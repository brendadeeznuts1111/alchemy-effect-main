import {
  $i as e,
  A as t,
  An as n,
  At as r,
  Bi as i,
  C as a,
  Cn as o,
  D as s,
  Di as c,
  Dr as l,
  Dt as u,
  E as d,
  Ei as ee,
  En as te,
  Et as ne,
  Fi as f,
  Gt as re,
  H as ie,
  I as ae,
  Ii as oe,
  Jn as p,
  Jt as m,
  L as se,
  Ln as h,
  Lt as g,
  M as ce,
  Mt as _,
  N as le,
  Ni as ue,
  Nn as v,
  Nt as de,
  O as fe,
  Oi as pe,
  P as me,
  Pi as y,
  Pt as b,
  Qi as he,
  Qn as ge,
  Qt as _e,
  Rt as ve,
  S as ye,
  Sn as be,
  T as xe,
  Ti as Se,
  Tn as Ce,
  Tt as x,
  Ui as we,
  Un as S,
  Ut as Te,
  Vt as Ee,
  Wt as De,
  Xn as C,
  Yn as Oe,
  _ as ke,
  _i as w,
  _n as Ae,
  _t as T,
  a as je,
  ai as E,
  an as Me,
  b as Ne,
  bn as D,
  br as Pe,
  bt as O,
  c as Fe,
  ci as Ie,
  cn as Le,
  cr as Re,
  d as k,
  da as A,
  di as ze,
  ea as Be,
  ei as Ve,
  f as He,
  fa as j,
  fr as Ue,
  g as We,
  ga as M,
  gi as Ge,
  gn as Ke,
  gr as qe,
  gt as Je,
  h as Ye,
  hn as Xe,
  hr as Ze,
  i as Qe,
  ir as $e,
  ji as et,
  jn as tt,
  jt as nt,
  ki as rt,
  kn as N,
  l as it,
  la as P,
  li as at,
  ln as ot,
  lr as F,
  m as st,
  mi as ct,
  mn as lt,
  mr as ut,
  mt as dt,
  n as ft,
  nt as pt,
  o as mt,
  oi as ht,
  p as gt,
  pa as I,
  pn as _t,
  pr as L,
  pt as R,
  qn as vt,
  r as yt,
  ra as z,
  rn as bt,
  rr as B,
  s as xt,
  sa as St,
  si as Ct,
  sn as wt,
  sr as Tt,
  t as Et,
  ta as Dt,
  ti as Ot,
  tn as kt,
  u as At,
  un as jt,
  v as Mt,
  vn as V,
  vr as Nt,
  vt as Pt,
  w as Ft,
  wn as H,
  wt as It,
  x as Lt,
  xt as Rt,
  y as zt,
  yn as U,
  yr as W,
  z as Bt,
  zt as G,
} from "./Channel-C7bUVWpS.js";
import * as K from "node:fs";
import * as q from "node:path";
import * as Vt from "node:os";
import * as Ht from "node:readline";
import "node:stream";
import * as Ut from "node:crypto";
import * as Wt from "node:url";
import * as Gt from "node:child_process";
const Kt = `~effect/MutableRef`,
  qt = {
    [Kt]: Kt,
    ...oe,
    toJSON() {
      return { _id: `MutableRef`, current: we(this.current) };
    },
  },
  Jt = M((e) => {
    let t = Object.create(qt);
    return ((t.current = e), t);
  }, `make`),
  Yt = {
    "~effect/Stream": { _R: A, _E: A, _A: A },
    pipe() {
      return I(this, arguments);
    },
  },
  Xt = M((e) => {
    let t = Object.create(Yt);
    return ((t.channel = e), t);
  }, `fromChannel`),
  Zt = { _tag: `Empty` },
  Qt = { _tag: `Closed` },
  $t = { _A: A, _E: A };
var RcRefImpl = class {
  "~effect/RcRef" = $t;
  pipe() {
    return I(this, arguments);
  }
  state = Zt;
  semaphore = s(1);
  acquire;
  context;
  scope;
  idleTimeToLive;
  constructor(e, t, n, r) {
    ((this.acquire = e),
      (this.context = t),
      (this.scope = n),
      (this.idleTimeToLive = r));
  }
};
const en = M(
    (e) =>
      tt((t) => {
        let n = t.context,
          r = Ie(n, vt),
          i = new RcRefImpl(
            e.acquire,
            n,
            r,
            e.idleTimeToLive ? Ve(e.idleTimeToLive) : void 0,
          );
        return T(
          Oe(r, () => {
            let e = i.state._tag === `Acquired` ? C(i.state.scope, L) : N;
            return ((i.state = Qt), e);
          }),
          i,
        );
      }),
    `make`,
  ),
  getState = (e) =>
    te((t) => {
      switch (e.state._tag) {
        case `Closed`:
          return re;
        case `Acquired`:
          return (
            e.state.refCount++,
            e.state.fiber ? T(se(e.state.fiber), e.state) : V(e.state)
          );
        case `Empty`: {
          let n = ge();
          return e.semaphore.withPermits(1)(
            t(Me(e.acquire, ht(e.context, vt, n))).pipe(
              m((t) => {
                let r = {
                  _tag: `Acquired`,
                  value: t,
                  scope: n,
                  fiber: void 0,
                  refCount: 1,
                  invalidated: !1,
                };
                return ((e.state = r), r);
              }),
            ),
          );
        }
      }
    }),
  tn = g(function* (e) {
    let t = e,
      n = yield* getState(t),
      r = yield* _t,
      i = t.idleTimeToLive !== void 0 && Ot(t.idleTimeToLive);
    return (
      yield* Oe(
        r,
        () => (
          n.refCount--,
          n.refCount > 0
            ? N
            : t.idleTimeToLive === void 0
              ? ((t.state = Zt), C(n.scope, L))
              : n.invalidated
                ? C(n.scope, L)
                : (i &&
                    (n.fiber = Ae(t.idleTimeToLive).pipe(
                      b(() =>
                        t.state._tag === `Acquired` && t.state.refCount === 0
                          ? ((t.state = Zt), C(n.scope, L))
                          : N,
                      ),
                      nt(
                        D(() => {
                          n.fiber = void 0;
                        }),
                      ),
                      ot(t.context),
                      Bt(t.scope),
                    )),
                  N)
        ),
      ),
      n.value
    );
  }),
  nn = en,
  rn = tn,
  an = `~effect/Sink`,
  on = V([void 0]),
  sn = { _A: A, _In: A, _L: A, _E: A, _R: A },
  cn = {
    [an]: sn,
    pipe() {
      return I(this, arguments);
    },
  },
  isSink = (e) => he(e, an),
  ln = M(
    (e) =>
      fromTransform((t, n) =>
        xe(e)(t, n).pipe(b(G({ disableYield: !0 })), v(V)),
      ),
    `fromChannel`,
  ),
  fromTransform = (e) => {
    let t = Object.create(cn);
    return ((t.transform = e), t);
  },
  un = M((e) => k((t, n) => V(b(e.transform(t, n), W))), `toChannel`),
  J = fromTransform((e) => v(G(e, { disableYield: !0 }), () => on)),
  reduceWhile = (e, t, n) =>
    fromTransform((r) => {
      let i = e(),
        a;
      return t(i)
        ? r.pipe(
            b((e) => {
              for (let r = 0; r < e.length; r++)
                if (((i = n(i, e[r])), !t(i)))
                  return (r + 1 < e.length && (a = e.slice(r + 1)), W());
              return N;
            }),
            G({ disableYield: !0 }),
            v(() => V([i, a])),
          )
        : V([i]);
    }),
  dn = reduceWhile(y, et, (e, t) => f(t)),
  head = () => dn,
  forEach = (e) => forEachArray(ve((t) => e(t), { discard: !0 })),
  forEachArray = (e) =>
    fromTransform((t) =>
      t.pipe(
        b(e),
        G({ disableYield: !0 }),
        v(() => on),
      ),
    ),
  fn = M((e) => ln(d(m(e, un))), `unwrap`),
  isStream = (e) => he(e, `~effect/Stream`),
  Y = Xt,
  fromPull = (e) => Y(At(e)),
  transformPull = (e, t) => Y(k((n, r) => b(Ft(e.channel, r), (e) => t(e, r)))),
  toChannel = (e) => e.channel,
  callback = (e, t) => Y(Et(e, t)),
  X = Y(yt),
  succeed = (e) => Y(Lt(w(e))),
  suspend = (e) => Y(ye(() => e().channel)),
  fail = (e) => Y(je(e)),
  fromIterable = (e, t) =>
    Array.isArray(e) && t?.chunkSize === void 0
      ? fromArray(e)
      : Y(it(e, t?.chunkSize)),
  fromIterableEffect = (e) => unwrap(m(e, fromIterable)),
  fromArray = (e) => (ct(e) ? Y(Lt(e)) : X),
  fromReadableStream = (e) =>
    Y(
      k(
        g(function* (t, n) {
          let r = e.evaluate().getReader();
          return (
            yield* p(
              n,
              e.releaseLockOnEnd
                ? D(() => r.releaseLock())
                : bt(() => r.cancel()),
            ),
            b(
              o({ try: () => r.read(), catch: (t) => e.onError(t) }),
              ({ done: e, value: t }) => (e ? W() : V(w(t))),
            )
          );
        }),
      ),
    ),
  unwrap = (e) => Y(d(m(e, toChannel))),
  pn = P(2, (e, t) =>
    suspend(() => {
      let n = 0;
      return Y(
        He(
          e.channel,
          Ge((e) => t(e, n++)),
        ),
      );
    }),
  ),
  mn = P(
    (e) => isStream(e[0]),
    (e, t, n) => e.channel.pipe(Fe, st(t, n), He(w), Y),
  ),
  hn = P(
    (e) => isStream(e[0]),
    (e, t, n) =>
      e.channel.pipe(
        Fe,
        xt((e) => t(e).channel, n),
        Y,
      ),
  ),
  gn = P(
    (e) => isStream(e[0]),
    (e, t) => hn(e, A, t),
  ),
  _n = P(2, (e, t) => gn(fromArray([e, t]))),
  vn = P(2, (e, t) => _n(fromIterable(t), e)),
  yn = P(
    (e) => isStream(e[0]) && isStream(e[1]),
    (e, t, n) => Y(We(toChannel(e), toChannel(t), n)),
  ),
  bn = P(2, (e, t) => Y(mt(toChannel(e), t))),
  xn = P(
    2,
    g(function* (e, t) {
      let n,
        r = yield* a(e.channel),
        i = fromPull(V(Rt(r, (e) => ((n = e), de(e))))),
        o = yield* Z(i, t);
      return n ? [o, X] : ((i = fromPull(V(r))), [o, i]);
    }),
  ),
  Sn = P(2, (e, t) =>
    e.channel.pipe(
      ft((e) => t(e).channel),
      Y,
    ),
  ),
  Cn = P(2, (e, t) => Y(Ye(e.channel, t))),
  wn = P(2, (e, t) =>
    transformPull(e, (e, n) =>
      D(() => {
        let r,
          i,
          a = U(() => {
            if (i !== void 0) {
              let e = i;
              return ((i = void 0), V(e));
            }
            return e;
          }).pipe(x((e) => ((r = F(e)), W()))),
          o = m(
            U(() => t.transform(a, n)),
            ([e, t]) => ((i = t), w(e)),
          );
        return U(() => r || o);
      }),
    ),
  ),
  Tn = P(2, (e, t) => Y(ke(e.channel, t))),
  En = P(
    (e) => isStream(e[0]),
    (e, t) =>
      suspend(() => {
        let n = new TextDecoder(t?.encoding);
        return pn(e, (e) => n.decode(e, { stream: !0 }));
      }),
  ),
  encodeText = (e) =>
    suspend(() => {
      let t = new TextEncoder();
      return pn(e, (e) => t.encode(e));
    }),
  splitLines = (e) => e.channel.pipe(ke(Ne()), Y),
  Dn = P(2, (e, t) => Y(Qe(e.channel, t))),
  Z = P(2, (e, t) =>
    Xe((n) =>
      Ft(e.channel, n).pipe(
        b((e) => t.transform(e, n)),
        m(([e]) => e),
      ),
    ),
  ),
  runCollect = (e) =>
    Mt(
      e.channel,
      () => [],
      (e, t) => {
        for (let n = 0; n < t.length; n++) e.push(t[n]);
        return e;
      },
    ),
  On = P(2, (e, t) =>
    zt(e.channel, (e) => {
      let r = 0;
      return n({ while: () => r < e.length, body: () => t(e[r++]), step: St });
    }),
  ),
  kn = P(2, (e, t) => zt(e.channel, t)),
  mkString = (e) =>
    Mt(
      e.channel,
      () => ``,
      (e, t) => e + t.join(``),
    ),
  mkUint8Array = (e) =>
    m(
      Mt(
        e.channel,
        () => ({ bytes: 0, arrays: [] }),
        (e, t) => {
          for (let n = 0; n < t.length; n++)
            ((e.bytes += t[n].length), e.arrays.push(t[n]));
          return e;
        },
      ),
      ({ arrays: e, bytes: t }) => {
        let n = new Uint8Array(t),
          r = 0;
        for (let t = 0; t < e.length; t++) {
          let i = e[t];
          (n.set(i, r), (r += i.length));
        }
        return n;
      },
    ),
  An = P(
    (e) => isStream(e[0]),
    (e, t, n) => {
      let r,
        i,
        a = ae(!1);
      return new ReadableStream(
        {
          start(n) {
            ((i = Le(
              Me(
                kn(e, (e) =>
                  a.whenOpen(
                    D(() => {
                      a.closeUnsafe();
                      for (let t = 0; t < e.length; t++) n.enqueue(e[t]);
                      (r(), (r = void 0));
                    }),
                  ),
                ),
                t,
              ),
            )),
              i.addObserver((e) => {
                e._tag === `Failure` ? n.error(l(e.cause)) : n.close();
              }));
          },
          pull() {
            return new Promise((e) => {
              ((r = e), a.openUnsafe());
            });
          },
          cancel() {
            if (i) return jt(Pt(se(i)));
          },
        },
        n?.strategy,
      );
    },
  ),
  jn = P(
    (e) => isStream(e[0]),
    (e, t) => An(e, Ct(), t),
  ),
  Mn = P(
    (e) => isStream(e[0]),
    (e, t) => m(u(), (n) => An(e, n, t)),
  );
function nominal() {
  return Object.assign((e) => e, {
    option: (e) => f(e),
    result: (e) => Se(e),
    is: (e) => !0,
  });
}
const Nn = `~effect/platform/PlatformError`;
var BadArgument = class extends Ze(`BadArgument`) {
    get message() {
      return `${this.module}.${this.method}${this.description ? `: ${this.description}` : ``}`;
    }
  },
  SystemError = class extends ut {
    get message() {
      return `${this._tag}: ${this.module}.${this.method}${this.pathOrDescriptor === void 0 ? `` : ` (${this.pathOrDescriptor})`}${this.description ? `: ${this.description}` : ``}`;
    }
  },
  PlatformError = class extends Ze(`PlatformError`) {
    constructor(e) {
      `cause` in e
        ? super({ reason: e, cause: e.cause })
        : super({ reason: e });
    }
    [Nn] = Nn;
    get message() {
      return this.reason.message;
    }
  };
const systemError = (e) => new PlatformError(new SystemError(e)),
  badArgument = (e) => new PlatformError(new BadArgument(e)),
  Pn = `~effect/platform/FileSystem`,
  Size = (e) => (typeof e == `bigint` ? e : BigInt(e)),
  Q = BigInt(1024);
Q * Q * Q * Q * Q;
const $ = E(`effect/platform/FileSystem`),
  Fn = M(
    (e) =>
      $.of({
        ...e,
        [Pn]: Pn,
        exists: (t) =>
          j(
            e.access(t),
            T(!0),
            It(`PlatformError`, (e) =>
              e.reason._tag === `NotFound` ? V(!1) : _(e),
            ),
          ),
        readFileString: (t, n) =>
          b(e.readFile(t), (e) =>
            H({
              try: () => new TextDecoder(n).decode(e),
              catch: (e) =>
                badArgument({
                  module: `FileSystem`,
                  method: `readFileString`,
                  description: `invalid encoding`,
                  cause: e,
                }),
            }),
          ),
        stream: g(function* (t, n) {
          let r = yield* e.open(t, { flag: `r` });
          n?.offset && (yield* r.seek(n.offset, `start`));
          let i = n?.bytesToRead === void 0 ? void 0 : Size(n.bytesToRead),
            a = BigInt(0),
            o = Size(n?.chunkSize ?? 64 * 1024),
            s = r.readAlloc(o);
          return fromPull(
            V(
              b(
                U(() =>
                  i !== void 0 && i <= a
                    ? W()
                    : i !== void 0 && i - a < o
                      ? r.readAlloc(i - a)
                      : s,
                ),
                ue({
                  onNone: () => W(),
                  onSome: (e) => ((a += BigInt(e.length)), V(w(e))),
                }),
              ),
            ),
          );
        }, unwrap),
        sink: (t, n) =>
          j(
            e.open(t, { flag: `w`, ...n }),
            m((e) => forEach((t) => e.writeAll(t))),
            fn,
          ),
        writeFileString: (t, n, r) =>
          b(
            H({
              try: () => new TextEncoder().encode(n),
              catch: (e) =>
                badArgument({
                  module: `FileSystem`,
                  method: `writeFileString`,
                  description: `could not encode string`,
                  cause: e,
                }),
            }),
            (n) => e.writeFile(t, n, r),
          ),
      }),
    `make`,
  ),
  In = `~effect/platform/FileSystem/File`,
  Ln = nominal();
var WatchBackend = class extends E()(
  `effect/platform/FileSystem/WatchBackend`,
) {};
const Rn = `~effect/platform/Path`,
  zn = E(`effect/Path`),
  Bn = nominal(),
  Vn = nominal(),
  Hn = `~effect/ChildProcessSpawner/ChildProcessHandle`,
  Un = {
    [Hn]: Hn,
    ...i,
    toJSON() {
      return { _id: `ChildProcessHandle`, pid: this.pid };
    },
  },
  makeHandle = (e) => Object.assign(Object.create(Un), e),
  Wn = M((e) => {
    let streamString = (t, n) =>
        e(t).pipe(
          m((e) => En(n?.includeStderr === !0 ? e.all : e.stdout)),
          unwrap,
        ),
      streamLines = (e, t) => splitLines(streamString(e, t));
    return ChildProcessSpawner.of({
      spawn: e,
      exitCode: (t) => lt(b(e(t), (e) => e.exitCode)),
      streamString,
      streamLines,
      lines: (e, t) => runCollect(streamLines(e, t)),
      string: (e, t) => mkString(streamString(e, t)),
    });
  }, `make`);
var ChildProcessSpawner = class extends E()(
  `effect/process/ChildProcessSpawner`,
) {};
const Gn = `~effect/unstable/process/ChildProcess`,
  Kn = {
    ...ze({
      label: `Command`,
      evaluate(e) {
        return at(e.context, ChildProcessSpawner).spawn(this);
      },
    }),
    [Gn]: Gn,
  },
  makeStandardCommand = (e, t, n) =>
    Object.assign(Object.create(Kn), {
      _tag: `StandardCommand`,
      command: e,
      args: t,
      options: n,
    }),
  qn = function make(...e) {
    if (isTemplateString(e[0])) {
      let [t, ...n] = e,
        r = parseTemplates(t, n);
      return makeStandardCommand(r[0] ?? ``, r.slice(1), {});
    }
    if (
      typeof e[0] == `object` &&
      !Array.isArray(e[0]) &&
      !isTemplateString(e[0])
    ) {
      let t = e[0];
      return function (e, ...n) {
        let r = parseTemplates(e, n);
        return makeStandardCommand(r[0] ?? ``, r.slice(1), t);
      };
    }
    if (typeof e[0] == `string` && !Array.isArray(e[1])) {
      let [t, n = {}] = e;
      return makeStandardCommand(t, [], n);
    }
    let [t, n = [], r = {}] = e;
    return makeStandardCommand(t, n, r);
  },
  isTemplateString = (e) =>
    Array.isArray(e) && `raw` in e && Array.isArray(e.raw),
  parseFdName = (e) => {
    let t = /^fd(\d+)$/.exec(e);
    if (t === null) return;
    let n = parseInt(t[1], 10);
    return n >= 3 ? n : void 0;
  },
  fdName = (e) => `fd${e}`,
  parseTemplates = (e, t) => {
    let n = [];
    for (let [r, i] of e.entries()) n = parseTemplate(e, t, n, i, r);
    return n;
  },
  parseTemplate = (e, t, n, r, i) => {
    let a = e.raw[i];
    if (a === void 0) throw Error(`Invalid backslash sequence: ${e.raw[i]}`);
    let {
        hasLeadingWhitespace: o,
        hasTrailingWhitespace: s,
        tokens: c,
      } = splitByWhitespaces(r, a),
      l = concatTokens(n, c, o);
    if (i === t.length) return l;
    let u = t[i];
    return concatTokens(
      l,
      Array.isArray(u)
        ? u.map((e) => parseExpression(e))
        : [parseExpression(u)],
      s,
    );
  },
  parseExpression = (e) => (typeof e == `string` ? e : String(e)),
  Jn = new Set([
    ` `,
    `	`,
    `\r`,
    `
`,
  ]),
  Yn = { x: 3, u: 5 },
  splitByWhitespaces = (e, t) => {
    if (t.length === 0)
      return {
        tokens: [],
        hasLeadingWhitespace: !1,
        hasTrailingWhitespace: !1,
      };
    let n = Jn.has(t[0]),
      r = [],
      i = 0;
    for (let n = 0, a = 0; n < e.length; n += 1, a += 1) {
      let o = t[a];
      if (Jn.has(o)) (i !== n && r.push(e.slice(i, n)), (i = n + 1));
      else if (o === `\\`) {
        let e = t[a + 1];
        e ===
        `
`
          ? (--n, (a += 1))
          : e === `u` && t[a + 2] === `{`
            ? (a = t.indexOf(`}`, a + 3))
            : (a += Yn[e] ?? 1);
      }
    }
    let a = i === e.length;
    return (
      a || r.push(e.slice(i)),
      { tokens: r, hasLeadingWhitespace: n, hasTrailingWhitespace: a }
    );
  },
  concatTokens = (e, t, n) =>
    n || e.length === 0 || t.length === 0
      ? [...e, ...t]
      : [...e.slice(0, -1), `${e.at(-1)}${t.at(0)}`, ...t.slice(1)],
  handleErrnoException =
    (e, t) =>
    (n, [r]) => {
      let i = `Unknown`;
      switch (n.code) {
        case `ENOENT`:
          i = `NotFound`;
          break;
        case `EACCES`:
          i = `PermissionDenied`;
          break;
        case `EEXIST`:
          i = `AlreadyExists`;
          break;
        case `EISDIR`:
          i = `BadResource`;
          break;
        case `ENOTDIR`:
          i = `BadResource`;
          break;
        case `EBUSY`:
          i = `Busy`;
          break;
        case `ELOOP`:
          i = `BadResource`;
          break;
      }
      return systemError({
        _tag: i,
        module: e,
        method: t,
        pathOrDescriptor: r,
        syscall: n.syscall,
        cause: n,
      });
    },
  fromWritable = (e) => ln(gt(fromWritableChannel(e), (e) => [e])),
  fromWritableChannel = (e) =>
    k((t) => {
      let n = e.evaluate();
      return V(pullIntoWritable({ ...e, writable: n, pull: t }));
    }),
  pullIntoWritable = (e) =>
    e.pull.pipe(
      b((t) => {
        let n = 0;
        return O(function loop(r) {
          for (; n < t.length; )
            if (!e.writable.write(t[n++], e.encoding)) {
              e.writable.once(`drain`, () => loop(r));
              return;
            }
          r(N);
        });
      }),
      G({ disableYield: !0 }),
      wt(
        O((t) => {
          let onError = (n) => t(_(e.onError(n)));
          return (
            e.writable.once(`error`, onError),
            D(() => {
              e.writable.off(`error`, onError);
            })
          );
        }),
      ),
      e.endOnDone === !1
        ? A
        : v((t) =>
            `closed` in e.writable && e.writable.closed
              ? W(t)
              : O((n) => {
                  (e.writable.once(`finish`, () => n(W(t))), e.writable.end());
                }),
          ),
    ),
  fromReadable = (e) => Y(fromReadableChannel(e)),
  fromReadableChannel = (e) =>
    k((t, n) =>
      readableToPullUnsafe({
        scope: n,
        readable: e.evaluate(),
        onError: e.onError ?? defaultOnError,
        chunkSize: e.chunkSize,
        closeOnDone: e.closeOnDone,
      }),
    ),
  readableToPullUnsafe = (e) => {
    let t = e.readable;
    if (t.readableEnded) return V(W());
    let n = e.closeOnDone ?? !0,
      r = e.exit ?? Jt(void 0),
      i = ae(!1);
    function onReadable() {
      i.openUnsafe();
    }
    function onError(t) {
      ((r.current = F(e.onError(t))), i.openUnsafe());
    }
    function onEnd() {
      ((r.current = F(qe())), i.openUnsafe());
    }
    (t.on(`readable`, onReadable),
      t.once(`error`, onError),
      t.once(`end`, onEnd));
    let a = U(function loop() {
      let t = e.readable.read(e.chunkSize);
      if (t === null)
        return r.current ? r.current : (i.closeUnsafe(), b(i.await, loop));
      let n = w(t);
      for (; (t = e.readable.read(e.chunkSize)), t !== null; ) n.push(t);
      return V(n);
    });
    return T(
      p(
        e.scope,
        D(() => {
          (t.off(`readable`, onReadable),
            t.off(`error`, onError),
            t.off(`end`, onEnd),
            n &&
              `closed` in e.readable &&
              !e.readable.closed &&
              e.readable.destroy());
        }),
      ),
      a,
    );
  },
  defaultOnError = (e) => new Nt(e),
  toError = (e) =>
    e instanceof globalThis.Error ? e : new globalThis.Error(String(e)),
  toPlatformError = (e, t, n) => {
    let { commands: r } = flattenCommand(n),
      i = r.reduce((e, t) => {
        let n = `${t.command} ${t.args.join(` `)}`;
        return e.length === 0 ? n : `${e} | ${n}`;
      }, ``);
    return handleErrnoException(`ChildProcess`, e)(t, [i]);
  },
  Xn = h(
    ChildProcessSpawner,
    Te(function* () {
      let t = yield* $,
        n = yield* zn,
        r = g(function* (e) {
          if (!z(e.cwd)) return (yield* t.access(e.cwd), n.resolve(e.cwd));
        }),
        resolveEnvironment = (e) =>
          e.extendEnv ? { ...globalThis.process.env, ...e.env } : e.env,
        inputToStdioOption = (e) => (isStream(e) ? `pipe` : e),
        outputToStdioOption = (e) => (isSink(e) ? `pipe` : e),
        resolveStdinOption = (e) => {
          let t = { stream: `pipe`, encoding: `utf-8`, endOnDone: !0 };
          return z(e.stdin)
            ? t
            : typeof e.stdin == `string` || isStream(e.stdin)
              ? { ...t, stream: e.stdin }
              : {
                  stream: e.stdin.stream,
                  encoding: e.stdin.encoding ?? t.encoding,
                  endOnDone: e.stdin.endOnDone ?? t.endOnDone,
                };
        },
        resolveOutputOption = (e, t) => {
          let n = e[t];
          return z(n)
            ? { stream: `pipe` }
            : typeof n == `string` || isSink(n)
              ? { stream: n }
              : { stream: n.stream };
        },
        resolveAdditionalFds = (e) => {
          if (z(e.additionalFds)) return [];
          let t = [];
          for (let [n, r] of Object.entries(e.additionalFds)) {
            let e = parseFdName(n);
            Be(e) && t.push({ fd: e, config: r });
          }
          return t.sort((e, t) => e.fd - t.fd);
        },
        buildStdioArray = (e, t, n, r) => {
          let i = [
            inputToStdioOption(e.stream),
            outputToStdioOption(t.stream),
            outputToStdioOption(n.stream),
          ];
          if (r.length === 0) return i;
          let a = r.reduce((e, { fd: t }) => Math.max(e, t), 2);
          for (let e = 3; e <= a; e++) i[e] = `ignore`;
          for (let { fd: e } of r) i[e] = `pipe`;
          return i;
        },
        i = g(function* (e, t, n) {
          if (n.length === 0)
            return { getInputFd: () => J, getOutputFd: () => X };
          let r = new Map(),
            i = new Map();
          for (let { config: a, fd: o } of n) {
            let n = t.stdio[o];
            switch (a.type) {
              case `input`: {
                let t = J;
                (n &&
                  `write` in n &&
                  (t = fromWritable({
                    evaluate: () => n,
                    onError: (t) =>
                      toPlatformError(`fromWritable(fd${o})`, toError(t), e),
                  })),
                  a.stream && (yield* Ee(Z(a.stream, t))),
                  r.set(o, t));
                break;
              }
              case `output`: {
                let t = X;
                (n &&
                  `read` in n &&
                  (t = fromReadable({
                    evaluate: () => n,
                    onError: (t) =>
                      toPlatformError(`fromReadable(fd${o})`, toError(t), e),
                  })),
                  a.sink && (t = wn(t, a.sink)),
                  i.set(o, t));
                break;
              }
            }
          }
          return {
            getInputFd: (e) => r.get(e) ?? J,
            getOutputFd: (e) => i.get(e) ?? X,
          };
        }),
        setupChildStdin = (t, n, r) =>
          U(() => {
            let i = J;
            return (
              e(n.stdin) &&
                (i = fromWritable({
                  evaluate: () => n.stdin,
                  onError: (e) =>
                    toPlatformError(`fromWritable(stdin)`, toError(e), t),
                  endOnDone: r.endOnDone,
                  encoding: r.encoding,
                })),
              isStream(r.stream) ? T(Ee(Z(r.stream, i)), i) : V(i)
            );
          }),
        setupChildOutputStreams = (e, t, n, r) => {
          let i = t.stdout
              ? fromReadable({
                  evaluate: () => t.stdout,
                  onError: (t) =>
                    toPlatformError(`fromReadable(stdout)`, toError(t), e),
                })
              : X,
            a = t.stderr
              ? fromReadable({
                  evaluate: () => t.stderr,
                  onError: (t) =>
                    toPlatformError(`fromReadable(stderr)`, toError(t), e),
                })
              : X;
          (isSink(n.stream) && (i = wn(i, n.stream)),
            isSink(r.stream) && (a = wn(a, r.stream)));
          let o = yn(i, a);
          return { stdout: i, stderr: a, all: o };
        },
        spawn = (e, t) =>
          O((n) => {
            let r = Re(),
              i = Gt.spawn(e.command, e.args, t);
            return (
              i.on(`error`, (t) => {
                n(_(toPlatformError(`spawn`, t, e)));
              }),
              i.on(`exit`, (...e) => {
                $e(r, Ue(e));
              }),
              i.on(`spawn`, () => {
                n(V([i, r]));
              }),
              D(() => {
                i.kill(`SIGTERM`);
              })
            );
          }),
        killProcessGroup = (e, t, n) =>
          globalThis.process.platform === `win32`
            ? O((n) => {
                Gt.exec(`taskkill /pid ${t.pid} /T /F`, (t) => {
                  n(t ? _(toPlatformError(`kill`, toError(t), e)) : N);
                });
              })
            : H({
                try: () => {
                  globalThis.process.kill(-t.pid, n);
                },
                catch: (t) => toPlatformError(`kill`, toError(t), e),
              }),
        killProcessGroupOnExit = (e, t) => {
          if (globalThis.process.platform === `win32`) {
            Gt.exec(`taskkill /pid ${e.pid} /T /F`, () => {});
            return;
          }
          try {
            globalThis.process.kill(-e.pid, t);
          } catch {}
        },
        killProcess = (e, t, n) =>
          U(() =>
            t.kill(n)
              ? N
              : _(
                  toPlatformError(
                    `kill`,
                    new globalThis.Error(`Failed to kill child process`),
                    e,
                  ),
                ),
          ),
        withTimeout = (e, t, n) => (r) => {
          let i = n?.killSignal ?? `SIGTERM`;
          return z(n?.forceKillAfter)
            ? r(t, e, i)
            : be(r(t, e, i), {
                duration: n.forceKillAfter,
                orElse: () => r(t, e, `SIGKILL`),
              });
        },
        getSourceStream = (e, t) => {
          let n = t ?? `stdout`;
          switch (n) {
            case `stdout`:
              return e.stdout;
            case `stderr`:
              return e.stderr;
            case `all`:
              return e.all;
            default: {
              let t = parseFdName(n);
              return Be(t) ? e.getOutputFd(t) : e.stdout;
            }
          }
        },
        a = g(function* (t) {
          switch (t._tag) {
            case `StandardCommand`: {
              let n = resolveStdinOption(t.options),
                a = resolveOutputOption(t.options, `stdout`),
                o = resolveOutputOption(t.options, `stderr`),
                s = resolveAdditionalFds(t.options),
                c = !0,
                l = !1,
                [u, d] = yield* R(
                  spawn(t, {
                    cwd: yield* r(t.options),
                    env: resolveEnvironment(t.options),
                    stdio: buildStdioArray(n, a, o, s),
                    detached:
                      t.options.detached ?? process.platform !== `win32`,
                    shell: t.options.shell,
                  }),
                  g(function* ([n, r]) {
                    let i = yield* Tt(r),
                      a = withTimeout(n, t, t.options);
                    if (i) {
                      let [t] = yield* B(r);
                      return t !== 0 && e(t)
                        ? yield* De(a(killProcessGroup))
                        : yield* N;
                    }
                    return c
                      ? yield* a((e, t, n) =>
                          x(killProcessGroup(e, t, n), () =>
                            killProcess(e, t, n),
                          ),
                        ).pipe(Je(B(r)), De)
                      : yield* N;
                  }),
                ),
                ee = Vn(u.pid);
              u.on(`exit`, (n) => {
                l &&
                  n !== 0 &&
                  e(n) &&
                  killProcessGroupOnExit(u, t.options.killSignal ?? `SIGTERM`);
              });
              let te = D(() => {
                  c || (u.ref(), (c = !0), (l = !1));
                }),
                ne = D(() => (c && (u.unref(), (c = !1), (l = !0)), te)),
                f = yield* setupChildStdin(t, u, n),
                {
                  all: re,
                  stderr: ie,
                  stdout: ae,
                } = setupChildOutputStreams(t, u, a, o),
                { getInputFd: oe, getOutputFd: p } = yield* i(t, u, s),
                se = m(Tt(d), (e) => !e),
                h = b(B(d), ([n, r]) =>
                  e(n)
                    ? V(Bn(n))
                    : _(
                        toPlatformError(
                          `exitCode`,
                          new globalThis.Error(
                            `Process interrupted due to receipt of signal: '${r}'`,
                          ),
                          t,
                        ),
                      ),
                ),
                kill = (e) =>
                  withTimeout(
                    u,
                    t,
                    e,
                  )((e, t, n) =>
                    x(killProcessGroup(e, t, n), () => killProcess(e, t, n)),
                  ).pipe(Je(B(d)), Pt);
              return makeHandle({
                pid: ee,
                exitCode: h,
                isRunning: se,
                kill,
                stdin: f,
                stdout: ae,
                stderr: ie,
                all: re,
                getInputFd: oe,
                getOutputFd: p,
                unref: ne,
              });
            }
            case `PipedCommand`: {
              let { commands: e, pipeOptions: n } = flattenCommand(t),
                [r, ...i] = e,
                o = [yield* a(r)];
              for (let e = 0; e < i.length; e++) {
                let t = i[e],
                  r = n[e] ?? {},
                  s = resolveStdinOption(t.options),
                  c = unwrap(V(getSourceStream(o[o.length - 1], r.from))),
                  l = r.to ?? `stdin`;
                if (l === `stdin`)
                  o.push(
                    yield* a(
                      qn(t.command, t.args, {
                        ...t.options,
                        stdin: { ...s, stream: c },
                      }),
                    ),
                  );
                else {
                  let e = parseFdName(l);
                  if (Be(e)) {
                    let n = fdName(e),
                      r = t.options.additionalFds ?? {};
                    o.push(
                      yield* a(
                        qn(t.command, t.args, {
                          ...t.options,
                          additionalFds: {
                            ...r,
                            [n]: { type: `input`, stream: c },
                          },
                        }),
                      ),
                    );
                  } else
                    o.push(
                      yield* a(
                        qn(t.command, t.args, {
                          ...t.options,
                          stdin: { ...s, stream: c },
                        }),
                      ),
                    );
                }
              }
              let s = o[o.length - 1],
                c = Te(function* () {
                  let e = [];
                  for (let t of o) e.push(yield* t.unref);
                  return ve([...e].reverse(), (e) => e, { discard: !0 });
                });
              return makeHandle({
                pid: s.pid,
                exitCode: s.exitCode,
                isRunning: s.isRunning,
                kill: s.kill,
                stdin: s.stdin,
                stdout: s.stdout,
                stderr: s.stderr,
                all: s.all,
                getInputFd: s.getInputFd,
                getOutputFd: s.getOutputFd,
                unref: c,
              });
            }
          }
        });
      return Wn(a);
    }),
  ),
  flattenCommand = (e) => {
    let t = [],
      n = [],
      flatten = (e) => {
        switch (e._tag) {
          case `StandardCommand`:
            t.push(e);
            break;
          case `PipedCommand`:
            (flatten(e.left), n.push(e.options), flatten(e.right));
            break;
        }
      };
    if ((flatten(e), t.length === 0))
      throw Error(`flattenCommand produced empty commands array`);
    let [r, ...i] = t;
    return { commands: [r, ...i], pipeOptions: n };
  },
  Zn = `~effect/platform/Crypto`,
  Qn = E(`effect/Crypto`),
  $n = M((e) => {
    let t = e.randomBytes,
      randomBytes = (e) => m(validateSize(`randomBytes`, e), t),
      nextDoubleUnsafe = () => {
        let e = t(7);
        return (
          ((e[0] & 31) * 2 ** 48 +
            e[1] * 2 ** 40 +
            e[2] * 2 ** 32 +
            e[3] * 2 ** 24 +
            e[4] * 2 ** 16 +
            e[5] * 2 ** 8 +
            e[6]) /
          2 ** 53
        );
      },
      nextIntUnsafe = () =>
        Math.floor(nextDoubleUnsafe() * (2 ** 53 - 1 - -(2 ** 53 - 1) + 1)) +
        -(2 ** 53 - 1);
    return Qn.of({
      [Zn]: Zn,
      randomBytes,
      nextDoubleUnsafe,
      nextIntUnsafe,
      digest: e.digest,
      random: D(() => nextDoubleUnsafe()),
      randomBoolean: D(() => nextDoubleUnsafe() > 0.5),
      randomInt: D(() => nextIntUnsafe()),
      randomBetween: (e, t) => D(() => nextDoubleUnsafe() * (t - e) + e),
      randomIntBetween(e, t, n) {
        let r = n?.halfOpen === !0 ? 0 : 1;
        return D(() => {
          let n = Math.ceil(e),
            i = Math.floor(t);
          return Math.floor(nextDoubleUnsafe() * (i - n + r)) + n;
        });
      },
      randomShuffle: (e) =>
        D(() => {
          let t = Array.from(e);
          for (let e = t.length - 1; e >= 1; --e) {
            let n = Math.min(e, Math.floor(nextDoubleUnsafe() * (e + 1))),
              r = t[e];
            ((t[e] = t[n]), (t[n] = r));
          }
          return t;
        }),
      randomUUIDv4: D(() => formatUUIDv4(t(16))),
      randomUUIDv7: ne((e) =>
        V(formatUUIDv7(e.currentTimeMillisUnsafe(), t(16))),
      ),
    });
  }, `make`),
  validateSize = (e, t) =>
    Number.isSafeInteger(t) && t >= 0
      ? V(t)
      : _(
          badArgument({
            module: `Crypto`,
            method: e,
            description: `size must be a non-negative safe integer`,
          }),
        ),
  hex = (e) => e.toString(16).padStart(2, `0`),
  formatUUID = (e) =>
    [
      e.subarray(0, 4),
      e.subarray(4, 6),
      e.subarray(6, 8),
      e.subarray(8, 10),
      e.subarray(10, 16),
    ]
      .map((e) => Array.from(e, hex).join(``))
      .join(`-`),
  formatUUIDv4 = (e) => (
    (e[6] = (e[6] & 15) | 64),
    (e[8] = (e[8] & 63) | 128),
    formatUUID(e)
  ),
  er = 2 ** 48 - 1,
  formatUUIDv7 = (e, t) => {
    let n = Math.min(Math.max(0, Math.trunc(e)), er);
    return (
      (t[0] = Math.floor(n / 2 ** 40)),
      (t[1] = Math.floor(n / 2 ** 32) & 255),
      (t[2] = Math.floor(n / 2 ** 24) & 255),
      (t[3] = Math.floor(n / 2 ** 16) & 255),
      (t[4] = Math.floor(n / 2 ** 8) & 255),
      (t[5] = n & 255),
      (t[6] = (t[6] & 15) | 112),
      (t[8] = (t[8] & 63) | 128),
      formatUUID(t)
    );
  },
  toHashAlgorithm = (e) => {
    switch (e) {
      case `SHA-1`:
        return `sha1`;
      case `SHA-256`:
        return `sha256`;
      case `SHA-384`:
        return `sha384`;
      case `SHA-512`:
        return `sha512`;
    }
  },
  digest = (e, t) =>
    H({
      try: () =>
        Uint8Array.from(Ut.createHash(toHashAlgorithm(e)).update(t).digest()),
      catch: (e) =>
        systemError({
          module: `Crypto`,
          method: `digest`,
          _tag: `Unknown`,
          description: `Could not compute digest`,
          cause: e,
        }),
    }),
  tr = S(Qn, $n({ randomBytes: Ut.randomBytes, digest })),
  handleBadArgument = (e) => (t) =>
    badArgument({
      module: `FileSystem`,
      method: e,
      description: t.message ?? String(t),
    }),
  nr = (() => {
    let e = r(
      K.access,
      handleErrnoException(`FileSystem`, `access`),
      handleBadArgument(`access`),
    );
    return (t, n) => {
      let r = K.constants.F_OK;
      return (
        n?.readable && (r |= K.constants.R_OK),
        n?.writable && (r |= K.constants.W_OK),
        e(t, r)
      );
    };
  })(),
  rr = (() => {
    let e = r(
      K.cp,
      handleErrnoException(`FileSystem`, `copy`),
      handleBadArgument(`copy`),
    );
    return (t, n, r) =>
      e(t, n, {
        force: r?.overwrite ?? !1,
        preserveTimestamps: r?.preserveTimestamps ?? !1,
        recursive: !0,
      });
  })(),
  ir = (() => {
    let e = r(
      K.copyFile,
      handleErrnoException(`FileSystem`, `copyFile`),
      handleBadArgument(`copyFile`),
    );
    return (t, n) => e(t, n);
  })(),
  ar = (() => {
    let e = r(
      K.chmod,
      handleErrnoException(`FileSystem`, `chmod`),
      handleBadArgument(`chmod`),
    );
    return (t, n) => e(t, n);
  })(),
  or = (() => {
    let e = r(
      K.chown,
      handleErrnoException(`FileSystem`, `chown`),
      handleBadArgument(`chown`),
    );
    return (t, n, r) => e(t, n, r);
  })(),
  sr = (() => {
    let e = r(
      K.link,
      handleErrnoException(`FileSystem`, `link`),
      handleBadArgument(`link`),
    );
    return (t, n) => e(t, n);
  })(),
  cr = (() => {
    let e = r(
      K.mkdir,
      handleErrnoException(`FileSystem`, `makeDirectory`),
      handleBadArgument(`makeDirectory`),
    );
    return (t, n) => e(t, { recursive: n?.recursive ?? !1, mode: n?.mode });
  })(),
  makeTempDirectoryFactory = (e) => {
    let t = r(
      K.mkdtemp,
      handleErrnoException(`FileSystem`, e),
      handleBadArgument(e),
    );
    return (e) =>
      U(() => {
        let n = e?.prefix ?? ``,
          r =
            typeof e?.directory == `string`
              ? q.join(e.directory, `.`)
              : Vt.tmpdir();
        return t(n ? q.join(r, n) : r + `/`);
      });
  },
  lr = makeTempDirectoryFactory(`makeTempDirectory`),
  removeFactory = (e) => {
    let t = r(
      K.rm,
      handleErrnoException(`FileSystem`, e),
      handleBadArgument(e),
    );
    return (e, n) =>
      t(e, { recursive: n?.recursive ?? !1, force: n?.force ?? !1 });
  },
  ur = removeFactory(`remove`),
  dr = (() => {
    let e = makeTempDirectoryFactory(`makeTempDirectoryScoped`),
      t = removeFactory(`makeTempDirectoryScoped`);
    return (n) => R(e(n), (e) => kt(t(e, { recursive: !0 })));
  })(),
  openFactory = (e) => {
    let t = r(
        K.open,
        handleErrnoException(`FileSystem`, e),
        handleBadArgument(e),
      ),
      n = r(
        K.close,
        handleErrnoException(`FileSystem`, e),
        handleBadArgument(e),
      );
    return (e, r) =>
      j(
        R(t(e, r?.flag ?? `r`, r?.mode), (e) => kt(n(e))),
        m((e) => pr(Ln(e), r?.flag?.startsWith(`a`) ?? !1)),
      );
  },
  fr = openFactory(`open`),
  pr = (() => {
    let nodeReadFactory = (e) =>
        r(K.read, handleErrnoException(`FileSystem`, e), handleBadArgument(e)),
      e = nodeReadFactory(`read`),
      t = nodeReadFactory(`readAlloc`),
      n = r(
        K.fstat,
        handleErrnoException(`FileSystem`, `stat`),
        handleBadArgument(`stat`),
      ),
      i = r(
        K.ftruncate,
        handleErrnoException(`FileSystem`, `truncate`),
        handleBadArgument(`truncate`),
      ),
      a = r(
        K.fsync,
        handleErrnoException(`FileSystem`, `sync`),
        handleBadArgument(`sync`),
      ),
      nodeWriteFactory = (e) =>
        r(K.write, handleErrnoException(`FileSystem`, e), handleBadArgument(e)),
      o = nodeWriteFactory(`write`),
      s = nodeWriteFactory(`writeAll`);
    class FileImpl {
      [In];
      fd;
      append;
      position = BigInt(0);
      constructor(e, t) {
        ((this[In] = In), (this.fd = e), (this.append = t));
      }
      get stat() {
        return m(n(this.fd), makeFileInfo);
      }
      get sync() {
        return a(this.fd);
      }
      seek(e, t) {
        let n = Size(e);
        return D(
          () => (
            t === `start`
              ? (this.position = n)
              : t === `current` && (this.position += n),
            this.position
          ),
        );
      }
      read(t) {
        return U(() => {
          let n = this.position;
          return m(e(this.fd, { buffer: t, position: n }), (e) => {
            let t = Size(e);
            return ((this.position = n + t), t);
          });
        });
      }
      readAlloc(e) {
        let n = Number(e);
        return U(() => {
          let e = Buffer.allocUnsafeSlow(n),
            r = this.position;
          return m(t(this.fd, { buffer: e, position: r }), (t) => {
            if (t === 0) return y();
            if (((this.position = r + BigInt(t)), t === n)) return f(e);
            let i = Buffer.allocUnsafeSlow(t);
            return (e.copy(i, 0, 0, t), f(i));
          });
        });
      }
      truncate(e) {
        return m(i(this.fd, e ? Number(e) : void 0), () => {
          if (!this.append) {
            let t = BigInt(e ?? 0);
            this.position > t && (this.position = t);
          }
        });
      }
      write(e) {
        return U(() => {
          let t = this.position;
          return m(
            o(this.fd, e, void 0, void 0, this.append ? void 0 : Number(t)),
            (e) => {
              let n = Size(e);
              return (this.append || (this.position = t + n), n);
            },
          );
        });
      }
      writeAllChunk(e) {
        return U(() => {
          let t = this.position;
          return b(
            s(this.fd, e, void 0, void 0, this.append ? void 0 : Number(t)),
            (n) =>
              n === 0
                ? _(
                    systemError({
                      module: `FileSystem`,
                      method: `writeAll`,
                      _tag: `WriteZero`,
                      pathOrDescriptor: this.fd,
                      description: `write returned 0 bytes written`,
                    }),
                  )
                : (this.append || (this.position = t + BigInt(n)),
                  n < e.length ? this.writeAllChunk(e.subarray(n)) : N),
          );
        });
      }
      writeAll(e) {
        return this.writeAllChunk(e);
      }
    }
    return (e, t) => new FileImpl(e, t);
  })(),
  makeTempFileFactory = (e) => {
    let t = makeTempDirectoryFactory(e);
    return g(function* (e) {
      let n = yield* t(e),
        r = Ut.randomBytes(6).toString(`hex`),
        i = q.join(n, e?.suffix ? `${r}${e.suffix}` : r);
      return (yield* writeFile(i, new Uint8Array()), i);
    });
  },
  mr = makeTempFileFactory(`makeTempFile`),
  hr = (() => {
    let e = makeTempFileFactory(`makeTempFileScoped`),
      t = removeFactory(`makeTempFileScoped`);
    return (n) => R(e(n), (e) => kt(t(q.dirname(e), { recursive: !0 })));
  })(),
  readDirectory = (e, t) =>
    o({
      try: () => K.promises.readdir(e, t),
      catch: (t) => handleErrnoException(`FileSystem`, `readDirectory`)(t, [e]),
    }),
  readFile = (e) =>
    O((t, n) => {
      try {
        K.readFile(e, { signal: n }, (n, r) => {
          t(
            n
              ? _(handleErrnoException(`FileSystem`, `readFile`)(n, [e]))
              : V(r),
          );
        });
      } catch (e) {
        t(_(handleBadArgument(`readFile`)(e)));
      }
    }),
  gr = (() => {
    let e = r(
      K.readlink,
      handleErrnoException(`FileSystem`, `readLink`),
      handleBadArgument(`readLink`),
    );
    return (t) => e(t);
  })(),
  _r = (() => {
    let e = r(
      K.realpath,
      handleErrnoException(`FileSystem`, `realPath`),
      handleBadArgument(`realPath`),
    );
    return (t) => e(t);
  })(),
  vr = (() => {
    let e = r(
      K.rename,
      handleErrnoException(`FileSystem`, `rename`),
      handleBadArgument(`rename`),
    );
    return (t, n) => e(t, n);
  })(),
  makeFileInfo = (e) => ({
    type: e.isFile()
      ? `File`
      : e.isDirectory()
        ? `Directory`
        : e.isSymbolicLink()
          ? `SymbolicLink`
          : e.isBlockDevice()
            ? `BlockDevice`
            : e.isCharacterDevice()
              ? `CharacterDevice`
              : e.isFIFO()
                ? `FIFO`
                : e.isSocket()
                  ? `Socket`
                  : `Unknown`,
    mtime: c(e.mtime),
    atime: c(e.atime),
    birthtime: c(e.birthtime),
    dev: e.dev,
    rdev: c(e.rdev),
    ino: c(e.ino),
    mode: e.mode,
    nlink: c(e.nlink),
    uid: c(e.uid),
    gid: c(e.gid),
    size: Size(e.size),
    blksize: e.blksize === void 0 ? y() : f(Size(e.blksize)),
    blocks: c(e.blocks),
  }),
  yr = (() => {
    let e = r(
      K.stat,
      handleErrnoException(`FileSystem`, `stat`),
      handleBadArgument(`stat`),
    );
    return (t) => m(e(t), makeFileInfo);
  })(),
  br = (() => {
    let e = r(
      K.symlink,
      handleErrnoException(`FileSystem`, `symlink`),
      handleBadArgument(`symlink`),
    );
    return (t, n) => e(t, n);
  })(),
  xr = (() => {
    let e = r(
      K.truncate,
      handleErrnoException(`FileSystem`, `truncate`),
      handleBadArgument(`truncate`),
    );
    return (t, n) => e(t, n === void 0 ? void 0 : Number(n));
  })(),
  Sr = (() => {
    let e = r(
      K.utimes,
      handleErrnoException(`FileSystem`, `utime`),
      handleBadArgument(`utime`),
    );
    return (t, n, r) => e(t, n, r);
  })(),
  watchNode = (e) =>
    callback((n) =>
      R(
        D(() => {
          let r = K.watch(e, { recursive: !0 }, (e, t) => {
            if (t)
              switch (e) {
                case `rename`:
                  Le(
                    _e(yr(t), {
                      onSuccess: (e) => le(n, { _tag: `Create`, path: t }),
                      onFailure: (e) => le(n, { _tag: `Remove`, path: t }),
                    }),
                  );
                  return;
                case `change`:
                  me(n, { _tag: `Update`, path: t });
                  return;
              }
          });
          return (
            r.on(`error`, (r) => {
              t(
                n,
                Pe(
                  systemError({
                    module: `FileSystem`,
                    _tag: `Unknown`,
                    method: `watch`,
                    pathOrDescriptor: e,
                    cause: r,
                  }),
                ),
              );
            }),
            r.on(`close`, () => {
              fe(n);
            }),
            r
          );
        }),
        (e) => D(() => e.close()),
      ),
    ),
  watch = (e, t) =>
    yr(t).pipe(
      m((n) =>
        e.pipe(
          ee((e) => e.register(t, n)),
          rt(() => watchNode(t)),
        ),
      ),
      unwrap,
    ),
  writeFile = (e, t, n) =>
    O((r, i) => {
      try {
        K.writeFile(e, t, { signal: i, flag: n?.flag, mode: n?.mode }, (t) => {
          r(t ? _(handleErrnoException(`FileSystem`, `writeFile`)(t, [e])) : N);
        });
      } catch (e) {
        r(_(handleBadArgument(`writeFile`)(e)));
      }
    }),
  Cr = m(Ke(WatchBackend), (e) =>
    Fn({
      access: nr,
      chmod: ar,
      chown: or,
      copy: rr,
      copyFile: ir,
      link: sr,
      makeDirectory: cr,
      makeTempDirectory: lr,
      makeTempDirectoryScoped: dr,
      makeTempFile: mr,
      makeTempFileScoped: hr,
      open: fr,
      readDirectory,
      readFile,
      readLink: gr,
      realPath: _r,
      remove: ur,
      rename: vr,
      stat: yr,
      symlink: br,
      truncate: xr,
      utimes: Sr,
      watch(t) {
        return watch(e, t);
      },
      writeFile,
    }),
  ),
  wr = h($)(Cr),
  fromFileUrl = (e) =>
    H({
      try: () => Wt.fileURLToPath(e),
      catch: (e) =>
        new BadArgument({ module: `Path`, method: `fromFileUrl`, cause: e }),
    }),
  toFileUrl = (e) =>
    H({
      try: () => Wt.pathToFileURL(e),
      catch: (e) =>
        new BadArgument({ module: `Path`, method: `toFileUrl`, cause: e }),
    });
(({ ...q.posix }), { ...q.win32 });
const Tr = S(zn)({ [Rn]: Rn, ...q, fromFileUrl, toFileUrl }),
  Er = `~effect/Stdio`,
  Dr = S(
    E(Er),
    M(
      (e) => ({ [Er]: Er, ...e }),
      `make`,
    )({
      args: D(() => process.argv.slice(2)),
      stdout: (e) =>
        fromWritable({
          evaluate: () => process.stdout,
          onError: (e) =>
            systemError({
              module: `Stdio`,
              method: `stdout`,
              _tag: `Unknown`,
              cause: e,
            }),
          endOnDone: e?.endOnDone ?? !1,
        }),
      stderr: (e) =>
        fromWritable({
          evaluate: () => process.stderr,
          onError: (e) =>
            systemError({
              module: `Stdio`,
              method: `stderr`,
              _tag: `Unknown`,
              cause: e,
            }),
          endOnDone: e?.endOnDone ?? !1,
        }),
      stdin: fromReadable({
        evaluate: () => process.stdin,
        onError: (e) =>
          systemError({
            module: `Stdio`,
            method: `stdin`,
            _tag: `Unknown`,
            cause: e,
          }),
        closeOnDone: !1,
      }),
    }),
  ),
  Or = `~effect/platform/Terminal`;
ie(`QuitError`)({ _tag: pt(`QuitError`) });
const kr = E(`effect/platform/Terminal`),
  Ar = M((e) => kr.of({ ...e, [Or]: Or }), `make`),
  jr = h(
    kr,
    g(function* (e = defaultShouldQuit) {
      let t = process.stdin,
        n = process.stdout,
        r = yield* nn({
          acquire: R(
            D(() => {
              let e = Ht.createInterface({ input: t, escapeCodeTimeout: 50 });
              return (
                Ht.emitKeypressEvents(t, e), t.isTTY && t.setRawMode(!0), e
              );
            }),
            (e) =>
              D(() => {
                (t.isTTY && t.setRawMode(!1), e.close());
              }),
          ),
        }),
        i = D(() => n.columns ?? 0),
        a = D(() => n.rows ?? 0),
        o = Te(function* () {
          yield* rn(r);
          let n = yield* ce(),
            handleKeypress = (t, r) => {
              let i = {
                input: pe(t),
                key: {
                  name: r.name ?? ``,
                  ctrl: !!r.ctrl,
                  meta: !!r.meta,
                  shift: !!r.shift,
                },
              };
              (me(n, i), e(i) && fe(n));
            };
          return (
            yield* dt(() => D(() => t.off(`keypress`, handleKeypress))),
            t.on(`keypress`, handleKeypress),
            n
          );
        }),
        s = lt(
          b(rn(r), (e) =>
            O((t) => {
              let onLine = (e) => t(V(e));
              return (e.once(`line`, onLine), D(() => e.off(`line`, onLine)));
            }),
          ),
        ),
        display = (e) =>
          Ce(
            O((t) => {
              n.write(e, (e) =>
                Dt(e)
                  ? t(N)
                  : t(
                      _(
                        badArgument({
                          module: `Terminal`,
                          method: `display`,
                          description: `Failed to write prompt to stdout`,
                          cause: e,
                        }),
                      ),
                    ),
              );
            }),
          );
      return Ar({ columns: i, rows: a, readInput: o, readLine: s, display });
    })(defaultShouldQuit),
  );
function defaultShouldQuit(e) {
  return e.key.ctrl && (e.key.name === `c` || e.key.name === `d`);
}
export {
  Z as A,
  pn as C,
  xn as D,
  mkUint8Array as E,
  jn as F,
  Mn as I,
  An as L,
  splitLines as M,
  succeed as N,
  Tn as O,
  suspend as P,
  unwrap as R,
  isStream as S,
  Cn as T,
  hn as _,
  tr as a,
  fromIterableEffect as b,
  $ as c,
  En as d,
  X as f,
  bn as g,
  fail as h,
  wr as i,
  On as j,
  vn as k,
  Size as l,
  Dn as m,
  Dr as n,
  Xn as o,
  encodeText as p,
  Tr as r,
  zn as s,
  jr as t,
  Sn as u,
  Y as v,
  mn as w,
  fromReadableStream as x,
  fromIterable as y,
  head as z,
};
