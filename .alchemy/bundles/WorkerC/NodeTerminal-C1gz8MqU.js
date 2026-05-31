import {
  $i as e,
  Ai as t,
  An as n,
  Ar as r,
  At as i,
  Bt as a,
  C as o,
  Ca as s,
  Ci as c,
  Cn as l,
  Cr as u,
  Ct as d,
  D as f,
  Di as p,
  Dt as ee,
  E as te,
  En as m,
  F as ne,
  Fa as h,
  Fn as g,
  Fr as _,
  Ft as v,
  Gr as re,
  Ht as y,
  Ii as ie,
  In as ae,
  Ir as oe,
  It as se,
  Ji as ce,
  Jt as le,
  Kt as ue,
  L as de,
  Ln as fe,
  Lt as b,
  Mn as pe,
  Mr as me,
  N as he,
  Na as x,
  Ni as ge,
  Nt as S,
  O as _e,
  Oi as ve,
  On as ye,
  Or as be,
  Ot as xe,
  P as Se,
  Pa as C,
  Pr as Ce,
  Pt as we,
  Ra as w,
  Ri as Te,
  S as Ee,
  Sa as De,
  Si as Oe,
  Sn as ke,
  T as Ae,
  Ta as T,
  Tn as E,
  Tr as je,
  V as Me,
  Vt as Ne,
  W as Pe,
  Wn as D,
  Wt as Fe,
  Xi as Ie,
  Yi as O,
  Yn as k,
  Zi as Le,
  Zt as A,
  _ as Re,
  _r as j,
  _t as ze,
  a as Be,
  ar as Ve,
  b as He,
  ba as Ue,
  bn as We,
  bt as M,
  c as Ge,
  ca as Ke,
  cr as N,
  d as P,
  da as qe,
  dn as Je,
  dr as Ye,
  f as Xe,
  g as Ze,
  gn as Qe,
  h as $e,
  hn as et,
  ht as F,
  i as tt,
  ia as nt,
  it as rt,
  j as it,
  ja as I,
  ji as at,
  jn as ot,
  jr as st,
  k as ct,
  ka as lt,
  ki as ut,
  kn as dt,
  kr as L,
  kt as ft,
  l as pt,
  ln as mt,
  m as ht,
  mn as gt,
  n as _t,
  na as R,
  o as vt,
  or as yt,
  p as bt,
  pn as xt,
  qi as St,
  qt as Ct,
  r as wt,
  ra as z,
  rn as Tt,
  s as Et,
  sn as B,
  sr as Dt,
  t as Ot,
  ta as kt,
  tr as V,
  u as At,
  v as jt,
  vr as Mt,
  w as Nt,
  wn as H,
  wt as Pt,
  x as Ft,
  xa as U,
  xn as It,
  xr as Lt,
  xt as Rt,
  y as W,
  yn as zt,
  yt as Bt,
  z as Vt,
  zi as G,
} from "./Channel-BjrpyLFT.js";
import * as K from "node:fs";
import * as q from "node:path";
import * as Ht from "node:os";
import * as Ut from "node:readline";
import "node:stream";
import * as Wt from "node:crypto";
import * as Gt from "node:url";
import * as Kt from "node:child_process";
const qt = `~effect/MutableRef`,
  Jt = {
    [qt]: qt,
    ...nt,
    toJSON() {
      return { _id: `MutableRef`, current: qe(this.current) };
    },
  },
  Yt = w((e) => {
    let t = Object.create(Jt);
    return ((t.current = e), t);
  }, `make`),
  Xt = {
    "~effect/Stream": { _R: x, _E: x, _A: x },
    pipe() {
      return h(this, arguments);
    },
  },
  Zt = w((e) => {
    let t = Object.create(Xt);
    return ((t.channel = e), t);
  }, `fromChannel`),
  Qt = { _tag: `Empty` },
  $t = { _tag: `Closed` },
  en = { _A: x, _E: x };
var RcRefImpl = class {
  "~effect/RcRef" = en;
  pipe() {
    return h(this, arguments);
  }
  state = Qt;
  semaphore = _e(1);
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
const tn = w(
    (e) =>
      fe((n) => {
        let r = n.context,
          i = t(r, Ve),
          a = new RcRefImpl(
            e.acquire,
            r,
            i,
            e.idleTimeToLive ? Oe(e.idleTimeToLive) : void 0,
          );
        return M(
          Dt(i, () => {
            let e = a.state._tag === `Acquired` ? N(a.state.scope, L) : g;
            return ((a.state = $t), e);
          }),
          a,
        );
      }),
    `make`,
  ),
  getState = (e) =>
    pe((t) => {
      switch (e.state._tag) {
        case `Closed`:
          return le;
        case `Acquired`:
          return (
            e.state.refCount++,
            e.state.fiber ? M(Vt(e.state.fiber), e.state) : H(e.state)
          );
        case `Empty`: {
          let n = Ye();
          return e.semaphore.withPermits(1)(
            t(Je(e.acquire, ve(e.context, Ve, n))).pipe(
              A((t) => {
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
  nn = a(function* (e) {
    let t = e,
      n = yield* getState(t),
      r = yield* zt,
      i = t.idleTimeToLive !== void 0 && c(t.idleTimeToLive);
    return (
      yield* Dt(
        r,
        () => (
          n.refCount--,
          n.refCount > 0
            ? g
            : t.idleTimeToLive === void 0
              ? ((t.state = Qt), N(n.scope, L))
              : n.invalidated
                ? N(n.scope, L)
                : (i &&
                    (n.fiber = l(t.idleTimeToLive).pipe(
                      b(() =>
                        t.state._tag === `Acquired` && t.state.refCount === 0
                          ? ((t.state = Qt), N(n.scope, L))
                          : g,
                      ),
                      we(
                        m(() => {
                          n.fiber = void 0;
                        }),
                      ),
                      et(t.context),
                      Me(t.scope),
                    )),
                  g)
        ),
      ),
      n.value
    );
  }),
  rn = tn,
  an = nn,
  on = `~effect/Sink`,
  sn = H([void 0]),
  cn = { _A: x, _In: x, _L: x, _E: x, _R: x },
  ln = {
    [on]: cn,
    pipe() {
      return h(this, arguments);
    },
  },
  isSink = (e) => Ue(e, on),
  un = w(
    (e) =>
      fromTransform((t, n) =>
        te(e)(t, n).pipe(b(y({ disableYield: !0 })), D(H)),
      ),
    `fromChannel`,
  ),
  fromTransform = (e) => {
    let t = Object.create(ln);
    return ((t.transform = e), t);
  },
  dn = w((e) => P((t, n) => H(b(e.transform(t, n), _))), `toChannel`),
  J = fromTransform((e) => D(y(e, { disableYield: !0 }), () => sn)),
  reduceWhile = (e, t, n) =>
    fromTransform((r) => {
      let i = e(),
        a;
      return t(i)
        ? r.pipe(
            b((e) => {
              for (let r = 0; r < e.length; r++)
                if (((i = n(i, e[r])), !t(i)))
                  return (r + 1 < e.length && (a = e.slice(r + 1)), _());
              return g;
            }),
            y({ disableYield: !0 }),
            D(() => H([i, a])),
          )
        : H([i]);
    }),
  fn = reduceWhile(R, e, (e, t) => z(t)),
  head = () => fn,
  forEach = (e) => forEachArray(Ne((t) => e(t), { discard: !0 })),
  forEachArray = (e) =>
    fromTransform((t) =>
      t.pipe(
        b(e),
        y({ disableYield: !0 }),
        D(() => sn),
      ),
    ),
  pn = w((e) => un(f(A(e, dn))), `unwrap`),
  isStream = (e) => Ue(e, `~effect/Stream`),
  Y = Zt,
  fromPull = (e) => Y(At(e)),
  transformPull = (e, t) => Y(P((n, r) => b(Ae(e.channel, r), (e) => t(e, r)))),
  toChannel = (e) => e.channel,
  callback = (e, t) => Y(Ot(e, t)),
  X = Y(wt),
  succeed = (e) => Y(Ee(G(e))),
  suspend = (e) => Y(o(() => e().channel)),
  fail = (e) => Y(Be(e)),
  fromIterable = (e, t) =>
    Array.isArray(e) && t?.chunkSize === void 0
      ? fromArray(e)
      : Y(pt(e, t?.chunkSize)),
  fromIterableEffect = (e) => unwrap(A(e, fromIterable)),
  fromArray = (e) => (ie(e) ? Y(Ee(e)) : X),
  fromReadableStream = (e) =>
    Y(
      P(
        a(function* (t, n) {
          let r = e.evaluate().getReader();
          return (
            yield* yt(
              n,
              e.releaseLockOnEnd
                ? m(() => r.releaseLock())
                : mt(() => r.cancel()),
            ),
            b(
              dt({ try: () => r.read(), catch: (t) => e.onError(t) }),
              ({ done: e, value: t }) => (e ? _() : H(G(t))),
            )
          );
        }),
      ),
    ),
  unwrap = (e) => Y(f(A(e, toChannel))),
  mn = I(2, (e, t) =>
    suspend(() => {
      let n = 0;
      return Y(
        Xe(
          e.channel,
          Te((e) => t(e, n++)),
        ),
      );
    }),
  ),
  hn = I(
    (e) => isStream(e[0]),
    (e, t, n) => e.channel.pipe(Ge, ht(t, n), Xe(G), Y),
  ),
  gn = I(
    (e) => isStream(e[0]),
    (e, t, n) =>
      e.channel.pipe(
        Ge,
        Et((e) => t(e).channel, n),
        Y,
      ),
  ),
  _n = I(
    (e) => isStream(e[0]),
    (e, t) => gn(e, x, t),
  ),
  vn = I(2, (e, t) => _n(fromArray([e, t]))),
  yn = I(2, (e, t) => vn(fromIterable(t), e)),
  bn = I(
    (e) => isStream(e[0]) && isStream(e[1]),
    (e, t, n) => Y(Ze(toChannel(e), toChannel(t), n)),
  ),
  xn = I(2, (e, t) => Y(vt(toChannel(e), t))),
  Sn = I(
    2,
    a(function* (e, t) {
      let n,
        r = yield* Nt(e.channel),
        i = fromPull(H(Pt(r, (e) => ((n = e), se(e))))),
        a = yield* Z(i, t);
      return n ? [a, X] : ((i = fromPull(H(r))), [a, i]);
    }),
  ),
  Cn = I(2, (e, t) =>
    e.channel.pipe(
      _t((e) => t(e).channel),
      Y,
    ),
  ),
  wn = I(2, (e, t) => Y($e(e.channel, t))),
  Tn = I(2, (e, t) =>
    transformPull(e, (e, n) =>
      m(() => {
        let r,
          i,
          a = E(() => {
            if (i !== void 0) {
              let e = i;
              return ((i = void 0), H(e));
            }
            return e;
          }).pipe(xe((e) => ((r = je(e)), _()))),
          o = A(
            E(() => t.transform(a, n)),
            ([e, t]) => ((i = t), G(e)),
          );
        return E(() => r || o);
      }),
    ),
  ),
  En = I(2, (e, t) => Y(jt(e.channel, t))),
  Dn = I(
    (e) => isStream(e[0]),
    (e, t) =>
      suspend(() => {
        let n = new TextDecoder(t?.encoding);
        return mn(e, (e) => n.decode(e, { stream: !0 }));
      }),
  ),
  encodeText = (e) =>
    suspend(() => {
      let t = new TextEncoder();
      return mn(e, (e) => t.encode(e));
    }),
  splitLines = (e) => e.channel.pipe(jt(Ft()), Y),
  On = I(2, (e, t) => Y(Re(e.channel, t))),
  kn = I(2, (e, t) => Y(tt(e.channel, t))),
  Z = I(2, (e, t) =>
    It((n) =>
      Ae(e.channel, n).pipe(
        b((e) => t.transform(e, n)),
        A(([e]) => e),
      ),
    ),
  ),
  runCollect = (e) =>
    W(
      e.channel,
      () => [],
      (e, t) => {
        for (let n = 0; n < t.length; n++) e.push(t[n]);
        return e;
      },
    ),
  An = I(2, (e, t) =>
    He(e.channel, (e) => {
      let n = 0;
      return ae({ while: () => n < e.length, body: () => t(e[n++]), step: lt });
    }),
  ),
  jn = I(2, (e, t) => He(e.channel, t)),
  mkString = (e) =>
    W(
      e.channel,
      () => ``,
      (e, t) => e + t.join(``),
    ),
  mkUint8Array = (e) =>
    A(
      W(
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
  Mn = I(
    (e) => isStream(e[0]),
    (e, t, n) => {
      let r,
        i,
        a = de(!1);
      return new ReadableStream(
        {
          start(n) {
            ((i = gt(
              Je(
                jn(e, (e) =>
                  a.whenOpen(
                    m(() => {
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
                e._tag === `Failure` ? n.error(re(e.cause)) : n.close();
              }));
          },
          pull() {
            return new Promise((e) => {
              ((r = e), a.openUnsafe());
            });
          },
          cancel() {
            if (i) return Qe(Rt(Vt(i)));
          },
        },
        n?.strategy,
      );
    },
  ),
  Nn = I(
    (e) => isStream(e[0]),
    (e, t) => Mn(e, ut(), t),
  ),
  Pn = I(
    (e) => isStream(e[0]),
    (e, t) => A(i(), (n) => Mn(e, n, t)),
  );
function nominal() {
  return Object.assign((e) => e, {
    option: (e) => z(e),
    result: (e) => St(e),
    is: (e) => !0,
  });
}
const Fn = `~effect/platform/PlatformError`;
var BadArgument = class extends st(`BadArgument`) {
    get message() {
      return `${this.module}.${this.method}${this.description ? `: ${this.description}` : ``}`;
    }
  },
  SystemError = class extends r {
    get message() {
      return `${this._tag}: ${this.module}.${this.method}${this.pathOrDescriptor === void 0 ? `` : ` (${this.pathOrDescriptor})`}${this.description ? `: ${this.description}` : ``}`;
    }
  },
  PlatformError = class extends st(`PlatformError`) {
    constructor(e) {
      `cause` in e
        ? super({ reason: e, cause: e.cause })
        : super({ reason: e });
    }
    [Fn] = Fn;
    get message() {
      return this.reason.message;
    }
  };
const systemError = (e) => new PlatformError(new SystemError(e)),
  badArgument = (e) => new PlatformError(new BadArgument(e)),
  In = `~effect/platform/FileSystem`,
  Size = (e) => (typeof e == `bigint` ? e : BigInt(e)),
  Q = BigInt(1024);
Q * Q * Q * Q * Q;
const $ = p(`effect/platform/FileSystem`),
  Ln = w(
    (e) =>
      $.of({
        ...e,
        [In]: In,
        exists: (t) =>
          C(
            e.access(t),
            M(!0),
            ee(`PlatformError`, (e) =>
              e.reason._tag === `NotFound` ? H(!1) : v(e),
            ),
          ),
        readFileString: (t, r) =>
          b(e.readFile(t), (e) =>
            n({
              try: () => new TextDecoder(r).decode(e),
              catch: (e) =>
                badArgument({
                  module: `FileSystem`,
                  method: `readFileString`,
                  description: `invalid encoding`,
                  cause: e,
                }),
            }),
          ),
        stream: a(function* (t, n) {
          let r = yield* e.open(t, { flag: `r` });
          n?.offset && (yield* r.seek(n.offset, `start`));
          let i = n?.bytesToRead === void 0 ? void 0 : Size(n.bytesToRead),
            a = BigInt(0),
            o = Size(n?.chunkSize ?? 64 * 1024),
            s = r.readAlloc(o);
          return fromPull(
            H(
              b(
                E(() =>
                  i !== void 0 && i <= a
                    ? _()
                    : i !== void 0 && i - a < o
                      ? r.readAlloc(i - a)
                      : s,
                ),
                kt({
                  onNone: () => _(),
                  onSome: (e) => ((a += BigInt(e.length)), H(G(e))),
                }),
              ),
            ),
          );
        }, unwrap),
        sink: (t, n) =>
          C(
            e.open(t, { flag: `w`, ...n }),
            A((e) => forEach((t) => e.writeAll(t))),
            pn,
          ),
        writeFileString: (t, r, i) =>
          b(
            n({
              try: () => new TextEncoder().encode(r),
              catch: (e) =>
                badArgument({
                  module: `FileSystem`,
                  method: `writeFileString`,
                  description: `could not encode string`,
                  cause: e,
                }),
            }),
            (n) => e.writeFile(t, n, i),
          ),
      }),
    `make`,
  ),
  Rn = `~effect/platform/FileSystem/File`,
  zn = nominal();
var WatchBackend = class extends p()(
  `effect/platform/FileSystem/WatchBackend`,
) {};
const Bn = `~effect/platform/Path`,
  Vn = p(`effect/Path`),
  Hn = nominal(),
  Un = nominal(),
  Wn = `~effect/ChildProcessSpawner/ChildProcessHandle`,
  Gn = {
    [Wn]: Wn,
    ...Ke,
    toJSON() {
      return { _id: `ChildProcessHandle`, pid: this.pid };
    },
  },
  makeHandle = (e) => Object.assign(Object.create(Gn), e),
  Kn = w((e) => {
    let streamString = (t, n) =>
        e(t).pipe(
          A((e) => Dn(n?.includeStderr === !0 ? e.all : e.stdout)),
          unwrap,
        ),
      streamLines = (e, t) => splitLines(streamString(e, t));
    return ChildProcessSpawner.of({
      spawn: e,
      exitCode: (t) => We(b(e(t), (e) => e.exitCode)),
      streamString,
      streamLines,
      lines: (e, t) => runCollect(streamLines(e, t)),
      string: (e, t) => mkString(streamString(e, t)),
    });
  }, `make`);
var ChildProcessSpawner = class extends p()(
  `effect/process/ChildProcessSpawner`,
) {};
const qn = `~effect/unstable/process/ChildProcess`,
  Jn = {
    ...ge({
      label: `Command`,
      evaluate(e) {
        return at(e.context, ChildProcessSpawner).spawn(this);
      },
    }),
    [qn]: qn,
  },
  makeStandardCommand = (e, t, n) =>
    Object.assign(Object.create(Jn), {
      _tag: `StandardCommand`,
      command: e,
      args: t,
      options: n,
    }),
  Yn = function make(...e) {
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
  Xn = new Set([
    ` `,
    `	`,
    `\r`,
    `
`,
  ]),
  Zn = { x: 3, u: 5 },
  splitByWhitespaces = (e, t) => {
    if (t.length === 0)
      return {
        tokens: [],
        hasLeadingWhitespace: !1,
        hasTrailingWhitespace: !1,
      };
    let n = Xn.has(t[0]),
      r = [],
      i = 0;
    for (let n = 0, a = 0; n < e.length; n += 1, a += 1) {
      let o = t[a];
      if (Xn.has(o)) (i !== n && r.push(e.slice(i, n)), (i = n + 1));
      else if (o === `\\`) {
        let e = t[a + 1];
        e ===
        `
`
          ? (--n, (a += 1))
          : e === `u` && t[a + 2] === `{`
            ? (a = t.indexOf(`}`, a + 3))
            : (a += Zn[e] ?? 1);
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
  Qn = `~effect/Stdio`,
  $n = p(Qn),
  er = w((e) => ({ [Qn]: Qn, ...e }), `make`),
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
  fromWritable = (e) => un(bt(fromWritableChannel(e), (e) => [e])),
  fromWritableChannel = (e) =>
    P((t) => {
      let n = e.evaluate();
      return H(pullIntoWritable({ ...e, writable: n, pull: t }));
    }),
  pullIntoWritable = (e) =>
    e.pull.pipe(
      b((t) => {
        let n = 0;
        return d(function loop(r) {
          for (; n < t.length; )
            if (!e.writable.write(t[n++], e.encoding)) {
              e.writable.once(`drain`, () => loop(r));
              return;
            }
          r(g);
        });
      }),
      y({ disableYield: !0 }),
      xt(
        d((t) => {
          let onError = (n) => t(v(e.onError(n)));
          return (
            e.writable.once(`error`, onError),
            m(() => {
              e.writable.off(`error`, onError);
            })
          );
        }),
      ),
      e.endOnDone === !1
        ? x
        : D((t) =>
            `closed` in e.writable && e.writable.closed
              ? _(t)
              : d((n) => {
                  (e.writable.once(`finish`, () => n(_(t))), e.writable.end());
                }),
          ),
    ),
  fromReadable = (e) => Y(fromReadableChannel(e)),
  fromReadableChannel = (e) =>
    P((t, n) =>
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
    if (t.readableEnded) return H(_());
    let n = e.closeOnDone ?? !0,
      r = e.exit ?? Yt(void 0),
      i = de(!1);
    function onReadable() {
      i.openUnsafe();
    }
    function onError(t) {
      ((r.current = je(e.onError(t))), i.openUnsafe());
    }
    function onEnd() {
      ((r.current = je(me())), i.openUnsafe());
    }
    (t.on(`readable`, onReadable),
      t.once(`error`, onError),
      t.once(`end`, onEnd));
    let a = E(function loop() {
      let t = e.readable.read(e.chunkSize);
      if (t === null)
        return r.current ? r.current : (i.closeUnsafe(), b(i.await, loop));
      let n = G(t);
      for (; (t = e.readable.read(e.chunkSize)), t !== null; ) n.push(t);
      return H(n);
    });
    return M(
      yt(
        e.scope,
        m(() => {
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
  defaultOnError = (e) => new Ce(e),
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
  tr = k(
    ChildProcessSpawner,
    ue(function* () {
      let e = yield* $,
        t = yield* Vn,
        r = a(function* (n) {
          if (!T(n.cwd)) return (yield* e.access(n.cwd), t.resolve(n.cwd));
        }),
        resolveEnvironment = (e) =>
          e.extendEnv ? { ...globalThis.process.env, ...e.env } : e.env,
        inputToStdioOption = (e) => (isStream(e) ? `pipe` : e),
        outputToStdioOption = (e) => (isSink(e) ? `pipe` : e),
        resolveStdinOption = (e) => {
          let t = { stream: `pipe`, encoding: `utf-8`, endOnDone: !0 };
          return T(e.stdin)
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
          return T(n)
            ? { stream: `pipe` }
            : typeof n == `string` || isSink(n)
              ? { stream: n }
              : { stream: n.stream };
        },
        resolveAdditionalFds = (e) => {
          if (T(e.additionalFds)) return [];
          let t = [];
          for (let [n, r] of Object.entries(e.additionalFds)) {
            let e = parseFdName(n);
            De(e) && t.push({ fd: e, config: r });
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
        i = a(function* (e, t, n) {
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
                  a.stream && (yield* Fe(Z(a.stream, t))),
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
                  a.sink && (t = Tn(t, a.sink)),
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
        setupChildStdin = (e, t, n) =>
          E(() => {
            let r = J;
            return (
              U(t.stdin) &&
                (r = fromWritable({
                  evaluate: () => t.stdin,
                  onError: (t) =>
                    toPlatformError(`fromWritable(stdin)`, toError(t), e),
                  endOnDone: n.endOnDone,
                  encoding: n.encoding,
                })),
              isStream(n.stream) ? M(Fe(Z(n.stream, r)), r) : H(r)
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
          (isSink(n.stream) && (i = Tn(i, n.stream)),
            isSink(r.stream) && (a = Tn(a, r.stream)));
          let o = bn(i, a);
          return { stdout: i, stderr: a, all: o };
        },
        spawn = (e, t) =>
          d((n) => {
            let r = u(),
              i = Kt.spawn(e.command, e.args, t);
            return (
              i.on(`error`, (t) => {
                n(v(toPlatformError(`spawn`, t, e)));
              }),
              i.on(`exit`, (...e) => {
                Mt(r, be(e));
              }),
              i.on(`spawn`, () => {
                n(H([i, r]));
              }),
              m(() => {
                i.kill(`SIGTERM`);
              })
            );
          }),
        killProcessGroup = (e, t, r) =>
          globalThis.process.platform === `win32`
            ? d((n) => {
                Kt.exec(`taskkill /pid ${t.pid} /T /F`, (t) => {
                  n(t ? v(toPlatformError(`kill`, toError(t), e)) : g);
                });
              })
            : n({
                try: () => {
                  globalThis.process.kill(-t.pid, r);
                },
                catch: (t) => toPlatformError(`kill`, toError(t), e),
              }),
        killProcessGroupOnExit = (e, t) => {
          if (globalThis.process.platform === `win32`) {
            Kt.exec(`taskkill /pid ${e.pid} /T /F`, () => {});
            return;
          }
          try {
            globalThis.process.kill(-e.pid, t);
          } catch {}
        },
        killProcess = (e, t, n) =>
          E(() =>
            t.kill(n)
              ? g
              : v(
                  toPlatformError(
                    `kill`,
                    new globalThis.Error(`Failed to kill child process`),
                    e,
                  ),
                ),
          ),
        withTimeout = (e, t, n) => (r) => {
          let i = n?.killSignal ?? `SIGTERM`;
          return T(n?.forceKillAfter)
            ? r(t, e, i)
            : ye(r(t, e, i), {
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
              return De(t) ? e.getOutputFd(t) : e.stdout;
            }
          }
        },
        o = a(function* (e) {
          switch (e._tag) {
            case `StandardCommand`: {
              let t = resolveStdinOption(e.options),
                n = resolveOutputOption(e.options, `stdout`),
                o = resolveOutputOption(e.options, `stderr`),
                s = resolveAdditionalFds(e.options),
                c = !0,
                l = !1,
                [u, d] = yield* F(
                  spawn(e, {
                    cwd: yield* r(e.options),
                    env: resolveEnvironment(e.options),
                    stdio: buildStdioArray(t, n, o, s),
                    detached:
                      e.options.detached ?? process.platform !== `win32`,
                    shell: e.options.shell,
                  }),
                  a(function* ([t, n]) {
                    let r = yield* Lt(n),
                      i = withTimeout(t, e, e.options);
                    if (r) {
                      let [e] = yield* j(n);
                      return e !== 0 && U(e)
                        ? yield* Ct(i(killProcessGroup))
                        : yield* g;
                    }
                    return c
                      ? yield* i((e, t, n) =>
                          xe(killProcessGroup(e, t, n), () =>
                            killProcess(e, t, n),
                          ),
                        ).pipe(Bt(j(n)), Ct)
                      : yield* g;
                  }),
                ),
                f = Un(u.pid);
              u.on(`exit`, (t) => {
                l &&
                  t !== 0 &&
                  U(t) &&
                  killProcessGroupOnExit(u, e.options.killSignal ?? `SIGTERM`);
              });
              let p = m(() => {
                  c || (u.ref(), (c = !0), (l = !1));
                }),
                ee = m(() => (c && (u.unref(), (c = !1), (l = !0)), p)),
                te = yield* setupChildStdin(e, u, t),
                {
                  all: ne,
                  stderr: h,
                  stdout: _,
                } = setupChildOutputStreams(e, u, n, o),
                { getInputFd: re, getOutputFd: y } = yield* i(e, u, s),
                ie = A(Lt(d), (e) => !e),
                ae = b(j(d), ([t, n]) =>
                  U(t)
                    ? H(Hn(t))
                    : v(
                        toPlatformError(
                          `exitCode`,
                          new globalThis.Error(
                            `Process interrupted due to receipt of signal: '${n}'`,
                          ),
                          e,
                        ),
                      ),
                ),
                kill = (t) =>
                  withTimeout(
                    u,
                    e,
                    t,
                  )((e, t, n) =>
                    xe(killProcessGroup(e, t, n), () => killProcess(e, t, n)),
                  ).pipe(Bt(j(d)), Rt);
              return makeHandle({
                pid: f,
                exitCode: ae,
                isRunning: ie,
                kill,
                stdin: te,
                stdout: _,
                stderr: h,
                all: ne,
                getInputFd: re,
                getOutputFd: y,
                unref: ee,
              });
            }
            case `PipedCommand`: {
              let { commands: t, pipeOptions: n } = flattenCommand(e),
                [r, ...i] = t,
                a = [yield* o(r)];
              for (let e = 0; e < i.length; e++) {
                let t = i[e],
                  r = n[e] ?? {},
                  s = resolveStdinOption(t.options),
                  c = unwrap(H(getSourceStream(a[a.length - 1], r.from))),
                  l = r.to ?? `stdin`;
                if (l === `stdin`)
                  a.push(
                    yield* o(
                      Yn(t.command, t.args, {
                        ...t.options,
                        stdin: { ...s, stream: c },
                      }),
                    ),
                  );
                else {
                  let e = parseFdName(l);
                  if (De(e)) {
                    let n = fdName(e),
                      r = t.options.additionalFds ?? {};
                    a.push(
                      yield* o(
                        Yn(t.command, t.args, {
                          ...t.options,
                          additionalFds: {
                            ...r,
                            [n]: { type: `input`, stream: c },
                          },
                        }),
                      ),
                    );
                  } else
                    a.push(
                      yield* o(
                        Yn(t.command, t.args, {
                          ...t.options,
                          stdin: { ...s, stream: c },
                        }),
                      ),
                    );
                }
              }
              let s = a[a.length - 1],
                c = ue(function* () {
                  let e = [];
                  for (let t of a) e.push(yield* t.unref);
                  return Ne([...e].reverse(), (e) => e, { discard: !0 });
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
      return Kn(o);
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
  nr = `~effect/platform/Crypto`,
  rr = p(`effect/Crypto`),
  ir = w((e) => {
    let t = e.randomBytes,
      randomBytes = (e) => A(validateSize(`randomBytes`, e), t),
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
    return rr.of({
      [nr]: nr,
      randomBytes,
      nextDoubleUnsafe,
      nextIntUnsafe,
      digest: e.digest,
      random: m(() => nextDoubleUnsafe()),
      randomBoolean: m(() => nextDoubleUnsafe() > 0.5),
      randomInt: m(() => nextIntUnsafe()),
      randomBetween: (e, t) => m(() => nextDoubleUnsafe() * (t - e) + e),
      randomIntBetween(e, t, n) {
        let r = n?.halfOpen === !0 ? 0 : 1;
        return m(() => {
          let n = Math.ceil(e),
            i = Math.floor(t);
          return Math.floor(nextDoubleUnsafe() * (i - n + r)) + n;
        });
      },
      randomShuffle: (e) =>
        m(() => {
          let t = Array.from(e);
          for (let e = t.length - 1; e >= 1; --e) {
            let n = Math.min(e, Math.floor(nextDoubleUnsafe() * (e + 1))),
              r = t[e];
            ((t[e] = t[n]), (t[n] = r));
          }
          return t;
        }),
      randomUUIDv4: m(() => formatUUIDv4(t(16))),
      randomUUIDv7: ft((e) =>
        H(formatUUIDv7(e.currentTimeMillisUnsafe(), t(16))),
      ),
    });
  }, `make`),
  validateSize = (e, t) =>
    Number.isSafeInteger(t) && t >= 0
      ? H(t)
      : v(
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
  ar = 2 ** 48 - 1,
  formatUUIDv7 = (e, t) => {
    let n = Math.min(Math.max(0, Math.trunc(e)), ar);
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
    n({
      try: () =>
        Uint8Array.from(Wt.createHash(toHashAlgorithm(e)).update(t).digest()),
      catch: (e) =>
        systemError({
          module: `Crypto`,
          method: `digest`,
          _tag: `Unknown`,
          description: `Could not compute digest`,
          cause: e,
        }),
    }),
  or = V(rr, ir({ randomBytes: Wt.randomBytes, digest })),
  handleBadArgument = (e) => (t) =>
    badArgument({
      module: `FileSystem`,
      method: e,
      description: t.message ?? String(t),
    }),
  sr = (() => {
    let e = S(
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
  cr = (() => {
    let e = S(
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
  lr = (() => {
    let e = S(
      K.copyFile,
      handleErrnoException(`FileSystem`, `copyFile`),
      handleBadArgument(`copyFile`),
    );
    return (t, n) => e(t, n);
  })(),
  ur = (() => {
    let e = S(
      K.chmod,
      handleErrnoException(`FileSystem`, `chmod`),
      handleBadArgument(`chmod`),
    );
    return (t, n) => e(t, n);
  })(),
  dr = (() => {
    let e = S(
      K.chown,
      handleErrnoException(`FileSystem`, `chown`),
      handleBadArgument(`chown`),
    );
    return (t, n, r) => e(t, n, r);
  })(),
  fr = (() => {
    let e = S(
      K.link,
      handleErrnoException(`FileSystem`, `link`),
      handleBadArgument(`link`),
    );
    return (t, n) => e(t, n);
  })(),
  pr = (() => {
    let e = S(
      K.mkdir,
      handleErrnoException(`FileSystem`, `makeDirectory`),
      handleBadArgument(`makeDirectory`),
    );
    return (t, n) => e(t, { recursive: n?.recursive ?? !1, mode: n?.mode });
  })(),
  makeTempDirectoryFactory = (e) => {
    let t = S(
      K.mkdtemp,
      handleErrnoException(`FileSystem`, e),
      handleBadArgument(e),
    );
    return (e) =>
      E(() => {
        let n = e?.prefix ?? ``,
          r =
            typeof e?.directory == `string`
              ? q.join(e.directory, `.`)
              : Ht.tmpdir();
        return t(n ? q.join(r, n) : r + `/`);
      });
  },
  mr = makeTempDirectoryFactory(`makeTempDirectory`),
  removeFactory = (e) => {
    let t = S(
      K.rm,
      handleErrnoException(`FileSystem`, e),
      handleBadArgument(e),
    );
    return (e, n) =>
      t(e, { recursive: n?.recursive ?? !1, force: n?.force ?? !1 });
  },
  hr = removeFactory(`remove`),
  gr = (() => {
    let e = makeTempDirectoryFactory(`makeTempDirectoryScoped`),
      t = removeFactory(`makeTempDirectoryScoped`);
    return (n) => F(e(n), (e) => B(t(e, { recursive: !0 })));
  })(),
  openFactory = (e) => {
    let t = S(
        K.open,
        handleErrnoException(`FileSystem`, e),
        handleBadArgument(e),
      ),
      n = S(
        K.close,
        handleErrnoException(`FileSystem`, e),
        handleBadArgument(e),
      );
    return (e, r) =>
      C(
        F(t(e, r?.flag ?? `r`, r?.mode), (e) => B(n(e))),
        A((e) => vr(zn(e), r?.flag?.startsWith(`a`) ?? !1)),
      );
  },
  _r = openFactory(`open`),
  vr = (() => {
    let nodeReadFactory = (e) =>
        S(K.read, handleErrnoException(`FileSystem`, e), handleBadArgument(e)),
      e = nodeReadFactory(`read`),
      t = nodeReadFactory(`readAlloc`),
      n = S(
        K.fstat,
        handleErrnoException(`FileSystem`, `stat`),
        handleBadArgument(`stat`),
      ),
      r = S(
        K.ftruncate,
        handleErrnoException(`FileSystem`, `truncate`),
        handleBadArgument(`truncate`),
      ),
      i = S(
        K.fsync,
        handleErrnoException(`FileSystem`, `sync`),
        handleBadArgument(`sync`),
      ),
      nodeWriteFactory = (e) =>
        S(K.write, handleErrnoException(`FileSystem`, e), handleBadArgument(e)),
      a = nodeWriteFactory(`write`),
      o = nodeWriteFactory(`writeAll`);
    class FileImpl {
      [Rn];
      fd;
      append;
      position = BigInt(0);
      constructor(e, t) {
        ((this[Rn] = Rn), (this.fd = e), (this.append = t));
      }
      get stat() {
        return A(n(this.fd), makeFileInfo);
      }
      get sync() {
        return i(this.fd);
      }
      seek(e, t) {
        let n = Size(e);
        return m(
          () => (
            t === `start`
              ? (this.position = n)
              : t === `current` && (this.position += n),
            this.position
          ),
        );
      }
      read(t) {
        return E(() => {
          let n = this.position;
          return A(e(this.fd, { buffer: t, position: n }), (e) => {
            let t = Size(e);
            return ((this.position = n + t), t);
          });
        });
      }
      readAlloc(e) {
        let n = Number(e);
        return E(() => {
          let e = Buffer.allocUnsafeSlow(n),
            r = this.position;
          return A(t(this.fd, { buffer: e, position: r }), (t) => {
            if (t === 0) return R();
            if (((this.position = r + BigInt(t)), t === n)) return z(e);
            let i = Buffer.allocUnsafeSlow(t);
            return (e.copy(i, 0, 0, t), z(i));
          });
        });
      }
      truncate(e) {
        return A(r(this.fd, e ? Number(e) : void 0), () => {
          if (!this.append) {
            let t = BigInt(e ?? 0);
            this.position > t && (this.position = t);
          }
        });
      }
      write(e) {
        return E(() => {
          let t = this.position;
          return A(
            a(this.fd, e, void 0, void 0, this.append ? void 0 : Number(t)),
            (e) => {
              let n = Size(e);
              return (this.append || (this.position = t + n), n);
            },
          );
        });
      }
      writeAllChunk(e) {
        return E(() => {
          let t = this.position;
          return b(
            o(this.fd, e, void 0, void 0, this.append ? void 0 : Number(t)),
            (n) =>
              n === 0
                ? v(
                    systemError({
                      module: `FileSystem`,
                      method: `writeAll`,
                      _tag: `WriteZero`,
                      pathOrDescriptor: this.fd,
                      description: `write returned 0 bytes written`,
                    }),
                  )
                : (this.append || (this.position = t + BigInt(n)),
                  n < e.length ? this.writeAllChunk(e.subarray(n)) : g),
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
    return a(function* (e) {
      let n = yield* t(e),
        r = Wt.randomBytes(6).toString(`hex`),
        i = q.join(n, e?.suffix ? `${r}${e.suffix}` : r);
      return (yield* writeFile(i, new Uint8Array()), i);
    });
  },
  yr = makeTempFileFactory(`makeTempFile`),
  br = (() => {
    let e = makeTempFileFactory(`makeTempFileScoped`),
      t = removeFactory(`makeTempFileScoped`);
    return (n) => F(e(n), (e) => B(t(q.dirname(e), { recursive: !0 })));
  })(),
  readDirectory = (e, t) =>
    dt({
      try: () => K.promises.readdir(e, t),
      catch: (t) => handleErrnoException(`FileSystem`, `readDirectory`)(t, [e]),
    }),
  readFile = (e) =>
    d((t, n) => {
      try {
        K.readFile(e, { signal: n }, (n, r) => {
          t(
            n
              ? v(handleErrnoException(`FileSystem`, `readFile`)(n, [e]))
              : H(r),
          );
        });
      } catch (e) {
        t(v(handleBadArgument(`readFile`)(e)));
      }
    }),
  xr = (() => {
    let e = S(
      K.readlink,
      handleErrnoException(`FileSystem`, `readLink`),
      handleBadArgument(`readLink`),
    );
    return (t) => e(t);
  })(),
  Sr = (() => {
    let e = S(
      K.realpath,
      handleErrnoException(`FileSystem`, `realPath`),
      handleBadArgument(`realPath`),
    );
    return (t) => e(t);
  })(),
  Cr = (() => {
    let e = S(
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
    mtime: O(e.mtime),
    atime: O(e.atime),
    birthtime: O(e.birthtime),
    dev: e.dev,
    rdev: O(e.rdev),
    ino: O(e.ino),
    mode: e.mode,
    nlink: O(e.nlink),
    uid: O(e.uid),
    gid: O(e.gid),
    size: Size(e.size),
    blksize: e.blksize === void 0 ? R() : z(Size(e.blksize)),
    blocks: O(e.blocks),
  }),
  wr = (() => {
    let e = S(
      K.stat,
      handleErrnoException(`FileSystem`, `stat`),
      handleBadArgument(`stat`),
    );
    return (t) => A(e(t), makeFileInfo);
  })(),
  Tr = (() => {
    let e = S(
      K.symlink,
      handleErrnoException(`FileSystem`, `symlink`),
      handleBadArgument(`symlink`),
    );
    return (t, n) => e(t, n);
  })(),
  Er = (() => {
    let e = S(
      K.truncate,
      handleErrnoException(`FileSystem`, `truncate`),
      handleBadArgument(`truncate`),
    );
    return (t, n) => e(t, n === void 0 ? void 0 : Number(n));
  })(),
  Dr = (() => {
    let e = S(
      K.utimes,
      handleErrnoException(`FileSystem`, `utime`),
      handleBadArgument(`utime`),
    );
    return (t, n, r) => e(t, n, r);
  })(),
  watchNode = (e) =>
    callback((t) =>
      F(
        m(() => {
          let n = K.watch(e, { recursive: !0 }, (e, n) => {
            if (n)
              switch (e) {
                case `rename`:
                  gt(
                    Tt(wr(n), {
                      onSuccess: (e) => Se(t, { _tag: `Create`, path: n }),
                      onFailure: (e) => Se(t, { _tag: `Remove`, path: n }),
                    }),
                  );
                  return;
                case `change`:
                  ne(t, { _tag: `Update`, path: n });
                  return;
              }
          });
          return (
            n.on(`error`, (n) => {
              it(
                t,
                oe(
                  systemError({
                    module: `FileSystem`,
                    _tag: `Unknown`,
                    method: `watch`,
                    pathOrDescriptor: e,
                    cause: n,
                  }),
                ),
              );
            }),
            n.on(`close`, () => {
              ct(t);
            }),
            n
          );
        }),
        (e) => m(() => e.close()),
      ),
    ),
  watch = (e, t) =>
    wr(t).pipe(
      A((n) =>
        e.pipe(
          ce((e) => e.register(t, n)),
          Le(() => watchNode(t)),
        ),
      ),
      unwrap,
    ),
  writeFile = (e, t, n) =>
    d((r, i) => {
      try {
        K.writeFile(e, t, { signal: i, flag: n?.flag, mode: n?.mode }, (t) => {
          r(t ? v(handleErrnoException(`FileSystem`, `writeFile`)(t, [e])) : g);
        });
      } catch (e) {
        r(v(handleBadArgument(`writeFile`)(e)));
      }
    }),
  Or = A(ke(WatchBackend), (e) =>
    Ln({
      access: sr,
      chmod: ur,
      chown: dr,
      copy: cr,
      copyFile: lr,
      link: fr,
      makeDirectory: pr,
      makeTempDirectory: mr,
      makeTempDirectoryScoped: gr,
      makeTempFile: yr,
      makeTempFileScoped: br,
      open: _r,
      readDirectory,
      readFile,
      readLink: xr,
      realPath: Sr,
      remove: hr,
      rename: Cr,
      stat: wr,
      symlink: Tr,
      truncate: Er,
      utimes: Dr,
      watch(t) {
        return watch(e, t);
      },
      writeFile,
    }),
  ),
  kr = k($)(Or),
  fromFileUrl = (e) =>
    n({
      try: () => Gt.fileURLToPath(e),
      catch: (e) =>
        new BadArgument({ module: `Path`, method: `fromFileUrl`, cause: e }),
    }),
  toFileUrl = (e) =>
    n({
      try: () => Gt.pathToFileURL(e),
      catch: (e) =>
        new BadArgument({ module: `Path`, method: `toFileUrl`, cause: e }),
    });
(({ ...q.posix }), { ...q.win32 });
const Ar = V(Vn)({ [Bn]: Bn, ...q, fromFileUrl, toFileUrl }),
  jr = V(
    $n,
    er({
      args: m(() => process.argv.slice(2)),
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
  Mr = `~effect/platform/Terminal`;
Pe(`QuitError`)({ _tag: rt(`QuitError`) });
const Nr = p(`effect/platform/Terminal`),
  Pr = w((e) => Nr.of({ ...e, [Mr]: Mr }), `make`),
  Fr = k(
    Nr,
    a(function* (e = defaultShouldQuit) {
      let t = process.stdin,
        n = process.stdout,
        r = yield* rn({
          acquire: F(
            m(() => {
              let e = Ut.createInterface({ input: t, escapeCodeTimeout: 50 });
              return (
                Ut.emitKeypressEvents(t, e), t.isTTY && t.setRawMode(!0), e
              );
            }),
            (e) =>
              m(() => {
                (t.isTTY && t.setRawMode(!1), e.close());
              }),
          ),
        }),
        i = m(() => n.columns ?? 0),
        a = m(() => n.rows ?? 0),
        o = ue(function* () {
          yield* an(r);
          let n = yield* he(),
            handleKeypress = (t, r) => {
              let i = {
                input: Ie(t),
                key: {
                  name: r.name ?? ``,
                  ctrl: !!r.ctrl,
                  meta: !!r.meta,
                  shift: !!r.shift,
                },
              };
              (ne(n, i), e(i) && ct(n));
            };
          return (
            yield* ze(() => m(() => t.off(`keypress`, handleKeypress))),
            t.on(`keypress`, handleKeypress),
            n
          );
        }),
        c = We(
          b(an(r), (e) =>
            d((t) => {
              let onLine = (e) => t(H(e));
              return (e.once(`line`, onLine), m(() => e.off(`line`, onLine)));
            }),
          ),
        ),
        display = (e) =>
          ot(
            d((t) => {
              n.write(e, (e) =>
                s(e)
                  ? t(g)
                  : t(
                      v(
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
      return Pr({ columns: i, rows: a, readInput: o, readLine: c, display });
    })(defaultShouldQuit),
  );
function defaultShouldQuit(e) {
  return e.key.ctrl && (e.key.name === `c` || e.key.name === `d`);
}
export {
  yn as A,
  head as B,
  mn as C,
  On as D,
  mkUint8Array as E,
  suspend as F,
  Nn as I,
  Pn as L,
  An as M,
  splitLines as N,
  Sn as O,
  succeed as P,
  Mn as R,
  isStream as S,
  wn as T,
  gn as _,
  or as a,
  fromIterableEffect as b,
  $ as c,
  Dn as d,
  X as f,
  xn as g,
  fail as h,
  kr as i,
  Z as j,
  En as k,
  Size as l,
  kn as m,
  jr as n,
  tr as o,
  encodeText as p,
  Ar as r,
  Vn as s,
  Fr as t,
  Cn as u,
  Y as v,
  hn as w,
  fromReadableStream as x,
  fromIterable as y,
  unwrap as z,
};
