import {
  $n as e,
  Ma as t,
  Yn as n,
  rr as r,
  tr as i,
} from "./Channel-BjrpyLFT.js";
import { c as a, f as o, t as s } from "./Socket-B5eNQsB_.js";
import {
  a as c,
  i as l,
  n as u,
  o as d,
  r as f,
  t as p,
} from "./NodeSocket-CeWg7iSU.js";
const m = r(a)(() =>
    `WebSocket` in globalThis
      ? (e, t) => new globalThis.WebSocket(e, t)
      : (e, t) => new f.WebSocket(e, t),
  ),
  h = i(a)((e, t) => new f.WebSocket(e, t)),
  g = t(o, n(s), e(m));
export {
  p as NetSocket,
  f as NodeWS,
  u as fromDuplex,
  l as layerNet,
  g as layerWebSocket,
  m as layerWebSocketConstructor,
  h as layerWebSocketConstructorWS,
  c as makeNet,
  d as makeNetChannel,
};
