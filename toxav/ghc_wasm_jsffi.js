// This file implements the JavaScript runtime logic for Haskell
// modules that use JSFFI. It is not an ESM module, but the template
// of one; the post-linker script will copy all contents into a new
// ESM module.

// Manage a mapping from 32-bit ids to actual JavaScript values.
class JSValManager {
  #lastk = 0;
  #kv = new Map();

  newJSVal(v) {
    const k = ++this.#lastk;
    this.#kv.set(k, v);
    return k;
  }

  // A separate has() call to ensure we can store undefined as a value
  // too. Also, unconditionally check this since the check is cheap
  // anyway, if the check fails then there's a use-after-free to be
  // fixed.
  getJSVal(k) {
    if (!this.#kv.has(k)) {
      throw new WebAssembly.RuntimeError(`getJSVal(${k})`);
    }
    return this.#kv.get(k);
  }

  // Check for double free as well.
  freeJSVal(k) {
    if (!this.#kv.delete(k)) {
      throw new WebAssembly.RuntimeError(`freeJSVal(${k})`);
    }
  }
}

// The actual setImmediate() to be used. This is a ESM module top
// level binding and doesn't pollute the globalThis namespace.
//
// To benchmark different setImmediate() implementations in the
// browser, use https://github.com/jphpsf/setImmediate-shim-demo as a
// starting point.
const setImmediate = (() => {
  // node, bun, or other scripts might have set this up in the browser
  if (globalThis.setImmediate) {
    return globalThis.setImmediate;
  }

  // https://developer.mozilla.org/en-US/docs/Web/API/Scheduler/postTask
  if (globalThis.scheduler) {
    return (cb, ...args) => scheduler.postTask(() => cb(...args));
  }

  // Cloudflare workers doesn't support MessageChannel
  if (globalThis.MessageChannel) {
    // A simple & fast setImmediate() implementation for browsers. It's
    // not a drop-in replacement for node.js setImmediate() because:
    // 1. There's no clearImmediate(), and setImmediate() doesn't return
    //    anything
    // 2. There's no guarantee that callbacks scheduled by setImmediate()
    //    are executed in the same order (in fact it's the opposite lol),
    //    but you are never supposed to rely on this assumption anyway
    class SetImmediate {
      #fs = [];
      #mc = new MessageChannel();

      constructor() {
        this.#mc.port1.addEventListener("message", () => {
          this.#fs.pop()();
        });
        this.#mc.port1.start();
      }

      setImmediate(cb, ...args) {
        this.#fs.push(() => cb(...args));
        this.#mc.port2.postMessage(undefined);
      }
    }

    const sm = new SetImmediate();
    return (cb, ...args) => sm.setImmediate(cb, ...args);
  }

  return (cb, ...args) => setTimeout(cb, 0, ...args);
})();

export default (__exports) => {
const __ghc_wasm_jsffi_jsval_manager = new JSValManager();
const __ghc_wasm_jsffi_finalization_registry = globalThis.FinalizationRegistry ? new FinalizationRegistry(sp => __exports.rts_freeStablePtr(sp)) : { register: () => {}, unregister: () => true };
return {
newJSVal: (v) => __ghc_wasm_jsffi_jsval_manager.newJSVal(v),
getJSVal: (k) => __ghc_wasm_jsffi_jsval_manager.getJSVal(k),
freeJSVal: (k) => __ghc_wasm_jsffi_jsval_manager.freeJSVal(k),
scheduleWork: () => setImmediate(__exports.rts_schedulerLoop),
ZC2ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC1ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC($1, ...args)),
ZC4ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC3ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC($1, ...args)),
ZC6ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC5ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC($1, ...args)),
ZC8ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC7ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC($1, ...args)),
ZC9ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2,$3,$4,$5) => (({sendVideo: $1, sendAudio: $2, pull: $3, picture: $4, stats: $5})),
ZC10ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: () => (null),
ZC11ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2,$3,$4) => (({spedUp: $1, slowedDown: $2, madeUp: $3, skipped: $4})),
ZC12ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2,$3,$4,$5) => (({width: $1, height: $2, bytes: new Uint8Array(__exports.memory.buffer, $3, $4).slice(), delay: $5})),
ZC13ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2,$3,$4,$5) => (({rate: $1, channels: $2, samples: new Int16Array(__exports.memory.buffer, $3, $4).slice(), delay: $5})),
ZC14ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2) => (new Uint8Array(__exports.memory.buffer, $2, $1.byteLength).set(new Uint8Array($1.buffer, $1.byteOffset, $1.byteLength))),
ZC15ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1) => ($1.byteLength),
ZC16ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1) => ($1.length),
ZC17ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2) => ($1.log($2)),
ZC18ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2,$3) => ($1.audioCame($2, $3)),
ZC19ZCtoxavzm0zi0zi1zminplacezmtoxavzmbrowserZCMainZC: ($1,$2) => ($1.pictureCame($2)),
ZC0ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1) => {$1.closed = true;if ($1.codec !== null && $1.codec.state !== 'closed') $1.codec.close();},
ZC1ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1,$2) => (new Uint8Array(__exports.memory.buffer, $2, $1.byteLength).set($1)),
ZC2ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1) => ($1.byteLength),
ZC3ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1,$2) => ($1[$2].bytes),
ZC4ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1,$2) => ($1[$2].key),
ZC5ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1) => ($1.length),
ZC6ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1) => ($1 === null),
ZC8ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC7ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC($1, ...args)),
ZC9ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1,$2,$3,$4,$5) => {const s = $1, key = $4 !== 0;if (s.codec === null && key) s.open();if (s.codec === null) return;const data = new Uint8Array(__exports.memory.buffer, $2, $3).slice();try { s.codec.decode(new EncodedVideoChunk({type: key ? 'key' : 'delta', timestamp: $5 * 1000, data})); }catch (e) { s.say('warn', e.message); if (s.codec.state !== 'closed') s.codec.close(); s.codec = null; }},
ZC10ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: async ($1,$2,$3) => {const config = {codec: $1, optimizeForLatency: true, hardwareAcceleration: 'prefer-software'};if (!(await VideoDecoder.isConfigSupported(config)).supported) delete config.hardwareAcceleration;if (!(await VideoDecoder.isConfigSupported(config)).supported) return null;const s = {codec: null, max: $2, give: $3, closed: false, said: new Set(), done: Promise.resolve()};s.say = (level, message) => { if (!s.said.has(message)) { s.said.add(message); console[level](`video decoder: ${message}`); } };s.copy = async frame => {  const {x, y, width: w, height: h} = frame.visibleRect, cw = (w + 1) >> 1, ch = (h + 1) >> 1;  const rgb = /^(RGB|BGR)[AX]$/.test(frame.format);  if (w * h <= s.max && (rgb || frame.format === 'I420' || frame.format === 'I420A' || frame.format === 'NV12')) {    const bytes = new Uint8Array(w * h + 2 * cw * ch);    if (rgb) {      const px = new Uint8Array(4 * w * h), r = frame.format[0] === 'R' ? 0 : 2, b = 2 - r;      await frame.copyTo(px, {rect: {x, y, width: w, height: h}, layout: [{offset: 0, stride: 4 * w}]});      for (let i = 0; i < w * h; i++) bytes[i] = (47 * px[4 * i + r] + 157 * px[4 * i + 1] + 16 * px[4 * i + b] + 128 >> 8) + 16;      for (let row = 0; row < ch; row++) for (let col = 0; col < cw; col++) {        let sr = 0, sg = 0, sb = 0, n = 0;        for (let dy = 0; dy < 2 && 2 * row + dy < h; dy++) for (let dx = 0; dx < 2 && 2 * col + dx < w; dx++) {          const p = 4 * ((2 * row + dy) * w + 2 * col + dx);          sr += px[p + r]; sg += px[p + 1]; sb += px[p + b]; n++;        }        sr /= n; sg /= n; sb /= n;        bytes[w * h + row * cw + col] = (-26 * sr - 87 * sg + 112 * sb + 128 >> 8) + 128;        bytes[w * h + cw * ch + row * cw + col] = (112 * sr - 102 * sg - 10 * sb + 128 >> 8) + 128;      }    } else if (frame.format === 'NV12') {      const nv12 = new Uint8Array(w * h + 2 * cw * ch);      await frame.copyTo(nv12, {rect: {x, y, width: w, height: h}, layout: [{offset: 0, stride: w}, {offset: w * h, stride: 2 * cw}]});      bytes.set(nv12.subarray(0, w * h));      for (let i = 0; i < cw * ch; i++) { bytes[w * h + i] = nv12[w * h + 2 * i]; bytes[w * h + cw * ch + i] = nv12[w * h + 2 * i + 1]; }    } else {      const planes = frame.format === 'I420' ? bytes : new Uint8Array(2 * w * h + 2 * cw * ch);      const layout = [{offset: 0, stride: w}, {offset: w * h, stride: cw}, {offset: w * h + cw * ch, stride: cw}];      if (frame.format === 'I420A') layout.push({offset: w * h + 2 * cw * ch, stride: w});      await frame.copyTo(planes, {rect: {x, y, width: w, height: h}, layout});      if (planes !== bytes) bytes.set(planes.subarray(0, bytes.length));    }    s.say('info', `${frame.format} ${w}x${h}`);    if (!s.closed) await s.give(w, h, frame.timestamp / 1000, bytes);  } else s.say('warn', `dropped ${frame.format} ${w}x${h}`);};s.open = () => {  const codec = new VideoDecoder({    output: frame => { s.done = s.done.then(() => s.copy(frame)).catch(e => s.say('warn', e.message)).finally(() => frame.close()); },    error: e => { s.say('warn', e.message); if (s.codec === codec) s.codec = null; }});  codec.configure(config);  s.codec = codec; };return s;},
ZC11ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: async ($1,$2,$3,$4,$5,$6,$7,$8) => {const s = $1, w = $5, h = $6, c = ((w + 1) >> 1) * ((h + 1) >> 1);if (s.failed) return null;const memory = new Uint8Array(__exports.memory.buffer);const data = new Uint8Array(w * h + 2 * c);data.set(memory.subarray($2, $2 + w * h));data.set(memory.subarray($3, $3 + c), w * h);data.set(memory.subarray($4, $4 + c), w * h + c);const frame = new VideoFrame(data, {format: 'I420', codedWidth: w, codedHeight: h, timestamp: $7 * 1000});try { s.codec.encode(frame, {keyFrame: $8 !== 0}); await s.codec.flush(); } catch (e) { s.failed = true; console.warn(`video encoder: ${e.message}`); } finally { frame.close(); }if (s.failed) return null;const chunks = s.chunks; s.chunks = []; return chunks;},
ZC12ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: async ($1) => {if (!(await VideoEncoder.isConfigSupported($1)).supported) return null;const s = {chunks: [], failed: false};s.codec = new VideoEncoder({  output: chunk => { const bytes = new Uint8Array(chunk.byteLength); chunk.copyTo(bytes); s.chunks.push({key: chunk.type === 'key', bytes}); },  error: e => { s.failed = true; console.warn(`video encoder: ${e.message}`); }});s.codec.configure($1);return s;},
ZC13ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: async ($1) => {return typeof VideoDecoder !== 'undefined' && (await VideoDecoder.isConfigSupported({codec: $1})).supported === true;},
ZC14ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: async ($1) => {return typeof VideoEncoder !== 'undefined' && (await VideoEncoder.isConfigSupported($1)).supported === true;},
ZC15ZCtoxavzm0zi0zi1zminplaceZCToxziAVziCodecziVideoZC: ($1,$2,$3,$4) => (({codec: $1, width: $2, height: $3, bitrate: $4, framerate: 30, latencyMode: 'realtime', avc: {format: 'annexb'}, hevc: {format: 'annexb'}})),
ZC0ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1,$2) => ($1.reject(new WebAssembly.RuntimeError($2))),
ZC18ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1,$2) => ($1.resolve($2)),
ZC19ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1) => ($1.resolve()),
ZC20ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1) => {$1.throwTo = () => {};},
ZC21ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1,$2) => {$1.throwTo = (err) => __exports.rts_promiseThrowTo($2, err);},
ZC22ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: () => {let res, rej; const p = new Promise((resolve, reject) => { res = resolve; rej = reject; }); p.resolve = res; p.reject = rej; return p;},
ZC23ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1,$2) => (__ghc_wasm_jsffi_finalization_registry.register($1, $2, $1)),
ZC0ZCghczminternalZCGHCziInternalziWasmziPrimziImportsZC: ($1,$2) => ($1.then(res => __exports.rts_promiseResolveBool($2, res), err => __exports.rts_promiseReject($2, err))),
ZC17ZCghczminternalZCGHCziInternalziWasmziPrimziImportsZC: ($1,$2) => ($1.then(res => __exports.rts_promiseResolveJSVal($2, res), err => __exports.rts_promiseReject($2, err))),
ZC18ZCghczminternalZCGHCziInternalziWasmziPrimziImportsZC: ($1,$2) => ($1.then(() => __exports.rts_promiseResolveUnit($2), err => __exports.rts_promiseReject($2, err))),
ZC0ZCghczminternalZCGHCziInternalziWasmziPrimziTypesZC: ($1) => (`${$1.stack ? $1.stack : $1}`),
ZC1ZCghczminternalZCGHCziInternalziWasmziPrimziTypesZC: ($1,$2) => ((new TextDecoder('utf-8', {fatal: true})).decode(new Uint8Array(__exports.memory.buffer, $1, $2))),
ZC2ZCghczminternalZCGHCziInternalziWasmziPrimziTypesZC: ($1,$2,$3) => ((new TextEncoder()).encodeInto($1, new Uint8Array(__exports.memory.buffer, $2, $3)).written),
ZC3ZCghczminternalZCGHCziInternalziWasmziPrimziTypesZC: ($1) => ($1.length),
ZC4ZCghczminternalZCGHCziInternalziWasmziPrimziTypesZC: ($1) => {try { __ghc_wasm_jsffi_finalization_registry.unregister($1); } catch {}},
ZC0ZCghczminternalZCGHCziInternalziWasmziPrimziConcziInternalZC: async ($1) => (new Promise(res => setTimeout(res, $1 / 1000))),
};
};
