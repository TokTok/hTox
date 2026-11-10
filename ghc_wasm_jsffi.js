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
ZC1ZChtoxzmbrowserzm0zi1zi0zminplacezmhtoxZCMainZC: () => {if (globalThis.toxGateway === undefined) globalThis.toxGateway = 'wss://tox-gateway.onrender.com'},
ZC0ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (window.innerHeight),
ZC1ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (window.innerWidth),
ZC3ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC2ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC5ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC4ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC7ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC6ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC9ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC8ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC11ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC10ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC13ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC12ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC15ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC14ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC17ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC16ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC19ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC18ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC21ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC20ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC23ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC22ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC25ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ((...args) => __exports.ghczuwasmzujsffiZC24ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC($1, ...args)),
ZC26ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) => (globalThis.hatter.start({render: $1, event: $2, text: $3, scroll: $4, back: $5, dialog: $6, sheet: $7, picked: $8, picture: $9, callFrame: $10, cameraFrame: $11, bind: $12, key: $13})),
ZC27ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.redraw()),
ZC28ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3,$4) => (globalThis.hatter.showFrame($4, $1, $2, new Uint8Array(__exports.memory.buffer, $3, $1 * $2 + 2 * (($1 + 1) >> 1) * (($2 + 1) >> 1)).slice())),
ZC29ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.cameraStop()),
ZC30ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.cameraStart($1 !== 0)),
ZC31ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.callAudioTone($1)),
ZC32ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.callAudioLatency()),
ZC33ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3,$4) => (globalThis.hatter.callAudioPlay($1, $2, new Int16Array(new Uint8Array(__exports.memory.buffer, $3, 2 * $4).slice().buffer))),
ZC34ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.callAudioStop()),
ZC35ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2) => (globalThis.hatter.callAudioStart($1, $2)),
ZC36ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => ($1.byteLength),
ZC37ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2) => (new Uint8Array(__exports.memory.buffer, $2, $1.byteLength).set(new Uint8Array($1.buffer, $1.byteOffset, $1.byteLength))),
ZC38ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.imageSize($3, new DataView(__exports.memory.buffer), $1, $2)),
ZC39ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.openFile($1)),
ZC40ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.saveFile($1, $2, $3)),
ZC41ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.pickPicture(new Int32Array(__exports.memory.buffer, $1, $2).slice(), $3)),
ZC42ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.pickFiles($3, $1, $2)),
ZC43ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.sheet($1, $2, $3)),
ZC44ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3,$4,$5,$6) => (globalThis.hatter.dialog($1, $2, $3, $4, $5, $6)),
ZC45ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.openUri($1)),
ZC46ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.copyText($1)),
ZC47ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.clock24()),
ZC48ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.darkMode()),
ZC49ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: () => (globalThis.hatter.clear()),
ZC50ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.setRoot($1)),
ZC51ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.destroyNode($1)),
ZC52ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2) => (globalThis.hatter.removeChild($1, $2)),
ZC53ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2) => (globalThis.hatter.addChild($1, $2)),
ZC54ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.setHandler($1, $2, $3)),
ZC55ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.setImageData($1, new Uint8Array(__exports.memory.buffer, $2, $3).slice())),
ZC56ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.setNumProp($1, $2, $3)),
ZC57ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1,$2,$3) => (globalThis.hatter.setStrProp($1, $2, $3)),
ZC58ZChatterzmbrowserzm0zi1zi0zminplaceZCHatterziBrowserZC: ($1) => (globalThis.hatter.createNode($1)),
ZC0ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: ($1,$2) => (new Uint8Array(__exports.memory.buffer, $2, $1.length).set($1)),
ZC1ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: ($1) => ($1.length),
ZC2ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: ($1) => ($1.ws.close()),
ZC3ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: () => (globalThis.toxGateway === undefined ? '' : globalThis.toxGateway),
ZC4ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: async ($1,$2,$3,$4) => {const s = $1;if (s.closed) throw new Error('Closed');s.ws.send(new Uint8Array(__exports.memory.buffer, $2, $3).slice());while (s.ws.bufferedAmount > $4 && !s.closed) await new Promise(resolve => setTimeout(resolve, 10));},
ZC5ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: async ($1) => {const s = $1;while (s.chunks.length === 0 && !s.closed) await new Promise(resolve => { s.wake = resolve; });return s.chunks.length > 0 ? s.chunks.shift() : new Uint8Array(0);},
ZC6ZCtoxcorezm0zi2zi12zminplaceZCToxziApplicationziSocketsZC: async ($1) => {const s = {ws: new WebSocket($1), chunks: [], closed: false, wake: () => {}};s.ws.binaryType = 'arraybuffer';const woken = () => { const wake = s.wake; s.wake = () => {}; wake(); };s.ws.onmessage = e => { s.chunks.push(new Uint8Array(e.data)); woken(); };s.ws.onclose = () => { s.closed = true; woken(); };await new Promise((resolve, reject) => {  s.ws.onopen = resolve;  s.ws.onerror = () => reject(new Error('No connection to the gateway'));});s.ws.onerror = null;return s;},
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
ZC1ZCghczminternalZCGHCziInternalziWasmziPrimziExportsZC: ($1,$2) => ($1.resolve($2)),
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
