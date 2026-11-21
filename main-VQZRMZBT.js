var Rw=Object.defineProperty,Ow=Object.defineProperties;var Fw=Object.getOwnPropertyDescriptors;var uh=Object.getOwnPropertySymbols;var Lw=Object.prototype.hasOwnProperty,Pw=Object.prototype.propertyIsEnumerable;var mh=(t,n,e)=>n in t?Rw(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,y=(t,n)=>{for(var e in n||={})Lw.call(n,e)&&mh(t,e,n[e]);if(uh)for(var e of uh(n))Pw.call(n,e)&&mh(t,e,n[e]);return t},W=(t,n)=>Ow(t,Fw(n));var ro=[null],Ta=[!1],ri=1,Vw=null,Fe=Symbol("SIGNAL");function S(t){let n=ro[0];return ro[0]=t,n}function Fa(){return ro[0]}var ai={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Tn(t){if(Ta[0])throw new Error("");let n=ro[0];if(n===null)return;n.consumerOnSignalRead(t);let e=n.producersTail;if(e!==void 0&&e.producer===t)return;let i,r=n.recomputing;if(r&&(i=e!==void 0?e.nextProducer:n.producers,i!==void 0&&i.producer===t)){n.producersTail=i,i.lastReadVersion=t.version,i.knownValidAtEpoch=ri;return}let o=t.consumersTail;if(o!==void 0&&o.consumer===n&&(!r||o.knownValidAtEpoch===ri))return;let a=tr(n),s={producer:t,consumer:n,nextProducer:i,prevConsumer:void 0,knownValidAtEpoch:ri,lastReadVersion:t.version,nextConsumer:void 0};n.producersTail=s,e!==void 0?e.nextProducer=s:n.producers=s,a&&gh(t,s)}function fh(){ri++}function oo(t){if(!(tr(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===ri)){if(!t.producerMustRecompute(t)&&!er(t)){Oa(t);return}t.producerRecomputeValue(t),Oa(t)}}function Fc(t){if(t.consumers===void 0)return;let n=Ta[0];Ta[0]=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||Bw(i)}}finally{Ta[0]=n}}function Lc(){return ro[0]?.consumerAllowSignalWrites!==!1}function Bw(t){t.dirty=!0,Fc(t),t.consumerMarkedDirty?.(t)}function Oa(t){t.dirty=!1,t.lastCleanEpoch=ri}function An(t){return t&&hh(t),S(t)}function hh(t){if(t.producersTail?.knownValidAtEpoch===ri){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function si(t,n){S(n),t&&ph(t)}function ph(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(tr(t))do e=Pc(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function er(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(oo(e),i!==e.version))return!0}return!1}function Rn(t){if(tr(t)){let n=t.producers;for(;n!==void 0;)n=Pc(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function gh(t,n){let e=t.consumersTail,i=tr(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)gh(r.producer,r)}function Pc(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!tr(n)){let o=n.producers;for(;o!==void 0;)o=Pc(o)}return e}function tr(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function La(t){Vw?.(t)}function Pa(t,n){return Object.is(t,n)}function ao(t,n){let e=Object.create(jw);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(oo(e),Tn(e),e.value===oi)throw e.error;return e.value};return i[Fe]=e,La(e),i}var Aa=Symbol("UNSET"),Ra=Symbol("COMPUTING"),oi=Symbol("ERRORED"),jw=W(y({},ai),{value:Aa,dirty:!0,error:null,equal:Pa,kind:"computed",producerMustRecompute(t){return t.value===Aa||t.value===Ra},producerRecomputeValue(t){if(t.value===Ra)throw new Error("");let n=t.value;t.value=Ra;let e=An(t),i,r=!1;try{i=t.computation(),S(null),r=n!==Aa&&n!==oi&&i!==oi&&t.equal(n,i)}catch(o){i=oi,t.error=o}finally{si(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function zw(){throw new Error}var bh=zw;function vh(t){bh(t)}function Vc(t){bh=t}var Hw=null;function Bc(t,n){let e=Object.create(nr);e.value=t,n!==void 0&&(e.equal=n);let i=()=>_h(e);return i[Fe]=e,La(e),[i,a=>li(e,a),a=>jc(e,a)]}function _h(t){return Tn(t),t.value}function li(t,n){Lc()||vh(t),t.equal(t.value,n)||(t.value=n,Uw(t))}function jc(t,n){Lc()||vh(t),li(t,n(t.value))}var nr=W(y({},ai),{equal:Pa,value:void 0,kind:"signal"});function Uw(t){t.version++,fh(),Fc(t),Hw?.(t)}var zc=W(y({},ai),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Hc(t){if(t.dirty=!1,t.version>0&&!er(t))return;t.version++;let n=An(t);try{t.cleanup(),t.fn()}finally{si(t,n)}}var Uc=[void 0];function Va(){return Uc[0]}function Gt(t){let n=Uc[0];return Uc[0]=t,n}var yh=Symbol("NotFound");function ir(t){return t===yh||t?.name==="\u0275NotFound"}function wh(t){let n=S(null);try{return t()}finally{S(n)}}function Z(t){return typeof t=="function"}function Ba(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var ja=Ba(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function ci(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var be=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(Z(i))try{i()}catch(o){n=o instanceof ja?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{xh(o)}catch(a){n=n??[],a instanceof ja?n=[...n,...a.errors]:n.push(a)}}if(n)throw new ja(n)}}add(n){var e;if(n&&n!==this)if(this.closed)xh(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&ci(e,n)}remove(n){let{_finalizers:e}=this;e&&ci(e,n),n instanceof t&&n._removeParent(this)}};be.EMPTY=(()=>{let t=new be;return t.closed=!0,t})();var $c=be.EMPTY;function za(t){return t instanceof be||t&&"closed"in t&&Z(t.remove)&&Z(t.add)&&Z(t.unsubscribe)}function xh(t){Z(t)?t():t.unsubscribe()}var Ft={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var rr={setTimeout(t,n,...e){let{delegate:i}=rr;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=rr;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Ha(t){rr.setTimeout(()=>{let{onUnhandledError:n}=Ft;if(n)n(t);else throw t})}function so(){}var Sh=Gc("C",void 0,void 0);function Ch(t){return Gc("E",void 0,t)}function Dh(t){return Gc("N",t,void 0)}function Gc(t,n,e){return{kind:t,value:n,error:e}}var di=null;function or(t){if(Ft.useDeprecatedSynchronousErrorHandling){let n=!di;if(n&&(di={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=di;if(di=null,e)throw i}}else t()}function Eh(t){Ft.useDeprecatedSynchronousErrorHandling&&di&&(di.errorThrown=!0,di.error=t)}var ui=class extends be{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,za(n)&&n.add(this)):this.destination=Ww}static create(n,e,i){return new sn(n,e,i)}next(n){this.isStopped?qc(Dh(n),this):this._next(n)}error(n){this.isStopped?qc(Ch(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?qc(Sh,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},$w=Function.prototype.bind;function Wc(t,n){return $w.call(t,n)}var Yc=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Ua(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Ua(i)}else Ua(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Ua(e)}}},sn=class extends ui{constructor(n,e,i){super();let r;if(Z(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Ft.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Wc(n.next,o),error:n.error&&Wc(n.error,o),complete:n.complete&&Wc(n.complete,o)}):r=n}this.destination=new Yc(r)}};function Ua(t){Ft.useDeprecatedSynchronousErrorHandling?Eh(t):Ha(t)}function Gw(t){throw t}function qc(t,n){let{onStoppedNotification:e}=Ft;e&&rr.setTimeout(()=>e(t,n))}var Ww={closed:!0,next:so,error:Gw,complete:so};var ar=typeof Symbol=="function"&&Symbol.observable||"@@observable";function ln(t){return t}function Nh(t){return t.length===0?ln:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var z=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=Yw(n)?n:new sn(n,e,i);return or(()=>{let{operator:o,source:a}=this;r.add(o?o.call(r,a):a?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=kh(e),new e((i,r)=>{let o=new sn({next:a=>{try{n(a)}catch(s){r(s),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[ar](){return this}pipe(...n){return Nh(n)(this)}toPromise(n){return n=kh(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};z.create=t=>new z(t);function kh(t){var n;return(n=t??Ft.Promise)!==null&&n!==void 0?n:Promise}function qw(t){return t&&Z(t.next)&&Z(t.error)&&Z(t.complete)}function Yw(t){return t&&t instanceof ui||qw(t)&&za(t)}function Zw(t){return Z(t?.lift)}function pe(t){return n=>{if(Zw(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ge(t,n,e,i,r){return new Zc(t,n,e,i,r)}var Zc=class extends ui{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Ih=Ba(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var x=class extends z{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new $a(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new Ih}next(n){or(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){or(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){or(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?$c:(this.currentObservers=null,r.push(n),new be(()=>{this.currentObservers=null,ci(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new z;return n.source=this,n}};x.create=(t,n)=>new $a(t,n);var $a=class extends x{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:$c}};var mi=class extends x{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var lo={now(){return(lo.delegate||Date).now()},delegate:void 0};var On=class extends x{constructor(n=1/0,e=1/0,i=lo){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var Ga=class extends be{constructor(n,e){super()}schedule(n,e=0){return this}};var co={setInterval(t,n,...e){let{delegate:i}=co;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=co;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Wa=class extends Ga{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return co.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&co.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,ci(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var sr=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};sr.now=lo.now;var qa=class extends sr{constructor(n,e=sr.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var uo=new qa(Wa),Mh=uo;var fi=new z(t=>t.complete());function Ya(t){return t&&Z(t.schedule)}function Kc(t){return t[t.length-1]}function Za(t){return Z(Kc(t))?t.pop():void 0}function Wt(t){return Ya(Kc(t))?t.pop():void 0}function Th(t,n){return typeof Kc(t)=="number"?t.pop():n}function Rh(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(d){try{c(i.next(d))}catch(u){a(u)}}function l(d){try{c(i.throw(d))}catch(u){a(u)}}function c(d){d.done?o(d.value):r(d.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function Ah(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function hi(t){return this instanceof hi?(this.v=t,this):new hi(t)}function Oh(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(f){return function(p){return Promise.resolve(p).then(f,u)}}function s(f,p){i[f]&&(r[f]=function(g){return new Promise(function(_,w){o.push([f,g,_,w])>1||l(f,g)})},p&&(r[f]=p(r[f])))}function l(f,p){try{c(i[f](p))}catch(g){h(o[0][3],g)}}function c(f){f.value instanceof hi?Promise.resolve(f.value.v).then(d,u):h(o[0][2],f)}function d(f){l("next",f)}function u(f){l("throw",f)}function h(f,p){f(p),o.shift(),o.length&&l(o[0][0],o[0][1])}}function Fh(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Ah=="function"?Ah(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var Ka=(t=>t&&typeof t.length=="number"&&typeof t!="function");function Xa(t){return Z(t?.then)}function Qa(t){return Z(t[ar])}function Ja(t){return Symbol.asyncIterator&&Z(t?.[Symbol.asyncIterator])}function es(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Kw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ts=Kw();function ns(t){return Z(t?.[ts])}function is(t){return Oh(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield hi(e.read());if(r)return yield hi(void 0);yield yield hi(i)}}finally{e.releaseLock()}})}function rs(t){return Z(t?.getReader)}function xe(t){if(t instanceof z)return t;if(t!=null){if(Qa(t))return Xw(t);if(Ka(t))return Qw(t);if(Xa(t))return Jw(t);if(Ja(t))return Lh(t);if(ns(t))return ex(t);if(rs(t))return tx(t)}throw es(t)}function Xw(t){return new z(n=>{let e=t[ar]();if(Z(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Qw(t){return new z(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Jw(t){return new z(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Ha)})}function ex(t){return new z(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Lh(t){return new z(n=>{nx(t,n).catch(e=>n.error(e))})}function tx(t){return Lh(is(t))}function nx(t,n){var e,i,r,o;return Rh(this,void 0,void 0,function*(){try{for(e=Fh(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function at(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function os(t,n=0){return pe((e,i)=>{e.subscribe(ge(i,r=>at(i,t,()=>i.next(r),n),()=>at(i,t,()=>i.complete(),n),r=>at(i,t,()=>i.error(r),n)))})}function as(t,n=0){return pe((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Ph(t,n){return xe(t).pipe(as(n),os(n))}function Vh(t,n){return xe(t).pipe(as(n),os(n))}function Bh(t,n){return new z(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function jh(t,n){return new z(e=>{let i;return at(e,n,()=>{i=t[ts](),at(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>Z(i?.return)&&i.return()})}function ss(t,n){if(!t)throw new Error("Iterable cannot be null");return new z(e=>{at(e,n,()=>{let i=t[Symbol.asyncIterator]();at(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function zh(t,n){return ss(is(t),n)}function Hh(t,n){if(t!=null){if(Qa(t))return Ph(t,n);if(Ka(t))return Bh(t,n);if(Xa(t))return Vh(t,n);if(Ja(t))return ss(t,n);if(ns(t))return jh(t,n);if(rs(t))return zh(t,n)}throw es(t)}function St(t,n){return n?Hh(t,n):xe(t)}function pi(...t){let n=Wt(t);return St(t,n)}function Uh(t){return t instanceof Date&&!isNaN(t)}function je(t,n){return pe((e,i)=>{let r=0;e.subscribe(ge(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:ix}=Array;function rx(t,n){return ix(n)?t(...n):t(n)}function ls(t){return je(n=>rx(t,n))}var{isArray:ox}=Array,{getPrototypeOf:ax,prototype:sx,keys:lx}=Object;function cs(t){if(t.length===1){let n=t[0];if(ox(n))return{args:n,keys:null};if(cx(n)){let e=lx(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function cx(t){return t&&typeof t=="object"&&ax(t)===sx}function ds(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Xc(...t){let n=Wt(t),e=Za(t),{args:i,keys:r}=cs(t);if(i.length===0)return St([],n);let o=new z(dx(i,n,r?a=>ds(r,a):ln));return e?o.pipe(ls(e)):o}function dx(t,n,e=ln){return i=>{$h(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)$h(n,()=>{let c=St(t[l],n),d=!1;c.subscribe(ge(i,u=>{o[l]=u,d||(d=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function $h(t,n,e){t?at(e,t,n):n()}function Gh(t,n,e,i,r,o,a,s){let l=[],c=0,d=0,u=!1,h=()=>{u&&!l.length&&!c&&n.complete()},f=g=>c<i?p(g):l.push(g),p=g=>{o&&n.next(g),c++;let _=!1;xe(e(g,d++)).subscribe(ge(n,w=>{r?.(w),o?f(w):n.next(w)},()=>{_=!0},void 0,()=>{if(_)try{for(c--;l.length&&c<i;){let w=l.shift();a?at(n,a,()=>p(w)):p(w)}h()}catch(w){n.error(w)}}))};return t.subscribe(ge(n,f,()=>{u=!0,h()})),()=>{s?.()}}function Qc(t,n,e=1/0){return Z(n)?Qc((i,r)=>je((o,a)=>n(i,o,r,a))(xe(t(i,r))),e):(typeof n=="number"&&(e=n),pe((i,r)=>Gh(i,r,t,e)))}function us(t=1/0){return Qc(ln,t)}function Wh(){return us(1)}function lr(...t){return Wh()(St(t,Wt(t)))}function mo(t){return new z(n=>{xe(t()).subscribe(n)})}function Jc(...t){let n=Za(t),{args:e,keys:i}=cs(t),r=new z(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let d=0;d<a;d++){let u=!1;xe(e[d]).subscribe(ge(o,h=>{u||(u=!0,c--),s[d]=h},()=>l--,void 0,()=>{(!l||!u)&&(c||o.next(i?ds(i,s):s),o.complete())}))}});return n?r.pipe(ls(n)):r}function qh(t=0,n,e=Mh){let i=-1;return n!=null&&(Ya(n)?e=n:i=n),new z(r=>{let o=Uh(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function cn(...t){let n=Wt(t),e=Th(t,1/0),i=t;return i.length?i.length===1?xe(i[0]):us(e)(St(i,n)):fi}function Ee(t,n){return pe((e,i)=>{let r=0;e.subscribe(ge(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Yh(t){return pe((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(ge(e,c=>{i=!0,r=c,o||xe(t(c)).subscribe(o=ge(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function cr(t,n=uo){return Yh(()=>qh(t,n))}function fo(t,n=uo){return pe((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,d=n.now();if(d<c){r=this.schedule(void 0,c-d),i.add(r);return}s()}e.subscribe(ge(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function mt(t){return t<=0?()=>fi:pe((n,e)=>{let i=0;n.subscribe(ge(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function ms(t,n=ln){return t=t??ux,pe((e,i)=>{let r,o=!0;e.subscribe(ge(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function ux(t,n){return t===n}function fs(){return pe((t,n)=>{let e,i=!1;t.subscribe(ge(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Zh(t={}){let{connector:n=()=>new x,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,d=!1,u=!1,h=()=>{s?.unsubscribe(),s=void 0},f=()=>{h(),a=l=void 0,d=u=!1},p=()=>{let g=a;f(),g?.unsubscribe()};return pe((g,_)=>{c++,!u&&!d&&h();let w=l=l??n();_.add(()=>{c--,c===0&&!u&&!d&&(s=ed(p,r))}),w.subscribe(_),!a&&c>0&&(a=new sn({next:U=>w.next(U),error:U=>{u=!0,h(),s=ed(f,e,U),w.error(U)},complete:()=>{d=!0,h(),s=ed(f,i),w.complete()}}),xe(g).subscribe(a))})(o)}}function ed(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new sn({next:()=>{i.unsubscribe(),t()}});return xe(n(...e)).subscribe(i)}function hs(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Zh({connector:()=>new On(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function ho(t){return Ee((n,e)=>t<=e)}function qt(...t){let n=Wt(t);return pe((e,i)=>{(n?lr(t,e,n):lr(t,e)).subscribe(i)})}function Ct(t){return pe((n,e)=>{xe(t).subscribe(ge(e,()=>e.complete(),so)),!e.closed&&n.subscribe(e)})}var gd="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",k=class extends Error{code;constructor(n,e){super(mr(n,e)),this.code=n}};function mx(t){return`NG0${Math.abs(t)}`}function mr(t,n){return`${mx(t)}${n?": "+n:""}`}function me(t){for(let n in t)if(t[n]===me)return n;throw Error("")}function tp(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function ws(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ws).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function xs(t,n){return t?n?`${t} ${n}`:t:n||""}var fx=me({__forward_ref__:me});function st(t){return t.__forward_ref__=st,t}function Le(t){return bd(t)?t():t}function bd(t){return typeof t=="function"&&Object.hasOwn(t,fx)&&t.__forward_ref__===st}function ne(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function Me(t){return{providers:t.providers||[],imports:t.imports||[]}}function Ss(t){return hx(t,Cs)}function hx(t,n){return Object.hasOwn(t,n)&&t[n]||null}function px(t){let n=t?.[Cs]??null;return n||null}function nd(t){return t&&Object.hasOwn(t,gs)?t[gs]:null}var Cs=me({\u0275prov:me}),gs=me({\u0275inj:me}),b=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=ne({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function vd(t){return t&&!!t.\u0275providers}var wo=me({\u0275cmp:me}),xo=me({\u0275dir:me}),_d=me({\u0275pipe:me});var bo=me({\u0275fac:me}),xi=me({__NG_ELEMENT_ID__:me}),Kh=me({__NG_ENV_ID__:me});function Ln(t){return yd(t,"@Component"),t[wo]||null}function So(t){return yd(t,"@Directive"),t[xo]||null}function np(t){return yd(t,"@Pipe"),t[_d]||null}function yd(t,n){if(t==null)throw new k(-919,!1)}function Ds(t){return typeof t=="string"?t:t==null?"":String(t)}var ip=me({ngErrorCode:me}),gx=me({ngErrorMessage:me}),bx=me({ngTokenPath:me});function wd(t,n){return rp("",-200,n)}function Es(t,n){throw new k(-201,!1)}function rp(t,n,e){let i=new k(n,t);return i[ip]=n,i[gx]=t,e&&(i[bx]=e),i}function vx(t){return t[ip]}var id;function op(){return id}function ft(t){let n=id;return id=t,n}function xd(t,n,e){let i=Ss(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Es(t,"")}var Pn=globalThis;var _x={},gi=_x,yx="__NG_DI_FLAG__",rd=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=bi(e)||0;try{return this.injector.get(n,i&8?null:gi,i)}catch(r){if(ir(r))return r;throw r}}};function wx(t,n=0){let e=Va();if(e===void 0)throw new k(-203,!1);if(e===null)return xd(t,void 0,n);{let i=xx(n),r=e.retrieve(t,i);if(ir(r)){if(i.optional)return null;throw r}return r}}function G(t,n=0){return(op()||wx)(Le(t),n)}function m(t,n){return G(t,bi(n))}function bi(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function xx(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function od(t){let n=[];for(let e=0;e<t.length;e++){let i=Le(t[e]);if(Array.isArray(i)){if(i.length===0)throw new k(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=Sx(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(G(r,o))}else n.push(G(i))}return n}function Sx(t){return t[yx]}function vi(t,n){let e=Object.hasOwn(t,bo);return e?t[bo]:null}function ap(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function sp(t){return t.flat(Number.POSITIVE_INFINITY)}function Ns(t,n){t.forEach(e=>Array.isArray(e)?Ns(e,n):n(e))}function Sd(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Co(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function lp(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function cp(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function ks(t,n,e){let i=fr(t,n);return i>=0?t[i|1]=e:(i=~i,cp(t,i,n,e)),i}function Is(t,n){let e=fr(t,n);if(e>=0)return t[e|1]}function fr(t,n){return Cx(t,n,1)}function Cx(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var Vn={},qe=[],hr=new b(""),Cd=new b("",-1),Dd=new b(""),ur=class{get(n,e=gi){if(e===gi){let r=rp("",-201);throw r.name="\u0275NotFound",r}return e}};function dp(...t){return{\u0275providers:Ed(!0,t),\u0275fromNgModule:!0}}function Ed(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return Ns(n,a=>{let s=a;bs(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&up(r,o),e}function up(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Nd(r,o=>{n(o,i)})}}function bs(t,n,e,i){if(t=Le(t),!t)return!1;let r=null,o=nd(t),a=!o&&Ln(t);if(!o&&!a){let l=t.ngModule;if(o=nd(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)bs(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;Ns(o.imports,d=>{bs(d,n,e,i)&&(c||=[],c.push(d))}),c!==void 0&&up(c,n)}if(!s){let c=vi(r)||(()=>new r);n({provide:r,useFactory:c,deps:qe},r),n({provide:Dd,useValue:r,multi:!0},r),n({provide:hr,useValue:()=>G(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;Nd(l,d=>{n(d,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Nd(t,n){for(let e of t)vd(e)&&(e=e.\u0275providers),Array.isArray(e)?Nd(e,n):n(e)}var Dx=me({provide:String,useValue:me});function mp(t){return t!==null&&typeof t=="object"&&Dx in t}function Ex(t){return!!(t&&t.useExisting)}function Nx(t){return!!(t&&t.useFactory)}function _i(t){return typeof t=="function"}function fp(t){return!!t.useClass}var Do=new b(""),ps={},Xh={},td;function pr(){return td===void 0&&(td=new ur),td}var Pe=class{},yi=class extends Pe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,sd(n,a=>this.processProvider(a)),this.records.set(Cd,dr(void 0,this)),r.has("environment")&&this.records.set(Pe,dr(void 0,this));let o=this.records.get(Do);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Dd,qe,{self:!0}))}retrieve(n,e){let i=bi(e)||0;try{return this.get(n,gi,i)}catch(r){if(ir(r))return r;throw r}}destroy(){po(this),this._destroyed=!0;let n=S(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),S(n)}}onDestroy(n){return po(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){po(this);let e=Gt(this),i=ft(void 0),r;try{return n()}finally{Gt(e),ft(i)}}get(n,e=gi,i){if(po(this),Object.hasOwn(n,Kh))return n[Kh](this);let r=bi(i),o,a=Gt(this),s=ft(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let d=Ax(n)&&Ss(n);d&&this.injectableDefInScope(d)?c=dr(ad(n),ps):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?pr():this.parent;return e=r&8&&e===gi?null:e,l.get(n,e)}catch(l){let c=vx(l);throw c===-200||c===-201?new k(c,null):l}finally{ft(s),Gt(a)}}resolveInjectorInitializers(){let n=S(null),e=Gt(this),i=ft(void 0),r;try{let o=this.get(hr,qe,{self:!0});for(let a of o)a()}finally{Gt(e),ft(i),S(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Le(n);let e=_i(n)?n:Le(n&&n.provide),i=Ix(n);if(!_i(n)&&n.multi===!0){let r=this.records.get(e);r||(r=dr(void 0,ps,!0),r.factory=()=>od(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=S(null);try{if(e.value===Xh)throw wd("");return e.value===ps&&(e.value=Xh,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&Tx(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{S(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Le(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function ad(t){let n=Ss(t),e=n!==null?n.factory:vi(t);if(e!==null)return e;if(t instanceof b)throw new k(-204,!1);if(t instanceof Function)return kx(t);throw new k(-204,!1)}function kx(t){if(t.length>0)throw new k(-204,!1);let e=px(t);return e!==null?()=>e.factory(t):()=>new t}function Ix(t){if(mp(t))return dr(void 0,t.useValue);{let n=kd(t);return dr(n,ps)}}function kd(t,n,e){let i;if(_i(t)){let r=Le(t);return vi(r)||ad(r)}else if(mp(t))i=()=>Le(t.useValue);else if(Nx(t))i=()=>t.useFactory(...od(t.deps||[]));else if(Ex(t))i=(r,o)=>G(Le(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Le(t&&(t.useClass||t.provide));if(Mx(t))i=()=>new r(...od(t.deps));else return vi(r)||ad(r)}return i}function po(t){if(t.destroyed)throw new k(-205,!1)}function dr(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function Mx(t){return!!t.deps}function Tx(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function Ax(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function sd(t,n){for(let e of t)Array.isArray(e)?sd(e,n):e&&vd(e)?sd(e.\u0275providers,n):n(e)}function Ms(t,n){let e;t instanceof yi?(po(t),e=t):e=new rd(t);let i,r=Gt(e),o=ft(void 0);try{return n()}finally{Gt(r),ft(o)}}function hp(){return op()!==void 0||Va()!=null}var Lt=0,I=1,A=2,Ne=3,Dt=4,Ue=5,Si=6,gr=7,Ce=8,hn=9,Pt=10,we=11,br=12,Id=13,Bn=14,Ye=15,jn=16,Ci=17,Zt=18,Eo=19,Md=20,dn=21,Ts=22,un=23,pt=24,Di=25,pp=26,vr=27,ke=28,gp=1,Td=6,Ei=7,No=8,Ni=9,De=10;function pn(t){return Array.isArray(t)&&typeof t[gp]=="object"}function Et(t){return Array.isArray(t)&&t[gp]===!0}function Ad(t){return(t.flags&4)!==0}function gn(t){return t.componentOffset>-1}function ko(t){return(t.flags&1)===1}function Vt(t){return!!t.template}function _r(t){return(t[A]&512)!==0}function ki(t){return(t[A]&256)===256}var ve=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(ve||{}),go,wi="svg",As="math",ld="",Qh="*",cd=()=>Object.create(null);function Rx(){return go||(go=cd(),Fn(ve.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),Fn(ve.STYLE,void 0,[["*",["style"]]]),Fn(ve.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]]]),Fn(ve.URL,As,[["*",["href","xlink:href"]]]),Fn(ve.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),Fn(ve.URL,wi,[["a",["href","xlink:href"]]]),Fn(ve.ATTRIBUTE_NO_BINDING,wi,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),Fn(ve.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]]]),go)}function Fn(t,n,e){let i=n??ld;for(let[r,o]of e){let a=r.toLowerCase();for(let s of o){let l=s.toLowerCase(),c=go[l]??=cd(),d=c[i]??=cd();d[a]=t}}}function bp(t,n,e){let r=Rx()[n.toLowerCase()];if(!r)return ve.NONE;let o=t.toLowerCase(),a;if(e){let s=r[e];s&&(a=s[o]??s[Qh])}if(a===void 0){let s=r[ld];s&&(a=s[o]??s[Qh])}if(a===void 0&&(!e||e===ld)){let s=r[wi];s&&(a=s[o])}return a??ve.NONE}function $e(t){for(;Array.isArray(t);)t=t[Lt];return t}function Rd(t,n){return $e(n[t])}function Nt(t,n){return $e(n[t.index])}function Rs(t,n){return t.data[n]}function vp(t,n){return t[n]}function kt(t,n){let e=n[t];return pn(e)?e:e[Lt]}function _p(t){return(t[A]&4)===4}function Os(t){return(t[A]&128)===128}function yp(t){return Et(t[Ne])}function It(t,n){return n==null?null:t[n]}function Od(t){t[Ci]=0}function Fd(t){t[A]&1024||(t[A]|=1024,Os(t)&&Ii(t))}function wp(t,n){for(;t>0;)n=n[Bn],t--;return n}function Io(t){return!!(t[A]&9216||t[pt]?.dirty)}function Fs(t){t[Pt].changeDetectionScheduler?.notify(8),t[A]&64&&(t[A]|=1024),Io(t)&&Ii(t)}function Ii(t){t[Pt].changeDetectionScheduler?.notify(0);let n=mn(t);for(;n!==null&&!(n[A]&8192||(n[A]|=8192,!Os(n)));)n=mn(n)}function Ls(t,n){if(ki(t))throw new k(911,!1);t[dn]===null&&(t[dn]=[]),t[dn].push(n)}function xp(t,n){if(t[dn]===null)return;let e=t[dn].indexOf(n);e!==-1&&t[dn].splice(e,1)}function mn(t){let n=t[Ne];return Et(n)?n[Ne]:n}function Ld(t){return t[gr]??=[]}function Pd(t){return t.cleanup??=[]}function Sp(t,n,e,i){let r=Ld(n);r.push(e),t.firstCreatePass&&Pd(t).push(i,r.length-1)}var $={lFrame:Op(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var dd=!1;function Cp(){return $.lFrame.elementDepthCount}function Dp(){$.lFrame.elementDepthCount++}function Vd(){$.lFrame.elementDepthCount--}function Bd(){return $.bindingsEnabled}function jd(){return $.skipHydrationRootTNode!==null}function zd(t){return $.skipHydrationRootTNode===t}function Hd(){$.skipHydrationRootTNode=null}function j(){return $.lFrame.lView}function Te(){return $.lFrame.tView}function bn(t){return $.lFrame.contextLView=t,t[Ce]}function vn(t){return $.lFrame.contextLView=null,t}function Ze(){let t=Ud();for(;t!==null&&t.type===64;)t=t.parent;return t}function Ud(){return $.lFrame.currentTNode}function Ep(){let t=$.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function yr(t,n){let e=$.lFrame;e.currentTNode=t,e.isParent=n}function $d(){return $.lFrame.isParent}function Gd(){$.lFrame.isParent=!1}function Np(){return $.lFrame.contextLView}function Wd(){return dd}function vo(t){let n=dd;return dd=t,n}function kp(t){return $.lFrame.bindingIndex=t}function Mi(){return $.lFrame.bindingIndex++}function qd(t){let n=$.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Ip(){return $.lFrame.inI18n}function Mp(t,n){let e=$.lFrame;e.bindingIndex=e.bindingRootIndex=t,Ps(n)}function Tp(){return $.lFrame.currentDirectiveIndex}function Ps(t){$.lFrame.currentDirectiveIndex=t}function Ap(t){let n=$.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Vs(){return $.lFrame.currentQueryIndex}function Mo(t){$.lFrame.currentQueryIndex=t}function Ox(t){let n=t[I];return n.type===2?n.declTNode:n.type===1?t[Ue]:null}function Yd(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Ox(o),r===null||(o=o[Bn],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=$.lFrame=Rp();return i.currentTNode=n,i.lView=t,!0}function Bs(t){let n=Rp(),e=t[I];$.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Rp(){let t=$.lFrame,n=t===null?null:t.child;return n===null?Op(t):n}function Op(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Fp(){let t=$.lFrame;return $.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Zd=Fp;function js(){let t=Fp();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Lp(t){return($.lFrame.contextLView=wp(t,$.lFrame.contextLView))[Ce]}function Kt(){return $.lFrame.selectedIndex}function zn(t){$.lFrame.selectedIndex=t}function To(){let t=$.lFrame;return Rs(t.tView,t.selectedIndex)}function Ao(){$.lFrame.currentNamespace=wi}function Kd(){return $.lFrame.currentNamespace}var Pp=!0;function zs(){return Pp}function Hs(t){Pp=t}function ud(t,n=null,e=null,i){let r=Vp(t,n,e,i);return r.resolveInjectorInitializers(),r}function Vp(t,n=null,e=null,i,r=new Set){let o=[e||qe,dp(t)],a;return new yi(o,n||pr(),a||null,r)}var T=class t{static THROW_IF_NOT_FOUND=gi;static NULL=new ur;static create(n,e){if(Array.isArray(n))return ud({name:""},e,n,"");{let i=n.name??"";return ud({name:i},n.parent,n.providers,i)}}static \u0275prov=ne({token:t,providedIn:"any",factory:()=>G(Cd)});static __NG_ELEMENT_ID__=-1},V=new b(""),ht=class{static __NG_ELEMENT_ID__=Fx;static __NG_ENV_ID__=n=>n},vs=class extends ht{_lView;constructor(n){super(),this._lView=n}get destroyed(){return ki(this._lView)}onDestroy(n){let e=this._lView;return Ls(e,n),()=>xp(e,n)}};function Fx(){return new vs(j())}var Bp=!1,jp=new b(""),wr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new mi(!1);debugTaskTracker=m(jp,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new z(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=ne({token:t,providedIn:"root",factory:()=>new t})}return t})(),md=class extends x{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,hp()&&(this.destroyRef=m(ht,{optional:!0})??void 0,this.pendingTasks=m(wr,{optional:!0})??void 0)}emit(n){let e=S(null);try{super.next(n)}finally{S(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof be&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},K=md;function _s(...t){}function Xd(t){let n,e;function i(){t=_s;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function zp(t){return queueMicrotask(()=>t()),()=>{t=_s}}var Qd="isAngularZone",_o=Qd+"_ID",Lx=0,C=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new K(!1);onMicrotaskEmpty=new K(!1);onStable=new K(!1);onError=new K(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Bp}=n;if(typeof Zone>"u")throw new k(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,Bx(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Qd)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new k(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new k(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,Px,_s,_s);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},Px={};function Jd(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function Vx(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Xd(()=>{t.callbackScheduled=!1,fd(t),t.isCheckStableRunning=!0,Jd(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),fd(t)}function Bx(t){let n=()=>{Vx(t)},e=Lx++;t._inner=t._inner.fork({name:"angular",properties:{[Qd]:!0,[_o]:e,[_o+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(jx(l))return i.invokeTask(o,a,s,l);try{return Jh(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),ep(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return Jh(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!zx(l)&&n(),ep(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,fd(t),Jd(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function fd(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Jh(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function ep(t){t._nesting--,Jd(t)}var yo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new K;onMicrotaskEmpty=new K;onStable=new K;onError=new K;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function jx(t){return Hp(t,"__ignore_ng_zone__")}function zx(t){return Hp(t,"__scheduler_tick__")}function Hp(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Yt=class{_console=console;handleError(n){this._console.error("ERROR",n)}};function Up(t){return Hx(t)?t:new hd(t)}function Hx(t){return t instanceof Error||typeof t=="object"&&t!==null&&typeof t.name=="string"&&typeof t.message=="string"}var hd=class extends Error{constructor(n){super(String(n),{cause:n}),this.name="ErrorBoundaryWrappedError"}},Ti=new b("",{factory:()=>{let t=m(C),n=m(Pe),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Yt),e.handleError(i))})}}}),$p={provide:hr,useValue:()=>{let t=m(Yt,{optional:!0})},multi:!0};function X(t,n){let[e,i,r]=Bc(t,n?.equal),o=e,a=o[Fe];return o.set=i,o.update=r,o.asReadonly=Gp.bind(o),o}function Gp(){let t=this[Fe];if(t.readonlyFn===void 0){let n=()=>this();n[Fe]=t,t.readonlyFn=n}return t.readonlyFn}var Ai=new b("",{factory:()=>Ux}),Ux="ng";var Us=new b(""),Ri=new b("",{providedIn:"platform",factory:()=>"unknown"}),Ro=new b(""),Oi=new b("",{factory:()=>m(V).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var xr=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=$x}return t})();function $x(){return new xr(j(),Ze())}var fn=class{},Oo=new b("",{factory:()=>!0});var eu=new b(""),$s=(()=>{class t{static \u0275prov=ne({token:t,providedIn:"root",factory:()=>new pd})}return t})(),pd=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},ys=class{[Fe];constructor(n){this[Fe]=n}destroy(){this[Fe].destroy()}};function _n(t,n){let e=n?.injector??m(T),i=n?.manualCleanup!==!0?e.get(ht):null,r,o=e.get(xr,null,{optional:!0}),a=e.get(fn);return o!==null?(r=qp(o.view,a,t),i instanceof vs&&i._lView===o.view&&(i=null)):r=qx(t,e.get($s),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new ys(r)}var Wp=W(y({},zc),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=vo(!1);try{Hc(this)}finally{vo(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=S(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],S(t)}}}),Gx=W(y({},Wp),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Rn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Wx=W(y({},Wp),{consumerMarkedDirty(){this.view[A]|=8192,Ii(this.view),this.notifier.notify(13)},destroy(){if(Rn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[un]?.delete(this)}});function qp(t,n,e){let i=Object.create(Wx);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.userFn=e,i.fn=Yp(i,e),t[un]??=new Set,t[un].add(i),i.consumerMarkedDirty(i),i}function qx(t,n,e){let i=Object.create(Gx);return i.userFn=t,i.fn=Yp(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Yp(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function yn(t){return typeof t=="function"&&t[Fe]!==void 0}var Ku=Symbol("InputSignalNode#UNSET"),Og=W(y({},nr),{transformFn:void 0,applyValueToInputSignal(t,n){li(t,n)}});function $o(t){return{toString:t}.toString()}var Q=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Q||{}),Js=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function Fg(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Lg=null,Ke=(()=>{Lg=Zp;let t=()=>Zp;return t.ngInherit=!0,t})();function c0(){return Lg}function Zp(t){return t.type.prototype.ngOnChanges&&(t.setInput=u0),d0}function d0(){let t=Pg(this),n=t?.current;if(n){let e=t.previous;if(e===Vn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function u0(t,n,e,i,r){let o=this.declaredInputs[i],a=Pg(t)||m0(t,{previous:Vn,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Js(c&&c.currentValue,e,l===Vn),Fg(t,n,r,e)}var mu="__ngSimpleChanges__";function Pg(t){return Object.hasOwn(t,mu)&&t[mu]||null}function m0(t,n){return t[mu]=n}var Kp=[];var oe=function(t,n=null,e){for(let i=0;i<Kp.length;i++){let r=Kp[i];r(t,n,e)}};function f0(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=c0()(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Vg(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:d}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function Zs(t,n,e){Bg(t,n,3,e)}function Ks(t,n,e,i){(t[A]&3)===e&&Bg(t,n,e,i)}function tu(t,n){let e=t[A];(e&3)===n&&(e&=16383,e+=1,t[A]=e)}function Bg(t,n,e,i){let r=i!==void 0?t[Ci]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[Ci]+=65536),(s<o||o==-1)&&(h0(t,e,n,l),t[Ci]=(t[Ci]&4294901760)+l+2),l++}function Xp(t,n){oe(Q.LifecycleHookStart,t,n);let e=S(null);try{n.call(t)}finally{S(e),oe(Q.LifecycleHookEnd,t,n)}}function h0(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[A]>>14<t[Ci]>>16&&(t[A]&3)===n&&(t[A]+=16384,Xp(s,o)):Xp(s,o)}var Dr=-1,Li=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function p0(t){return(t.flags&8)!==0}function g0(t){return(t.flags&16)!==0}function b0(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];v0(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function jg(t){return t===3||t===4||t===6}function v0(t){return t.charCodeAt(0)===64}function Er(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?Qp(t,e,r,null,n[++i]):Qp(t,e,r,null,null))}}return t}function Qp(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function zg(t){return t!==Dr}function el(t){return t&32767}function _0(t){return t>>16}function tl(t,n){let e=_0(t),i=n;for(;e>0;)i=i[Bn],e--;return i}var fu=!0;function Jp(t){let n=fu;return fu=t,n}var y0=256,Hg=y0-1,Ug=5,w0=0,Xt={};function x0(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:Object.hasOwn(e,xi)&&(i=e[xi]),i==null&&(i=e[xi]=w0++);let r=i&Hg,o=1<<r;n.data[t+(r>>Ug)]|=o}function nl(t,n){let e=$g(t,n);if(e!==-1)return e;let i=n[I];i.firstCreatePass&&(t.injectorIndex=n.length,nu(i.data,t),nu(n,null),nu(i.blueprint,null));let r=Xu(t,n),o=t.injectorIndex;if(zg(r)){let a=el(r),s=tl(r,n),l=s[I].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function nu(t,n){t.push(0,0,0,0,0,0,0,0,n)}function $g(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Xu(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=Zg(r),i===null)return Dr;if(e++,r=r[Bn],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Dr}function hu(t,n,e){x0(t,n,e)}function S0(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(jg(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Gg(t,n,e){if(e&8||t!==void 0)return t;Es(n,"NodeInjector")}function Wg(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[hn],o=ft(void 0);try{return r?r.get(n,i,e&8):xd(n,i,e&8)}finally{ft(o)}}return Gg(i,n,e)}function qg(t,n,e,i=0,r){if(t!==null){if(n[A]&2048&&!(i&2)){let a=N0(t,n,e,i,Xt);if(a!==Xt)return a}let o=Yg(t,n,e,i,Xt);if(o!==Xt)return o}return Wg(n,e,i,r)}function Yg(t,n,e,i,r){let o=D0(e);if(typeof o=="function"){if(!Yd(n,t,i))return i&1?Gg(r,e,i):Wg(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))Es(e);else return a}finally{Zd()}}else if(typeof o=="number"){let a=null,s=$g(t,n),l=Dr,c=i&1?n[Ye][Ue]:null;for((s===-1||i&4)&&(l=s===-1?Xu(t,n):n[s+8],l===Dr||!tg(i,!1)?s=-1:(a=n[I],s=el(l),n=tl(l,n)));s!==-1;){let d=n[I];if(eg(o,s,d.data)){let u=C0(s,n,e,a,i,c);if(u!==Xt)return u}l=n[s+8],l!==Dr&&tg(i,n[I].data[s+8]===c)&&eg(o,s,n)?(a=d,s=el(l),n=tl(l,n)):s=-1}}return r}function C0(t,n,e,i,r,o){let a=n[I],s=a.data[t+8],l=i==null?gn(s)&&fu:i!=a&&(s.type&3)!==0,c=r&1&&o===s,d=Xs(s,a,e,l,c);return d!==null?Po(n,a,d,s,r):Xt}function Xs(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,d=o>>20,u=i?s:s+d,h=r?s+d:c;for(let f=u;f<h;f++){let p=a[f];if(f<l&&e===p||f>=l&&p.type===e)return f}if(r){let f=a[l];if(f&&Vt(f)&&f.type===e)return l}return null}function Po(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof Li){let s=o;if(s.resolving)throw wd("");let l=Jp(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],d,u=s.injectImpl?ft(s.injectImpl):null,h=Yd(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&f0(e,a[e],n)}finally{u!==null&&ft(u),Jp(l),s.resolving=!1,Zd()}}return o}function D0(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,xi)?t[xi]:void 0;return typeof n=="number"?n>=0?n&Hg:E0:n}function eg(t,n,e){let i=1<<t;return!!(e[n+(t>>Ug)]&i)}function tg(t,n){return!(t&2)&&!(t&1&&n)}var wn=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return qg(this._tNode,this._lView,n,bi(i),e)}};function E0(){return new wn(Ze(),j())}function de(t){return $o(()=>{let n=t.prototype.constructor,e=n[bo]||pu(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[bo]||pu(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function pu(t){return bd(t)?()=>{let n=pu(Le(t));return n&&n()}:vi(t)}function N0(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[A]&2048&&!_r(a);){let s=Yg(o,a,e,i|2,Xt);if(s!==Xt)return s;i&=-5;let l=o.parent;if(!l){let c=a[Md];if(c){let d=c.get(e,Xt,i);if(d!==Xt)return d}l=Zg(a),a=a[Bn]}o=l}return r}function Zg(t){let n=t[I],e=n.type;return e===2?n.declTNode:e===1?t[Ue]:null}function Qu(t){return S0(Ze(),t)}function E(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function k0(){return Tr(Ze(),j())}function Tr(t,n){return new M(Nt(t,n))}var M=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=k0}return t})();function Kg(t){return t instanceof M?t.nativeElement:t}function I0(){return this._results[Symbol.iterator]()}var il=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new x}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=sp(n);(this._changesDetected=!ap(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=I0};function Xg(t){return(t.flags&128)===128}var Ju=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Ju||{}),Qg=new Map,M0=0;function T0(){return M0++}function A0(t){Qg.set(t[Eo],t)}function gu(t){Qg.delete(t[Eo])}var ng="__ngContext__";function Nr(t,n){pn(n)?(t[ng]=n[Eo],A0(n)):t[ng]=n}function Jg(t){return tb(t[br])}function eb(t){return tb(t[Dt])}function tb(t){for(;t!==null&&!Et(t);)t=t[Dt];return t}var bu;function em(t){bu=t}function tm(){if(bu!==void 0)return bu;if(typeof document<"u")return document;throw new k(210,!1)}var nb="r";var ib="di";var rb=!1,ob=new b("",{factory:()=>rb});var ig=new WeakMap;function R0(t,n){if(t==null||typeof t!="object")return;let e=ig.get(t);e||(e=new WeakSet,ig.set(t,e)),e.add(n)}var O0=(t,n,e,i)=>{};function F0(t,n,e,i){O0(t,n,e,i)}function fl(t){return(t.flags&32)===32}var L0=()=>null;function ab(t,n,e=!1){return L0(t,n,e)}function sb(t,n){let e=t.contentQueries;if(e!==null){let i=S(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];Mo(o),s.contentQueries(2,n[a],a)}}}finally{S(i)}}}function vu(t,n,e){Mo(0);let i=S(null);try{n(t,e)}finally{S(i)}}function lb(t,n,e){if(Ad(n)){let i=S(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{S(i)}}}var zt=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(zt||{}),xn=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}`}},_u=class extends xn{getTypeName(){return"HTML"}},yu=class extends xn{getTypeName(){return"Style"}},wu=class extends xn{getTypeName(){return"Script"}},xu=class extends xn{getTypeName(){return"URL"}},Su=class extends xn{getTypeName(){return"ResourceURL"}};function gt(t){return t instanceof xn?t.changingThisBreaksApplicationSecurity:t}function Sn(t,n){let e=cb(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${gd})`)}return e===n}function cb(t){return t instanceof xn&&t.getTypeName()||null}function nm(t){return new _u(t)}function im(t){return new yu(t)}function rm(t){return new wu(t)}function om(t){return new xu(t)}function am(t){return new Su(t)}var P0={"http://www.w3.org/2000/svg":wi,"http://www.w3.org/1998/Math/MathML":As},Gs;function V0(){if(Gs===void 0&&(Gs=null,Pn.trustedTypes))try{Gs=Pn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Gs}function hl(t){return V0()?.createHTML(t)||t}var Ws;function db(){if(Ws===void 0&&(Ws=null,Pn.trustedTypes))try{Ws=Pn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Ws}function rg(t){return db()?.createHTML(t)||t}function og(t){return db()?.createScriptURL(t)||t}function B0(t){let n=new Du(t);return j0()?new Cu(n):n}var Cu=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(hl(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Du=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=hl(n),e}};function j0(){try{return!!new window.DOMParser().parseFromString(hl(""),"text/html")}catch{return!1}}var z0=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function pl(t){return t=String(t),t.match(z0)?t:"unsafe:"+t}function Cn(t){let n=Object.create(null);for(let e of t.split(","))n[e]=!0;return n}function Go(...t){let n=Object.create(null);for(let e of t)for(let i in e)Object.hasOwn(e,i)&&(n[i]=!0);return n}var ub=Cn("area,br,col,hr,img,wbr"),mb=Cn("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),fb=Cn("rp,rt"),H0=Go(fb,mb),U0=Go(mb,Cn("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),$0=Go(fb,Cn("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),ag=Go(ub,U0,$0,H0),hb=Cn("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),G0=Cn("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),W0=Cn("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),q0=Go(hb,G0,W0),Y0=Cn("script,style,template"),Eu=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=X0(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=K0(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=sg(n).toLowerCase();if(!Object.hasOwn(ag,e))return this.sanitizedSomething=!0,!Object.hasOwn(Y0,e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!Object.hasOwn(q0,s)){this.sanitizedSomething=!0;continue}let l=o.value;hb[s]&&(l=pl(l)),this.buf.push(" ",a,'="',lg(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=sg(n).toLowerCase();Object.hasOwn(ag,e)&&!Object.hasOwn(ub,e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(lg(n))}};function Z0(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function K0(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw pb(n);return n}function X0(t){let n=t.firstChild;if(n&&Z0(t,n))throw pb(n);return n}function sg(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function pb(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var Q0=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,J0=/([^\#-~ |!])/g;function lg(t){return t.replace(/&/g,"&amp;").replace(Q0,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(J0,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var qs;function gl(t,n){let e=null;try{qs=qs||B0(t);let i=n?String(n):"";e=qs.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=qs.getInertBodyElement(i)}while(i!==o);let s=new Eu().sanitizeChildren(cg(e)||e);return hl(s)}finally{if(e){let i=cg(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function cg(t){return"content"in t&&eS(t)?t.content:null}function eS(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function tS(t,n){return t.createText(n)}function nS(t,n,e){t.setValue(n,e)}function gb(t,n,e){return t.createElement(n,e)}function Fi(t,n,e,i,r){t.insertBefore(n,e,i,r)}function bb(t,n,e){t.appendChild(n,e)}function dg(t,n,e,i,r){i!==null?Fi(t,n,e,i,r):bb(t,n,e)}function vb(t,n,e,i){t.removeChild(null,n,e,i)}function iS(t,n,e){t.setAttribute(n,"style",e)}function rS(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function _b(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&b0(t,n,i),r!==null&&rS(t,n,r),o!==null&&iS(t,n,o)}function oS(t,n=!0){if(t[0]!=":")return[null,t];let e=t.indexOf(":",1);if(e===-1){if(n)throw new Error(`Unsupported format "${t}" expecting ":namespace:name"`);return[null,t]}return[t.slice(1,e),t.slice(e+1)]}function sm(t,n,e){if(n!==void 0&&e!==void 0&&aS(n,e)!==ve.HTML)return t;let i=yb();return i?rg(i.sanitize(ve.HTML,t)||""):Sn(t,"HTML")?rg(gt(t)):gl(tm(),Ds(t))}function lm(t){let n=yb();if(n)return og(n.sanitize(ve.RESOURCE_URL,t)||"");if(Sn(t,"ResourceURL"))return og(gt(t));throw new k(-904,!1)}function yb(){let t=j();return t&&t[Pt].sanitizer}function aS(t,n){let[e,i]=sS(t);return bp(i,n,e)}function sS(t){t=t.toLowerCase();let n=oS(t,!1);if(n[0])return n;let i=Kt()===-1?null:To(),r=i?.namespace;if(t==="#host"&&i?.type===2){let o=Nt(i,j());if(o.tagName&&(t=o.tagName.toLowerCase()),r==null){let a=o.namespaceURI;r=a&&P0[a]}}return[r,t]}function lS(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var wb="ng-template";function cS(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&lS(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(cm(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function cm(t){return t.type===4&&t.value!==wb}function dS(t,n,e){let i=t.type===4&&!e?wb:t.value;return n===i}function uS(t,n,e){let i=4,r=t.attrs,o=r!==null?hS(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!Bt(i)&&!Bt(l))return!1;if(a&&Bt(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!dS(t,l,e)||l===""&&n.length===1){if(Bt(i))return!1;a=!0}}else if(i&8){if(r===null||!cS(t,r,l,e)){if(Bt(i))return!1;a=!0}}else{let c=n[++s],d=mS(l,r,cm(t),e);if(d===-1){if(Bt(i))return!1;a=!0;continue}if(c!==""){let u;if(d>o?u="":u=r[d+1].toLowerCase(),i&2&&c!==u){if(Bt(i))return!1;a=!0}}}}return Bt(i)||a}function Bt(t){return(t&1)===0}function mS(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return pS(n,t)}function xb(t,n,e=!1){for(let i=0;i<n.length;i++)if(uS(t,n[i],e))return!0;return!1}function fS(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function hS(t){for(let n=0;n<t.length;n++){let e=t[n];if(jg(e))return n}return t.length}function pS(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function gS(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function ug(t,n){return t?":not("+n.trim()+")":n}function bS(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!Bt(a)&&(n+=ug(o,r),r=""),i=a,o=o||!Bt(i);e++}return r!==""&&(n+=ug(o,r)),n}function vS(t){return t.map(bS).join(",")}function _S(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Bt(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Ht={},Qt=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Qt||{}),yS;function dm(t,n){return yS(t,n)}var um=new Set;var nL=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Nu=new WeakMap;function Sb(t){return t?t[Bn]??t:null}var Sr=new WeakSet;function wS(t,n,e){let i=Nu.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,a=Sb(e);for(let s=i.length-1;s>=0;s--){let{el:l,declarationView:c}=i[s],d=l.parentNode;l===n?(i.splice(s,1),Sr.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(i.splice(s,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):d&&r&&d!==r&&(a===null||c===null||a===c)&&(i.splice(s,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function xS(t,n,e){let i=Sb(e),r=Nu.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Nu.set(t,[{el:n,declarationView:i}])}var bl=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(bl||{}),Jt=new b(""),mg=new Set;function Vi(t){mg.has(t)||(mg.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var vl=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=ne({token:t,providedIn:"root",factory:()=>new t})}return t})(),mm=[0,1,2,3],fm=(()=>{class t{ngZone=m(C);scheduler=m(fn);errorHandler=m(Yt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){m(Jt,{optional:!0})}execute(){let e=this.sequences.size>0;e&&oe(Q.AfterRenderHooksStart),this.executing=!0;for(let i of mm)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&oe(Q.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Di]??=[]).push(e),Ii(i),i[A]|=8192):this.addSequence(e)}addSequence(e){this.executing?this.deferredRegistrations.add(e):this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(bl.AFTER_NEXT_RENDER,e):e()}static \u0275prov=ne({token:t,providedIn:"root",factory:()=>new t})}return t})(),Vo=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Di];n&&(this.view[Di]=n.filter(e=>e!==this))}};function Je(t,n){let e=n?.injector??m(T);return Vi("NgAfterNextRender"),CS(t,e,n,!0)}function SS(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function CS(t,n,e,i){let r=n.get(vl);r.impl??=n.get(fm);let o=n.get(Jt,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(ht):null,s=n.get(xr,null,{optional:!0}),l=new Vo(r.impl,SS(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var ku=null,Cb=null;function fg(t,n,e,i){ku?.maybeQueueEnterAnimation(t,n,e,i)}function hg(t,n,e,i){ku===null?i(!1):ku.runLeaveAnimationsWithCallback(t,n,e,i)}function DS(t){Cb?.initViewDetachAnimations(t)}function ES(t){Cb?.clearViewDetachAnimations(t)}function Cr(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;Et(r)?l=r:pn(r)&&(c=!0,r=r[Lt]);let d=$e(r);t===0&&i!==null?(fg(s,i,o,e),a==null?bb(n,i,d):Fi(n,i,d,a||null,!0)):t===1&&i!==null?(Fi(n,i,d,a||null,!0),wS(o,d,s),Sr.has(d)||fg(s,i,o,e)):t===2?(s?.[pp]?.leave?.has(o.index)&&xS(o,d,s),Sr.delete(d),hg(s,o,e,u=>{if(Sr.has(d)){Sr.delete(d);return}vb(n,d,c,u)})):t===3&&(Sr.delete(d),hg(s,o,e,()=>{n.destroyNode(d)})),l!=null&&PS(n,t,e,l,o,i,a)}}function NS(t,n){Db(t,n),n[Lt]=null,n[Ue]=null}function kS(t,n,e,i,r,o){i[Lt]=r,i[Ue]=n,yl(t,i,e,1,r,o)}function Db(t,n){n[Pt].changeDetectionScheduler?.notify(9),yl(t,n,n[we],2,null,null)}function IS(t){let n=t[br];if(!n)return iu(t[I],t);for(;n;){let e=null;if(pn(n))e=n[br];else{let i=n[De];i&&(e=i)}if(!e){for(;n&&!n[Dt]&&n!==t;)pn(n)&&iu(n[I],n),n=n[Ne];n===null&&(n=t),pn(n)&&iu(n[I],n),e=n&&n[Dt]}n=e}}function hm(t,n){let e=t[Ni],i=e.indexOf(n);e.splice(i,1)}function _l(t,n){if(ki(n))return;let e=n[we];e.destroyNode&&yl(t,n,e,3,null,null),IS(n)}function iu(t,n){if(ki(n))return;let e=S(null);try{n[A]&=-129,n[A]|=256,n[pt]&&Rn(n[pt]),TS(t,n),MS(t,n),n[I].type===1&&n[we].destroy();let i=n[jn];if(i!==null&&Et(n[Ne])){i!==n[Ne]&&hm(i,n);let r=n[Zt];r!==null&&r.detachView(t)}gu(n)}finally{S(e)}}function MS(t,n){let e=t.cleanup,i=n[gr];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[gr]=null);let r=n[dn];if(r!==null){n[dn]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[un];if(o!==null){n[un]=null;for(let a of o)a.destroy()}}function TS(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Li)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];oe(Q.LifecycleHookStart,s,l);try{l.call(s)}finally{oe(Q.LifecycleHookEnd,s,l)}}else{oe(Q.LifecycleHookStart,r,o);try{o.call(r)}finally{oe(Q.LifecycleHookEnd,r,o)}}}}}function Eb(t,n,e){if(n===null)throw new k(510,!1);return AS(t,n.parent,e)}function AS(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Lt];if(gn(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===zt.None||r===zt.Emulated)return null}return Nt(i,e)}function Nb(t,n,e){return OS(t,n,e)}function RS(t,n,e){return t.type&40?Nt(t,e):null}var OS=RS,pg;function pm(t,n,e,i){let r=Eb(t,i,n),o=n[we],a=i.parent||n[Ue],s=Nb(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)dg(o,r,e[l],s,!1);else dg(o,r,e,s,!1);pg!==void 0&&pg(o,i,n,e,r)}function Fo(t,n){if(n!==null){let e=n.type;if(e&3)return Nt(n,t);if(e&4)return Iu(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Fo(t,i);{let r=t[n.index];return Et(r)?Iu(-1,r):$e(r)}}else{if(e&128)return Fo(t,n.next);if(e&32)return dm(n,t)()||$e(t[n.index]);{let i=kb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=mn(t[Ye]);return Fo(r,i)}else return Fo(t,n.next)}}}return null}function kb(t,n){if(n!==null){let i=t[Ye][Ue],r=n.projection;return i.projection[r]}return null}function Iu(t,n){let e=De+t+1;if(e<n.length){let i=n[e],r=i[I].firstChild;if(r!==null)return Fo(i,r)}return n[Ei]}function gm(t,n,e,i,r,o,a){for(;e!=null;){let s=i[hn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&Nr($e(l),i),e.flags|=2),!fl(e))if(c&8)gm(t,n,e.child,i,r,o,!1),Cr(n,t,s,r,l,e,o,i);else if(c&32){let d=dm(e,i),u;for(;u=d();)Cr(n,t,s,r,u,e,o,i);Cr(n,t,s,r,l,e,o,i)}else c&16?Ib(t,n,i,e,r,o):Cr(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function yl(t,n,e,i,r,o){t.type===3?FS(e,i,n,r,o):gm(e,i,t.firstChild,n,r,o,!1)}function FS(t,n,e,i,r){let a=e[I].firstChild,s=a.next,l=$e(e[a.index]),c=$e(e[s.index]),d=s.index+1,u=e[d];if(n===1||n===0)i!==null&&(u&&u.hasChildNodes()?Fi(t,i,u,r,!0):(Fi(t,i,l,r,!0),Fi(t,i,c,r,!0)));else if(n===2){if(u||(u=document.createDocumentFragment(),e[d]=u),l&&l.parentNode===u)return;let h=l;for(;h!==null;){let f=h.nextSibling;if(u.appendChild(h),h===c)break;h=f}}}function LS(t,n,e){let i=n[we],r=Eb(t,e,n),o=e.parent||n[Ue],a=Nb(o,e,n);Ib(i,0,n,e,r,a)}function Ib(t,n,e,i,r,o){let a=e[Ye],l=a[Ue].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let d=l[c];Cr(n,t,e[hn],r,d,i,o,e)}else{let c=l,d=a[Ne];Xg(i)&&(c.flags|=128),gm(t,n,c,d,r,o,!0)}}function PS(t,n,e,i,r,o,a){let s=i[Ei],l=$e(i);if(s!==l&&Cr(n,t,e,o,s,r,a),(i[A]&4)===0)for(let c=De;c<i.length;c++){let d=i[c];yl(d[I],d,t,n,o,s)}}function VS(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Qt.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Qt.Important),t.setStyle(e,i,r,o))}}function bm(t,n,e,i,r,o,a,s,l,c,d){let u=ke+i,h=u+r,f=BS(u,h),p=typeof c=="function"?c():c;return f[I]={type:t,blueprint:f,template:e,queries:null,viewQuery:s,declTNode:n,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:p,incompleteFirstPass:!1,ssrId:d}}function BS(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Ht);return e}function jS(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=bm(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function vm(t,n,e,i,r,o,a,s,l,c,d){let u=n.blueprint.slice();return u[Lt]=r,u[A]=i|4|128|8|64|1024,(c!==null||t&&t[A]&2048)&&(u[A]|=2048),Od(u),u[Ne]=u[Bn]=t,u[Ce]=e,u[Pt]=a||t&&t[Pt],u[we]=s||t&&t[we],u[hn]=l||t&&t[hn]||null,u[Ue]=o,u[Eo]=T0(),u[Si]=d,u[Md]=c,u[Ye]=n.type==2?t[Ye]:u,u}function zS(t,n,e){let i=Nt(n,t),r=jS(e),o=t[Pt].rendererFactory,a=_m(t,vm(t,r,null,Mb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function Mb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Tb(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function _m(t,n){return t[br]?t[Id][Dt]=n:t[br]=n,t[Id]=n,n}function F(t=1){Ab(Te(),j(),Kt()+t,!1)}function Ab(t,n,e,i){if(!i)if((n[A]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Zs(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Ks(n,o,0,e)}zn(e)}var wl=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(wl||{});function Mu(t,n,e,i){let r=S(null);try{let[o,a,s]=t.inputs[e],l=null;(a&wl.SignalBased)!==0&&(l=n[o][Fe]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):Fg(n,l,o,i)}finally{S(r)}}function Rb(t,n,e,i,r){let o=Kt(),a=i&2;try{zn(-1),a&&n.length>ke&&Ab(t,n,ke,!1);let s=a?Q.TemplateUpdateStart:Q.TemplateCreateStart;oe(s,r,e),e(i,r)}finally{zn(o);let s=a?Q.TemplateUpdateEnd:Q.TemplateCreateEnd;oe(s,r,e)}}function ym(t,n,e){YS(t,n,e),(e.flags&64)===64&&ZS(t,n,e)}function xl(t,n,e=Nt){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function HS(t,n,e,i){let o=i.get(ob,rb)||e===zt.ShadowDom||e===zt.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return US(a),a}function US(t){$S(t)}var $S=()=>null;function GS(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function WS(t,n,e,i,r,o){let a=n[I];if(wm(t,a,n,e,i)){gn(t)&&qS(n,t.index);return}t.type&3&&(e=GS(e)),Ob(t,n,e,i,r,o)}function Ob(t,n,e,i,r,o){if(t.type&3){let a=Nt(t,n);i=o!=null?o(i,t.value||"",e):i,i=Fb(e,a,i),r.setProperty(a,e,i)}else t.type&12}function Fb(t,n,e,i=!1){return t==="src"&&(n.tagName==="IMG"||n.tagName==="VIDEO")?e==null?i?e:"":gt(e):e}function qS(t,n){let e=kt(n,t);e[A]&16||(e[A]|=64)}function YS(t,n,e){let i=e.directiveStart,r=e.directiveEnd;gn(e)&&zS(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||nl(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=Po(n,t,a,e);if(Nr(l,n),o!==null&&JS(n,a-i,l,s,e,o),Vt(s)){let c=kt(e.index,n);c[Ce]=Po(n,t,a,e)}}}function ZS(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=Tp();try{zn(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];Ps(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&KS(l,c)}}finally{zn(-1),Ps(a)}}function KS(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Lb(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];xb(n,o.selectors,!1)&&(i??=[],Vt(o)?i.unshift(o):i.push(o))}return i}function XS(t,n,e,i,r,o){let a=Nt(t,n);i=Fb(e,a,i,!0),QS(n[we],a,o,t.value,e,i,r)}function QS(t,n,e,i,r,o,a){if(o==null)a?.(o,i||"",r),t.removeAttribute(n,r,e);else{let s=a==null?Ds(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function JS(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];Mu(i,e,l,c)}}function Pb(t,n,e,i,r){let o=ke+e,a=n[I],s=r(a,n,t,i,e);n[o]=s,yr(t,!0);let l=t.type===2;return l?(_b(n[we],s,t),(Cp()===0||ko(t))&&Nr(s,n),Dp()):Nr(s,n),zs()&&(!l||!fl(t))&&pm(a,n,s,t),t}function Vb(t){let n=t;return $d()?Gd():(n=n.parent,yr(n,!1)),n}function eC(t,n){let e=t[hn];if(!e)return;let i;try{i=e.get(Ti,null)}catch{i=null}i?.(n)}function wm(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],d=a[l+1],u=n.data[c];Mu(u,e[c],d,r),s=!0}if(o)for(let l of o){let c=e[l],d=n.data[l];Mu(d,c,i,r),s=!0}return s}function tC(t,n){let e=kt(n,t),i=e[I];nC(i,e);let r=e[Lt];r!==null&&e[Si]===null&&(e[Si]=ab(r,e[hn])),oe(Q.ComponentStart);try{xm(i,e,e[Ce])}finally{oe(Q.ComponentEnd,e[Ce])}}function nC(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function xm(t,n,e){Bs(n);try{let i=t.viewQuery;i!==null&&vu(1,i,e);let r=t.template;r!==null&&Rb(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Zt]?.finishViewCreation(t),t.staticContentQueries&&sb(t,n),t.staticViewQueries&&vu(2,t.viewQuery,e);let o=t.components;o!==null&&iC(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[A]&=-5,js()}}function iC(t,n){for(let e=0;e<n.length;e++)tC(t,n[e])}function Wo(t,n,e,i){let r=S(null);try{let o=n.tView,s=t[A]&4096?4096:16,l=vm(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[jn]=c;let d=t[Zt];return d!==null&&(l[Zt]=d.createEmbeddedView(o)),xm(o,l,e),l}finally{S(r)}}function kr(t,n){return!n||n.firstChild===null||Xg(t)}function Bo(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,a=o.next,s=$e(n[o.index]),l=$e(n[a.index]),c=s;for(;c!==null&&(i.push(c),c!==l);)c=c.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Et(o)){let s=o[Ei];s!==o[Lt]&&i.push($e(o)),o[A]&4||Bb(o,i),i.push(s)}else i.push($e(o));let a=e.type;if(a&8)Bo(t,n,e.child,i);else if(a&32){let s=dm(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=kb(n,e);if(Array.isArray(s))i.push(...s);else{let l=mn(n[Ye]);Bo(l[I],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function Bb(t,n){for(let e=De;e<t.length;e++){let i=t[e],r=i[I].firstChild;r!==null&&Bo(i[I],i,r,n)}}function jb(t){if(t[Di]!==null){for(let n of t[Di])n.impl.addSequence(n);t[Di].length=0}}var zb=[];function rC(t){return t[pt]??oC(t)}function oC(t){let n=zb.pop()??Object.create(sC);return n.lView=t,n}function aC(t){t.lView[pt]!==t&&(t.lView=null,zb.push(t))}var sC=W(y({},ai),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Ii(t.lView)},consumerOnSignalRead(){this.lView[pt]=this}});function lC(t){let n=t[pt]??Object.create(cC);return n.lView=t,n}var cC=W(y({},ai),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=mn(t.lView);for(;n&&!Hb(n[I]);)n=mn(n);n&&Fd(n)},consumerOnSignalRead(){this.lView[pt]=this}});function Hb(t){return t.type!==2}function Ub(t){if(t[un]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[un])if(i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),t[un]===null))return;n=e&&!!(t[A]&8192)}}var dC=100;function $b(t,n=0){let i=t[Pt].rendererFactory,r=!1;r||i.begin?.();try{uC(t,n)}finally{r||i.end?.()}}function uC(t,n){let e=Wd();try{vo(!0),Tu(t,n);let i=0;for(;Io(t);){if(i===dC)throw new k(103,!1);i++,Tu(t,1)}}finally{vo(e)}}function mC(t,n,e,i){if(ki(n))return;let r=n[A],o=!1,a=!1;Bs(n);let s=!0,l=null,c=null;o||(Hb(t)?(c=rC(n),l=An(c)):Fa()===null?(s=!1,c=lC(n),l=An(c)):n[pt]&&(Rn(n[pt]),n[pt]=null));try{Od(n),kp(t.bindingStartIndex),e!==null&&Rb(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let f=t.preOrderCheckHooks;f!==null&&Zs(n,f,null)}else{let f=t.preOrderHooks;f!==null&&Ks(n,f,0,null),tu(n,0)}if(a||fC(n),Ub(n),Wb(n,0),t.contentQueries!==null&&sb(t,n),!o)if(d){let f=t.contentCheckHooks;f!==null&&Zs(n,f)}else{let f=t.contentHooks;f!==null&&Ks(n,f,1),tu(n,1)}pC(t,n);let u=t.components;u!==null&&Yb(n,u,0);let h=t.viewQuery;if(h!==null&&vu(2,h,i),!o)if(d){let f=t.viewCheckHooks;f!==null&&Zs(n,f)}else{let f=t.viewHooks;f!==null&&Ks(n,f,2),tu(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Ts]){for(let f of n[Ts])f();n[Ts]=null}o||(jb(n),n[A]&=-73)}catch(d){Gb(n,d,o)}finally{c!==null&&(si(c,l),s&&aC(c)),js()}}function Gb(t,n,e){let i=!1,r=n,o=t,a=S(null);try{for(;o!==null;){if(Et(o)){o=o[Ne];continue}let s=o[vr];if(s)try{let l=t[Ye][Ce],c=l?.constructor,d={declarationInstance:l,declarationType:c,caughtBy:s},u=S(null);try{s(Up(r),d)}finally{S(u)}i=!0;break}catch(l){r=l}o=o[Ne]}}finally{S(a)}if(!i)throw e||Ii(t),r}function Wb(t,n){for(let e=Jg(t);e!==null;e=eb(e))for(let i=De;i<e.length;i++){let r=e[i];qb(r,n)}}function fC(t){for(let n=Jg(t);n!==null;n=eb(n)){if(!(n[A]&2))continue;let e=n[Ni];for(let i=0;i<e.length;i++){let r=e[i];Fd(r)}}}function hC(t,n,e){oe(Q.ComponentStart);let i=kt(n,t);try{qb(i,e)}finally{oe(Q.ComponentEnd,i[Ce])}}function qb(t,n){Os(t)&&Tu(t,n)}function Tu(t,n){let i=t[I],r=t[A],o=t[pt],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&er(o)),a||=!1,o&&(o.dirty=!1),t[A]&=-9217,a)mC(i,t,i.template,t[Ce]);else if(r&8192){let s=S(null);try{try{Ub(t)}catch(c){Gb(t,c,!1)}Wb(t,1);let l=i.components;l!==null&&Yb(t,l,1),jb(t)}finally{S(s)}}}function Yb(t,n,e){for(let i=0;i<n.length;i++)hC(t,n[i],e)}function pC(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)zn(~r);else{let o=r,a=e[++i],s=e[++i];Mp(a,o);let l=n[o];oe(Q.HostBindingsUpdateStart,l);try{s(2,l)}finally{oe(Q.HostBindingsUpdateEnd,l)}}}}finally{zn(-1)}}function Sm(t,n){let e=Wd()?64:1088;for(t[Pt].changeDetectionScheduler?.notify(n);t;){t[A]|=e;let i=mn(t);if(_r(t)&&!i)return t;t=i}return null}function Zb(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function Kb(t,n){let e=De+n;if(e<t.length)return t[e]}function qo(t,n,e,i=!0){let r=n[I];if(gC(r,n,t,e),i){let a=Iu(e,t),s=n[we],l=s.parentNode(t[Ei]);l!==null&&kS(r,t[Ue],s,n,l,a)}let o=n[Si];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Xb(t,n){let e=jo(t,n);return e!==void 0&&_l(e[I],e),e}function jo(t,n){if(t.length<=De)return;let e=De+n,i=t[e];if(i){let r=i[jn];r!==null&&r!==t&&hm(r,i),n>0&&(t[e-1][Dt]=i[Dt]);let o=Co(t,De+n);NS(i[I],i);let a=o[Zt];a!==null&&a.detachView(o[I]),i[Ne]=null,i[Dt]=null,i[A]&=-129}return i}function gC(t,n,e,i){let r=De+i,o=e.length;i>0&&(e[r-1][Dt]=n),i<o-De?(n[Dt]=e[r],Sd(e,De+i,n)):(e.push(n),n[Dt]=null),n[Ne]=e;let a=n[jn];a!==null&&e!==a&&Qb(a,n);let s=n[Zt];s!==null&&s.insertView(t),Fs(n),n[A]|=128}function Qb(t,n){let e=t[Ni],i=n[Ne];if(pn(i))t[A]|=2;else{let r=i[Ne][Ye];n[Ye]!==r&&(t[A]|=2)}e===null?t[Ni]=[n]:e.push(n)}var Hn=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[I];return Bo(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[Ce]}set context(n){this._lView[Ce]=n}get destroyed(){return ki(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Ne];if(Et(n)){let e=n[No],i=e?e.indexOf(this):-1;i>-1&&(jo(n,i),Co(e,i))}this._attachedToViewContainer=!1}_l(this._lView[I],this._lView)}onDestroy(n){Ls(this._lView,n)}markForCheck(){Sm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[A]&=-129}reattach(){Fs(this._lView),this._lView[A]|=128}detectChanges(){this._lView[A]|=1024,$b(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new k(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=_r(this._lView),e=this._lView[jn];e!==null&&!n&&hm(e,this._lView),Db(this._lView[I],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new k(902,!1);this._appRef=n;let e=_r(this._lView),i=this._lView[jn];i!==null&&!e&&Qb(i,this._lView),Fs(this._lView)}};var bt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=bC;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Wo(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Hn(o)}}return t})();function bC(){return Sl(Ze(),j())}function Sl(t,n){return t.type&4?new bt(n,t,Tr(t,n)):null}function Ar(t,n,e,i,r){let o=t.data[n];if(o===null)o=vC(t,n,e,i,r),Ip()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=Ep();o.injectorIndex=a===null?-1:a.injectorIndex}return yr(o,!0),o}function vC(t,n,e,i,r){let o=Ud(),a=$d(),s=a?o:o&&o.parent,l=t.data[n]=yC(t,s,e,n,i,r);return _C(t,l,o,a),l}function _C(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function yC(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return jd()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:Kd(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function wC(t){let n=t[Td]??[],i=t[Ne][we],r=[];for(let o of n)o.data[ib]!==void 0?r.push(o):xC(o,i);t[Td]=r}function xC(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[nb];for(;e<r;){let o=i.nextSibling;vb(n,i,!1),i=o,e++}}}var SC=()=>null,CC=()=>null;function rl(t,n){return SC(t,n)}function Jb(t,n,e){return CC(t,n,e)}var ev=class{},Ae=class{},Se=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>DC()};function DC(){let t=j(),n=Ze(),e=kt(n.index,t);return(pn(e)?e:t)[we]}var tv=(()=>{class t{static \u0275prov=ne({token:t,providedIn:"root",factory:()=>null})}return t})();function nv(t){return t.debugInfo?.className||t.type.name||null}var Qs={},ol=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Qs,i);return r!==Qs||e===Qs?r:this.parentInjector.get(n,e,i)}};function $n(t,n,e){if(e===Ht)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function ru(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&R0(r,o);let a=gn(t)?kt(t.index,n):n;Sm(a,5);let s=n[Ce],l=gg(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=gg(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function gg(t,n,e,i){let r=S(null);try{return oe(Q.OutputStart,n,e),e(i)!==!1}catch(o){return eC(t,o),!1}finally{oe(Q.OutputEnd,n,e),S(r)}}function EC(t,n,e,i,r,o,a,s){let l=ko(t),c=!1,d=null;if(!i&&l&&(d=kC(n,e,o,t.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=a,d.__ngLastListenerFn__=a,c=!0}else{let u=Nt(t,e),h=i?i(u):u;F0(e,h,o,s),i||(s.__ngNativeEl__=u);let f=r.listen(h,o,s);if(!NC(o)){let p=i?g=>i($e(g[t.index])):t.index;iv(p,n,e,o,s,f,!1)}}return c}function NC(t){return t.startsWith("animation")||t.startsWith("transition")}function kC(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[gr],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function iv(t,n,e,i,r,o,a){let s=n.firstCreatePass?Pd(n):null,l=Ld(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function bg(t,n,e,i,r,o){let a=n[e],s=n[I],c=s.data[e].outputs[i],u=a[c].subscribe(o);iv(t.index,s,n,r,o,u,!0)}var Au=Symbol("BINDING");var Bi=new b("");function al(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=xs(r,s);else if(o==2){let l=s,c=n[++a];i=xs(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ze(t,n=0){let e=j();if(e===null)return G(t,n);let i=Ze();return qg(i,e,Le(t),n)}function rv(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let d of a)if(d.resolveHostDirectives!==null){[s,l,c]=d.resolveHostDirectives(a);break}TC(t,n,e,s,o,l,c)}o!==null&&i!==null&&IC(e,i,o)}function IC(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new k(-301,!1);i.push(n[r],o)}}function MC(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function TC(t,n,e,i,r,o,a){let s=i.length,l=null;for(let h=0;h<s;h++){let f=i[h];l===null&&Vt(f)&&(l=f,MC(t,e,h)),hu(nl(e,n),t,f.type)}PC(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<s;h++){let f=i[h];f.providersResolver&&f.providersResolver(f)}let c=!1,d=!1,u=Tb(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let h=0;h<s;h++){let f=i[h];if(e.mergedAttrs=Er(e.mergedAttrs,f.hostAttrs),RC(t,e,n,u,f),LC(u,f,r),a!==null&&a.has(f)){let[g,_]=a.get(f);e.directiveToIndex.set(f.type,[u,g+e.directiveStart,_+e.directiveStart])}else(o===null||!o.has(f))&&e.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(e.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(e.flags|=64);let p=f.type.prototype;!c&&(p.ngOnChanges||p.ngOnInit||p.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!d&&(p.ngOnChanges||p.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}AC(t,e,o)}function AC(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))vg(0,n,r,i),vg(1,n,r,i),yg(n,i,!1);else{let o=e.get(r);_g(0,n,o,i),_g(1,n,o,i),yg(n,i,!0)}}}function vg(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),ov(n,o)}}function _g(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),ov(n,a)}}function ov(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function yg(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||cm(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&Object.hasOwn(r,l)){let c=r[l];for(let d of c)if(d===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&Object.hasOwn(o,l)){let c=o[l];for(let d=0;d<c.length;d+=2)if(c[d]===n){a??=[],a.push(c[d+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function RC(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=vi(r.type,!0)),a=new Li(o,Vt(r),ze,null);t.blueprint[i]=a,e[i]=a,OC(t,n,i,Tb(t,e,r.hostVars,Ht),r)}function OC(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;FC(a)!=s&&a.push(s),a.push(e,i,o)}}function FC(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function LC(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Vt(n)&&(e[""]=t)}}function PC(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function av(t,n,e,i,r,o,a,s){let l=n[I],c=l.consts,d=It(c,a),u=Ar(l,t,e,i,d);return o&&rv(l,n,u,It(c,s),r),u.mergedAttrs=Er(u.mergedAttrs,u.attrs),u.attrs!==null&&al(u,u.attrs,!1),u.mergedAttrs!==null&&al(u,u.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,u),u}function sv(t,n){Vg(t,n),Ad(n)&&t.queries.elementEnd(n)}function VC(t,n,e,i,r,o){let a=n.consts,s=It(a,r),l=Ar(n,t,e,i,s);if(l.mergedAttrs=Er(l.mergedAttrs,l.attrs),o!=null){let c=It(a,o);l.localNames=[];for(let d=0;d<c.length;d+=2)l.localNames.push(c[d],-1)}return l.attrs!==null&&al(l,l.attrs,!1),l.mergedAttrs!==null&&al(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var lv=typeof ShadowRoot<"u",BC=typeof Document<"u";function jC(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&wl.SignalBased)!==0};return r&&(o.transform=r),o})}function zC(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function HC(t,n,e){let i=n instanceof Pe?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new ol(e,i):e}function UC(t){let n=t.get(Ae,null);if(n===null)throw new k(407,!1);let e=t.get(tv,null),i=t.get(fn,null),r=t.get(Jt,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function $C(t,n,e){let i=cv(t);return gb(n,i,i==="svg"?wi:i==="math"?As:e)}function GC(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new k(905,!1)}function cv(t){return(t.selectors[0][0]||"div").toLowerCase()}var Ir=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=jC(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=zC(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=vS(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a,s,l){oe(Q.DynamicComponentStart);let c=S(null);try{let d=this.componentDef,u=HC(d,r||this.ngModule,n),h=UC(u),f=h.tracingService;return f&&f.componentCreate?f.componentCreate(nv(d),()=>this.createComponentRef(h,u,e,i,o,a,s,l)):this.createComponentRef(h,u,e,i,o,a,s,l)}finally{S(c)}}createComponentRef(n,e,i,r,o,a,s,l){let c=this.componentDef,d=WC(r,c,a,o,l),u=n.rendererFactory.createRenderer(null,c),h=r?HS(u,r,c.encapsulation,e):$C(c,u,s??null);GC(h);let f=e.get(Bi,null),p=qC(h,()=>e.get(V,null)??tm());f&&f.addHost(p);let g=a?.some(wg)||o?.some(U=>typeof U!="function"&&U.bindings.some(wg)),_=vm(null,d,null,512|Mb(c),null,null,n,u,e,null,ab(h,e,!0));f&&lv&&p instanceof ShadowRoot&&Ls(_,()=>{f.removeHost(p)}),_[ke]=h,Bs(_);let w=null;try{let U=av(ke,_,2,"#host",()=>d.directiveRegistry,!0,0);_b(u,h,U),Nr(h,_),ym(d,_,U),lb(d,U,_),sv(d,U),i!==void 0&&ZC(U,this.ngContentSelectors,i),w=kt(U.index,_),_[Ce]=w[Ce],xm(d,_,null)}catch(U){throw w!==null&&gu(w),gu(_),U}finally{oe(Q.DynamicComponentEnd),js()}return new sl(this.componentType,_,!!g)}};function WC(t,n,e,i,r){let o=t?["ng-version","22.2.5"]:_S(n.selectors[0]),a=null,s=null,l=0;if(e)for(let u of e)l+=u[Au].requiredVars,u.create&&(u.targetIdx=0,(a??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let h=i[u];if(typeof h!="function")for(let f of h.bindings){l+=f[Au].requiredVars;let p=u+1;f.create&&(f.targetIdx=p,(a??=[]).push(f)),f.update&&(f.targetIdx=p,(s??=[]).push(f))}}let c=[n];if(i)for(let u of i){let h=typeof u=="function"?u:u.type,f=So(h);c.push(f)}return bm(0,null,YC(a,s),1,l,c,null,null,null,[o],null)}function qC(t,n){let e=t.getRootNode?.();return BC&&e instanceof Document?e.head:e&&lv&&e instanceof ShadowRoot?e:n().head}function YC(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function wg(t){let n=t[Au].kind;return n==="input"||n==="twoWay"}var sl=class extends ev{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Rs(e[I],ke),this.location=Tr(this._tNode,e),this.instance=kt(this._tNode.index,e)[Ce],this.hostView=this.changeDetectorRef=new Hn(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=wm(i,r[I],r,n,e);this.previousInputValues.set(n,e);let a=kt(i.index,r);Sm(a,1)}get injector(){return new wn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function ZC(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var en=(()=>{class t{static __NG_ELEMENT_ID__=KC}return t})();function KC(){let t=Ze();return dv(t,j())}var Ru=class t extends en{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Tr(this._hostTNode,this._hostLView)}get injector(){return new wn(this._hostTNode,this._hostLView)}get parentInjector(){let n=Xu(this._hostTNode,this._hostLView);if(zg(n)){let e=tl(n,this._hostLView),i=el(n),r=e[I].data[i+8];return new wn(r,e)}else return new wn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=xg(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-De}createEmbeddedView(n,e,i){let r,o,a;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector,a=i.onError);let s=rl(this._lContainer,n.ssrId),l=n.createEmbeddedViewImpl(e||{},o,s);return a&&(l._lView[vr]=a),this.insertImpl(l,r,kr(this._hostTNode,s)),l}createComponent(n,e,i,r,o,a,s){let l,c,d=e||{};l=d.index,i=d.injector,r=d.projectableNodes,o=d.environmentInjector||d.ngModuleRef,a=d.directives,s=d.bindings,c=d.onError;let u=new Ir(Ln(n)),h=i||this.parentInjector;if(!o&&u.ngModule==null){let U=this.parentInjector.get(Pe,null);U&&(o=U)}let f=Ln(u.componentType??{}),p=rl(this._lContainer,f?.id??null),g=p?.firstChild??null,_=u.create(h,r,g,o,a,s,this._getHostElementNamespace());return c&&(_.hostView._lView[vr]=c),this.insertImpl(_.hostView,l,kr(this._hostTNode,p)),_}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[Ue];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(yp(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[Ne],c=new t(l,l[Ue],l[Ne]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return qo(a,r,o,i),n.attachToViewContainerRef(),Sd(ou(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=xg(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=jo(this._lContainer,e);i&&(Co(ou(this._lContainer),e),_l(i[I],i))}detach(n){let e=this._adjustIndex(n,-1),i=jo(this._lContainer,e);return i&&Co(ou(this._lContainer),e)!=null?new Hn(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function xg(t){return t[No]}function ou(t){return t[No]||(t[No]=[])}function dv(t,n){let e,i=n[t.index];return Et(i)?e=i:(e=Zb(i,n,null,t),n[t.index]=e,_m(n,e)),QC(e,n,t,i),new Ru(e,t,n)}function XC(t,n){let e=t[we],i=e.createComment(""),r=Nt(n,t),o=e.parentNode(r);return Fi(e,o,i,e.nextSibling(r),!1),i}var QC=tD,JC=()=>!1;function eD(t,n,e){return JC(t,n,e)}function tD(t,n,e,i){if(t[Ei])return;let r;e.type&8?r=$e(i):r=XC(n,e),t[Ei]=r}var Ou=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Fu=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Dm(n,e).matches!==null&&this.queries[e].setDirty()}},ll=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=aD(n):this.predicate=n}},Lu=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Pu=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,nD(e,o)),this.matchTNodeWithReadOption(n,e,Xs(e,n,o,!1,!1))}else i===bt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Xs(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===M||r===en||r===T||r===bt&&e.type&4)this.addMatch(e.index,-2);else{let o=Xs(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function nD(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function iD(t,n){return t.type&11?Tr(t,n):t.type&4?Sl(t,n):null}function rD(t,n,e,i){return e===-1?iD(n,t):e===-2?oD(t,n,i):Po(t,t[I],e,n)}function oD(t,n,e){if(e===M)return Tr(n,t);if(e===bt)return Sl(n,t);if(e===en)return dv(n,t);if(e===T)return new wn(n,t)}function uv(t,n,e,i){let r=n[Zt].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let d=o[c];s.push(rD(n,d,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function Vu(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=uv(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],d=n[-l];for(let u=De;u<d.length;u++){let h=d[u];h[jn]===h[Ne]&&Vu(h[I],h,c,i)}if(d[Ni]!==null){let u=d[Ni];for(let h=0;h<u.length;h++){let f=u[h];Vu(f[I],f,c,i)}}}}}return i}function Cm(t,n){return t[Zt].queries[n].queryList}function mv(t,n,e){let i=new il((e&4)===4);return Sp(t,n,i,i.destroy),(n[Zt]??=new Fu).queries.push(new Ou(i))-1}function fv(t,n,e){let i=Te();return i.firstCreatePass&&(pv(i,new ll(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),mv(i,j(),n)}function hv(t,n,e,i){let r=Te();if(r.firstCreatePass){let o=Ze();pv(r,new ll(n,e,i),o.index),sD(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return mv(r,j(),e)}function aD(t){return t.split(",").map(n=>n.trim())}function pv(t,n,e){t.queries===null&&(t.queries=new Lu),t.queries.track(new Pu(n,e))}function sD(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Dm(t,n){return t.queries.getByIndex(n)}function gv(t,n){let e=t[I],i=Dm(e,n);return i.crossesNgTemplate?Vu(e,t,n,[]):uv(e,t,i,n)}function bv(t,n,e){let i,r=ao(()=>{i._dirtyCounter();let o=lD(i,t);if(n&&o===void 0)throw new k(-951,!1);return o});return i=r[Fe],i._dirtyCounter=X(0),i._flatValue=void 0,r}function Em(t){return bv(!0,!1,t)}function Nm(t){return bv(!0,!0,t)}function vv(t,n){let e=t[Fe];e._lView=j(),e._queryIndex=n,e._queryList=Cm(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function lD(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[A]&4)return n?void 0:qe;let r=Cm(e,i),o=gv(e,i);return r.reset(o,Kg),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function ji(t){return!!t&&typeof t.then=="function"}function km(t){return!!t&&typeof t.subscribe=="function"}var Un=class{};var zo=class extends Un{injector;instance=null;constructor(n){super();let e=new yi([...n.providers,{provide:Un,useValue:this}],n.parent||pr(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function _v(t,n,e=null){return new zo({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var cD=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Ed(!1,e.type),r=i.length>0?_v([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=ne({token:t,providedIn:"environment",factory:()=>new t(G(Pe))})}return t})();function N(t){return $o(()=>{let n=yv(t),e=W(y({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==Ju.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(cD).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||zt.Emulated,styles:t.styles||qe,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Vi("NgStandalone"),wv(e);let i=t.dependencies;return e.directiveDefs=Sg(i,dD),e.pipeDefs=Sg(i,np),e.id=fD(e),e})}function dD(t){return Ln(t)||So(t)}function Ve(t){return $o(()=>({type:t.type,bootstrap:t.bootstrap||qe,declarations:t.declarations||qe,imports:t.imports||qe,exports:t.exports||qe,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function uD(t,n){if(t==null)return Vn;let e={};for(let i in t)if(Object.hasOwn(t,i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=wl.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function mD(t){if(t==null)return Vn;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function D(t){return $o(()=>{let n=yv(t);return wv(n),n})}function yv(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Vn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||qe,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:uD(t.inputs,n),outputs:mD(t.outputs),debugInfo:null}}function wv(t){t.features?.forEach(n=>n(t))}function Sg(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function fD(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var xv=new b("");var Im=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=m(xv,{optional:!0})??[];injector=m(T);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Ms(this.injector,r);if(ji(o))e.push(o);else if(km(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Mm(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=hD,e.hostDirectives=i?t.map(Bu):[t]):i?e.hostDirectives.unshift(...t.map(Bu)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function hD(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,Sv(a,n,i,t),r.set(a,[s,n.length-1])}o===0&&Vt(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return i!==null&&i.forEach((o,a)=>{pD(a.declaredInputs,o.inputs)}),[n,i,r]}function Sv(t,n,e,i){if(t.hostDirectives!==null)for(let r of t.hostDirectives)if(typeof r=="function"){let o=r();for(let a of o)Cg(Bu(a),n,e,i)}else Cg(r,n,e,i)}function Cg(t,n,e,i){let r=So(t.directive);if(Sv(r,n,e,i),e.has(r)){let o=e.get(r);Dg(o,t.inputs,"input"),Dg(o,t.outputs,"output")}else i.includes(r)||(e.set(r,t),n.push(r))}function Dg(t,n,e){let i=e==="input"?t.inputs:t.outputs;Object.keys(n).forEach(r=>{let o=n[r];(!Object.hasOwn(i,r)||i[r]===o)&&(i[r]=o)})}function Bu(t){return typeof t=="function"?{directive:Le(t),inputs:{},outputs:{}}:{directive:Le(t.directive),inputs:Eg(t.inputs),outputs:Eg(t.outputs)}}function Eg(t){let n={};if(t!==void 0&&t.length>0)for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function pD(t,n){for(let e in n)if(Object.hasOwn(n,e)){let i=n[e],r=t[e];t[i]=r}}function gD(t){return Object.getPrototypeOf(t.prototype).constructor}function L(t){let n=gD(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,wo)?n[wo]:void 0,a=Object.hasOwn(n,xo)?n[xo]:void 0;if(Vt(t))r=o??a;else{if(o)throw new k(903,!1);r=a}if(r){if(e){i.push(r);let l=t;l.inputs=au(t.inputs),l.declaredInputs=au(t.declaredInputs),l.outputs=au(t.outputs);let c=r.hostBindings;c&&wD(t,c);let d=r.viewQuery,u=r.contentQueries;if(d&&_D(t,d),u&&yD(t,u),bD(t,r),tp(t.outputs,r.outputs),Vt(r)&&r.data.animation){let h=t.data;h.animation=(h.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===L&&(e=!1)}}n=Object.getPrototypeOf(n)}vD(i)}function bD(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function vD(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Er(r.hostAttrs,e=Er(e,r.hostAttrs))}}function au(t){return t===Vn?{}:t===qe?[]:t}function _D(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function yD(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function wD(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function Cv(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=Er(t.mergedAttrs,t.attrs);let d=t.tView=bm(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),yr(t,!1);let l=SD(e,n,t,i);zs()&&pm(e,n,l,t),Nr(l,n);let c=Zb(l,n,l,t);n[i+ke]=c,_m(n,c),eD(c,t,n)}function xD(t,n,e,i,r,o,a,s,l,c,d){let u=e+ke,h;return n.firstCreatePass?(h=Ar(n,u,4,a||null,s||null),Bd()&&rv(n,t,h,It(n.consts,c),Lb),Vg(n,h)):h=n.data[u],Cv(h,t,n,e,i,r,o,l),ko(h)&&ym(n,t,h),c!=null&&xl(t,h,d),h}function Ho(t,n,e,i,r,o,a,s,l,c,d){let u=e+ke,h;if(n.firstCreatePass){if(h=Ar(n,u,4,a||null,s||null),c!=null){let f=It(n.consts,c);h.localNames=[];for(let p=0;p<f.length;p+=2)h.localNames.push(f[p],-1)}}else h=n.data[u];return Cv(h,t,n,e,i,r,o,l),c!=null&&xl(t,h,d),h}function et(t,n,e,i,r,o,a,s){let l=j(),c=Te(),d=It(c.consts,o);return xD(l,c,t,n,e,i,r,d,void 0,a,s),et}var SD=CD;function CD(t,n,e,i){return Hs(!0),n[we].createComment("")}var Tm=new b("");var Am=new b("");function Dv(){Vc(t=>{let n="";throw new k(-600,n)})}var DD=10;var vt=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=m(Ti);afterRenderManager=m(vl);zonelessEnabled=m(Oo);rootEffectScheduler=m($s);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new x;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=m(wr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(je(e=>!e))}constructor(){m(Jt,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=m(Pe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=T.NULL){return this._injector.get(C).run(()=>{if(oe(Q.BootstrapComponentStart),!this._injector.get(Im).done){let w="";throw new k(405,w)}let s=Ln(e),l=this._injector.get(Un),c=new Ir(s,l);this.componentTypes.push(e);let{hostElement:d,directives:u,bindings:h}=ED(i),f=d||c.selector,p=c.create(r,[],f,l.injector,u,h),g=p.location.nativeElement,_=p.injector.get(Tm,null);return _?.registerApplication(g),p.onDestroy(()=>{this.detachView(p.hostView),Lo(this.components,p),_?.unregisterApplication(g)}),this._loadComponent(p),oe(Q.BootstrapComponentEnd,p),p})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){oe(Q.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(bl.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw oe(Q.ChangeDetectionEnd),new k(101,!1);let e=S(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,S(e),this.afterTick.next(),oe(Q.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ae,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<DD;){oe(Q.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{oe(Q.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Io(r))continue;let o=i&&!this.zonelessEnabled?0:1;$b(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Io(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Lo(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Am,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Lo(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new k(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function ED(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function Lo(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ae(t,n,e,i){let r=j(),o=Mi();if($n(r,o,n)){let a=Te(),s=To();XS(s,r,t,n,e,i)}return ae}var ju=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function su(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function ND(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){S(i);let c=n.length-1;for(S(null);a<=s&&a<=c;){let d=t.at(a),u=n[a],h=su(a,d,a,u,e);if(h!==0){h<0&&t.updateValue(a,u),a++;continue}let f=t.at(s),p=n[c],g=su(s,f,c,p,e);if(g!==0){g<0&&t.updateValue(s,p),s--,c--;continue}let _=e(a,d),w=e(s,f),U=e(a,u);if(Object.is(U,w)){let $t=e(c,p);Object.is($t,_)?(t.swap(a,s),t.updateValue(s,p),c--,s--):t.move(s,a),t.updateValue(a,u),a++;continue}if(r??=new cl,o??=kg(t,a,s,e),zu(t,r,a,U))t.updateValue(a,u),a++,s++;else if(o.has(U))r.set(_,t.detach(a)),s--;else{let $t=t.create(a,n[a]);t.attach(a,$t),a++,s++}}for(;a<=c;)Ng(t,r,e,a,n[a]),a++}else if(n!=null){S(i);let c=n[Symbol.iterator]();S(null);let d=c.next();for(;!d.done&&a<=s;){let u=t.at(a),h=d.value,f=su(a,u,a,h,e);if(f!==0)f<0&&t.updateValue(a,h),a++,d=c.next();else{r??=new cl,o??=kg(t,a,s,e);let p=e(a,h);if(zu(t,r,a,p))t.updateValue(a,h),a++,s++,d=c.next();else if(!o.has(p))t.attach(a,t.create(a,h)),a++,s++,d=c.next();else{let g=e(a,u);r.set(g,t.detach(a)),s--}}}for(;!d.done;)Ng(t,r,e,t.length,d.value),d=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function zu(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Ng(t,n,e,i,r){if(zu(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function kg(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var cl=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function se(t,n,e,i,r,o,a,s){Vi("NgControlFlow");let l=j(),c=Te(),d=It(c.consts,o);return Ho(l,c,t,n,e,i,r,d,256,a,s),Rm}function Rm(t,n,e,i,r,o,a,s){return Vi("NgControlFlow"),kD(t,n,e,i,r,o,a,s),Rm}function kD(t,n,e,i,r,o,a,s){let l=j(),c=Te(),d=It(c.consts,o);Ho(l,c,t,n,e,i,r,d,512,a,s)}function le(t,n){Vi("NgControlFlow");let e=j(),i=Mi(),r=e[i]!==Ht?e[i]:-1,o=r!==-1?dl(e,ke+r):void 0,a=0;if($n(e,i,t)){let s=S(null);try{if(o!==void 0&&Xb(o,a),t!==-1){let l=ke+t,c=dl(e,l),d=Gu(e[I],l),u=Jb(c,d,e),h=Wo(e,d,n,{dehydratedView:u});qo(c,h,a,kr(d,u))}}finally{S(s)}}else if(o!==void 0){let s=Kb(o,a);s!==void 0&&(s[Ce]=n)}}var Hu=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-De}};function Om(t){return t}var Uu=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Cl(t,n,e,i,r,o,a,s,l,c,d,u,h){Vi("NgControlFlow");let f=j(),p=Te(),g=l!==void 0,_=j(),w=s?a.bind(_[Ye][Ce]):a,U=new Uu(g,w);_[ke+t]=U,Ho(f,p,t+1,n,e,i,r,It(p.consts,o),256),g&&Ho(f,p,t+2,l,c,d,u,It(p.consts,h),512)}var $u=class extends ju{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-De}at(n){return this.getLView(n)[Ce].$implicit}attach(n,e){let i=e[Si];this.needsIndexUpdate||=n!==this.length,qo(this.lContainer,e,n,kr(this.templateTNode,i)),ID(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,MD(this.lContainer,n),TD(this.lContainer,n)}create(n,e){let i=rl(this.lContainer,this.templateTNode.tView.ssrId);return Wo(this.hostLView,this.templateTNode,new Hu(this.lContainer,e,n),{dehydratedView:i})}destroy(n){_l(n[I],n)}updateValue(n,e){this.getLView(n)[Ce].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[Ce].$index=n}getLView(n){return AD(this.lContainer,n)}};function Dl(t){let n=S(null),e=Kt();try{let i=j(),r=i[I],o=i[e],a=e+1,s=dl(i,a);if(o.liveCollection===void 0){let c=Gu(r,a);o.liveCollection=new $u(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(ND(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Mi(),d=l.length===0;if($n(i,c,d)){let u=e+2,h=dl(i,u);if(d){let f=Gu(r,u),p=Jb(h,f,i),g=Wo(i,f,void 0,{dehydratedView:p});qo(h,g,0,kr(f,p))}else r.firstUpdatePass&&wC(h),Xb(h,0)}}}finally{S(n)}}function dl(t,n){return t[n]}function ID(t,n){if(t.length<=De)return;let e=De+n,i=t[e];i&&ES(i)}function MD(t,n){if(t.length<=De)return;let e=De+n,i=t[e];i&&DS(i)}function TD(t,n){return jo(t,n)}function AD(t,n){return Kb(t,n)}function Gu(t,n){return Rs(t,n)}function Re(t,n,e){let i=j(),r=Mi();if($n(i,r,n)){let o=Te(),a=To();WS(a,i,t,n,i[we],e)}return Re}function Wu(t,n,e,i,r){wm(n,t,e,r?"class":"style",i)}function R(t,n,e,i){let r=j(),o=r[I],a=t+ke,s=o.firstCreatePass?av(a,r,2,n,Lb,Bd(),e,i):o.data[a];if(gn(s)){let l=r[Pt].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(nv(c),()=>(Ig(t,n,r,s,i),R))}}return Ig(t,n,r,s,i),R}function Ig(t,n,e,i,r){if(Pb(i,e,t,n,Ev),ko(i)){let o=e[I];ym(o,e,i),lb(o,i,e)}r!=null&&xl(e,i)}function O(){let t=Te(),n=Ze(),e=Vb(n);return t.firstCreatePass&&sv(t,e),zd(e)&&Hd(),Vd(),e.classesWithoutHost!=null&&p0(e)&&Wu(t,e,j(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&g0(e)&&Wu(t,e,j(),e.stylesWithoutHost,!1),O}function Ge(t,n,e,i){return R(t,n,e,i),O(),Ge}function tn(t,n,e,i){let r=j(),o=r[I],a=t+ke,s=o.firstCreatePass?VC(a,o,2,n,e,i):o.data[a];return Pb(s,r,t,n,Ev),i!=null&&xl(r,s),tn}function nn(){let t=Ze(),n=Vb(t);return zd(n)&&Hd(),Vd(),nn}function tt(t,n,e,i){return tn(t,n,e,i),nn(),tt}var Ev=(t,n,e,i,r)=>(Hs(!0),gb(n[we],i,Kd()));function Gn(){return j()}function nt(t,n,e){let i=j(),r=Mi();if($n(i,r,n)){let o=Te(),a=To();Ob(a,i,t,n,i[we],e)}return nt}var Yo="en-US";var RD=Yo;function Nv(t){typeof t=="string"&&(RD=t.toLowerCase().replace(/_/g,"-"))}function ce(t,n,e){let i=j(),r=Te(),o=Ze();return OD(r,i,i[we],o,t,n,e),ce}function OD(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=ru(i,n,o),EC(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let u=0;u<d.length;u+=2){let h=d[u],f=d[u+1];l??=ru(i,n,o),bg(i,n,h,f,r,l)}if(c&&c.length)for(let u of c)l??=ru(i,n,o),bg(i,n,u,r,r,l)}}function _e(t=1){return Lp(t)}function FD(t,n){let e=null,i=fS(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?xb(t,o,!0):gS(i,o))return r}return e}function it(t){let n=j()[Ye][Ue];if(!n.projection){let e=t?t.length:1,i=n.projection=lp(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?FD(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function ie(t,n=0,e,i,r,o){let a=j(),s=Te(),l=i?t+1:null;l!==null&&Ho(a,s,l,i,r,o,null,e);let c=Ar(s,ke+t,16,null,e||null);c.projection===null&&(c.projection=n),Gd();let u=!a[Si]||jd();a[Ye][Ue].projection[c.projection]===null&&l!==null?LD(a,s,l):u&&!fl(c)&&LS(s,a,c)}function LD(t,n,e){let i=ke+e,r=n.data[i],o=t[i],a=rl(o,r.tView.ssrId),s=Wo(t,r,void 0,{dehydratedView:a});qo(o,s,0,kr(r,a))}function zi(t,n,e,i){return hv(t,n,e,i),zi}function lt(t,n,e){return fv(t,n,e),lt}function ee(t){let n=j(),e=Te(),i=Vs();Mo(i+1);let r=Dm(e,i);if(t.dirty&&_p(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=gv(n,i);t.reset(o,Kg),t.notifyOnChanges()}return!0}return!1}function te(){return Cm(j(),Vs())}function El(t,n,e,i,r){return vv(n,hv(t,e,i,r)),El}function Hi(t,n,e,i){return vv(t,fv(n,e,i)),Hi}function Ui(t=1){Mo(Vs()+t)}function $i(t){let n=Np();return vp(n,ke+t)}function Ys(t,n){return t<<17|n<<2}function Pi(t){return t>>17&32767}function PD(t){return(t&2)==2}function VD(t,n){return t&131071|n<<17}function qu(t){return t|2}function Mr(t){return(t&131068)>>2}function lu(t,n){return t&-131069|n<<2}function BD(t){return(t&1)===1}function Yu(t){return t|1}function jD(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=Pi(a),l=Mr(a);t[i]=e;let c=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||fr(u,d)>0)&&(c=!0)}else d=e;if(r)if(l!==0){let h=Pi(t[s+1]);t[i+1]=Ys(h,s),h!==0&&(t[h+1]=lu(t[h+1],i)),t[s+1]=VD(t[s+1],i)}else t[i+1]=Ys(s,0),s!==0&&(t[s+1]=lu(t[s+1],i)),s=i;else t[i+1]=Ys(l,0),s===0?s=i:t[l+1]=lu(t[l+1],i),l=i;c&&(t[i+1]=qu(t[i+1])),Mg(t,d,i,!0),Mg(t,d,i,!1),zD(n,d,t,i,o),a=Ys(s,l),o?n.classBindings=a:n.styleBindings=a}function zD(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&fr(o,n)>=0&&(e[i+1]=Yu(e[i+1]))}function Mg(t,n,e,i){let r=t[e+1],o=n===null,a=i?Pi(r):Mr(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];HD(l,n)&&(s=!0,t[a+1]=i?Yu(c):qu(c)),a=i?Pi(c):Mr(c)}s&&(t[e+1]=i?qu(r):Yu(r))}function HD(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?fr(t,n)>=0:!1}var jt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function UD(t){return t.substring(jt.key,jt.keyEnd)}function $D(t){return GD(t),kv(t,Iv(t,0,jt.textEnd))}function kv(t,n){let e=jt.textEnd;return e===n?-1:(n=jt.keyEnd=WD(t,jt.key=n,e),Iv(t,n,e))}function GD(t){jt.key=0,jt.keyEnd=0,jt.value=0,jt.valueEnd=0,jt.textEnd=t.length}function Iv(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function WD(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Rr(t,n,e){return Mv(t,n,e,!1),Rr}function q(t,n){return Mv(t,n,null,!0),q}function Zo(t){YD(eE,qD,t,!0)}function qD(t,n){for(let e=$D(n);e>=0;e=kv(n,e))ks(t,UD(n),!0)}function Mv(t,n,e,i){let r=j(),o=Te(),a=qd(2);if(o.firstUpdatePass&&Av(o,t,a,i),n!==Ht&&$n(r,a,n)){let s=o.data[Kt()];Rv(o,s,r,r[we],t,r[a+1]=nE(n,e),i,a)}}function YD(t,n,e,i){let r=Te(),o=qd(2);r.firstUpdatePass&&Av(r,null,o,i);let a=j();if(e!==Ht&&$n(a,o,e)){let s=r.data[Kt()];if(Ov(s,i)&&!Tv(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=xs(l,e||"")),Wu(r,s,a,e,i)}else tE(r,s,a,a[we],a[o+1],a[o+1]=JD(t,n,e),i,o)}}function Tv(t,n){return n>=t.expandoStartIndex}function Av(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Kt()],a=Tv(t,e);Ov(o,i)&&n===null&&!a&&(n=!1),n=ZD(r,o,n,i),jD(r,o,n,e,a,i)}}function ZD(t,n,e,i){let r=Ap(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=cu(null,t,n,e,i),e=Uo(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=cu(r,t,n,e,i),o===null){let l=KD(t,n,i);l!==void 0&&Array.isArray(l)&&(l=cu(null,t,n,l[1],i),l=Uo(l,n.attrs,i),XD(t,n,i,l))}else o=QD(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function KD(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Mr(i)!==0)return t[Pi(i)]}function XD(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Pi(r)]=i}function QD(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Uo(i,a,e)}return Uo(i,n.attrs,e)}function cu(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Uo(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Uo(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),ks(t,a,e?!0:n[++o]))}return t===void 0?null:t}function JD(t,n,e){if(e==null||e==="")return qe;let i=[],r=gt(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function eE(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&ks(t,i,e)}function tE(t,n,e,i,r,o,a,s){r===Ht&&(r=qe);let l=0,c=0,d=0<r.length?r[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let h=l<r.length?r[l+1]:void 0,f=c<o.length?o[c+1]:void 0,p=null,g;d===u?(l+=2,c+=2,h!==f&&(p=u,g=f)):u===null||d!==null&&d<u?(l+=2,p=d):(c+=2,p=u,g=f),p!==null&&Rv(t,n,e,i,p,g,a,s),d=l<r.length?r[l]:null,u=c<o.length?o[c]:null}}function Rv(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],d=BD(c)?Tg(l,n,e,r,Mr(c),a):void 0;if(!ul(d)){ul(o)||PD(c)&&(o=Tg(l,null,e,r,s,a));let u=Rd(Kt(),e);VS(i,a,u,r,o)}}function Tg(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),d=c?l[1]:l,u=d===null,h=e[r+1];h===Ht&&(h=u?qe:void 0);let f=u?Is(h,i):d===i?h:void 0;if(c&&!ul(f)&&(f=Is(l,i)),ul(f)&&(s=f,a))return s;let p=t[r+1];r=a?Pi(p):Mr(p)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=Is(l,i))}return s}function ul(t){return t!==void 0}function nE(t,n){return t==null||t===""||(typeof n=="string"?t=gt(t)+n:typeof t=="object"&&(t=ws(gt(t)))),t}function Ov(t,n){return(t.flags&(n?8:16))!==0}function Xe(t,n=""){let e=j(),i=Te(),r=t+ke,o=i.firstCreatePass?Ar(i,r,1,n,null):i.data[r],a=iE(i,e,o,n);e[r]=a,zs()&&pm(i,e,a,o),yr(o,!1)}var iE=(t,n,e,i)=>(Hs(!0),tS(n[we],i));function rE(t,n,e,i=""){return $n(t,Mi(),e)?n+Ds(e)+i:Ht}function _t(t){return Or("",t),_t}function Or(t,n,e){let i=j(),r=rE(i,t,n,e);return r!==Ht&&oE(i,Kt(),r),Or}function oE(t,n,e){let i=Rd(n,t);nS(t[we],i,e)}function Ag(t,n,e){let i=Te();i.firstCreatePass&&Fv(n,i.data,i.blueprint,Vt(t),e)}function Fv(t,n,e,i,r){if(t=Le(t),Array.isArray(t))for(let o=0;o<t.length;o++)Fv(t[o],n,e,i,r);else{let o=Te(),a=j(),s=Ze(),l=_i(t)?t:Le(t.provide),c=kd(t),d=s.providerIndexes&1048575,u=s.directiveStart,h=s.providerIndexes>>20;if(_i(t)||!t.multi){let f=new Li(c,r,ze,null),p=uu(l,n,r?d:d+h,u);p===-1?(hu(nl(s,a),o,l),du(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(f),a.push(f)):(e[p]=f,a[p]=f)}else{let f=uu(l,n,d+h,u),p=uu(l,n,d,d+h),g=f>=0&&e[f],_=p>=0&&e[p];if(r&&!_||!r&&!g){hu(nl(s,a),o,l);let w=lE(r?sE:aE,e.length,r,i,c,t);!r&&_&&(e[p].providerFactory=w),du(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(w),a.push(w)}else{let w=Lv(e[r?p:f],c,!r&&i);du(o,t,f>-1?f:p,w)}!r&&i&&_&&e[p].componentProviders++}}}function du(t,n,e,i){let r=_i(n),o=fp(n);if(r||o){let l=(o?Le(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=c.indexOf(e);d===-1?c.push(e,[i,l]):c[d+1].push(i,l)}else c.push(e,l)}}}function Lv(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function uu(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function aE(t,n,e,i,r){return Zu(this.multi,[])}function sE(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=Po(i,i[I],this.providerFactory.index,r);a=l.slice(0,s),Zu(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],Zu(o,a);return a}function Zu(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function lE(t,n,e,i,r,o){let a=new Li(t,e,ze,null);return a.multi=[],a.index=n,a.componentProviders=0,Lv(a,r,i&&!e),a}function rt(t,n){return e=>{e.providersResolver=(i,r)=>Ag(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Ag(i,r?r(n):n,!0))}}function Nl(t,n){return Sl(t,n)}var Pv=(()=>{class t{applicationErrorHandler=m(Ti);appRef=m(vt);taskService=m(wr);ngZone=m(C);zonelessEnabled=m(Oo);tracing=m(Jt,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new be;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(_o):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(m(eu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?zp:Xd;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(_o+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Vv(){return[{provide:fn,useExisting:Pv},{provide:C,useClass:yo},{provide:Oo,useValue:!0}]}function cE(){return typeof $localize<"u"&&$localize.locale||Yo}var kl=new b("",{factory:()=>m(kl,{optional:!0,skipSelf:!0})||cE()});function yt(t,n){return ao(t,n?.equal)}function Oe(t){return wh(t)}var Bv=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")};function $v(t,n){let e=Object.create(Og);e.value=t,e.transformFn=n?.transform;function i(){if(Tn(e),e.value===Ku){let r=null;throw new k(-950,r)}return e.value}return i[Fe]=e,i}var Ml=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Qu(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function jv(t,n){return $v(t,n)}function kE(t){return $v(Ku,t)}var Bm=(jv.required=kE,jv);function zv(t,n){return Em(n)}function IE(t,n){return Nm(n)}var Wn=(zv.required=IE,zv);function Hv(t,n){return Em(n)}function ME(t,n){return Nm(n)}var Gv=(Hv.required=ME,Hv);var TE=1e4;var Mz=TE-1e3;var ct=(()=>{class t{static __NG_ELEMENT_ID__=AE}return t})();function AE(t){return RE(Ze(),j(),(t&16)===16)}function RE(t,n,e){if(gn(t)&&!e){let i=kt(t.index,n);return new Hn(i,i)}else if(t.type&175){let i=n[Ye];return new Hn(i,n)}return null}var Lm=new b(""),OE=new b("");function Ko(t){return!t.moduleRef}function FE(t){let n=Ko(t)?t.r3Injector:t.moduleRef.injector,e=n.get(C);return e.run(()=>{Ko(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Ti),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ko(t)){let o=()=>n.destroy(),a=t.platformInjector.get(Lm);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(Lm);a.add(o),t.moduleRef.onDestroy(()=>{Lo(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return PE(i,e,()=>{let o=n.get(wr),a=o.add(),s=n.get(Im);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(kl,Yo);if(Nv(l||Yo),!n.get(OE,!0))return Ko(t)?n.get(vt):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ko(t)){let d=n.get(vt);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return LE?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var LE;function PE(t,n,e){try{let i=e();return ji(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Il=null;function VE(t=[],n){return T.create({name:n,providers:[{provide:Do,useValue:"platform"},{provide:Lm,useValue:new Set([()=>Il=null])},...t]})}function BE(t=[]){if(Il)return Il;let n=VE(t);return Il=n,Dv(),jE(n),n}function jE(t){let n=t.get(Us,null);Ms(t,()=>{n?.forEach(e=>e())})}function Wv(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;oe(Q.BootstrapApplicationStart);try{let o=r?.injector??BE(i),a=[Vv(),$p,...e||[]],s=new zo({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return FE({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{oe(Q.BootstrapApplicationEnd)}}function ye(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Tl(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Fm=Symbol("NOT_SET"),qv=new Set,zE=W(y({},nr),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Fm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Fm&&!er(this))return this.signal;try{for(let r of this.cleanup??qv)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=An(this),i;try{i=this.userFn.apply(null,n)}finally{si(this,e)}return(this.value===Fm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Pm=class extends Vo{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null,s){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(ht),a),this.scheduler=r;for(let l of mm){let c=e[l];if(c===void 0)continue;let d=Object.create(zE);d.sequence=this,d.phase=l,d.userFn=c,d.dirty=!0,d.signal=()=>(Tn(d),d.value),d.signal[Fe]=d,d.registerCleanupFn=u=>(d.cleanup??=new Set).add(u),this.nodes[l]=d,this.hooks[l]=u=>d.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??qv)e()}finally{Rn(n)}}};function jm(t,n){let e=n?.injector??m(T),i=e.get(fn),r=e.get(vl),o=e.get(Jt,null,{optional:!0});r.impl??=e.get(fm);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(xr,null,{optional:!0}),l=new Pm(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null),void 0);return r.impl.register(l),l}function Fr(t,n){let e=Ln(t),i=n.elementInjector||pr(),o=new Ir(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings);return n.onError&&(o.hostView._lView[vr]=n.onError),o}var Yv=null;function Mt(){return Yv}function zm(t){Yv??=t}var Xo=class{},Al=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=ne({token:t,factory:()=>m(Zv),providedIn:"platform"})}return t})();var Zv=(()=>{class t extends Al{_location;_history;_doc=m(V);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Mt().getBaseHref(this._doc)}onPopState(e){let i=Mt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Mt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=ne({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function Qv(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function Kv(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function qn(t){return t&&t[0]!=="?"?`?${t}`:t}var Rl=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=ne({token:t,factory:()=>m(UE),providedIn:"root"})}return t})(),HE=new b(""),UE=(()=>{class t extends Rl{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??m(V).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return Qv(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+qn(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+qn(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+qn(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(G(Al),G(HE,8))};static \u0275prov=ne({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ol=(()=>{class t{_subject=new x;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=WE(Kv(Xv(i))),this._locationStrategy.onPopState(r=>{let o={url:this.path(!0),pop:!0,state:r.state,type:r.type};r.hasUAVisualTransition&&(o.hasUAVisualTransition=!0),this._subject.next(o)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+qn(i))}normalize(e){return t.stripTrailingSlash(GE(this._basePath,Xv(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+qn(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+qn(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=qn;static joinWithSlash=Qv;static stripTrailingSlash=Kv;static \u0275fac=function(i){return new(i||t)(G(Rl))};static \u0275prov=ne({token:t,factory:()=>$E(),providedIn:"root"})}return t})();function $E(){return new Ol(G(Rl))}function GE(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function Xv(t){return t.replace(/\/index\.html$/,"")}function WE(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Hm=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=m(T);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ze(en))};static \u0275dir=D({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ke]})}return t})();function Um(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()!==n)continue;let a=o;try{a=decodeURIComponent(o)}catch{}return a.length>1&&a[0]==='"'&&a[a.length-1]==='"'&&(a=a.slice(1,-1)),a}return null}var $m="browser";function Jv(t){return t===$m}var Qo=class{_doc;constructor(n){this._doc=n}manager},Fl=(()=>{class t extends Qo{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(G(V))};static \u0275prov=ne({token:t,factory:t.\u0275fac})}return t})(),Vl=new b(""),Ym=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof Fl));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof Fl);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new k(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(G(Vl),G(C))};static \u0275prov=ne({token:t,factory:t.\u0275fac})}return t})(),Gm="ng-app-id";function e_(t){for(let n of t)n.remove()}function t_(t,n){let e=n.createElement("style");return e.textContent=t,e}function KE(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Gm}="${n}"],link[${Gm}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Gm),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function qm(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Zm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,KE(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,t_);i?.forEach(r=>this.addUsage(r,this.external,qm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(e_(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])e_(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,t_(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,qm(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(G(V),G(Ai),G(Oi,8),G(Ri))};static \u0275prov=ne({token:t,factory:t.\u0275fac})}return t})(),Wm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Km=/%COMP%/g;var i_="%COMP%",XE=`_nghost-${i_}`,QE=`_ngcontent-${i_}`,JE=!0,eN=new b("",{factory:()=>JE}),tN=new b("");function nN(t){return QE.replace(Km,t)}function iN(t){return XE.replace(Km,t)}function r_(t,n){return n.map(e=>e.replace(Km,t))}var Xm=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,a,s,l=null,c=null,d=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.cssVarNamespace=d??"",this.defaultRenderer=new Jo(e,a,s,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Pl?r.applyToHost(e):r instanceof ea&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case zt.Emulated:o=new Pl(l,c,i,this.appId,d,a,s,u,this.cssVarNamespace);break;case zt.ShadowDom:return new Ll(l,e,i,a,s,this.nonce,u,this.cssVarNamespace,c);case zt.ExperimentalIsolatedShadowDom:return new Ll(l,e,i,a,s,this.nonce,u,this.cssVarNamespace);default:o=new ea(l,c,i,d,a,s,u,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(G(Ym),G(Bi),G(Ai),G(eN),G(V),G(C),G(Oi),G(Jt,8),G(tN,8))};static \u0275prov=ne({token:t,factory:t.\u0275fac})}return t})(),Jo=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Wm[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(n_(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){if(n){let r=n_(n)?n.content:n;if(i!=null&&i.parentNode!==r)throw new k(-5106,!1);r.insertBefore(e,i)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new k(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Wm[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Wm[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(Qt.DashCase|Qt.Important)?n.style.setProperty(e,i,r&Qt.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&Qt.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Mt().getGlobalEventTarget(this.doc,n),!n))throw new k(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function n_(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Ll=class extends Jo{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l,c){super(n,r,o,s,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=i.styles;d=r_(i.id,d).map(h=>h.replace(/%NS%/g,l));for(let h of d){let f=document.createElement("style");a&&f.setAttribute("nonce",a),f.textContent=h,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let h of u){let f=qm(h,r);a&&f.setAttribute("nonce",a),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ea=class extends Jo{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l,c){super(n,o,a,s,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let d=i.styles,u=c?r_(c,d):d;this.styles=u.map(h=>h.replace(/%NS%/g,l)),this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&um.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Pl=class extends ea{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l,c){let d=r+"-"+i.id;super(n,e,i,o,a,s,l,c,d),this.contentAttr=nN(d),this.hostAttr=iN(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Bl=class t extends Xo{supportsDOMEvents=!0;static makeCurrent(){zm(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=rN();return e==null?null:oN(e)}resetBaseElement(){ta=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Um(document.cookie,n)}},ta=null;function rN(){return ta=ta||document.head.querySelector("base"),ta?ta.getAttribute("href"):null}function oN(t){return new URL(t,document.baseURI).pathname}var o_=["alt","control","meta","shift"],aN={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},sN={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},a_=(()=>{class t extends Qo{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Mt().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),o_.forEach(c=>{let d=i.indexOf(c);d>-1&&(i.splice(d,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=aN[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),o_.forEach(a=>{if(a!==r){let s=sN[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(G(V))};static \u0275prov=ne({token:t,factory:t.\u0275fac})}return t})();async function Qm(t,n,e){let i=y({rootComponent:t},lN(n,e));return Wv(i)}function lN(t,n){return{platformRef:n?.platformRef,appProviders:[...fN,...t?.providers??[]],platformProviders:mN}}function cN(){Bl.makeCurrent()}function dN(){return new Yt}function uN(){return em(document),document}var mN=[{provide:Ri,useValue:$m},{provide:Us,useValue:cN,multi:!0},{provide:V,useFactory:uN}];var fN=[{provide:Do,useValue:"root"},{provide:Yt,useFactory:dN},{provide:Vl,useClass:Fl,multi:!0},{provide:Vl,useClass:a_,multi:!0},Xm,{provide:Bi,useClass:Zm},{provide:Zm,useExisting:Bi},Ym,{provide:Ae,useExisting:Xm},[]];var Lr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=ne({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=G(hN),r},providedIn:"root"})}return t})(),hN=(()=>{class t extends Lr{_doc=m(V);sanitize(e,i){if(i==null)return null;switch(e){case ve.NONE:return i;case ve.HTML:return Sn(i,"HTML")?gt(i):gl(this._doc,String(i)).toString();case ve.STYLE:return Sn(i,"Style")?gt(i):i;case ve.SCRIPT:if(Sn(i,"Script"))return gt(i);throw new k(5200,!1);case ve.URL:return Sn(i,"URL")?gt(i):pl(String(i));case ve.RESOURCE_URL:if(Sn(i,"ResourceURL"))return gt(i);throw new k(-5201,!1);default:throw new k(5202,!1)}}bypassSecurityTrustHtml(e){return nm(e)}bypassSecurityTrustStyle(e){return im(e)}bypassSecurityTrustScript(e){return rm(e)}bypassSecurityTrustUrl(e){return om(e)}bypassSecurityTrustResourceUrl(e){return am(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var pN=`
registerProcessor('hatter-recorder', class extends AudioWorkletProcessor {
  process([input]) {
    if (input.length > 0) this.port.postMessage(input[0].slice());
    return true;
  }
});`;var gN={1:[[659,.4],[880,.4],[0,2]],2:[[425,1],[0,4]]},bN=.2,vN=t=>Math.max(-32768,Math.min(32767,Math.round(t*32768))),jl=class{context;end=0;microphone;call=0;toneContext;start(n,e,i){this.stop();let r=++this.call,o=new AudioContext({sampleRate:n,latencyHint:"interactive"});this.context=o,this.end=0,o.resume(),this.record(r,o,e,i).catch(a=>console.warn("Hatter: no microphone:",a))}stop(){this.call++,this.microphone?.getTracks().forEach(n=>n.stop()),this.microphone=void 0,this.context?.close(),this.context=void 0}play(n,e,i){let r=this.context;if(!r||i.length<e)return 0;let o=Math.floor(i.length/e),a=r.createBuffer(e,o,n);for(let l=0;l<e;l++){let c=a.getChannelData(l);for(let d=0;d<o;d++)c[d]=i[d*e+l]/32768}let s=r.createBufferSource();return s.buffer=a,s.connect(r.destination),this.end=Math.max(this.end,r.currentTime),s.start(this.end),this.end+=a.duration,Math.round((this.end-r.currentTime)*1e3)}latency(){let n=this.context;return n?Math.round(((n.outputLatency||0)+n.baseLatency)*1e3):0}tone(n){this.toneContext?.close(),this.toneContext=void 0;let e=gN[n];if(!e)return;let i=new AudioContext;this.toneContext=i;let r=i.sampleRate,o=i.createBuffer(1,Math.round(e.reduce((c,[,d])=>c+d,0)*r),r),a=o.getChannelData(0),s=0;for(let[c,d]of e){let u=Math.round(d*r),h=Math.min(u/2,r/100);for(let f=0;c>0&&f<u&&s+f<a.length;f++)a[s+f]=bN*Math.sin(2*Math.PI*c*f/r)*Math.min(1,f/h,(u-f)/h);s+=u}let l=i.createBufferSource();l.buffer=o,l.loop=!0,l.connect(i.destination),l.start()}async record(n,e,i,r){let o=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:!0,noiseSuppression:!0,autoGainControl:!0}});if(n!==this.call)return o.getTracks().forEach(d=>d.stop());this.microphone=o;let a=URL.createObjectURL(new Blob([pN],{type:"text/javascript"}));try{await e.audioWorklet.addModule(a)}finally{URL.revokeObjectURL(a)}if(n!==this.call)return;let s=new AudioWorkletNode(e,"hatter-recorder",{numberOfOutputs:0}),l=new Int16Array(i),c=0;s.port.onmessage=({data:d})=>{for(let u of d)l[c++]=vN(u),c===i&&(r(l.slice()),c=0)},e.createMediaStreamSource(o).connect(s)}};var s_={width:640,height:480};function _N(t,n,e){let i=n>>1,r=e>>1,o=new Uint8Array(n*e+2*i*r);for(let a=0;a<n*e;a++)o[a]=(66*t[4*a]+129*t[4*a+1]+25*t[4*a+2]+128>>8)+16;for(let a=0;a<r;a++)for(let s=0;s<i;s++){let l=0,c=0,d=0;for(let u of[0,1,n,n+1]){let h=4*(2*a*n+2*s+u);l+=t[h],c+=t[h+1],d+=t[h+2]}l/=4,c/=4,d/=4,o[n*e+a*i+s]=(-38*l-74*c+112*d+128>>8)+128,o[n*e+i*r+a*i+s]=(112*l-94*c-18*d+128>>8)+128}return o}async function yN(t,n,e){let i;try{i=new VideoFrame(t);let{fullRange:r,matrix:o}=i.colorSpace,{rotation:a,flip:s}=i,{x:l,y:c,width:d,height:u}=i.visibleRect;if(r!==!1||o!=="smpte170m"&&o!=="bt470bg"||a||s||(d&-2)!==n||(u&-2)!==e)return;let h=n>>1,f=e>>1,p={x:l,y:c,width:n,height:e},g=new Uint8Array(n*e+2*h*f);switch(i.format){case"I420":return await i.copyTo(g,{rect:p,layout:[{offset:0,stride:n},{offset:n*e,stride:h},{offset:n*e+h*f,stride:h}]}),g;case"NV12":{let _=new Uint8Array(g.length);await i.copyTo(_,{rect:p,layout:[{offset:0,stride:n},{offset:n*e,stride:2*h}]}),g.set(_.subarray(0,n*e));for(let w=0;w<h*f;w++)g[n*e+w]=_[n*e+2*w],g[n*e+h*f+w]=_[n*e+2*w+1];return g}}return}catch{return}finally{i?.close()}}var zl=class{watching=new Map;camera;filming=0;watch(n,e){e.showStream(n);let i=this.watching.get(n);i||this.watching.set(n,i=new Set),i.add(e)}show(n,e,i,r){let o=this.watching.get(n);if(!o)return;let a=new VideoFrame(r,{format:"I420",codedWidth:e,codedHeight:i,timestamp:0});try{for(let s of o)s.stream!==n?o.delete(s):s.draw(a)}finally{a.close()}}startCamera(n,e){this.stopCamera();let i=this.filming;this.film(i,n,e).catch(r=>console.warn("Hatter: no camera:",r))}stopCamera(){this.filming++,this.camera?.getTracks().forEach(n=>n.stop()),this.camera=void 0}async film(n,e,i){let r=await navigator.mediaDevices.getUserMedia({video:{facingMode:e?"user":"environment",width:{ideal:s_.width},height:{ideal:s_.height}}});if(n!==this.filming)return r.getTracks().forEach(d=>d.stop());this.camera=r;let o=document.createElement("video");o.muted=!0,o.playsInline=!0,o.srcObject=r,await o.play();let a=new OffscreenCanvas(1,1),s=a.getContext("2d",{willReadFrequently:!0}),l=(d,u)=>((a.width!==d||a.height!==u)&&(a.width=d,a.height=u),s.drawImage(o,0,0,d,u),_N(s.getImageData(0,0,d,u).data,d,u)),c=async()=>{if(n!==this.filming)return;let d=o.videoWidth&-2,u=o.videoHeight&-2;if(d>0&&u>0){let h=await yN(o,d,u);if(n!==this.filming)return;i(d,u,h??l(d,u))}o.requestVideoFrameCallback(c)};o.requestVideoFrameCallback(c)}};var wN={avif:"image/avif",gif:"image/gif",jpeg:"image/jpeg",jpg:"image/jpeg",m4a:"audio/mp4",mp3:"audio/mpeg",mp4:"video/mp4",oga:"audio/ogg",ogg:"audio/ogg",opus:"audio/ogg",pdf:"application/pdf",png:"image/png",txt:"text/plain; charset=utf-8",wav:"audio/wav",webm:"video/webm",webp:"image/webp"},Hl="/cache",xN=0;function l_(t,n,e=!1){let i=document.createElement("input");return i.type="file",i.accept=t==="*/*"?"":t,i.multiple=n,e&&(i.capture="environment"),new Promise(r=>{i.addEventListener("change",()=>r([...i.files??[]])),i.addEventListener("cancel",()=>r([])),i.click()})}async function c_(t,n,e,i=!1){let r=[];for(let o of await l_(n,e,i)){let a=`${Hl}/picked/${xN++}/${o.name.replace(/[^A-Za-z0-9._-]/g,"_")||"file"}`;t.add(a,new Uint8Array(await o.arrayBuffer())),r.push({path:a,name:o.name,size:o.size})}return r}async function d_(t,n){let[e]=await l_("image/*",!1);if(!e)return;let i=await createImageBitmap(e).catch(()=>{});if(!i)return;let r=Math.min(i.width,i.height),o=Math.floor((i.width-r)/2),a=Math.floor((i.height-r)/2);try{for(let s of t){let l=new OffscreenCanvas(s,s),c=l.getContext("2d");c.imageSmoothingQuality="high",c.drawImage(i,o,a,r,r,0,0,s,s);let d=await l.convertToBlob({type:"image/png"});if(d.size<=n)return new Uint8Array(await d.arrayBuffer())}return}finally{i.close()}}function u_(t,n,e,i){let r=t.file(n);r&&(f_(new Blob([r.data],{type:i}),e),t.remove(n))}function m_(t,n){let e=t.file(n);if(!e)return;let i=n.slice(n.lastIndexOf("/")+1),r=wN[i.slice(i.lastIndexOf(".")+1).toLowerCase()],o=new Blob([e.data],{type:r??"application/octet-stream"});if(!r)return f_(o,i);let a=URL.createObjectURL(o);window.open(a,"_blank","noopener"),setTimeout(()=>URL.revokeObjectURL(a),6e4)}function f_(t,n){let e=URL.createObjectURL(t),i=document.createElement("a");i.href=e,i.download=n,i.click(),setTimeout(()=>URL.revokeObjectURL(e),6e4)}function h_(t){let n=new DataView(t.buffer,t.byteOffset,t.byteLength),e=(i,r)=>String.fromCharCode(...t.subarray(i,i+r));try{if(e(1,3)==="PNG")return[n.getUint32(16),n.getUint32(20)];if(e(0,4)==="GIF8")return[n.getUint16(6,!0),n.getUint16(8,!0)];if(e(0,4)==="RIFF"&&e(8,4)==="WEBP")return SN(t,n,e(12,4));if(n.getUint16(0)===65496)return CN(t,n)}catch{}}function SN(t,n,e){switch(e){case"VP8 ":return[n.getUint16(26,!0)&16383,n.getUint16(28,!0)&16383];case"VP8L":{let i=n.getUint32(21,!0);return[(i&16383)+1,(i>>>14&16383)+1]}case"VP8X":return[(n.getUint32(24,!0)&16777215)+1,(n.getUint32(26,!0)>>>8)+1]}}function CN(t,n){let e=!1;for(let i=2;i+4<=t.length;){let r=n.getUint16(i),o=n.getUint16(i+2);if(r===65505&&(e||=DN(t.subarray(i+4,i+2+o))),r>=65472&&r<=65487&&![65476,65480,65484].includes(r)){let a=[n.getUint16(i+7),n.getUint16(i+5)];return e?[a[1],a[0]]:a}i+=2+o}}function DN(t){if(String.fromCharCode(...t.subarray(0,4))!=="Exif")return!1;let n=new DataView(t.buffer,t.byteOffset+6,t.byteLength-6),e=n.getUint16(0)===18761,i=n.getUint32(4,e),r=n.getUint16(i,e);for(let o=0;o<r;o++){let a=i+2+o*12;if(n.getUint16(a,e)===274)return n.getUint16(a+8,e)>=5}return!1}var Jm;try{Jm=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Jm=!1}var re=(()=>{class t{_platformId=m(Ri);isBrowser=this._platformId?Jv(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Jm)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Ul=new WeakMap,We=(()=>{class t{_appRef;_injector=m(T);_environmentInjector=m(Pe);load(e){let i=this._appRef=this._appRef||this._injector.get(vt),r=Ul.get(i);r||(r={loaders:new Set,refs:[]},Ul.set(i,r),i.onDestroy(()=>{Ul.get(i)?.refs.forEach(o=>o.destroy()),Ul.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Fr(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Tt(t,n=0){return p_(t)?Number(t):arguments.length===2?n:0}function p_(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function ot(t){return t instanceof M?t.nativeElement:t}var g_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),EN={passive:!0},b_=(()=>{class t{_platform=m(re);_ngZone=m(C);_renderer=m(Ae).createRenderer(null,null);_styleLoader=m(We);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return fi;this._styleLoader.load(g_);let i=ot(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new x,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,EN)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=ot(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var v_=(()=>{class t{_elementRef=m(M);_platform=m(re);_ngZone=m(C);_renderer=m(Se);_resizeEvents=new x;_previousValue;_initialHeight;_destroyed=new x;_listenerCleanups;_minRows;_maxRows;_enabled=!0;_previousMinRows=-1;_textareaElement;get minRows(){return this._minRows}set minRows(e){this._minRows=Tt(e),this._setMinHeight()}get maxRows(){return this._maxRows}set maxRows(e){this._maxRows=Tt(e),this._setMaxHeight()}get enabled(){return this._enabled}set enabled(e){this._enabled!==e&&((this._enabled=e)?this.resizeToFitContent(!0):this.reset())}get placeholder(){return this._textareaElement.placeholder}set placeholder(e){this._cachedPlaceholderHeight=void 0,e?this._textareaElement.setAttribute("placeholder",e):this._textareaElement.removeAttribute("placeholder"),this._cacheTextareaPlaceholderHeight()}_cachedLineHeight;_cachedPlaceholderHeight;_document=m(V);_hasFocus=!1;_isViewInited=!1;constructor(){m(We).load(g_),this._textareaElement=this._elementRef.nativeElement}_setMinHeight(){let e=this.minRows&&this._cachedLineHeight?`${this.minRows*this._cachedLineHeight}px`:null;e&&(this._textareaElement.style.minHeight=e)}_setMaxHeight(){let e=this.maxRows&&this._cachedLineHeight?`${this.maxRows*this._cachedLineHeight}px`:null;e&&(this._textareaElement.style.maxHeight=e)}ngAfterViewInit(){this._platform.isBrowser&&(this._initialHeight=this._textareaElement.style.height,this.resizeToFitContent(),this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[this._renderer.listen("window","resize",()=>this._resizeEvents.next()),this._renderer.listen(this._textareaElement,"focus",this._handleFocusEvent),this._renderer.listen(this._textareaElement,"blur",this._handleFocusEvent)],this._resizeEvents.pipe(cr(16)).subscribe(()=>{this._cachedLineHeight=this._cachedPlaceholderHeight=void 0,this.resizeToFitContent(!0)})}),this._isViewInited=!0,this.resizeToFitContent(!0))}ngOnDestroy(){this._listenerCleanups?.forEach(e=>e()),this._resizeEvents.complete(),this._destroyed.next(),this._destroyed.complete()}_cacheTextareaLineHeight(){if(this._cachedLineHeight)return;let e=this._textareaElement.cloneNode(!1),i=e.style;e.rows=1,i.position="absolute",i.visibility="hidden",i.border="none",i.padding="0",i.height="",i.minHeight="",i.maxHeight="",i.top=i.bottom=i.left=i.right="auto",i.overflow="hidden",this._textareaElement.parentNode.appendChild(e),this._cachedLineHeight=e.clientHeight,e.remove(),this._setMinHeight(),this._setMaxHeight()}_measureScrollHeight(){let e=this._textareaElement,i=e.style.marginBottom||"",r=this._platform.FIREFOX,o=this._hasFocus,a=r?"cdk-textarea-autosize-measuring-firefox":"cdk-textarea-autosize-measuring";o&&(e.style.marginBottom=`${e.clientHeight}px`),e.classList.add(a);let s=e.scrollHeight-4;return e.classList.remove(a),o&&(e.style.marginBottom=i),s}_cacheTextareaPlaceholderHeight(){if(!this._isViewInited||this._cachedPlaceholderHeight!=null)return;if(!this.placeholder){this._cachedPlaceholderHeight=0;return}let e=this._textareaElement.value;this._textareaElement.value=this._textareaElement.placeholder,this._cachedPlaceholderHeight=this._measureScrollHeight(),this._textareaElement.value=e}_handleFocusEvent=e=>{this._hasFocus=e.type==="focus"};ngDoCheck(){this._platform.isBrowser&&this.resizeToFitContent()}resizeToFitContent(e=!1){if(!this._enabled||(this._cacheTextareaLineHeight(),this._cacheTextareaPlaceholderHeight(),!this._cachedLineHeight))return;let i=this._elementRef.nativeElement,r=i.value;if(!e&&this._minRows===this._previousMinRows&&r===this._previousValue)return;let o=this._measureScrollHeight(),a=Math.max(o,this._cachedPlaceholderHeight||0);i.style.height=`${a}px`,this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame<"u"?requestAnimationFrame(()=>this._scrollToCaretPosition(i)):setTimeout(()=>this._scrollToCaretPosition(i))}),this._previousValue=r,this._previousMinRows=this._minRows}reset(){this._initialHeight!==void 0&&(this._textareaElement.style.height=this._initialHeight)}_noopInputHandler(){}_scrollToCaretPosition(e){let{selectionStart:i,selectionEnd:r}=e;!this._destroyed.isStopped&&this._hasFocus&&e.setSelectionRange(i,r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["textarea","cdkTextareaAutosize",""]],hostAttrs:["rows","1",1,"cdk-textarea-autosize"],hostBindings:function(i,r){i&1&&ce("input",function(){return r._noopInputHandler()})},inputs:{minRows:[0,"cdkAutosizeMinRows","minRows"],maxRows:[0,"cdkAutosizeMaxRows","maxRows"],enabled:[2,"cdkTextareaAutosize","enabled",ye],placeholder:"placeholder"},exportAs:["cdkTextareaAutosize"]})}return t})(),ef=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=Ve({type:t});static \u0275inj=Me({})}return t})();function na(t){return t.buttons===0||t.detail===0}function ia(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var tf;function __(){if(tf==null){let t=typeof document<"u"?document.head:null;tf=!!(t&&(t.createShadowRoot||t.attachShadow))}return tf}function nf(t){if(__()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Pr(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function dt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var ra;function y_(){if(ra==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>ra=!0}))}finally{ra=ra||!1}return ra}function Vr(t){return y_()?t:!!t.capture}var w_=new b("cdk-input-modality-detector-options"),x_={ignoreKeys:[18,17,224,91,16]},S_=650,rf={passive:!0,capture:!0},C_=(()=>{class t{_platform=m(re);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new mi(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=dt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<S_||(this._modality.next(na(e)?"keyboard":"mouse"),this._mostRecentTarget=dt(e))};_onTouchstart=e=>{if(ia(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=dt(e)};constructor(){let e=m(C),i=m(V),r=m(w_,{optional:!0});if(this._options=y(y({},x_),r),this.modalityDetected=this._modality.pipe(ho(1)),this.modalityChanged=this.modalityDetected.pipe(ms()),this._platform.isBrowser){let o=m(Ae).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,rf),o.listen(i,"mousedown",this._onMousedown,rf),o.listen(i,"touchstart",this._onTouchstart,rf)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),oa=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(oa||{}),D_=new b("cdk-focus-monitor-default-options"),$l=Vr({passive:!0,capture:!0}),Gi=(()=>{class t{_ngZone=m(C);_platform=m(re);_inputModalityDetector=m(C_);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=m(V);_stopInputModalityDetector=new x;constructor(){let e=m(D_,{optional:!0});this._detectionMode=e?.detectionMode||oa.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=dt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=ot(e);if(!this._platform.isBrowser||r.nodeType!==1)return pi();let o=nf(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new x,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=ot(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=ot(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===oa.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===oa.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?S_:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=dt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,$l),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,$l)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ct(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,$l),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,$l),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var E_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Gl;function kN(){if(Gl===void 0&&(Gl=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Gl=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Gl}function IN(t){return kN()?.createHTML(t)||t}function N_(t,n,e){let i=e.sanitize(ve.HTML,n);t.innerHTML=IN(i||"")}function aa(t){return Array.isArray(t)?t:[t]}var k_=new Set,Wi,Wl=(()=>{class t{_platform=m(re);_nonce=m(Oi,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):TN}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&MN(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function MN(t,n){if(!k_.has(t))try{Wi||(Wi=document.createElement("style"),n&&Wi.setAttribute("nonce",n),Wi.setAttribute("type","text/css"),document.head.appendChild(Wi)),Wi.sheet&&(Wi.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),k_.add(t))}catch(e){console.error(e)}}function TN(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var sa=(()=>{class t{_mediaMatcher=m(Wl);_zone=m(C);_queries=new Map;_destroySubject=new x;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return I_(aa(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=I_(aa(e)).map(a=>this._registerQuery(a).observable),o=Xc(r);return o=lr(o.pipe(mt(1)),o.pipe(ho(1),fo(0))),o.pipe(je(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new z(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(qt(i),je(({matches:a})=>({query:e,matches:a})),Ct(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function I_(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function AN(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var RN=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),ON=(()=>{class t{_mutationObserverFactory=m(RN);_observedElements=new Map;_ngZone=m(C);ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=ot(e);return new z(r=>{let a=this._observeElement(i).pipe(je(s=>s.filter(l=>!AN(l))),Ee(s=>!!s.length)).subscribe(s=>{this._ngZone.run(()=>{r.next(s)})});return()=>{a.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new x,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),M_=(()=>{class t{_contentObserver=m(ON);_elementRef=m(M);event=new K;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=Tt(e),this._subscribe()}_debounce;_currentSubscription=null;ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(fo(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",ye],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})();var af=(()=>{class t{_platform=m(re);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return LN(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=FN($N(e));if(i&&(T_(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=T_(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!HN(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return UN(e)&&!this.isDisabled(e)&&(i?.ignoreInert||!e.closest?.("[inert]"))&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function FN(t){try{return t.frameElement}catch{return null}}function LN(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function PN(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function VN(t){return jN(t)&&t.type=="hidden"}function BN(t){return zN(t)&&t.hasAttribute("href")}function jN(t){return t.nodeName.toLowerCase()=="input"}function zN(t){return t.nodeName.toLowerCase()=="a"}function A_(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function T_(t){if(!A_(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function HN(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function UN(t){return VN(t)?!1:PN(t)||BN(t)||t.hasAttribute("contenteditable")||A_(t)}function $N(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var of=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>{!this.focusLastTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};endAnchorListener=()=>{!this.focusFirstTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){Je(n,{injector:this._injector})}},sf=(()=>{class t{_checker=m(af);_ngZone=m(C);_document=m(V);_injector=m(T);constructor(){m(We).load(E_)}create(e,i=!1){return new of(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var R_=new b("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),O_=new b("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),GN=0,lf=(()=>{class t{_ngZone=m(C);_defaultOptions=m(O_,{optional:!0});_liveElement;_document=m(V);_sanitizer=m(Lr);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=m(R_,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:N_(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${GN++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Br(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var F_=new Map,Be=class t{_appId=m(Ai);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=F_.get(n);return i===void 0?i=0:i++,F_.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var Ut=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Ut||{}),ql,qi;function Yl(){if(qi==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return qi=!1,qi;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)qi=!0;else{let t=Element.prototype.scrollTo;t?qi=!/\{\s*\[native code\]\s*\}/.test(t.toString()):qi=!1}}return qi}function jr(){if(typeof document!="object"||!document)return Ut.NORMAL;if(ql==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),ql=Ut.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,ql=t.scrollLeft===0?Ut.NEGATED:Ut.INVERTED),t.remove()}return ql}function cf(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var zr,L_=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function df(){if(zr)return zr;if(typeof document!="object"||!document)return zr=new Set(L_),zr;let t=document.createElement("input");return zr=new Set(L_.filter(n=>(t.setAttribute("type",n),t.type===n))),zr}var Dn={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var qN=new b("MATERIAL_ANIMATIONS"),P_=null;function YN(){return m(qN,{optional:!0})?.animationsDisabled||m(Ro,{optional:!0})==="NoopAnimations"?"di-disabled":(P_??=m(Wl).matchMedia("(prefers-reduced-motion)").matches,P_?"reduced-motion":"enabled")}function Ie(){return YN()!=="enabled"}function En(t){return t==null?"":typeof t=="string"?t:`${t}px`}function At(t){return t!=null&&`${t}`!="false"}var Rt=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Rt||{}),uf=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Rt.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},V_=Vr({passive:!0,capture:!0}),mf=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,V_)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,V_)))}_delegateEventHandler=n=>{let e=dt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},la={enterDuration:225,exitDuration:150},ZN=800,B_=Vr({passive:!0,capture:!0}),j_=["mousedown","touchstart"],z_=["mouseup","mouseleave","touchend","touchcancel"],KN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),Yi=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new mf;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=ot(i)),o&&o.get(We).load(KN)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=y(y({},la),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||XN(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${s-a}px`,d.style.top=`${l-a}px`,d.style.height=`${a*2}px`,d.style.width=`${a*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),h=u.transitionProperty,f=u.transitionDuration,p=h==="none"||f==="0s"||f==="0s, 0s"||r.width===0&&r.height===0,g=new uf(this,d,i,p);d.style.transform="scale3d(1, 1, 1)",g.state=Rt.FADING_IN,i.persistent||(this._mostRecentTransientRipple=g);let _=null;return!p&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let w=()=>{_&&(_.fallbackTimer=null),clearTimeout($t),this._finishRippleTransition(g)},U=()=>this._destroyRipple(g),$t=setTimeout(U,c+100);d.addEventListener("transitionend",w),d.addEventListener("transitioncancel",U),_={onTransitionEnd:w,onTransitionCancel:U,fallbackTimer:$t}}),this._activeRipples.set(g,_),(p||!c)&&this._finishRippleTransition(g),g}fadeOutRipple(n){if(n.state===Rt.FADING_OUT||n.state===Rt.HIDDEN)return;let e=n.element,i=y(y({},la),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Rt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=ot(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,j_.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{z_.forEach(e=>{this._triggerElement.addEventListener(e,this,B_)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Rt.FADING_IN?this._startFadeOutTransition(n):n.state===Rt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Rt.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Rt.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=na(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+ZN;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!ia(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Rt.VISIBLE||n.config.terminateOnPointerUp&&n.state===Rt.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(j_.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(z_.forEach(e=>n.removeEventListener(e,this,B_)),this._pointerUpEventsRegistered=!1))}};function XN(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var ca=new b("mat-ripple-global-options"),H_=(()=>{class t{_elementRef=m(M);_animationsDisabled=Ie();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=m(C),i=m(re),r=m(ca,{optional:!0}),o=m(T);this._globalOptions=r||{},this._rippleRenderer=new Yi(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:y(y(y({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,y(y({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,y(y({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&q("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var QN={capture:!0},JN=["focus","mousedown","mouseenter","touchstart"],ff="mat-ripple-loader-uninitialized",hf="mat-ripple-loader-class-name",U_="mat-ripple-loader-centered",Zl="mat-ripple-loader-disabled",$_=(()=>{class t{_document=m(V);_animationsDisabled=Ie();_globalRippleOptions=m(ca,{optional:!0});_platform=m(re);_ngZone=m(C);_injector=m(T);_eventCleanups;_hosts=new Map;constructor(){let e=m(Ae).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>JN.map(i=>e.listen(this._document,i,this._onInteraction,QN)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(ff,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(hf))&&e.setAttribute(hf,i.className||""),i.centered&&e.setAttribute(U_,""),i.disabled&&e.setAttribute(Zl,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Zl,""):e.removeAttribute(Zl)}_onInteraction=e=>{let i=dt(e);if(i instanceof HTMLElement){let r=i.closest(`[${ff}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(hf)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??la.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??la.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Zl),rippleConfig:{centered:e.hasAttribute(U_),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Yi(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(ff)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Hr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var ek=new b("MAT_BUTTON_CONFIG");function G_(t){return t==null?void 0:Tl(t)}var W_=(()=>{class t{_elementRef=m(M);_ngZone=m(C);_animationsDisabled=Ie();_config=m(ek,{optional:!0});_focusMonitor=m(Gi);_cleanupClick;_renderer=m(Se);_rippleLoader=m($_);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=Bm(!1,{transform:ye});constructor(){m(We).load(Hr);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(ae("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Zo(r.color?"mat-"+r.color:""),q("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",ye],disabled:[2,"disabled","disabled",ye],ariaDisabled:[2,"aria-disabled","ariaDisabled",ye],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ye],tabIndex:[2,"tabIndex","tabIndex",G_],_tabindex:[2,"tabindex","_tabindex",G_],showProgress:[1,"showProgress"]}})}return t})();var tk=new b("cdk-dir-doc",{providedIn:"root",factory:()=>m(V)}),nk=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function q_(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?nk.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var rn=(()=>{class t{get value(){return this.valueSignal()}valueSignal=X("ltr");change=new K;constructor(){let e=m(tk,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(q_(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Y_=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Ur=(()=>{class t extends W_{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=ik(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Y_.get(this._appearance):null,o=Y_.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""],["",8,"material-symbols-outlined",3,"iconPositionEnd",""],["",8,"material-symbols-rounded",3,"iconPositionEnd",""],["",8,"material-symbols-sharp",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""],["","iconPositionEnd","",8,"material-symbols-outlined"],["","iconPositionEnd","",8,"material-symbols-rounded"],["","iconPositionEnd","",8,"material-symbols-sharp"]],[["","progressIndicator",""]]],i=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd]), .material-symbols-outlined:not([iconPositionEnd]), .material-symbols-rounded:not([iconPositionEnd]), .material-symbols-sharp:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd], .material-symbols-outlined[iconPositionEnd], .material-symbols-rounded[iconPositionEnd], .material-symbols-sharp[iconPositionEnd]","[progressIndicator]"];function r(o,a){o&1&&(tn(0,"div",2),ie(1,3),nn())}return N({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[L],ngContentSelectors:i,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(a,s){a&1&&(it(e),tt(0,"span",0),ie(1),tn(2,"span",1),ie(3,1),nn(),ie(4,2),se(5,r,2,0,"div",2),tt(6,"span",3)(7,"span",4)),a&2&&(q("mdc-button__ripple",!s._isFab)("mdc-fab__ripple",s._isFab),F(5),le(s.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, .material-symbols-outlined, .material-symbols-rounded,
.material-symbols-sharp, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return t})();function ik(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var ty=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ze(Se),ze(M))};static \u0275dir=D({type:t})}return t})(),rk=(()=>{class t extends ty{static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,features:[L]})}return t})(),bf=new b("");var ok={provide:bf,useExisting:st(()=>ny),multi:!0};function ak(){let t=Mt()?Mt().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var sk=new b(""),ny=(()=>{class t extends ty{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!ak())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ze(Se),ze(M),ze(sk,8))};static \u0275dir=D({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&ce("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[rt([ok]),L]})}return t})();function vf(t){return t==null||_f(t)===0}function _f(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var ga=new b(""),iy=new b(""),lk=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,pa=class{static min(n){return ck(n)}static max(n){return dk(n)}static required(n){return ry(n)}static requiredTrue(n){return uk(n)}static email(n){return mk(n)}static minLength(n){return fk(n)}static maxLength(n){return hk(n)}static pattern(n){return pk(n)}static nullValidator(n){return Xl()}static compose(n){return dy(n)}static composeAsync(n){return uy(n)}};function ck(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function dk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function ry(t){return vf(t.value)?{required:!0}:null}function uk(t){return t.value===!0?null:{required:!0}}function mk(t){return vf(t.value)||lk.test(t.value)?null:{email:!0}}function fk(t){return n=>{let e=n.value?.length??_f(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function hk(t){return n=>{let e=n.value?.length??_f(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function pk(t){if(!t)return Xl;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(vf(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Xl(t){return null}function oy(t){return t!=null}function ay(t){return ji(t)?St(t):t}function sy(t){let n={};return t.forEach(e=>{n=e!=null?y(y({},n),e):n}),Object.keys(n).length===0?null:n}function ly(t,n){return n.map(e=>e(t))}function gk(t){return!t.validate}function cy(t){return t.map(n=>gk(n)?n:e=>n.validate(e))}function dy(t){if(!t)return null;let n=t.filter(oy);return n.length==0?null:function(e){return sy(ly(e,n))}}function yf(t){return t!=null?dy(cy(t)):null}function uy(t){if(!t)return null;let n=t.filter(oy);return n.length==0?null:function(e){let i=ly(e,n).map(ay);return Jc(i).pipe(je(sy))}}function wf(t){return t!=null?uy(cy(t)):null}function Z_(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function my(t){return t._rawValidators}function fy(t){return t._rawAsyncValidators}function pf(t){return t?Array.isArray(t)?t:[t]:[]}function Ql(t,n){return Array.isArray(t)?t.includes(n):t===n}function K_(t,n){let e=pf(n);return pf(t).forEach(r=>{Ql(e,r)||e.push(r)}),e}function X_(t,n){return pf(n).filter(e=>!Ql(t,e))}var Jl=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=yf(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=wf(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Wr=class extends Jl{name;get formDirective(){return null}get path(){return null}};var da="VALID",Kl="INVALID",$r="PENDING",ua="DISABLED",Yn=class{},ec=class extends Yn{value;source;constructor(n,e){super(),this.value=n,this.source=e}},fa=class extends Yn{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},ha=class extends Yn{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Gr=class extends Yn{status;source;constructor(n,e){super(),this.status=n,this.source=e}},tc=class extends Yn{source;constructor(n){super(),this.source=n}},qr=class extends Yn{source;constructor(n){super(),this.source=n}};function hy(t){return(sc(t)?t.validators:t)||null}function bk(t){return Array.isArray(t)?yf(t):t||null}function py(t,n){return(sc(n)?n.asyncValidators:t)||null}function vk(t){return Array.isArray(t)?wf(t):t||null}function sc(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function _k(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new k(1e3,"");if(!gy(i,e))throw new k(1001,"")}function yk(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new k(-1002,"")})}var nc=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=X(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Oe(this.statusReactive)}set status(n){Oe(()=>this.statusReactive.set(n))}_status=yt(()=>this.statusReactive());statusReactive=X(void 0);get valid(){return this.status===da}get invalid(){return this.status===Kl}get pending(){return this.status===$r}get disabled(){return this.status===ua}get enabled(){return this.status!==ua}errors;get pristine(){return Oe(this.pristineReactive)}set pristine(n){Oe(()=>this.pristineReactive.set(n))}_pristine=yt(()=>this.pristineReactive());pristineReactive=X(!0);get dirty(){return!this.pristine}get touched(){return Oe(this.touchedReactive)}set touched(n){Oe(()=>this.touchedReactive.set(n))}_touched=yt(()=>this.touchedReactive());touchedReactive=X(!1);get untouched(){return!this.touched}_events=new x;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(K_(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(K_(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(X_(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(X_(n,this._rawAsyncValidators))}hasValidator(n){return Ql(this._rawValidators,n)}hasAsyncValidator(n){return Ql(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(W(y({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ha(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new ha(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(W(y({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new fa(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new fa(!0,i))}markAsPending(n={}){this.status=$r;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Gr(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(W(y({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=ua,this.errors=null,this._forEachChild(r=>{r.disable(W(y({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ec(this.value,i)),this._events.next(new Gr(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(W(y({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=da,this._forEachChild(i=>{i.enable(W(y({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(W(y({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===da||this.status===$r)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ec(this.value,e)),this._events.next(new Gr(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(W(y({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ua:da}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=$r,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=ay(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Gr(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new K,this.statusChanges=new K}_calculateStatus(){return this._allControlsDisabled()?ua:this.errors?Kl:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus($r)?$r:this._anyControlsHaveStatus(Kl)?Kl:da}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new fa(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new ha(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){sc(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=bk(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=vk(this._rawAsyncValidators)}_updateHasRequiredValidator(){Oe(()=>this._hasRequired.set(this.hasValidator(pa.required)))}};function gy(t,n){return Object.hasOwn(t,n)}function wk(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function xk(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var gf=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var Sk=(()=>{class t{_validator=Xl;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Xl,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,features:[Ke]})}return t})();var Ck={provide:ga,useExisting:st(()=>by),multi:!0};var by=(()=>{class t extends Sk{required;inputName="required";normalizeInput=ye;createValidator=e=>ry;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&ae("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[rt([Ck]),L]})}return t})();var vy=new b("",{factory:()=>Dk}),Dk="always";function Q_(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),rc(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function ic(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function _y(t,n){let e=my(t);n.validator!==null?t.setValidators(Z_(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=fy(t);n.asyncValidator!==null?t.setAsyncValidators(Z_(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();ic(n._rawValidators,r),ic(n._rawAsyncValidators,r)}function rc(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=my(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=fy(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return ic(n._rawValidators,i),ic(n._rawAsyncValidators,i),e}function yy(t,n){t==null,_y(t,n)}function Ek(t,n){return rc(t,n)}function Nk(t){return Object.getPrototypeOf(t.constructor)===rk}function wy(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function kk(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===ny?e=o:Nk(o)?i=o:r=o}),r||i||e||null}function Ik(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var oc=class extends Jl{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof qr&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=kk(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;pendingCustomValue=void 0;hasPendingCustomValue=!1;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(ht)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.control,e=this.injector?.get(ct);if(!n||!e)return;let i=e.markForCheck.bind(e);if(this.subscription=new be,this.subscription.add(()=>this.discardPendingCustomValue(n)),typeof n.registerOnChange=="function"){let r=(o,a)=>{this.discardPendingCustomValue(n),a&&this.viewToModelUpdate(o),i()};n.registerOnChange(r),this.subscription.add(()=>n._unregisterOnChange(r))}this.subscription.add(n.valueChanges.subscribe(i)),this.subscription.add(n.statusChanges.subscribe(i)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=n.events.subscribe(r=>{r instanceof qr&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&n.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{let o=this.control;o&&(o._pendingValue=r,o._pendingChange=!0,o._pendingDirty=!0,this.pendingCustomValue=r,this.hasPendingCustomValue=!0,o.updateOn==="change"&&this.updateControlFromCustom(o))}),n.listenToCustomControlOutput("touch",()=>{let r=this.control;r&&(r._pendingTouched=!0,r.updateOn==="blur"&&r._pendingChange&&this.updateControlFromCustom(r),r.updateOn!=="submit"&&r.markAsTouched())}),this.customControlBindings={},this.isNativeFormElement=wk(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof by))}updateControlFromCustom(n){let e=n._pendingValue;n._pendingChange=!1,this.clearPendingCustomValue(),this.customControlBindings&&(this.customControlBindings.value=e),n._pendingDirty&&n.markAsDirty(),n.setValue(e,{emitModelToViewChange:!1}),this.viewToModelUpdate(n.value)}discardPendingCustomValue(n){this.hasPendingCustomValue&&(this.customControlBindings&&(this.customControlBindings.value=this.pendingCustomValue),this.clearPendingCustomValue(),n._pendingValue=n.value,n._pendingChange=!1,n._pendingDirty=!1)}clearPendingCustomValue(){this.pendingCustomValue=void 0,this.hasPendingCustomValue=!1}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;if(!i||!r)return;this.hasPendingCustomValue&&!i._pendingChange&&(r.value=this.pendingCustomValue,this.clearPendingCustomValue()),Object.is(r.value,i.value)||(this.discardPendingCustomValue(i),r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let a=this._convertErrors(o);n.setInputOnDirectives("errors",a)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&xk(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new gf({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=yt(()=>{let r=n();return r.length===0?null:r.reduce((o,a)=>(o[a.kind]=a,o),{})});this.parseErrorsValidator=(()=>e).bind(this),_n(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var ac=class extends nc{constructor(n,e,i){super(hy(e),py(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Oe(()=>{yk(this,!0,n),Object.keys(n).forEach(i=>{_k(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,W(y({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new qr(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return gy(this.controls,n)?this.controls[n]:null}};var Mk={provide:Wr,useExisting:st(()=>xf)},ma=Promise.resolve(),xf=(()=>{class t extends Wr{callSetDisabledState;get submitted(){return Oe(this.submittedReactive)}_submitted=yt(()=>this.submittedReactive());submittedReactive=X(!1);_directives=new Set;form;ngSubmit=new K;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new ac({},yf(e),wf(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){ma.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){ma.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){ma.then(()=>{let i=this._findContainer(e.path),r=new ac({});yy(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){ma.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){ma.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),wy(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new tc(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ze(ga,10),ze(iy,10),ze(vy,8))};static \u0275dir=D({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&ce("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[rt([Mk]),L]})}return t})();function J_(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ey(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var Tk=class extends nc{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(hy(e),py(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),sc(e)&&(e.nonNullable||e.initialValueIsDefault)&&(ey(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Oe(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new qr(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){J_(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){J_(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){ey(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var Ak=t=>t instanceof Tk;var Rk=(()=>{class t extends Wr{callSetDisabledState;get submitted(){return Oe(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=yt(()=>this._submittedReactive());_submittedReactive=X(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){if(!this.form)throw new k(1052,!1);Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(rc(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);if(!i)throw new k(-1055,!1);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){Q_(e.control||null,e,!1),Ik(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,wy(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new tc(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(Q_(i||null,e),Ak(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);yy(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&Ek(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){_y(this.form,this),this._oldForm&&rc(this._oldForm,this)}static \u0275fac=function(i){return new(i||t)(ze(ga,10),ze(iy,10),ze(vy,8))};static \u0275dir=D({type:t,features:[L,Ke]})}return t})(),Ok={provide:Wr,useExisting:st(()=>Sf)},Sf=(()=>{class t extends Rk{form=null;ngSubmit=new K;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&ce("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[rt([Ok]),L]})}return t})();var Sy=Symbol("FIELD_TREE");var Cy=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),xy=class{reducer;create;brand;[Cy];constructor(n,e){this.reducer=n,this.create=e}};function ba(t){return typeof t=="function"&&t[Sy]===!0}var lc=new b("");var Cf=class{_box;_destroyed=new x;_resizeSubject=new x;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new z(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(je(e=>e.filter(i=>i.target===n)),Ee(e=>e.length>0),hs({bufferSize:1,refCount:!0}),Ct(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},Dy=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=m(C);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Cf(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Df=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-label"]]})}return t})(),Fk=new b("MatError");var Ef=(()=>{class t{align="start";id=m(Be).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(nt("id",r.id),ae("align",null),q("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),Lk=new b("MatPrefix");var Pk=new b("MatSuffix");var Ay=new b("FloatingLabelParent"),Ey=(()=>{class t{_elementRef=m(M);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=m(Dy);_ngZone=m(C);_parent=m(Ay);_resizeSubscription=new be;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return Vk(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&q("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function Vk(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var Ny="mdc-line-ripple--active",cc="mdc-line-ripple--deactivating",ky=(()=>{class t{_elementRef=m(M);_cleanupTransitionEnd;constructor(){let e=m(C),i=m(Se);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(cc),e.add(Ny)}deactivate(){this._elementRef.nativeElement.classList.add(cc)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(cc);e.propertyName==="opacity"&&r&&i.remove(Ny,cc)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),Iy=(()=>{class t{_elementRef=m(M);_ngZone=m(C);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["notch"];return N({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(o,a){if(o&1&&lt(e,5),o&2){let s;ee(s=te())&&(a._notch=s.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(o,a){o&2&&q("mdc-notched-outline--notched",a.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:["*"],decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(o,a){o&1&&(it(),tt(0,"div",1),tn(1,"div",2,0),ie(3),nn(),tt(4,"div",3))},encapsulation:2})})()}return t})(),Nf=(()=>{class t{id;ngField=null;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;stateChanges=null;value;static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t})}return t})();var kf=new b("MatFormField"),Bk=new b("MAT_FORM_FIELD_DEFAULT_OPTIONS"),My="fill",jk="auto",Ty="fixed",zk="translateY(-50%)",If=(()=>{class t{_elementRef=m(M);_changeDetectorRef=m(ct);_platform=m(re);_idGenerator=m(Be);_ngZone=m(C);_defaults=m(Bk,{optional:!0});_currentDirection;_unwrapMaybeSignal(e){return yn(e)?e():e}_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Wn("iconPrefixContainer");_textPrefixContainerSignal=Wn("textPrefixContainer");_iconSuffixContainerSignal=Wn("iconSuffixContainer");_textSuffixContainerSignal=Wn("textSuffixContainer");_prefixSuffixContainers=yt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Gv(Df);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=At(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||jk}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||My;this._appearanceSignal.set(i)}_appearanceSignal=X(My);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||Ty}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||Ty}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new x;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ie();constructor(){let e=this._defaults,i=m(rn);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),_n(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=ba(this._control.ngField)?null:this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&!ba(this._control.ngField)&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=yt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges?.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges?.pipe(qt([void 0,void 0]),je(()=>[this._unwrapMaybeSignal(i.errorState),i.userAriaDescribedBy]),fs(),Ee(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&!ba(i.ngField)&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ct(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),cn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._unwrapMaybeSignal(this._control.focused);e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){jm({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=yt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._shouldAlwaysFloat()||this._unwrapMaybeSignal(this._control.shouldLabelFloat):!1}_shouldForward(e){let i=this._control?.ngField||this._control?.ngControl;if(!i)return!1;if(ba(i)){let r=i();return e==="valid"?r.valid():e==="dirty"?r.dirty():e==="touched"?r.touched():e==="pending"?r.pending():e==="untouched"?!r.touched():e==="pristine"?!r.dirty():e==="invalid"?!r.valid():!1}else{let r=i;return e==="valid"?r.valid:e==="dirty"?r.dirty:e==="touched"?r.touched:e==="pending"?r.pending:e==="untouched"?r.untouched:e==="pristine"?r.pristine:e==="invalid"?r.invalid:!1}}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._unwrapMaybeSignal(this._control.errorState)?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${a+s}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,p=`var(--mat-mdc-form-field-label-transform, ${zk} translateX(${f}))`,g=a+s+l+c;return[p,g]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["iconPrefixContainer"],i=["textPrefixContainer"],r=["iconSuffixContainer"],o=["textSuffixContainer"],a=["textField"],s=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],l=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function c(B,fe){B&1&&Ge(0,"span",21)}function d(B,fe){if(B&1&&(R(0,"label",20),ie(1,1),se(2,c,1,0,"span",21),O()),B&2){let v=_e(2);Re("floating",v._shouldLabelFloat())("monitorResize",v._hasOutline())("id",v._labelId),ae("for",v._control.disableAutomaticLabeling?null:v._control.id),F(2),le(!v.hideRequiredMarker&&v._unwrapMaybeSignal(v._control.required)?2:-1)}}function u(B,fe){if(B&1&&se(0,d,3,5,"label",20),B&2){let v=_e();le(v._hasFloatingLabel()?0:-1)}}function h(B,fe){B&1&&Ge(0,"div",7)}function f(B,fe){}function p(B,fe){if(B&1&&et(0,f,0,0,"ng-template",13),B&2){_e(2);let v=$i(1);Re("ngTemplateOutlet",v)}}function g(B,fe){if(B&1&&(R(0,"div",9),se(1,p,1,1,null,13),O()),B&2){let v=_e();Re("matFormFieldNotchedOutlineOpen",v._shouldLabelFloat()),F(),le(v._forceDisplayInfixLabel()?-1:1)}}function _(B,fe){B&1&&(R(0,"div",10,2),ie(2,2),O())}function w(B,fe){B&1&&(R(0,"div",11,3),ie(2,3),O())}function U(B,fe){}function $t(B,fe){if(B&1&&et(0,U,0,0,"ng-template",13),B&2){_e();let v=$i(1);Re("ngTemplateOutlet",v)}}function Oc(B,fe){B&1&&(R(0,"div",14,4),ie(2,4),O())}function Nw(B,fe){B&1&&(R(0,"div",15,5),ie(2,5),O())}function kw(B,fe){B&1&&Ge(0,"div",16)}function Iw(B,fe){B&1&&(R(0,"div",18),ie(1,6),O())}function Mw(B,fe){if(B&1&&(R(0,"mat-hint",22),Xe(1),O()),B&2){let v=_e(2);Re("id",v._hintLabelId),F(),_t(v.hintLabel)}}function Tw(B,fe){if(B&1&&(R(0,"div",19),se(1,Mw,2,2,"mat-hint",22),ie(2,7),Ge(3,"div",23),ie(4,8),O()),B&2){let v=_e();F(),le(v.hintLabel?1:-1)}}return N({type:t,selectors:[["mat-form-field"]],contentQueries:function(fe,v,he){if(fe&1&&(El(he,v._labelChild,Df,5),zi(he,Nf,5)(he,Lk,5)(he,Pk,5)(he,Fk,5)(he,Ef,5)),fe&2){Ui();let Qe;ee(Qe=te())&&(v._formFieldControl=Qe.first),ee(Qe=te())&&(v._prefixChildren=Qe),ee(Qe=te())&&(v._suffixChildren=Qe),ee(Qe=te())&&(v._errorChildren=Qe),ee(Qe=te())&&(v._hintChildren=Qe)}},viewQuery:function(fe,v){if(fe&1&&(Hi(v._iconPrefixContainerSignal,e,5)(v._textPrefixContainerSignal,i,5)(v._iconSuffixContainerSignal,r,5)(v._textSuffixContainerSignal,o,5),lt(a,5)(e,5)(i,5)(r,5)(o,5)(Ey,5)(Iy,5)(ky,5)),fe&2){Ui(4);let he;ee(he=te())&&(v._textField=he.first),ee(he=te())&&(v._iconPrefixContainer=he.first),ee(he=te())&&(v._textPrefixContainer=he.first),ee(he=te())&&(v._iconSuffixContainer=he.first),ee(he=te())&&(v._textSuffixContainer=he.first),ee(he=te())&&(v._floatingLabel=he.first),ee(he=te())&&(v._notchedOutline=he.first),ee(he=te())&&(v._lineRipple=he.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(fe,v){fe&2&&q("mat-mdc-form-field-label-always-float",v._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",v._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",v._hasIconSuffix)("mat-form-field-invalid",v._unwrapMaybeSignal(v._control.errorState))("mat-form-field-disabled",v._unwrapMaybeSignal(v._control.disabled))("mat-form-field-autofilled",v._unwrapMaybeSignal(v._control.autofilled))("mat-form-field-appearance-fill",v.appearance=="fill")("mat-form-field-appearance-outline",v.appearance=="outline")("mat-form-field-hide-placeholder",v._hasFloatingLabel()&&!v._shouldLabelFloat())("mat-primary",v.color!=="accent"&&v.color!=="warn")("mat-accent",v.color==="accent")("mat-warn",v.color==="warn")("ng-untouched",v._shouldForward("untouched"))("ng-touched",v._shouldForward("touched"))("ng-pristine",v._shouldForward("pristine"))("ng-dirty",v._shouldForward("dirty"))("ng-valid",v._shouldForward("valid"))("ng-invalid",v._shouldForward("invalid"))("ng-pending",v._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[rt([{provide:kf,useExisting:t},{provide:Ay,useExisting:t}])],ngContentSelectors:l,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(fe,v){if(fe&1&&(it(s),et(0,u,1,1,"ng-template",null,0,Nl),R(2,"div",6,1),ce("click",function(Qe){return v._control.onContainerClick(Qe)}),se(4,h,1,0,"div",7),R(5,"div",8),se(6,g,2,2,"div",9),se(7,_,3,0,"div",10),se(8,w,3,0,"div",11),R(9,"div",12),se(10,$t,1,1,null,13),ie(11),O(),se(12,Oc,3,0,"div",14),se(13,Nw,3,0,"div",15),O(),se(14,kw,1,0,"div",16),O(),R(15,"div",17),se(16,Iw,2,0,"div",18)(17,Tw,5,1,"div",19),O()),fe&2){let he,Qe=v._unwrapMaybeSignal(v._control.disabled);F(2),q("mdc-text-field--filled",!v._hasOutline())("mdc-text-field--outlined",v._hasOutline())("mdc-text-field--no-label",!v._hasFloatingLabel())("mdc-text-field--disabled",Qe)("mdc-text-field--invalid",v._unwrapMaybeSignal(v._control.errorState)),F(2),le(!v._hasOutline()&&!Qe?4:-1),F(2),le(v._hasOutline()?6:-1),F(),le(v._hasIconPrefix?7:-1),F(),le(v._hasTextPrefix?8:-1),F(2),le(!v._hasOutline()||v._forceDisplayInfixLabel()?10:-1),F(2),le(v._hasTextSuffix?12:-1),F(),le(v._hasIconSuffix?13:-1),F(),le(v._hasOutline()?-1:14),F(),q("mat-mdc-form-field-subscript-dynamic-size",v.subscriptSizing==="dynamic");let Aw=v._getSubscriptMessageType();F(),le((he=Aw)==="error"?16:he==="hint"?17:-1)}},dependencies:[Ey,Iy,Hm,ky,Ef],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})})()}return t})();var Ry=new b("MAT_INPUT_VALUE_ACCESSOR");var Oy=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var dc=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?yn(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var Hk=["button","checkbox","file","hidden","image","radio","range","reset","submit"],Uk=new b("MAT_INPUT_CONFIG"),Fy=(()=>{class t{_elementRef=m(M);_platform=m(re);ngControl=m(oc,{optional:!0,self:!0});_autofillMonitor=m(b_);_ngZone=m(C);_formField=m(kf,{optional:!0});_renderer=m(Se);_uid=m(Be).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=m(Uk,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new x;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=At(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(pa.required)??!1}set required(e){this._required=At(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&df().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=At(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>df().has(e));constructor(){let e=m(xf,{optional:!0}),i=m(Sf,{optional:!0}),r=m(Oy),o=m(Ry,{optional:!0,self:!0}),a=m(lc,{optional:!0,self:!0}),s=this._elementRef.nativeElement,l=s.nodeName.toLowerCase();o?yn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new dc(r,a||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&_n(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){Hk.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&ce("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(nt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ae("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),q("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",ye]},exportAs:["matInput"],features:[rt([{provide:Nf,useExisting:t}]),Ke]})}return t})();var Ly=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){return N({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(r,o){r&2&&q("mdc-form-field--align-end",o.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:["*"],decls:1,vars:0,template:function(r,o){r&1&&(it(),ie(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})})()}return t})();var $k=new b("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),uc=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},Py=(()=>{class t{_elementRef=m(M);_focusMonitor=m(Gi);_changeDetectorRef=m(ct);defaults=m($k);_injector=m(T);_signalFormField=null;_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new uc(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Ie();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;fullWidth=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new K;toggleChange=new K;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){m(We).load(Hr);let e=m(new Ml("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=m(Be).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngOnInit(){this._signalFormField=this._injector.get(lc,void 0,{optional:!0,self:!0})}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0&&!this._signalFormField?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new uc(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["switch"],i=["*"];function r(o,a){o&1&&(R(0,"span",11),Ao(),R(1,"svg",13),Ge(2,"path",14),O(),R(3,"svg",15),Ge(4,"path",16),O()())}return N({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(a,s){if(a&1&&lt(e,5),a&2){let l;ee(l=te())&&(s._switchElement=l.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:15,hostBindings:function(a,s){a&2&&(nt("id",s.id),ae("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),Zo(s.color?"mat-"+s.color:""),q("mat-mdc-slide-toggle-focused",s._focused)("mat-mdc-slide-toggle-checked",s.checked)("mat-slide-toggle-full-width",s.fullWidth)("_mat-animation-noopable",s._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",ye],color:"color",disabled:[2,"disabled","disabled",ye],fullWidth:[2,"fullWidth","fullWidth",ye],disableRipple:[2,"disableRipple","disableRipple",ye],tabIndex:[2,"tabIndex","tabIndex",o=>o==null?0:Tl(o)],checked:[2,"checked","checked",ye],hideIcon:[2,"hideIcon","hideIcon",ye],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ye]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[rt([{provide:bf,useExisting:st(()=>t),multi:!0},{provide:ga,useExisting:t,multi:!0}]),Ke],ngContentSelectors:i,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(a,s){if(a&1&&(it(),R(0,"div",1)(1,"button",2,0),ce("click",function(){return s._handleClick()}),Ge(3,"div",3)(4,"span",4),R(5,"span",5)(6,"span",6)(7,"span",7),Ge(8,"span",8),O(),R(9,"span",9),Ge(10,"span",10),O(),se(11,r,5,0,"span",11),O()()(),R(12,"label",12),ce("click",function(c){return c.stopPropagation()}),ie(13),O()()),a&2){let l=$i(2);Re("labelPosition",s.labelPosition),F(),q("mdc-switch--selected",s.checked)("mdc-switch--unselected",!s.checked)("mdc-switch--checked",s.checked)("mdc-switch--disabled",s.disabled)("mat-mdc-slide-toggle-disabled-interactive",s.disabledInteractive),Re("tabIndex",s.disabled&&!s.disabledInteractive?-1:s.tabIndex)("disabled",s.disabled&&!s.disabledInteractive),ae("id",s.buttonId)("name",s.name)("aria-label",s.ariaLabel)("aria-labelledby",s._getAriaLabelledBy())("aria-describedby",s.ariaDescribedby)("aria-required",s.required||null)("aria-checked",s.checked)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),F(9),Re("matRippleTrigger",l)("matRippleDisabled",s.disableRipple||s.disabled)("matRippleCentered",!0),F(),le(s.hideIcon?-1:11),F(),Re("for",s.buttonId),ae("id",s._labelId)}},dependencies:[H_,Ly],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--%NS%mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--%NS%mat-slide-toggle-track-height, 32px);
  border-radius: var(--%NS%mat-slide-toggle-track-shape, var(--%NS%mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--%NS%mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--%NS%mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--%NS%mat-slide-toggle-track-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--%NS%mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--%NS%mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--%NS%mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--%NS%mat-slide-toggle-disabled-unselected-track-outline-color, var(--%NS%mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--%NS%mat-slide-toggle-unselected-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--%NS%mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--%NS%mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--%NS%mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--%NS%mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-unselected-hover-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-unselected-focus-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-unselected-pressed-track-color, var(--%NS%mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-disabled-unselected-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--%NS%mat-slide-toggle-selected-track-color, var(--%NS%mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--%NS%mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--%NS%mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--%NS%mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--%NS%mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-selected-hover-track-color, var(--%NS%mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-selected-focus-track-color, var(--%NS%mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-selected-pressed-track-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-disabled-selected-track-color, var(--%NS%mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--%NS%mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--%NS%mat-slide-toggle-handle-width);
  height: var(--%NS%mat-slide-toggle-handle-height);
  border-radius: var(--%NS%mat-slide-toggle-handle-shape, var(--%NS%mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--%NS%mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--%NS%mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--%NS%mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--%NS%mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--%NS%mat-slide-toggle-selected-handle-size, 24px);
  height: var(--%NS%mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--%NS%mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--%NS%mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--%NS%mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--%NS%mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--%NS%mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--%NS%mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--%NS%selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--%NS%mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--%NS%unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--%NS%mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--%NS%mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--%NS%mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--%NS%selected:enabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-handle-color, var(--%NS%mat-sys-on-primary));
}
.mdc-switch--%NS%selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-hover-handle-color, var(--%NS%mat-sys-primary-container));
}
.mdc-switch--%NS%selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-focus-handle-color, var(--%NS%mat-sys-primary-container));
}
.mdc-switch--%NS%selected:enabled:active .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-pressed-handle-color, var(--%NS%mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--%NS%selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--%NS%selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--%NS%selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-disabled-selected-handle-color, var(--%NS%mat-sys-surface));
}
.mdc-switch--%NS%unselected:enabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-handle-color, var(--%NS%mat-sys-outline));
}
.mdc-switch--%NS%unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-hover-handle-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-switch--%NS%unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-focus-handle-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-switch--%NS%unselected:enabled:active .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-pressed-handle-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-disabled-unselected-handle-color, var(--%NS%mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--%NS%mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--%NS%mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--%NS%mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--%NS%mat-slide-toggle-state-layer-size, 40px);
  height: var(--%NS%mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--%NS%unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-slide-toggle-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mdc-switch--%NS%unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-slide-toggle-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mdc-switch--%NS%unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-unselected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--%NS%selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
  opacity: var(--%NS%mat-slide-toggle-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mdc-switch--%NS%selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
  opacity: var(--%NS%mat-slide-toggle-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mdc-switch--%NS%selected:enabled:active .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-selected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
  opacity: var(--%NS%mat-slide-toggle-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--%NS%mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--%NS%mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--%NS%mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--%NS%mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--%NS%mat-slide-toggle-unselected-icon-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--%NS%mat-slide-toggle-disabled-unselected-icon-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--%NS%mat-slide-toggle-selected-icon-size, 16px);
  height: var(--%NS%mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--%NS%mat-slide-toggle-selected-icon-color, var(--%NS%mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--%NS%mat-slide-toggle-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--%NS%mat-slide-toggle-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-slide-toggle-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-slide-toggle-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-slide-toggle-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-slide-toggle-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-slide-toggle-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--%NS%mat-slide-toggle-disabled-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-slide-toggle-full-width {
  width: 100%;
}
.mat-slide-toggle-full-width .mat-internal-form-field {
  width: 100%;
  justify-content: space-between;
}
.mat-slide-toggle-full-width .mat-internal-form-field label {
  margin: 0;
  flex-grow: 1;
  text-align: end;
}
.mat-slide-toggle-full-width .mdc-form-field--align-end label {
  text-align: start;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2})})()}return t})();var Gk=["field"];function Wk(t,n){if(t&1){let e=Gn();R(0,"textarea",4,0),ce("input",function(r){bn(e);let o=_e();return vn(o.typed(r))}),O()}if(t&2){let e=_e();Re("value",e.text())("placeholder",e.hint())}}function qk(t,n){if(t&1){let e=Gn();R(0,"input",5,0),ce("input",function(r){bn(e);let o=_e();return vn(o.typed(r))}),O()}if(t&2){let e=_e();Re("type",e.inputType()===3?"password":"text")("value",e.text())("placeholder",e.hint()),ae("inputmode",e.inputType()===1?"decimal":null)}}var Yk=["canvas"];function Zk(t,n){t&1&&tt(0,"span",1),t&2&&nt("innerHTML",n,sm)}function Kk(t,n){if(t&1&&tt(0,"img",4),t&2){let e=_e();Rr("object-fit",e.fit()),nt("src",e.source())}}function Xk(t,n){if(t&1&&tt(0,"canvas",null,0),t&2){let e=_e();Rr("object-fit",e.fit())}}var wt=class{element=m(M).nativeElement;slot=this.element;text=X("")},Tf=class t extends wt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-text"]],features:[L],decls:1,vars:1,template:function(e,i){e&1&&Xe(0),e&2&&_t(i.text())},styles:["[_nghost-%COMP%]{display:block;white-space:pre-wrap;overflow-wrap:anywhere;line-height:normal}"]})},Af=class t extends wt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-button"]],features:[L],decls:2,vars:1,consts:[["matButton","filled"]],template:function(e,i){e&1&&(R(0,"button",0),Xe(1),O()),e&2&&(F(),_t(i.text()))},dependencies:[Ur],styles:["[_nghost-%COMP%]{display:block}button[_ngcontent-%COMP%]{width:100%}"]})},va=class t extends wt{checked=X(!1);onFlip=()=>{};static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-switch"]],features:[L],decls:1,vars:1,consts:[[3,"change","click","checked"]],template:function(e,i){e&1&&(R(0,"mat-slide-toggle",0),ce("change",function(){return i.onFlip()})("click",function(o){return o.stopPropagation()}),O()),e&2&&Re("checked",i.checked())},dependencies:[Py],styles:["[_nghost-%COMP%]{display:block}"]})},Zn=class t extends wt{hint=X("");inputType=X(0);onText=()=>{};sent=[];setText(n){let e=this.sent.indexOf(n);if(e>=0){this.sent.splice(0,e+1);return}this.sent.length=0;let i=this.field()?.nativeElement;i&&i.value!==n&&(i.value=n),this.text.set(n)}focus(){Je(()=>this.field()?.nativeElement.focus(),{injector:this.injector})}typed(n){let e=n.target.value;this.sent.push(e),this.onText(e)}injector=m(T);field=Wn("field");static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-text-input"]],viewQuery:function(e,i){e&1&&Hi(i.field,Gk,5),e&2&&Ui()},features:[L],decls:3,vars:1,consts:[["field",""],["appearance","outline","subscriptSizing","dynamic"],["matInput","","cdkTextareaAutosize","",3,"value","placeholder"],["matInput","",3,"type","value","placeholder"],["matInput","","cdkTextareaAutosize","",3,"input","value","placeholder"],["matInput","",3,"input","type","value","placeholder"]],template:function(e,i){e&1&&(R(0,"mat-form-field",1),se(1,Wk,2,2,"textarea",2)(2,qk,2,4,"input",3),O()),e&2&&(F(),le(i.inputType()===2?1:2))},dependencies:[If,Fy,ef,v_],styles:["[_nghost-%COMP%]{display:block}mat-form-field[_ngcontent-%COMP%]{width:100%}"]})},Mf=new Map,Nn=class t extends wt{sanitizer=m(Lr);source=X("");fit=X("contain");svg=X(null);streaming=X(!1);canvas=Wn("canvas");shown="";stream;showResource(n){this.stopStream(),this.shown=n,Mf.has(n)||Mf.set(n,fetch(`res/${n}.svg`).then(e=>e.ok?e.text():"")),Mf.get(n).then(e=>{this.shown===n&&this.svg.set(e?this.sanitizer.bypassSecurityTrustHtml(e):null)})}showPicture(n){this.stopStream(),this.shown="",this.svg.set(null),this.source.set(n)}showStream(n){this.shown="",this.svg.set(null),this.source.set(""),this.stream=n,this.streaming.set(!0)}draw(n){let e=this.canvas()?.nativeElement;e&&((e.width!==n.displayWidth||e.height!==n.displayHeight)&&(e.width=n.displayWidth,e.height=n.displayHeight),e.getContext("2d")?.drawImage(n,0,0))}stopStream(){this.stream=void 0,this.streaming.set(!1)}static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-image"]],viewQuery:function(e,i){e&1&&Hi(i.canvas,Yk,5),e&2&&Ui()},hostVars:2,hostBindings:function(e,i){e&2&&q("resource",i.svg()!==null)},features:[L],decls:3,vars:1,consts:[["canvas",""],[3,"innerHTML"],["alt","",3,"src","object-fit"],[3,"object-fit"],["alt","",3,"src"]],template:function(e,i){if(e&1&&se(0,Zk,1,1,"span",1)(1,Kk,1,3,"img",2)(2,Xk,2,2,"canvas",3),e&2){let r;le((r=i.svg())?0:i.source()?1:i.streaming()?2:-1,r)}},styles:["[_nghost-%COMP%]{display:block}img[_ngcontent-%COMP%], canvas[_ngcontent-%COMP%], span[_ngcontent-%COMP%], span[_ngcontent-%COMP%]     svg{display:block;width:100%;height:100%}.resource[_nghost-%COMP%]:not(.sized)   span[_ngcontent-%COMP%]{width:24px;height:24px;margin:0 auto}"]})},_a=class t extends wt{sanitizer=m(Lr);url=X("");load(n){this.url.set(this.sanitizer.bypassSecurityTrustResourceUrl(n))}static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-web-view"]],features:[L],decls:1,vars:1,consts:[[3,"src"]],template:function(e,i){e&1&&tt(0,"iframe",0),e&2&&nt("src",i.url(),lm)},styles:["[_nghost-%COMP%]{display:block}iframe[_ngcontent-%COMP%]{width:100%;height:100%;border:0}"]})},Rf=class t extends wt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-column"]],features:[L],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:flex;flex-direction:column}"]})},Of=class t extends wt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-row"]],features:[L],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:flex;flex-direction:row;align-items:flex-start}"]})},Ff=class t extends wt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-stack"]],features:[L],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:grid}"]})},ya=class extends wt{constructor(n){super(),this.slot=document.createElement("div"),this.slot.style.display="flex",this.slot.style.flexDirection=n,this.element.appendChild(this.slot)}},Lf=class t extends ya{constructor(){super("column")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=N({type:t,selectors:[["hatter-scroll-view"]],features:[L],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;overflow-y:auto}"]})},Pf=class t extends ya{constructor(){super("row")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=N({type:t,selectors:[["hatter-horizontal-scroll-view"]],features:[L],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;overflow-x:auto}"]})},Vf=class t extends ya{constructor(){super("column")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=N({type:t,selectors:[["hatter-lazy-column"]],features:[L],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;overflow-y:auto}"]})},Bf=class t extends wt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=de(t)))(i||t)}})();static \u0275cmp=N({type:t,selectors:[["hatter-map-view"]],features:[L],decls:1,vars:0,template:function(e,i){e&1&&Xe(0,"Map")},styles:["[_nghost-%COMP%]{display:grid;place-items:center;background:var(--%NS%mat-sys-surface-variant)}"]})},Vy={0:Tf,1:Af,2:Rf,3:Of,4:Zn,5:Lf,6:Nn,7:Bf,8:_a,9:Ff,10:Pf,11:Vf,12:va};var wa=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},kn=class extends wa{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,a){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=a||null}},Kn=class extends wa{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},jf=class extends wa{element;constructor(n){super(),this.element=n instanceof M?n.nativeElement:n}},Xn=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof kn)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Kn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof jf)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},mc=class extends Xn{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Un,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||T.NULL,o=r.get(Pe,i.injector);e=Fr(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var on=(()=>{class t extends Xn{_moduleRef=m(Un,{optional:!0});_document=m(V);_viewContainerRef=m(en);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new K;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._insertContent(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._insertContent(i),this._attachedPortal=e,super.setDisposeFn(()=>r.parentNode?.replaceChild(i,r))};_insertContent(e){let i=this._viewContainerRef.element.nativeElement;i.nodeType===i.ELEMENT_NODE?i.appendChild(e):i.parentNode?.insertBefore(e,i)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[L]})}return t})();var Qk=20,By=(()=>{class t{_ngZone=m(C);_platform=m(re);_renderer=m(Ae).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new x;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=Qk){return this._platform.isBrowser?new z(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(cr(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):pi()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ee(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=ot(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),jy=(()=>{class t{elementRef=m(M);scrollDispatcher=m(By);ngZone=m(C);dir=m(rn,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new x;_renderer=m(Se);_cleanupScroll;_elementScrolled=new x;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&jr()!=Ut.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),jr()==Ut.INVERTED?e.left=e.right:jr()==Ut.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Yl()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&jr()==Ut.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&jr()==Ut.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),Jk=20,zy=(()=>{class t{_platform=m(re);_listeners;_viewportSize=null;_change=new x;_document=m(V);constructor(){let e=m(C),i=m(Ae).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=Jk){return e>0?this._change.pipe(cr(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Hy=Yl();function Zi(t){return new fc(t.get(zy),t.get(V))}var fc=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=En(-this._previousScrollPosition.left),n.style.top=En(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),Hy&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),Hy&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};var hc=class{enable(){}disable(){}attach(){}};var Qn=class{positionStrategy;scrollStrategy=new hc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Gy=(()=>{class t{_attachedOverlays=[];_document=m(V);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),Wy=(()=>{class t extends Gy{_ngZone=m(C);_renderer=m(Ae).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),qy=(()=>{class t extends Gy{_platform=m(re);_ngZone=m(C);_renderer=m(Ae).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=dt(e)};_clickListener=e=>{let i=dt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(Uy(s.overlayElement,i)||Uy(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Uy(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var Yy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),Uf=(()=>{class t{_platform=m(re);_containerElement;_document=m(V);_styleLoader=m(We);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||cf()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),cf()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(Yy)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),Hf=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Zy(t){return t&&t.nodeType===1}var zf=X([]),Yr=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new x;_attachments=new x;_detachments=new x;_positionStrategy;_scrollStrategy;_locationChanges;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new x;_outsidePointerEvents=new x;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,d=!1,u,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=d,this._injector=u,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);if(this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),Oe(()=>{zf.update(i=>i.includes(this)?i:[...i,this])}),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Je(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation===!0||this._config.disposeOnNavigation==="pop-state"){let i=this._location.subscribe(()=>this.dispose());this._locationChanges=()=>i.unsubscribe()}else this._config.disposeOnNavigation==="url-change"&&(this._locationChanges=this._location.onUrlChange(()=>this.dispose()));return this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges?.(),this._outsideClickDispatcher.remove(this),Oe(()=>{zf.update(e=>e.filter(i=>i!==this))}),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges?.(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0,Oe(()=>{zf.update(e=>e.filter(i=>i!==this))})}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=y(y({},this._config),n),this._updateElementSize()}setDirection(n){this._config=W(y({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=En(this._config.width),n.height=En(this._config.height),n.minWidth=En(this._config.minWidth),n.minHeight=En(this._config.minHeight),n.maxWidth=En(this._config.maxWidth),n.maxHeight=En(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Zy(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Hf(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=aa(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Je(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}};var $y="cdk-global-overlay-wrapper";function In(t){return new pc}var pc=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add($y),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),d=this._xPosition,u=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",f="",p="",g="";l?g="flex-start":d==="center"?(g="center",h?p=u:f=u):h?d==="left"||d==="end"?(g="flex-end",f=u):(d==="right"||d==="start")&&(g="flex-start",p=u):d==="left"||d==="start"?(g="flex-start",f=u):(d==="right"||d==="end")&&(g="flex-end",p=u),n.position=this._cssPosition,n.marginLeft=l?"0":f,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":p,e.justifyContent=g,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove($y),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}};var Ky=new b("OVERLAY_DEFAULT_CONFIG");function xa(t,n){t.get(We).load(Yy);let e=t.get(Uf),i=t.get(V),r=t.get(Be),o=t.get(vt),a=t.get(rn),s=t.get(Se,null,{optional:!0})||t.get(Ae).createRenderer(null,null),l=new Qn(n),c=t.get(Ky,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,!i.body||!("showPopover"in i.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let d=i.createElement("div"),u=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),u.appendChild(d),l.usePopover&&(u.setAttribute("popover","manual"),u.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Zy(h)?h.after(u):h?.type==="parent"?h.element.appendChild(u):e.getContainerElement().appendChild(u),new Yr(new mc(d,o,t),u,d,l,t.get(C),t.get(Wy),i,t.get(Ol),t.get(qy),n?.disableAnimations??t.get(Ro,null,{optional:!0})==="NoopAnimations",t.get(Pe),s)}var Jn=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var Sa=(()=>{class t extends Xn{_elementRef=m(M);_focusTrapFactory=m(sf);_config;_interactivityChecker=m(af);_ngZone=m(C);_focusMonitor=m(Gi);_renderer=m(Se);_changeDetectorRef=m(ct);_injector=m(T);_platform=m(re);_document=m(V);_portalOutlet;_focusTrapped=new x;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=m(Jn,{optional:!0})||new Jn,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Je(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Pr(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Pr();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Pr()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,r){}return N({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(r,o){if(r&1&&lt(on,7),r&2){let a;ee(a=te())&&(o._portalOutlet=a.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(r,o){r&2&&ae("id",o._config.id||null)("role",o._config.role)("aria-modal",o._config.ariaModal)("aria-labelledby",o._config.ariaLabel?null:o._ariaLabelledByQueue[0])("aria-label",o._config.ariaLabel)("aria-describedby",o._config.ariaDescribedBy||null)},features:[L],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(r,o){r&1&&et(0,e,0,0,"ng-template",0)},dependencies:[on],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})})()}return t})(),Ki=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new x;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Br(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},tI=new b("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=m(T);return()=>Zi(t)}}),nI=new b("DialogData"),iI=new b("DefaultDialogConfig");function rI(t){let n=X(t),e=new K;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var vc=(()=>{class t{_injector=m(T);_defaultOptions=m(iI,{optional:!0});_parentDialog=m(t,{optional:!0,skipSelf:!0});_overlayContainer=m(Uf);_idGenerator=m(Be);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;_ariaHiddenElements=new Map;_scrollStrategy=m(tI);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=mo(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(qt(void 0)));open(e,i){let r=this._defaultOptions||new Jn;i=y(y({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=xa(this._injector,o),s=new Ki(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(mt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){$f(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){$f(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),$f(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Qn({positionStrategy:e.positionStrategy||In().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:Jn,useValue:r},{provide:Ki,useValue:i},{provide:Yr,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=Sa;let l=new kn(s,r.viewContainerRef,T.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof bt){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=y(y({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Kn(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new kn(e,o.viewContainerRef,a,null,o.bindings));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:nI,useValue:e.data},{provide:Ki,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(rn,null,{optional:!0}))&&s.push({provide:rn,useValue:rI(e.direction)}),T.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function $f(t,n){let e=t.length;for(;e--;)n(t[e])}var Xy="_mat-bottom-sheet-enter",Qy="_mat-bottom-sheet-exit",aI=(()=>{class t extends Sa{_breakpointSubscription;_animationsDisabled=Ie();_animationState="void";_animationStateChanged=new K;_destroyed=!1;constructor(){super();let e=m(sa);this._breakpointSubscription=e.observe([Dn.Medium,Dn.Large,Dn.XLarge]).subscribe(()=>{let i=this._elementRef.nativeElement.classList;i.toggle("mat-bottom-sheet-container-medium",e.isMatched(Dn.Medium)),i.toggle("mat-bottom-sheet-container-large",e.isMatched(Dn.Large)),i.toggle("mat-bottom-sheet-container-xlarge",e.isMatched(Dn.XLarge))})}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._animationsDisabled&&this._simulateAnimation(Xy))}exit(){this._destroyed||(this._elementRef.nativeElement.setAttribute("mat-exit",""),this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._animationsDisabled&&this._simulateAnimation(Qy))}ngOnDestroy(){super.ngOnDestroy(),this._breakpointSubscription.unsubscribe(),this._destroyed=!0}_simulateAnimation(e){this._ngZone.run(()=>{this._handleAnimationEvent(!0,e,this._elementRef.nativeElement),setTimeout(()=>this._handleAnimationEvent(!1,e,this._elementRef.nativeElement))})}_trapFocus(){super._trapFocus({preventScroll:!0})}_handleAnimationEvent(e,i,r){if(r===this._elementRef.nativeElement){let o=i===Xy;(o||i===Qy)&&this._animationStateChanged.emit({toState:o?"visible":"hidden",phase:e?"start":"done"})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,r){}return N({type:t,selectors:[["mat-bottom-sheet-container"]],hostAttrs:["tabindex","-1",1,"mat-bottom-sheet-container"],hostVars:9,hostBindings:function(r,o){r&1&&ce("animationstart",function(s){return o._handleAnimationEvent(!0,s.animationName,s.target)})("animationend",function(s){return o._handleAnimationEvent(!1,s.animationName,s.target)})("animationcancel",function(s){return o._handleAnimationEvent(!1,s.animationName,s.target)}),r&2&&(ae("role",o._config.role)("aria-modal",o._config.ariaModal)("aria-label",o._config.ariaLabel),q("mat-bottom-sheet-container-animations-enabled",!o._animationsDisabled)("mat-bottom-sheet-container-enter",o._animationState==="visible")("mat-bottom-sheet-container-exit",o._animationState==="hidden"))},features:[L],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(r,o){r&1&&et(0,e,0,0,"ng-template",0)},dependencies:[on],styles:[`@keyframes _mat-bottom-sheet-enter {
  from {
    transform: translateY(100%);
  }
  to {
    transform: none;
  }
}
@keyframes _mat-bottom-sheet-exit {
  from {
    transform: none;
  }
  to {
    transform: translateY(100%);
  }
}
.mat-bottom-sheet-container {
  box-shadow: 0px 8px 10px -5px rgba(0, 0, 0, 0.2), 0px 16px 24px 2px rgba(0, 0, 0, 0.14), 0px 6px 30px 5px rgba(0, 0, 0, 0.12);
  padding: 8px 16px;
  min-width: 100vw;
  box-sizing: border-box;
  display: block;
  outline: 0;
  max-height: 80vh;
  overflow: auto;
  position: relative;
  background: var(--%NS%mat-bottom-sheet-container-background-color, var(--%NS%mat-sys-surface-container-low));
  color: var(--%NS%mat-bottom-sheet-container-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-bottom-sheet-container-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-bottom-sheet-container-text-size, var(--%NS%mat-sys-body-large-size));
  line-height: var(--%NS%mat-bottom-sheet-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-weight: var(--%NS%mat-bottom-sheet-container-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-bottom-sheet-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
@media (forced-colors: active) {
  .mat-bottom-sheet-container {
    outline: 1px solid;
  }
}

.mat-bottom-sheet-container-animations-enabled {
  transform: translateY(100%);
}
.mat-bottom-sheet-container-animations-enabled.mat-bottom-sheet-container-enter {
  animation: _mat-bottom-sheet-enter 195ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-bottom-sheet-container-animations-enabled.mat-bottom-sheet-container-exit {
  animation: _mat-bottom-sheet-exit 375ms cubic-bezier(0.4, 0, 1, 1) backwards;
}

.mat-bottom-sheet-container-xlarge, .mat-bottom-sheet-container-large, .mat-bottom-sheet-container-medium {
  border-top-left-radius: var(--%NS%mat-bottom-sheet-container-shape, 28px);
  border-top-right-radius: var(--%NS%mat-bottom-sheet-container-shape, 28px);
}

.mat-bottom-sheet-container-medium {
  min-width: 384px;
  max-width: calc(100vw - 128px);
}

.mat-bottom-sheet-container-large {
  min-width: 512px;
  max-width: calc(100vw - 256px);
}

.mat-bottom-sheet-container-xlarge {
  min-width: 576px;
  max-width: calc(100vw - 384px);
}
`],encapsulation:2,changeDetection:1})})()}return t})(),qf=new b("MatBottomSheetData"),Wf=class{viewContainerRef;injector;panelClass;direction;data=null;hasBackdrop=!0;backdropClass;disableClose=!1;ariaLabel=null;ariaModal=!1;closeOnNavigation=!0;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;height="";minHeight;maxHeight;bindings},Zr=class{_ref;get instance(){return this._ref.componentInstance}get componentRef(){return this._ref.componentRef}containerInstance;disableClose;_afterOpened=new x;_result;_closeFallbackTimeout;constructor(n,e,i){this._ref=n,this.containerInstance=i,this.disableClose=e.disableClose,i._animationStateChanged.pipe(Ee(r=>r.phase==="done"&&r.toState==="visible"),mt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ee(r=>r.phase==="done"&&r.toState==="hidden"),mt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._ref.close(this._result)}),n.overlayRef.detachments().subscribe(()=>{this._ref.close(this._result)}),cn(this.backdropClick(),this.keydownEvents().pipe(Ee(r=>r.keyCode===27))).subscribe(r=>{!this.disableClose&&(r.type!=="keydown"||!Br(r))&&(r.preventDefault(),this.dismiss())})}dismiss(n){this.containerInstance&&(this.containerInstance._animationStateChanged.pipe(Ee(e=>e.phase==="start"),mt(1)).subscribe(()=>{this._closeFallbackTimeout=setTimeout(()=>this._ref.close(this._result),500),this._ref.overlayRef.detachBackdrop()}),this._result=n,this.containerInstance.exit(),this.containerInstance=null)}afterDismissed(){return this._ref.closed}afterOpened(){return this._afterOpened}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}},sI=new b("mat-bottom-sheet-default-options"),Jy=(()=>{class t{_injector=m(T);_parentBottomSheet=m(t,{optional:!0,skipSelf:!0});_animationsDisabled=Ie();_defaultOptions=m(sI,{optional:!0});_bottomSheetRefAtThisLevel=null;_dialog=m(vc);get _openedBottomSheetRef(){let e=this._parentBottomSheet;return e?e._openedBottomSheetRef:this._bottomSheetRefAtThisLevel}set _openedBottomSheetRef(e){this._parentBottomSheet?this._parentBottomSheet._openedBottomSheetRef=e:this._bottomSheetRefAtThisLevel=e}open(e,i){let r=y(y({},this._defaultOptions||new Wf),i),o;return this._dialog.open(e,W(y({},r),{disableClose:!0,closeOnOverlayDetachments:!1,maxWidth:"100%",container:aI,scrollStrategy:r.scrollStrategy||Zi(this._injector),positionStrategy:In(this._injector).centerHorizontally().bottom("0"),disableAnimations:this._animationsDisabled,templateContext:()=>({bottomSheetRef:o}),providers:(a,s,l)=>(o=new Zr(a,r,l),[{provide:Zr,useValue:o},{provide:qf,useValue:r.data}])})),o.afterDismissed().subscribe(()=>{this._openedBottomSheetRef===o&&(this._openedBottomSheetRef=null)}),this._openedBottomSheetRef?(this._openedBottomSheetRef.afterDismissed().subscribe(()=>o.containerInstance?.enter()),this._openedBottomSheetRef.dismiss()):o.containerInstance.enter(),this._openedBottomSheetRef=o,o}dismiss(e){this._openedBottomSheetRef&&this._openedBottomSheetRef.dismiss(e)}ngOnDestroy(){this._bottomSheetRefAtThisLevel&&this._bottomSheetRefAtThisLevel.dismiss()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var yc=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},Yf="mdc-dialog--open",ew="mdc-dialog--opening",tw="mdc-dialog--closing",lI=150,cI=75,dI=(()=>{class t extends Sa{_animationStateChanged=new K;_animationsEnabled=!Ie();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?iw(this._config.enterAnimationDuration)??lI:0;_exitAnimationDuration=this._animationsEnabled?iw(this._config.exitAnimationDuration)??cI:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(nw,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(ew,Yf)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Yf),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Yf),this._animationsEnabled?(this._hostElement.style.setProperty(nw,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(tw)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(ew,tw)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275cmp=(function(){function e(i,r){}return N({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(r,o){r&2&&(nt("id",o._config.id),ae("aria-modal",o._config.ariaModal)("role",o._config.role)("aria-labelledby",o._config.ariaLabel?null:o._ariaLabelledByQueue[0])("aria-label",o._config.ariaLabel)("aria-describedby",o._config.ariaDescribedBy||null),q("_mat-animation-noopable",!o._animationsEnabled)("mat-mdc-dialog-container-with-actions",o._actionSectionCount>0))},features:[L],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(r,o){r&1&&(R(0,"div",0)(1,"div",1),et(2,e,0,0,"ng-template",2),O()())},dependencies:[on],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--%NS%mat-dialog-container-max-width, 560px);
  min-width: var(--%NS%mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--%NS%mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--%NS%mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--%NS%mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--%NS%mat-dialog-container-elevation-shadow, none);
  border-radius: var(--%NS%mat-dialog-container-shape, var(--%NS%mat-sys-corner-extra-large, 4px));
  background-color: var(--%NS%mat-dialog-container-color, var(--%NS%mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--%NS%mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--%NS%mat-dialog-subhead-color, var(--%NS%mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--%NS%mat-dialog-subhead-font, var(--%NS%mat-sys-headline-small-font, inherit));
  line-height: var(--%NS%mat-dialog-subhead-line-height, var(--%NS%mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--%NS%mat-dialog-subhead-size, var(--%NS%mat-sys-headline-small-size, 1rem));
  font-weight: var(--%NS%mat-dialog-subhead-weight, var(--%NS%mat-sys-headline-small-weight, 400));
  letter-spacing: var(--%NS%mat-dialog-subhead-tracking, var(--%NS%mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--%NS%mat-dialog-supporting-text-color, var(--%NS%mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--%NS%mat-dialog-supporting-text-font, var(--%NS%mat-sys-body-medium-font, inherit));
  line-height: var(--%NS%mat-dialog-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--%NS%mat-dialog-supporting-text-size, var(--%NS%mat-sys-body-medium-size, 1rem));
  font-weight: var(--%NS%mat-dialog-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight, 400));
  letter-spacing: var(--%NS%mat-dialog-supporting-text-tracking, var(--%NS%mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--%NS%mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--%NS%mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--%NS%mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--%NS%mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2,changeDetection:1})})()}return t})(),nw="--mat-dialog-transition-duration";function iw(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Tt(t.substring(0,t.length-2)):t.endsWith("s")?Tt(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var _c=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(_c||{}),Ca=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new On(1);_beforeClosed=new On(1);_result;_closeFallbackTimeout;_state=_c.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(Ee(r=>r.state==="opened"),mt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ee(r=>r.state==="closed"),mt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),cn(this.backdropClick(),this.keydownEvents().pipe(Ee(r=>r.keyCode===27&&!this.disableClose&&!Br(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),rw(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(Ee(i=>i.state==="closing"),mt(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=_c.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=_c.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function rw(t,n,e){return t._closeInteractionType=n,t.close(e)}var Zf=new b("MatMdcDialogData"),uI=new b("mat-mdc-dialog-default-options"),mI=new b("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=m(T);return()=>Zi(t)}}),wc=(()=>{class t{_defaultOptions=m(uI,{optional:!0});_scrollStrategy=m(mI);_parentDialog=m(t,{optional:!0,skipSelf:!0});_idGenerator=m(Be);_injector=m(T);_dialog=m(vc);_animationsDisabled=Ie();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;dialogConfigClass=yc;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=mo(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(qt(void 0)));constructor(){this._dialogRefConstructor=Ca,this._dialogContainerType=dI,this._dialogDataToken=Zf}open(e,i){let r;i=y(y({},this._defaultOptions||new yc),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,W(y({},i),{positionStrategy:In(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:Jn,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r},{provide:Ki,useValue:null}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),ow=(()=>{class t{dialogRef=m(Ca,{optional:!0});_elementRef=m(M);_dialog=m(wc);ariaLabel;type="button";dialogResult;_matDialogClose;ngOnInit(){this.dialogRef||(this.dialogRef=dw(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){this._elementRef.nativeElement.getAttribute("aria-disabled")!=="true"&&rw(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&ce("click",function(a){return r._onButtonClick(a)}),i&2&&ae("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[Ke]})}return t})(),aw=(()=>{class t{_dialogRef=m(Ca,{optional:!0});_elementRef=m(M);_dialog=m(wc);ngOnInit(){this._dialogRef||(this._dialogRef=dw(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t})}return t})(),sw=(()=>{class t extends aw{id=m(Be).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&nt("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[L]})}return t})(),lw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Mm([jy])]})}return t})(),cw=(()=>{class t extends aw{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&q("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[L]})}return t})();function dw(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var fI=new b("ListOption"),hI=(()=>{class t{_elementRef=m(M);static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),pI=(()=>{class t{_elementRef=m(M);static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})(),gI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return t})(),uw=(()=>{class t{_listOption=m(fI,{optional:!0});_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,hostVars:4,hostBindings:function(i,r){i&2&&q("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),bI=(()=>{class t extends uw{static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[L]})}return t})(),vI=(()=>{class t extends uw{static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275dir=D({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[L]})}return t})(),_I=new b("MAT_LIST_CONFIG"),Kf=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=At(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(At(e))}_disabled=X(!1);_defaultOptions=m(_I,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,hostVars:1,hostBindings:function(i,r){i&2&&ae("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),yI=(()=>{class t{_elementRef=m(M);_ngZone=m(C);_listBase=m(Kf,{optional:!0});_platform=m(re);_hostElement;_isButtonElement;_noopAnimations=Ie();_avatars;_icons;set lines(e){this._explicitLines=Tt(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=At(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(At(e))}_disabled=X(!1);_subscriptions=new be;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){m(We).load(Hr);let e=m(ca,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new Yi(this,this._ngZone,this._hostElement,this._platform,m(T)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(cn(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,contentQueries:function(i,r,o){if(i&1&&zi(o,bI,4)(o,vI,4),i&2){let a;ee(a=te())&&(r._avatars=a),ee(a=te())&&(r._icons=a)}},hostVars:4,hostBindings:function(i,r){i&2&&(ae("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),q("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var mw=(()=>{class t extends Kf{_isNonInteractive=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275cmp=(function(){let e=["*"];return N({type:t,selectors:[["mat-action-list"]],hostAttrs:["role","group",1,"mat-mdc-action-list","mat-mdc-list-base","mdc-list"],exportAs:["matActionList"],features:[rt([{provide:Kf,useExisting:t}]),L],ngContentSelectors:e,decls:1,vars:0,template:function(r,o){r&1&&(it(),ie(0))},styles:[`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--%NS%mat-list-list-item-container-color, transparent);
  border-radius: var(--%NS%mat-list-list-item-container-shape, var(--%NS%mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--%NS%mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--%NS%mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--%NS%mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--%NS%mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--%NS%selected::before, .mdc-list-item.mdc-list-item--%NS%selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
  width: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
  height: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--%NS%with-leading-icon:hover .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--%NS%mat-list-list-item-leading-avatar-size, 40px);
  height: var(--%NS%mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--%NS%mat-list-list-item-trailing-supporting-text-font, var(--%NS%mat-sys-label-small-font));
  line-height: var(--%NS%mat-list-list-item-trailing-supporting-text-line-height, var(--%NS%mat-sys-label-small-line-height));
  font-size: var(--%NS%mat-list-list-item-trailing-supporting-text-size, var(--%NS%mat-sys-label-small-size));
  font-weight: var(--%NS%mat-list-list-item-trailing-supporting-text-weight, var(--%NS%mat-sys-label-small-weight));
  letter-spacing: var(--%NS%mat-list-list-item-trailing-supporting-text-tracking, var(--%NS%mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
  width: var(--%NS%mat-list-list-item-trailing-icon-size, 24px);
  height: var(--%NS%mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--%NS%with-trailing-icon:hover .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-trailing-supporting-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-selected-trailing-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--%NS%mat-list-list-item-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-list-list-item-label-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-list-list-item-label-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-list-list-item-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-list-list-item-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-list-list-item-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-focus-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--%NS%mat-list-list-item-supporting-text-color, var(--%NS%mat-sys-on-surface-variant));
  font-family: var(--%NS%mat-list-list-item-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-list-list-item-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-list-list-item-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-list-list-item-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  letter-spacing: var(--%NS%mat-list-list-item-supporting-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--%NS%mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-disabled-leading-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-disabled-label-text-color, var(--%NS%mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--%NS%mat-list-list-item-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--%NS%disabled::before {
  background-color: var(--%NS%mat-list-list-item-disabled-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--%NS%mat-list-list-item-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--%NS%mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--%NS%mat-list-list-item-leading-avatar-shape, var(--%NS%mat-sys-corner-full));
  background-color: var(--%NS%mat-list-list-item-leading-avatar-color, var(--%NS%mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--%NS%activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--%NS%mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--%NS%mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--%NS%mat-list-active-indicator-shape, var(--%NS%mat-sys-corner-full));
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-list-active-indicator-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--%NS%mat-list-active-indicator-color, var(--%NS%mat-sys-secondary-container));
}
`],encapsulation:2})})()}return t})();var fw=(()=>{class t extends yI{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(e){this._activated=At(e)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=de(t)))(r||t)}})();static \u0275cmp=(function(){let e=["unscopedContent"],i=["text"],r=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]];return N({type:t,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(s,l,c){if(s&1&&zi(c,pI,5)(c,hI,5)(c,gI,5),s&2){let d;ee(d=te())&&(l._lines=d),ee(d=te())&&(l._titles=d),ee(d=te())&&(l._meta=d)}},viewQuery:function(s,l){if(s&1&&lt(e,5)(i,5),s&2){let c;ee(c=te())&&(l._unscopedContent=c.first),ee(c=te())&&(l._itemText=c.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(s,l){s&2&&(ae("aria-current",l._getAriaCurrent()),q("mdc-list-item--activated",l.activated)("mdc-list-item--with-leading-avatar",l._avatars.length!==0)("mdc-list-item--with-leading-icon",l._icons.length!==0)("mdc-list-item--with-trailing-meta",l._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",l._hasBothLeadingAndTrailing())("_mat-animation-noopable",l._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[L],ngContentSelectors:["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"],decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(s,l){s&1&&(it(r),ie(0),R(1,"span",1),ie(2,1),ie(3,2),R(4,"span",2,0),ce("cdkObserveContent",function(){return l._updateItemLines(!0)}),ie(6,3),O()(),ie(7,4),ie(8,5),Ge(9,"div",3))},dependencies:[M_],encapsulation:2})})()}return t})();var hw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","mat-subheader",""],["","matSubheader",""]],hostAttrs:[1,"mat-mdc-subheader","mdc-list-group__subheader"]})}return t})();var wI=Math.pow(2,31)-1,Da=class{_overlayRef;instance;containerInstance;_afterDismissed=new x;_afterOpened=new x;_onAction=new x;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,wI))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},pw=new b("MatSnackBarData"),Kr=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},xI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),SI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),CI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),DI=(()=>{class t{snackBarRef=m(Da);data=m(pw);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,r){if(i&1){let o=Gn();R(0,"div",1)(1,"button",2),ce("click",function(){bn(o);let s=_e();return vn(s.action())}),Xe(2),O()()}if(i&2){let o=_e();F(2),Or(" ",o.data.action," ")}}return N({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(r,o){r&1&&(R(0,"div",0),Xe(1),O(),se(2,e,3,1,"div",1)),r&2&&(F(),Or(" ",o.data.message,`
`),F(),le(o.hasAction?2:-1))},dependencies:[Ur,xI,SI,CI],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})})()}return t})(),Xf="_mat-snack-bar-enter",Qf="_mat-snack-bar-exit",EI=(()=>{class t extends Xn{_ngZone=m(C);_elementRef=m(M);_changeDetectorRef=m(ct);_platform=m(re);_animationsDisabled=Ie();snackBarConfig=m(Kr);_document=m(V);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=m(T);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new x;_onExit=new x;_onEnter=new x;_animationState="void";_live;_label;_role;_liveElementId=m(Be).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===Qf?this._completeExit():e===Xf&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Je(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Xf)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Xf)},200)))}exit(){return this._destroyed?pi(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Je(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Qf)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Qf),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["label"];function i(r,o){}return N({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(o,a){if(o&1&&lt(on,7)(e,7),o&2){let s;ee(s=te())&&(a._portalOutlet=s.first),ee(s=te())&&(a._label=s.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(o,a){o&1&&ce("animationend",function(l){return a.onAnimationEnd(l.animationName)})("animationcancel",function(l){return a.onAnimationEnd(l.animationName)}),o&2&&q("mat-snack-bar-container-enter",a._animationState==="visible")("mat-snack-bar-container-exit",a._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!a._animationsDisabled)},features:[L],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(o,a){o&1&&(R(0,"div",1)(1,"div",2,0)(3,"div",3),et(4,i,0,0,"ng-template",4),O(),Ge(5,"div"),O()()),o&2&&(F(5),ae("aria-live",a._live)("role",a._role)("id",a._liveElementId))},dependencies:[on],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--%NS%mat-snack-bar-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-snack-bar-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-snack-bar-container-color, var(--%NS%mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--%NS%mat-snack-bar-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-snack-bar-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-snack-bar-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  line-height: var(--%NS%mat-snack-bar-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--%NS%mat-snack-bar-button-color, var(--%NS%mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --%NS%mat-button-text-state-layer-color: currentColor;
  --%NS%mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2,changeDetection:1})})()}return t})(),NI=new b("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Kr}),gw=(()=>{class t{_live=m(lf);_injector=m(T);_breakpointObserver=m(sa);_parentSnackBar=m(t,{optional:!0,skipSelf:!0});_defaultConfig=m(NI);_animationsDisabled=Ie();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=DI;snackBarContainerComponent=EI;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=y(y({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=T.create({parent:r||this._injector,providers:[{provide:Kr,useValue:i}]}),a=new kn(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=y(y(y({},new Kr),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new Da(a,o);if(e instanceof bt){let l=new Kn(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(l)}else{let l=this._createInjector(r,s),c=new kn(e,void 0,l),d=a.attachComponentPortal(c);s.instance=d.instance}return this._breakpointObserver.observe(Dn.HandsetPortrait).pipe(Ct(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Qn;i.direction=e.direction;let r=In(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,xa(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return T.create({parent:r||this._injector,providers:[{provide:Da,useValue:i},{provide:pw,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var kI=(t,n)=>n.action;function II(t,n){if(t&1&&(R(0,"h2",0),Xe(1),O()),t&2){let e=_e();F(),_t(e.data.title)}}function MI(t,n){if(t&1&&(R(0,"mat-dialog-content"),Xe(1),O()),t&2){let e=_e();F(),_t(e.data.message)}}function TI(t,n){if(t&1&&(R(0,"button",2),Xe(1),O()),t&2){let e=n.$implicit;Re("mat-dialog-close",e.action),F(),_t(e.label)}}function AI(t,n){if(t&1&&(R(0,"h3",0),Xe(1),O()),t&2){let e=_e();F(),_t(e.data.title)}}function RI(t,n){if(t&1){let e=Gn();R(0,"button",2),ce("click",function(){let r=bn(e).$index,o=_e();return vn(o.sheet.dismiss(r))}),Xe(1),O()}if(t&2){let e=n.$implicit;F(),_t(e)}}var OI=3,FI=-1,Jf=class t{data=m(Zf);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=N({type:t,selectors:[["hatter-dialog"]],decls:5,vars:2,consts:[["mat-dialog-title",""],["align","end"],["matButton","",3,"mat-dialog-close"]],template:function(e,i){e&1&&(se(0,II,2,1,"h2",0),se(1,MI,2,1,"mat-dialog-content"),R(2,"mat-dialog-actions",1),Cl(3,TI,2,2,"button",2,kI),O()),e&2&&(le(i.data.title?0:-1),F(),le(i.data.message?1:-1),F(2),Dl(i.data.buttons))},dependencies:[Ur,cw,ow,lw,sw],styles:["mat-dialog-content[_ngcontent-%COMP%]{white-space:pre-wrap}"]})},eh=class t{data=m(qf);sheet=m(Zr);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=N({type:t,selectors:[["hatter-sheet"]],decls:4,vars:1,consts:[["matSubheader",""],["mat-list-item",""],["mat-list-item","",3,"click"]],template:function(e,i){e&1&&(R(0,"mat-action-list"),se(1,AI,2,1,"h3",0),Cl(2,RI,2,1,"button",1,Om),O()),e&2&&(F(),le(i.data.title?1:-1),F(),Dl(i.data.items))},dependencies:[mw,fw,hw],encapsulation:2})},Xr=class t{dialogs=m(wc);sheets=m(Jy);notes=m(gw);open=0;note(n){this.notes.open(n,void 0,{duration:2e3})}notice(n){this.notes.open(n,"OK")}dialog(n,e,i){let r=i.map((a,s)=>({label:a,action:s})).filter(({label:a})=>a).reverse(),o=this.dialogs.open(Jf,{data:{title:n,message:e,buttons:r},autoFocus:"dialog"});return this.answer(o.afterClosed(),OI)}sheet(n,e){let i=this.sheets.open(eh,{data:{title:n,items:e},autoFocus:"dialog"});return this.answer(i.afterDismissed(),FI)}close(){return this.open?(this.dialogs.closeAll(),this.sheets.dismiss(),!0):!1}answer(n,e){return this.open++,new Promise(i=>n.subscribe(r=>{this.open--,i(r??e)}))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=ne({token:t,factory:t.\u0275fac,providedIn:"root"})};var bw=2147483647,LI=new Set([2,5,11]),PI=new Set([3,10]),vw=new Set([5,10,11]);function _w(t){return/^#[0-9a-fA-F]{8}$/.test(t)?`#${t.slice(3)}${t.slice(1,3)}`:t}var Qr=class t{appRef=m(vt);injector=m(Pe);popups=m(Xr);nodes=new Map;nextId=1;root;app;rendering=!1;rootElement;disk;callAudio=new jl;callVideo=new zl;constructor(){globalThis.hatter=this,matchMedia("(prefers-color-scheme: dark)").addEventListener("change",()=>this.redraw())}start(n){this.app=n,history.pushState(null,""),addEventListener("popstate",async()=>{this.popups.close()||await n.back()?history.pushState(null,""):history.back()}),addEventListener("visibilitychange",()=>{n.shown(document.visibilityState==="visible")}),addEventListener("resize",()=>{n.size(innerWidth,innerHeight).then(()=>this.redraw())})}redraw(){this.rendering||!this.app||(this.rendering=!0,requestAnimationFrame(()=>{this.rendering=!1,this.app?.render()}))}dialog(n,e,i,r,o,a){this.popups.dialog(e,i,[r,o,a]).then(s=>this.app?.dialog(n,s))}sheet(n,e,i){this.popups.sheet(e,i.split(`
`)).then(r=>this.app?.sheet(n,r))}pickFiles(n,e,i){this.disk&&c_(this.disk,n,e,i).then(async r=>{for(let{path:o,name:a,size:s}of r)await this.app?.picked(o,a,s)})}pickPicture(n,e){d_(n,e).then(i=>i&&this.app?.picture(i))}saveFile(n,e,i){this.disk&&u_(this.disk,n,e,i)}openFile(n){this.disk&&m_(this.disk,n)}imageSize(n,e,i,r){let o=this.disk?.file(n),a=o&&h_(o.data);return a?(e.setInt32(i,a[0],!0),e.setInt32(r,a[1],!0),!0):!1}callAudioStart(n,e){this.callAudio.start(n,e,i=>{this.app?.callFrame(i)})}callAudioStop(){this.callAudio.stop()}callAudioPlay(n,e,i){return this.callAudio.play(n,e,i)}callAudioLatency(){return this.callAudio.latency()}callAudioTone(n){this.callAudio.tone(n)}cameraStart(n){this.callVideo.startCamera(n,(e,i,r)=>{this.app?.cameraFrame(e,i,r)})}cameraStop(){this.callVideo.stopCamera()}showFrame(n,e,i,r){this.callVideo.show(n,e,i,r)}createNode(n){let e=Vy[n];if(!e)return 0;let i=Fr(e,{environmentInjector:this.injector});this.appRef.attachView(i.hostView),i.location.nativeElement.style.boxSizing="border-box";let r=this.nextId++,o={type:n,ref:i,layout:{width:NaN,height:NaN,weight:NaN,alignSelf:NaN,margin:NaN},handlers:new Map};return this.nodes.set(r,o),n===4&&(i.instance.onText=a=>{let s=o.handlers.get(1);s!==void 0&&this.app?.text(s,a)}),n===12&&(i.instance.onFlip=()=>{let a=o.handlers.get(0);a!==void 0&&this.app?.event(a)}),n===11&&(o.rows=new Map,o.spare=[]),vw.has(n)&&i.location.nativeElement.addEventListener("scroll",()=>this.scrolled(o),{passive:!0}),r}setStrProp(n,e,i){let r=this.nodes.get(n);if(!r)return;let o=r.ref.instance,a=o.element.style;switch(e){case 0:o instanceof Zn?o.setText(i):o.text.set(i);break;case 1:a.color=_w(i);break;case 2:o instanceof Zn&&o.hint.set(i);break;case 3:a.backgroundColor=_w(i);break;case 4:o instanceof Nn&&o.showResource(i);break;case 5:{let s=this.disk?.file(i);s&&this.showBytes(r,s.data);break}case 7:o instanceof Nn&&this.callVideo.watch(i,o);break;case 6:o instanceof _a&&o.load(i);break;default:console.warn(`Hatter: no string property ${e} in browsers`)}}setNumProp(n,e,i){let r=this.nodes.get(n);if(!r)return;let o=r.ref.instance,a=o.element,s=a.style,l=c=>`${c}px`;switch(e){case 0:s.fontSize=l(i);break;case 24:s.fontWeight=i&1?"700":"",s.fontFamily=i&2?"monospace":"",s.fontStyle=i&4?"italic":"";break;case 1:s.padding=i?l(i):"";break;case 30:o instanceof va&&o.checked.set(i!==0);break;case 25:s.paddingTop=s.paddingBottom=l(i);break;case 26:s.borderTopLeftRadius=Number.isNaN(i)?"":l(i);break;case 27:s.borderTopRightRadius=Number.isNaN(i)?"":l(i);break;case 28:s.borderBottomRightRadius=Number.isNaN(i)?"":l(i);break;case 29:s.borderBottomLeftRadius=Number.isNaN(i)?"":l(i);break;case 2:o instanceof Zn&&o.inputType.set(i);break;case 3:{let c=["","center","flex-end"][i]??"";r.type===2?s.alignItems=c:r.type===3?s.justifyContent=c:r.type!==5&&r.type!==9&&(s.textAlign=["","center","end"][i]??"");break}case 4:o instanceof Nn&&o.fit.set(["contain","cover","none"][i]??"contain");break;case 9:case 10:s.setProperty(e===9?"--hatter-x":"--hatter-y",l(i)),s.translate="var(--hatter-x, 0) var(--hatter-y, 0)";break;case 11:o instanceof Zn&&o.focus();break;case 12:s.pointerEvents=i===1?"none":"";break;case 13:case 14:case 15:case 16:case 17:{let c=["width","height","weight","alignSelf","margin"][e-13];r.layout[c]=i,this.applyLayout(r);break}case 18:s.maxWidth=i>=bw?"":l(i);break;case 19:s.borderRadius=i>0?l(i):"",s.overflow=i>0?"hidden":"";break;case 21:i>=bw?s.display=s.webkitLineClamp=s.webkitBoxOrient=s.overflow="":(s.display="-webkit-box",s.webkitLineClamp=String(i),s.webkitBoxOrient="vertical",s.overflow="hidden");break;case 20:i===1&&(r.type===5||r.type===11)&&this.keepAtEnd(r);break;case 22:r.type===11&&(r.count=i,queueMicrotask(()=>this.bindRows(n,r)));break;case 23:vw.has(r.type)&&a.scrollTo({top:a.scrollHeight,behavior:"smooth"});break;default:console.warn(`Hatter: no number property ${e} in browsers`)}}setImageData(n,e){let i=this.nodes.get(n);i&&this.showBytes(i,e)}showBytes(n,e){n.ref.instance instanceof Nn&&(n.objectUrl&&URL.revokeObjectURL(n.objectUrl),n.objectUrl=URL.createObjectURL(new Blob([e])),n.ref.instance.showPicture(n.objectUrl))}setHandler(n,e,i){let r=this.nodes.get(n);if(!r)return;let o=r.ref.instance.element,a=!r.handlers.has(e);if(r.handlers.set(e,i),!!a)switch((e===0||e===2)&&this.pressable(r),e){case 0:if(r.type===12||(o.addEventListener("click",s=>{s.stopPropagation(),this.app?.event(r.handlers.get(0))}),r.type===1))break;o.style.cursor="pointer",o.tabIndex=0,o.role="button",o.addEventListener("keydown",s=>{s.target!==o||s.key!=="Enter"&&s.key!==" "||(s.preventDefault(),o.click())});break;case 2:o.addEventListener("contextmenu",s=>{s.preventDefault(),s.stopPropagation(),this.app?.event(r.handlers.get(2))});break;case 3:r.position=2;break}}addChild(n,e){let i=this.nodes.get(n),r=this.nodes.get(e);!i||!r||(r.parent=i,i.ref.instance.slot.appendChild(r.ref.instance.element),this.applyLayout(r))}removeChild(n,e){let i=this.nodes.get(e);i&&(i.ref.instance.element.remove(),i.parent=void 0)}destroyNode(n){let e=this.nodes.get(n);e&&(this.nodes.delete(n),e.objectUrl&&URL.revokeObjectURL(e.objectUrl),e.ref.instance instanceof Nn&&e.ref.instance.stopStream(),e.ref.instance.element.remove(),e.ref.destroy())}setRoot(n){let e=this.nodes.get(n);if(!e||!this.rootElement)return;this.root?.ref.instance.element.remove(),this.root=e,e.parent=void 0;let i=e.ref.instance.element.style;i.width=i.height="100%",this.rootElement.replaceChildren(e.ref.instance.element)}clear(){for(let n of[...this.nodes.keys()])this.destroyNode(n);this.root=void 0}darkMode(){return matchMedia("(prefers-color-scheme: dark)").matches}clock24(){let n=new Intl.DateTimeFormat(void 0,{hour:"numeric"}).resolvedOptions();return n.hourCycle==="h23"||n.hourCycle==="h24"}copyText(n){navigator.clipboard?.writeText(n).then(()=>this.popups.note("Copied"))}openUri(n){window.open(n,"_blank","noopener")}pressable(n){if(n.pressable)return;n.pressable=!0;let e=n.ref.instance.element;e.style.userSelect="none",e.style.setProperty("-webkit-touch-callout","none"),e.addEventListener("mousedown",i=>{i.target.closest("input, textarea")||i.preventDefault()})}applyLayout(n){let e=n.ref.instance.element.style,{width:i,height:r,weight:o,alignSelf:a,margin:s}=n.layout,l=n.parent?.type,c=u=>u===-1?"100%":`${u}px`;e.width=isNaN(i)?"":c(i),e.height=isNaN(r)?"":c(r),n.ref.instance.element.classList.toggle("sized",!isNaN(i)||!isNaN(r)),e.flexGrow=isNaN(o)?"":String(o),e.flexShrink=isNaN(o)?"":"1000",e.minWidth=e.minHeight=isNaN(o)?"":"0",e.margin=isNaN(s)?"":`${s}px`,e.alignSelf=e.justifySelf="",e.gridArea=l===9?"1 / 1":"";let d=["flex-start","center","flex-end","stretch"];if(l!==void 0&&LI.has(l))e.alignSelf=isNaN(a)?isNaN(i)?"stretch":"":d[a];else if(l!==void 0&&PI.has(l))e.alignSelf=isNaN(a)?"":d[a];else if(l===9){let u=isNaN(a)?"stretch":["start","center","end","stretch"][a];e.alignSelf=e.justifySelf=u}}keepAtEnd(n){if(n.pinned!==void 0)return;let e=n.ref.instance.element,i=n.ref.instance.slot;i.style.minHeight="100%",i.style.justifyContent="flex-end",n.pinned=!0,e.addEventListener("scroll",()=>{n.pinned=e.scrollTop+e.clientHeight>=e.scrollHeight-1},{passive:!0}),new ResizeObserver(()=>{n.pinned&&(e.scrollTop=e.scrollHeight)}).observe(i)}scrolled(n){let e=n.handlers.get(3);if(e===void 0)return;let i=n.ref.instance.element,r=i.scrollTop+i.clientHeight>=i.scrollHeight-1?2:i.scrollTop<=0?0:1;r!==n.position&&(n.position=r,this.app?.scroll(e,r))}bindRows(n,e){let i=this.app;if(!i||!this.nodes.has(n)||e.count===void 0||!e.rows||!e.spare)return;let r=Array.from({length:e.count},(c,d)=>i.key(n,d)),o=new Set(r),a=[...e.spare,...[...e.rows].filter(([c])=>!o.has(c)).map(([,c])=>c)],s=new Map,l=[];r.forEach((c,d)=>{let u=i.bind(n,e.rows.get(c)??a.pop()??0,d),h=this.nodes.get(u);h&&(s.set(c,u),h.parent=e,this.applyLayout(h),l.push(h.ref.instance.element))}),e.rows=s,e.spare=a,e.ref.instance.slot.replaceChildren(...l)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=ne({token:t,factory:t.\u0275fac,providedIn:"root"})};var xc=class t{constructor(){m(Qr).rootElement=m(M).nativeElement}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=N({type:t,selectors:[["hatter-root"]],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;height:100%}"]})};var Ea=class t{static read_bytes(n,e){let i=new t;return i.buf=n.getUint32(e,!0),i.buf_len=n.getUint32(e+4,!0),i}static read_bytes_array(n,e,i){let r=[];for(let o=0;o<i;o++)r.push(t.read_bytes(n,e+8*o));return r}},Na=class t{static read_bytes(n,e){let i=new t;return i.buf=n.getUint32(e,!0),i.buf_len=n.getUint32(e+4,!0),i}static read_bytes_array(n,e,i){let r=[];for(let o=0;o<i;o++)r.push(t.read_bytes(n,e+8*o));return r}},nh=0,ih=1,Ec=2;var rh=2,xt=3,eo=4;var Jr=class{head_length(){return 24}name_length(){return this.dir_name.byteLength}write_head_bytes(n,e){n.setBigUint64(e,this.d_next,!0),n.setBigUint64(e+8,this.d_ino,!0),n.setUint32(e+16,this.dir_name.length,!0),n.setUint8(e+20,this.d_type)}write_name_bytes(n,e,i){n.set(this.dir_name.slice(0,Math.min(this.dir_name.byteLength,i)),e)}constructor(n,e,i,r){let o=new TextEncoder().encode(i);this.d_next=n,this.d_ino=e,this.d_namlen=o.byteLength,this.d_type=r,this.dir_name=o}};var oh=1;var Xi=class{write_bytes(n,e){n.setUint8(e,this.fs_filetype),n.setUint16(e+2,this.fs_flags,!0),n.setBigUint64(e+8,this.fs_rights_base,!0),n.setBigUint64(e+16,this.fs_rights_inherited,!0)}constructor(n,e){this.fs_rights_base=0n,this.fs_rights_inherited=0n,this.fs_filetype=n,this.fs_flags=e}};var Nc=1,to=2,ah=4,kc=8,Qi=class{write_bytes(n,e){n.setBigUint64(e,this.dev,!0),n.setBigUint64(e+8,this.ino,!0),n.setUint8(e+16,this.filetype),n.setBigUint64(e+24,this.nlink,!0),n.setBigUint64(e+32,this.size,!0),n.setBigUint64(e+38,this.atim,!0),n.setBigUint64(e+46,this.mtim,!0),n.setBigUint64(e+52,this.ctim,!0)}constructor(n,e,i){this.dev=0n,this.nlink=0n,this.atim=0n,this.mtim=0n,this.ctim=0n,this.ino=n,this.filetype=e,this.size=i}},yw=0;var ww=1,Sc=class t{static read_bytes(n,e){return new t(n.getBigUint64(e,!0),n.getUint8(e+8),n.getUint32(e+16,!0),n.getBigUint64(e+24,!0),n.getUint16(e+36,!0))}constructor(n,e,i,r,o){this.userdata=n,this.eventtype=e,this.clockid=i,this.timeout=r,this.flags=o}},Cc=class{write_bytes(n,e){n.setBigUint64(e,this.userdata,!0),n.setUint16(e+8,this.error,!0),n.setUint8(e+10,this.eventtype)}constructor(n,e,i){this.userdata=n,this.error=e,this.eventtype=i}};var VI=0,th=class{write_bytes(n,e){n.setUint32(e,this.pr_name.byteLength,!0)}constructor(n){this.pr_name=new TextEncoder().encode(n)}},Dc=class t{static dir(n){let e=new t;return e.tag=VI,e.inner=new th(n),e}write_bytes(n,e){n.setUint32(e,this.tag,!0),this.inner.write_bytes(n,e+4)}};var BI=class{enable(n){this.log=jI(n===void 0?!0:n,this.prefix)}get enabled(){return this.isEnabled}constructor(n){this.isEnabled=n,this.prefix="wasi:",this.enable(n)}};function jI(t,n){return t?console.log.bind(console,"%c%s","color: #265BA0",n):()=>{}}var He=new BI(!1);var Ia=class extends Error{constructor(n){super("exit with exit code "+n),this.code=n}},ch=class{start(n){this.inst=n;try{return n.exports._start(),0}catch(e){if(e instanceof Ia)return e.code;throw e}}initialize(n){this.inst=n,n.exports._initialize&&n.exports._initialize()}constructor(n,e,i,r={}){this.args=[],this.env=[],this.fds=[],He.enable(r.debug),this.args=n,this.env=e,this.fds=i;let o=this;this.wasiImport={args_sizes_get(a,s){let l=new DataView(o.inst.exports.memory.buffer);l.setUint32(a,o.args.length,!0);let c=0;for(let d of o.args)c+=d.length+1;return l.setUint32(s,c,!0),He.log(l.getUint32(a,!0),l.getUint32(s,!0)),0},args_get(a,s){let l=new DataView(o.inst.exports.memory.buffer),c=new Uint8Array(o.inst.exports.memory.buffer),d=s;for(let u=0;u<o.args.length;u++){l.setUint32(a,s,!0),a+=4;let h=new TextEncoder().encode(o.args[u]);c.set(h,s),l.setUint8(s+h.length,0),s+=h.length+1}return He.enabled&&He.log(new TextDecoder("utf-8").decode(c.slice(d,s))),0},environ_sizes_get(a,s){let l=new DataView(o.inst.exports.memory.buffer);l.setUint32(a,o.env.length,!0);let c=0;for(let d of o.env)c+=new TextEncoder().encode(d).length+1;return l.setUint32(s,c,!0),He.log(l.getUint32(a,!0),l.getUint32(s,!0)),0},environ_get(a,s){let l=new DataView(o.inst.exports.memory.buffer),c=new Uint8Array(o.inst.exports.memory.buffer),d=s;for(let u=0;u<o.env.length;u++){l.setUint32(a,s,!0),a+=4;let h=new TextEncoder().encode(o.env[u]);c.set(h,s),l.setUint8(s+h.length,0),s+=h.length+1}return He.enabled&&He.log(new TextDecoder("utf-8").decode(c.slice(d,s))),0},clock_res_get(a,s){let l;switch(a){case 1:{l=5000n;break}case 0:{l=1000000n;break}default:return 52}return new DataView(o.inst.exports.memory.buffer).setBigUint64(s,l,!0),0},clock_time_get(a,s,l){let c=new DataView(o.inst.exports.memory.buffer);if(a===0)c.setBigUint64(l,BigInt(new Date().getTime())*1000000n,!0);else if(a==1){let d;try{d=BigInt(Math.round(performance.now()*1e6))}catch{d=0n}c.setBigUint64(l,d,!0)}else c.setBigUint64(l,0n,!0);return 0},fd_advise(a,s,l,c){return o.fds[a]!=null?0:8},fd_allocate(a,s,l){return o.fds[a]!=null?o.fds[a].fd_allocate(s,l):8},fd_close(a){if(o.fds[a]!=null){let s=o.fds[a].fd_close();return o.fds[a]=void 0,s}else return 8},fd_datasync(a){return o.fds[a]!=null?o.fds[a].fd_sync():8},fd_fdstat_get(a,s){if(o.fds[a]!=null){let{ret:l,fdstat:c}=o.fds[a].fd_fdstat_get();return c?.write_bytes(new DataView(o.inst.exports.memory.buffer),s),l}else return 8},fd_fdstat_set_flags(a,s){return o.fds[a]!=null?o.fds[a].fd_fdstat_set_flags(s):8},fd_fdstat_set_rights(a,s,l){return o.fds[a]!=null?o.fds[a].fd_fdstat_set_rights(s,l):8},fd_filestat_get(a,s){if(o.fds[a]!=null){let{ret:l,filestat:c}=o.fds[a].fd_filestat_get();return c?.write_bytes(new DataView(o.inst.exports.memory.buffer),s),l}else return 8},fd_filestat_set_size(a,s){return o.fds[a]!=null?o.fds[a].fd_filestat_set_size(s):8},fd_filestat_set_times(a,s,l,c){return o.fds[a]!=null?o.fds[a].fd_filestat_set_times(s,l,c):8},fd_pread(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),h=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=Ea.read_bytes_array(u,s,l),p=0;for(let g of f){let{ret:_,data:w}=o.fds[a].fd_pread(g.buf_len,c);if(_!=0)return u.setUint32(d,p,!0),_;if(h.set(w,g.buf),p+=w.length,c+=BigInt(w.length),w.length!=g.buf_len)break}return u.setUint32(d,p,!0),0}else return 8},fd_prestat_get(a,s){let l=new DataView(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let{ret:c,prestat:d}=o.fds[a].fd_prestat_get();return d?.write_bytes(l,s),c}else return 8},fd_prestat_dir_name(a,s,l){if(o.fds[a]!=null){let{ret:c,prestat:d}=o.fds[a].fd_prestat_get();if(d==null)return c;let u=d.inner.pr_name;return new Uint8Array(o.inst.exports.memory.buffer).set(u.slice(0,l),s),u.byteLength>l?37:0}else return 8},fd_pwrite(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),h=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=Na.read_bytes_array(u,s,l),p=0;for(let g of f){let _=h.slice(g.buf,g.buf+g.buf_len),{ret:w,nwritten:U}=o.fds[a].fd_pwrite(_,c);if(w!=0)return u.setUint32(d,p,!0),w;if(p+=U,c+=BigInt(U),U!=_.byteLength)break}return u.setUint32(d,p,!0),0}else return 8},fd_read(a,s,l,c){let d=new DataView(o.inst.exports.memory.buffer),u=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let h=Ea.read_bytes_array(d,s,l),f=0;for(let p of h){let{ret:g,data:_}=o.fds[a].fd_read(p.buf_len);if(g!=0)return d.setUint32(c,f,!0),g;if(u.set(_,p.buf),f+=_.length,_.length!=p.buf_len)break}return d.setUint32(c,f,!0),0}else return 8},fd_readdir(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),h=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=0;for(;;){let{ret:p,dirent:g}=o.fds[a].fd_readdir_single(c);if(p!=0)return u.setUint32(d,f,!0),p;if(g==null)break;if(l-f<g.head_length()){f=l;break}let _=new ArrayBuffer(g.head_length());if(g.write_head_bytes(new DataView(_),0),h.set(new Uint8Array(_).slice(0,Math.min(_.byteLength,l-f)),s),s+=g.head_length(),f+=g.head_length(),l-f<g.name_length()){f=l;break}g.write_name_bytes(h,s,l-f),s+=g.name_length(),f+=g.name_length(),c=g.d_next}return u.setUint32(d,f,!0),0}else return 8},fd_renumber(a,s){if(o.fds[a]!=null&&o.fds[s]!=null){let l=o.fds[s].fd_close();return l!=0?l:(o.fds[s]=o.fds[a],o.fds[a]=void 0,0)}else return 8},fd_seek(a,s,l,c){let d=new DataView(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let{ret:u,offset:h}=o.fds[a].fd_seek(s,l);return d.setBigInt64(c,h,!0),u}else return 8},fd_sync(a){return o.fds[a]!=null?o.fds[a].fd_sync():8},fd_tell(a,s){let l=new DataView(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let{ret:c,offset:d}=o.fds[a].fd_tell();return l.setBigUint64(s,d,!0),c}else return 8},fd_write(a,s,l,c){let d=new DataView(o.inst.exports.memory.buffer),u=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let h=Na.read_bytes_array(d,s,l),f=0;for(let p of h){let g=u.slice(p.buf,p.buf+p.buf_len),{ret:_,nwritten:w}=o.fds[a].fd_write(g);if(_!=0)return d.setUint32(c,f,!0),_;if(f+=w,w!=g.byteLength)break}return d.setUint32(c,f,!0),0}else return 8},path_create_directory(a,s,l){let c=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let d=new TextDecoder("utf-8").decode(c.slice(s,s+l));return o.fds[a].path_create_directory(d)}else return 8},path_filestat_get(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),h=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=new TextDecoder("utf-8").decode(h.slice(l,l+c)),{ret:p,filestat:g}=o.fds[a].path_filestat_get(s,f);return g?.write_bytes(u,d),p}else return 8},path_filestat_set_times(a,s,l,c,d,u,h){let f=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let p=new TextDecoder("utf-8").decode(f.slice(l,l+c));return o.fds[a].path_filestat_set_times(s,p,d,u,h)}else return 8},path_link(a,s,l,c,d,u,h){let f=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null&&o.fds[d]!=null){let p=new TextDecoder("utf-8").decode(f.slice(l,l+c)),g=new TextDecoder("utf-8").decode(f.slice(u,u+h)),{ret:_,inode_obj:w}=o.fds[a].path_lookup(p,s);return w==null?_:o.fds[d].path_link(g,w,!1)}else return 8},path_open(a,s,l,c,d,u,h,f,p){let g=new DataView(o.inst.exports.memory.buffer),_=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let w=new TextDecoder("utf-8").decode(_.slice(l,l+c));He.log(w);let{ret:U,fd_obj:$t}=o.fds[a].path_open(s,w,d,u,h,f);if(U!=0)return U;o.fds.push($t);let Oc=o.fds.length-1;return g.setUint32(p,Oc,!0),0}else return 8},path_readlink(a,s,l,c,d,u){let h=new DataView(o.inst.exports.memory.buffer),f=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let p=new TextDecoder("utf-8").decode(f.slice(s,s+l));He.log(p);let{ret:g,data:_}=o.fds[a].path_readlink(p);if(_!=null){let w=new TextEncoder().encode(_);if(w.length>d)return h.setUint32(u,0,!0),8;f.set(w,c),h.setUint32(u,w.length,!0)}return g}else return 8},path_remove_directory(a,s,l){let c=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let d=new TextDecoder("utf-8").decode(c.slice(s,s+l));return o.fds[a].path_remove_directory(d)}else return 8},path_rename(a,s,l,c,d,u){let h=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null&&o.fds[c]!=null){let f=new TextDecoder("utf-8").decode(h.slice(s,s+l)),p=new TextDecoder("utf-8").decode(h.slice(d,d+u)),{ret:g,inode_obj:_}=o.fds[a].path_unlink(f);if(_==null)return g;if(g=o.fds[c].path_link(p,_,!0),g!=0&&o.fds[a].path_link(f,_,!0)!=0)throw"path_link should always return success when relinking an inode back to the original place";return g}else return 8},path_symlink(a,s,l,c,d){let u=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[l]!=null){let h=new TextDecoder("utf-8").decode(u.slice(a,a+s)),f=new TextDecoder("utf-8").decode(u.slice(c,c+d));return 58}else return 8},path_unlink_file(a,s,l){let c=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let d=new TextDecoder("utf-8").decode(c.slice(s,s+l));return o.fds[a].path_unlink_file(d)}else return 8},poll_oneoff(a,s,l){if(l===0)return 28;if(l>1)return He.log("poll_oneoff: only a single subscription is supported"),58;let c=new DataView(o.inst.exports.memory.buffer),d=Sc.read_bytes(c,a),u=d.eventtype,h=d.clockid,f=d.timeout;if(u!==yw)return He.log("poll_oneoff: only clock subscriptions are supported"),58;let p;if(h===1)p=()=>BigInt(Math.round(performance.now()*1e6));else if(h===0)p=()=>BigInt(new Date().getTime())*1000000n;else return 28;let g=(d.flags&ww)!==0?f:p()+f;for(;g>p(););return new Cc(d.userdata,0,u).write_bytes(c,s),0},proc_exit(a){throw new Ia(a)},proc_raise(a){throw"raised signal "+a},sched_yield(){},random_get(a,s){let l=new Uint8Array(o.inst.exports.memory.buffer).subarray(a,a+s);if("crypto"in globalThis&&(typeof SharedArrayBuffer>"u"||!(o.inst.exports.memory.buffer instanceof SharedArrayBuffer)))for(let c=0;c<s;c+=65536)crypto.getRandomValues(l.subarray(c,c+65536));else for(let c=0;c<s;c++)l[c]=Math.random()*256|0},sock_recv(a,s,l){throw"sockets not supported"},sock_send(a,s,l){throw"sockets not supported"},sock_shutdown(a,s){throw"sockets not supported"},sock_accept(a,s){throw"sockets not supported"}}}};var ti=class{fd_allocate(n,e){return 58}fd_close(){return 0}fd_fdstat_get(){return{ret:58,fdstat:null}}fd_fdstat_set_flags(n){return 58}fd_fdstat_set_rights(n,e){return 58}fd_filestat_get(){return{ret:58,filestat:null}}fd_filestat_set_size(n){return 58}fd_filestat_set_times(n,e,i){return 58}fd_pread(n,e){return{ret:58,data:new Uint8Array}}fd_prestat_get(){return{ret:58,prestat:null}}fd_pwrite(n,e){return{ret:58,nwritten:0}}fd_read(n){return{ret:58,data:new Uint8Array}}fd_readdir_single(n){return{ret:58,dirent:null}}fd_seek(n,e){return{ret:58,offset:0n}}fd_sync(){return 0}fd_tell(){return{ret:58,offset:0n}}fd_write(n){return{ret:58,nwritten:0}}path_create_directory(n){return 58}path_filestat_get(n,e){return{ret:58,filestat:null}}path_filestat_set_times(n,e,i,r,o){return 58}path_link(n,e,i){return 58}path_unlink(n){return{ret:58,inode_obj:null}}path_lookup(n,e){return{ret:58,inode_obj:null}}path_open(n,e,i,r,o,a){return{ret:54,fd_obj:null}}path_readlink(n){return{ret:58,data:null}}path_remove_directory(n){return 58}path_rename(n,e,i){return 58}path_unlink_file(n){return 58}},an=class t{static issue_ino(){return t.next_ino++}static root_ino(){return 0n}constructor(){this.ino=t.issue_ino()}};an.next_ino=1n;var ii=class extends ti{fd_allocate(n,e){if(!(this.file.size>n+e)){let i=new Uint8Array(Number(n+e));i.set(this.file.data,0),this.file.data=i}return 0}fd_fdstat_get(){return{ret:0,fdstat:new Xi(eo,0)}}fd_filestat_set_size(n){if(this.file.size>n)this.file.data=new Uint8Array(this.file.data.buffer.slice(0,Number(n)));else{let e=new Uint8Array(Number(n));e.set(this.file.data,0),this.file.data=e}return 0}fd_read(n){let e=this.file.data.slice(Number(this.file_pos),Number(this.file_pos+BigInt(n)));return this.file_pos+=BigInt(e.length),{ret:0,data:e}}fd_pread(n,e){return{ret:0,data:this.file.data.slice(Number(e),Number(e+BigInt(n)))}}fd_seek(n,e){let i;switch(e){case nh:i=n;break;case ih:i=this.file_pos+n;break;case Ec:i=BigInt(this.file.data.byteLength)+n;break;default:return{ret:28,offset:0n}}return i<0?{ret:28,offset:0n}:(this.file_pos=i,{ret:0,offset:this.file_pos})}fd_tell(){return{ret:0,offset:this.file_pos}}fd_write(n){if(this.file.readonly)return{ret:8,nwritten:0};if(this.file_pos+BigInt(n.byteLength)>this.file.size){let e=this.file.data;this.file.data=new Uint8Array(Number(this.file_pos+BigInt(n.byteLength))),this.file.data.set(e)}return this.file.data.set(n,Number(this.file_pos)),this.file_pos+=BigInt(n.byteLength),{ret:0,nwritten:n.byteLength}}fd_pwrite(n,e){if(this.file.readonly)return{ret:8,nwritten:0};if(e+BigInt(n.byteLength)>this.file.size){let i=this.file.data;this.file.data=new Uint8Array(Number(e+BigInt(n.byteLength))),this.file.data.set(i)}return this.file.data.set(n,Number(e)),{ret:0,nwritten:n.byteLength}}fd_filestat_get(){return{ret:0,filestat:this.file.stat()}}constructor(n){super(),this.file_pos=0n,this.file=n}},Ma=class extends ti{fd_seek(n,e){return{ret:8,offset:0n}}fd_tell(){return{ret:8,offset:0n}}fd_allocate(n,e){return 8}fd_fdstat_get(){return{ret:0,fdstat:new Xi(xt,0)}}fd_readdir_single(n){if(He.enabled&&(He.log("readdir_single",n),He.log(n,this.dir.contents.keys())),n==0n)return{ret:0,dirent:new Jr(1n,this.dir.ino,".",xt)};if(n==1n)return{ret:0,dirent:new Jr(2n,this.dir.parent_ino(),"..",xt)};if(n>=BigInt(this.dir.contents.size)+2n)return{ret:0,dirent:null};let[e,i]=Array.from(this.dir.contents.entries())[Number(n-2n)];return{ret:0,dirent:new Jr(n+1n,i.ino,e,i.stat().filetype)}}path_filestat_get(n,e){let{ret:i,path:r}=ni.from(e);if(r==null)return{ret:i,filestat:null};let{ret:o,entry:a}=this.dir.get_entry_for_path(r);return a==null?{ret:o,filestat:null}:{ret:0,filestat:a.stat()}}path_lookup(n,e){let{ret:i,path:r}=ni.from(n);if(r==null)return{ret:i,inode_obj:null};let{ret:o,entry:a}=this.dir.get_entry_for_path(r);return a==null?{ret:o,inode_obj:null}:{ret:0,inode_obj:a}}path_open(n,e,i,r,o,a){let{ret:s,path:l}=ni.from(e);if(l==null)return{ret:s,fd_obj:null};let{ret:c,entry:d}=this.dir.get_entry_for_path(l);if(d==null){if(c!=44)return{ret:c,fd_obj:null};if((i&Nc)==Nc){let{ret:u,entry:h}=this.dir.create_entry_for_path(e,(i&to)==to);if(h==null)return{ret:u,fd_obj:null};d=h}else return{ret:44,fd_obj:null}}else if((i&ah)==ah)return{ret:20,fd_obj:null};return(i&to)==to&&d.stat().filetype!==xt?{ret:54,fd_obj:null}:d.path_open(i,r,a)}path_create_directory(n){return this.path_open(0,n,Nc|to,0n,0n,0).ret}path_link(n,e,i){let{ret:r,path:o}=ni.from(n);if(o==null)return r;if(o.is_dir)return 44;let{ret:a,parent_entry:s,filename:l,entry:c}=this.dir.get_parent_dir_and_entry_for_path(o,!0);if(s==null||l==null)return a;if(c!=null){let d=e.stat().filetype==xt,u=c.stat().filetype==xt;if(d&&u)if(i&&c instanceof ut){if(c.contents.size!=0)return 55}else return 20;else{if(d&&!u)return 54;if(!d&&u)return 31;if(!(e.stat().filetype==eo&&c.stat().filetype==eo))return 20}}return!i&&e.stat().filetype==xt?63:(s.contents.set(l,e),0)}path_unlink(n){let{ret:e,path:i}=ni.from(n);if(i==null)return{ret:e,inode_obj:null};let{ret:r,parent_entry:o,filename:a,entry:s}=this.dir.get_parent_dir_and_entry_for_path(i,!0);return o==null||a==null?{ret:r,inode_obj:null}:s==null?{ret:44,inode_obj:null}:(o.contents.delete(a),{ret:0,inode_obj:s})}path_unlink_file(n){let{ret:e,path:i}=ni.from(n);if(i==null)return e;let{ret:r,parent_entry:o,filename:a,entry:s}=this.dir.get_parent_dir_and_entry_for_path(i,!1);return o==null||a==null||s==null?r:s.stat().filetype===xt?31:(o.contents.delete(a),0)}path_remove_directory(n){let{ret:e,path:i}=ni.from(n);if(i==null)return e;let{ret:r,parent_entry:o,filename:a,entry:s}=this.dir.get_parent_dir_and_entry_for_path(i,!1);return o==null||a==null||s==null?r:!(s instanceof ut)||s.stat().filetype!==xt?54:s.contents.size!==0?55:o.contents.delete(a)?0:44}fd_filestat_get(){return{ret:0,filestat:this.dir.stat()}}fd_filestat_set_size(n){return 8}fd_read(n){return{ret:8,data:new Uint8Array}}fd_pread(n,e){return{ret:8,data:new Uint8Array}}fd_write(n){return{ret:8,nwritten:0}}fd_pwrite(n,e){return{ret:8,nwritten:0}}constructor(n){super(),this.dir=n}},no=class extends Ma{fd_prestat_get(){return{ret:0,prestat:Dc.dir(this.prestat_name)}}constructor(n,e){super(new ut(e)),this.prestat_name=n}},Ot=class extends an{path_open(n,e,i){if(this.readonly&&(e&BigInt(64))==BigInt(64))return{ret:63,fd_obj:null};if((n&kc)==kc){if(this.readonly)return{ret:63,fd_obj:null};this.data=new Uint8Array([])}let r=new ii(this);return i&oh&&r.fd_seek(0n,Ec),{ret:0,fd_obj:r}}get size(){return BigInt(this.data.byteLength)}stat(){return new Qi(this.ino,eo,this.size)}constructor(n,e){super(),this.data=new Uint8Array(n),this.readonly=!!e?.readonly}},ni=class Dw{static from(n){let e=new Dw;if(e.is_dir=n.endsWith("/"),n.startsWith("/"))return{ret:76,path:null};if(n.includes("\0"))return{ret:28,path:null};for(let i of n.split("/"))if(!(i===""||i===".")){if(i===".."){if(e.parts.pop()==null)return{ret:76,path:null};continue}e.parts.push(i)}return{ret:0,path:e}}to_path_string(){let n=this.parts.join("/");return this.is_dir&&(n+="/"),n}constructor(){this.parts=[],this.is_dir=!1}},ut=class t extends an{parent_ino(){return this.parent==null?an.root_ino():this.parent.ino}path_open(n,e,i){return{ret:0,fd_obj:new Ma(this)}}stat(){return new Qi(this.ino,xt,0n)}get_entry_for_path(n){let e=this;for(let i of n.parts){if(!(e instanceof t))return{ret:54,entry:null};let r=e.contents.get(i);if(r!==void 0)e=r;else return He.log(i),{ret:44,entry:null}}return n.is_dir&&e.stat().filetype!=xt?{ret:54,entry:null}:{ret:0,entry:e}}get_parent_dir_and_entry_for_path(n,e){let i=n.parts.pop();if(i===void 0)return{ret:28,parent_entry:null,filename:null,entry:null};let{ret:r,entry:o}=this.get_entry_for_path(n);if(o==null)return{ret:r,parent_entry:null,filename:null,entry:null};if(!(o instanceof t))return{ret:54,parent_entry:null,filename:null,entry:null};let a=o.contents.get(i);return a===void 0?e?{ret:0,parent_entry:o,filename:i,entry:null}:{ret:44,parent_entry:null,filename:null,entry:null}:n.is_dir&&a.stat().filetype!=xt?{ret:54,parent_entry:null,filename:null,entry:null}:{ret:0,parent_entry:o,filename:i,entry:a}}create_entry_for_path(n,e){let{ret:i,path:r}=ni.from(n);if(r==null)return{ret:i,entry:null};let{ret:o,parent_entry:a,filename:s,entry:l}=this.get_parent_dir_and_entry_for_path(r,!0);if(a==null||s==null)return{ret:o,entry:null};if(l!=null)return{ret:20,entry:null};He.log("create",r);let c;return e?c=new t(new Map):c=new Ot(new ArrayBuffer(0)),a.contents.set(s,c),l=c,{ret:0,entry:l}}constructor(n){super(),this.parent=null,n instanceof Array?this.contents=new Map(n):this.contents=n;for(let e of this.contents.values())e instanceof t&&(e.parent=this)}},io=class t extends ti{fd_filestat_get(){return{ret:0,filestat:new Qi(this.ino,rh,BigInt(0))}}fd_fdstat_get(){let n=new Xi(rh,0);return n.fs_rights_base=BigInt(64),{ret:0,fdstat:n}}fd_write(n){return this.write(n),{ret:0,nwritten:n.byteLength}}static lineBuffered(n){let e=new TextDecoder("utf-8",{fatal:!1}),i="";return new t(r=>{i+=e.decode(r,{stream:!0});let o=i.split(`
`);for(let[a,s]of o.entries())a<o.length-1?n(s):i=s})}constructor(n){super(),this.ino=an.issue_ino(),this.write=n}};var Ac=class{constructor(n,e){this.roots=n;this.changed=e}roots;changed;file(n){let[e,i]=this.place(n)??[],r=i===void 0?void 0:e?.get(i);return r instanceof Ot?r:void 0}add(n,e){let[i,r]=this.place(n,!0)??[];if(!i||r===void 0)throw new Error(`No directory for ${n}`);i.set(r,new Ot(e)),this.changed()}remove(n){let[e,i]=this.place(n)??[];i!==void 0&&e?.delete(i)&&this.changed()}place(n,e=!1){for(let[i,r]of this.roots){if(!n.startsWith(i+"/"))continue;let o=n.slice(i.length+1).split("/"),a=o.pop(),s=r;for(let l of o){let c=s.get(l);if(!(c instanceof ut)&&e&&(c=new ut(new Map),s.set(l,c)),!(c instanceof ut))return;s=c.contents}return[s,a]}}};var UI=1e3,$I=["fd_write","fd_pwrite","fd_allocate","fd_filestat_set_size"],GI=["path_open","path_unlink_file","path_remove_directory","path_rename","path_create_directory","path_link","path_symlink"],Rc=class t{constructor(n){this.dir=n}dir;root=new Map;changed=new Set;saved=new Map;next=0;timer;saving=Promise.resolve();get kept(){return this.dir!==void 0}static async open(n){let e;try{e=await navigator.storage.getDirectory()}catch(o){return console.warn("The browser keeps no files for this page:",o),new t}let i=await e.getDirectoryHandle("hatter",{create:!0}),r=new t(await i.getDirectoryHandle(n,{create:!0}));return await r.load(),navigator.storage.persist?.(),r}watch(n){if(!this.kept)return;let e=n.wasiImport;for(let i of $I){let r=e[i];e[i]=(...o)=>{let a=n.fds[o[0]];return a instanceof ii&&this.changed.add(a.file),this.saveSoon(),r(...o)}}for(let i of GI){let r=e[i];e[i]=(...o)=>(this.saveSoon(),r(...o))}addEventListener("visibilitychange",()=>{document.visibilityState==="hidden"&&this.now()}),addEventListener("pagehide",()=>this.now())}saveSoon(){this.kept&&(this.timer??=setTimeout(()=>this.now(),UI))}now(){clearTimeout(this.timer),this.timer=void 0,this.saving=this.saving.then(()=>this.save()).catch(n=>console.error("Saving files failed:",n))}async load(){let n=await this.readManifest();if(n){this.next=n.next;for(let e of n.dirs)this.directory(e.split("/"));for(let[e,i]of Object.entries(n.files)){let r=await this.store.getFileHandle(i),o=new Uint8Array(await(await r.getFile()).arrayBuffer()),a=e.split("/"),s=new Ot(o);this.directory(a.slice(0,-1)).set(a[a.length-1],s),this.saved.set(e,{file:s,size:o.length,blob:i})}}}async readManifest(){try{let n=await this.store.getFileHandle("manifest.json");return JSON.parse(await(await n.getFile()).text())}catch{return}}get store(){if(!this.dir)throw new Error("These files are not kept.");return this.dir}directory(n){let e=this.root;for(let i of n){let r=e.get(i);r instanceof ut||(r=new ut(new Map),e.set(i,r)),e=r.contents}return e}walk(){let n=new Map,e=[],i=(r,o)=>{for(let[a,s]of r){let l=o+a;s instanceof Ot?n.set(l,s):s instanceof ut&&(e.push(l),i(s.contents,l+"/"))}};return i(this.root,""),{files:n,dirs:e}}async save(){let{files:n,dirs:e}=this.walk(),i=[];for(let[r,o]of n){let a=this.saved.get(r);(!a||a.file!==o||a.size!==o.data.length||this.changed.has(o))&&i.push([r,o,o.data.slice()])}this.changed.clear();try{await this.write(n,e,i)}catch(r){for(let[,o]of i)this.changed.add(o);throw r}}async write(n,e,i){let r=new Set(i.map(([l])=>l)),o=new Map;for(let l of n.keys()){let c=this.saved.get(l);c&&!r.has(l)&&o.set(l,c)}for(let[l,c,d]of i){let u=String(this.next++);await this.put(u,d),o.set(l,{file:c,size:d.length,blob:u})}let a={next:this.next,files:Object.fromEntries([...o].map(([l,{blob:c}])=>[l,c])),dirs:e};await this.put("manifest.json",new TextEncoder().encode(JSON.stringify(a)));let s=new Set(Object.values(a.files));for(let{blob:l}of this.saved.values())s.has(l)||await this.store.removeEntry(l).catch(()=>{});this.saved=o}async put(n,e){let r=await(await this.store.getFileHandle(n,{create:!0})).createWritable();await r.write(e),await r.close()}};function WI(){return new URL(document.baseURI).pathname.split("/").filter(Boolean).pop()??"app"}function qI(t){return new Promise(n=>{navigator.locks.request(`hatter:${t}`,{ifAvailable:!0},e=>(n(e!==null),e===null?void 0:new Promise(()=>{}))).catch(()=>n(!0))})}function dh(t){let n=document.querySelector("hatter-root");n&&(n.textContent=t)}async function Ew(t){let n=WI();if(!await qI(n)){dh("This is open in another tab already. Use that one, or close it and reload this one.");return}let e=await Rc.open(n);e.kept||t.injector.get(Xr).notice("Nothing here is kept after you close the page, because the browser doesn't let it store files.");let i=new Map;t.injector.get(Qr).disk=new Ac(new Map([["/data",e.root],[Hl,i]]),()=>e.saveSoon());let r=new ch([],[],[new ii(new Ot([])),io.lineBuffered(l=>console.log(l)),io.lineBuffered(l=>console.warn(l)),new no("/data",e.root),new no(Hl,i)],{debug:!1});e.watch(r);let o=await import(new URL("ghc_wasm_jsffi.js",document.baseURI).href),a={},{instance:s}=await WebAssembly.instantiateStreaming(fetch("app.wasm"),{wasi_snapshot_preview1:r.wasiImport,ghc_wasm_jsffi:o.default(a)});Object.assign(a,s.exports),r.initialize(s),a.hs_init(0,0),await a.start()}Qm(xc).then(Ew).catch(t=>{console.error(t),dh("This didn't load. Reload the page to try again.")});
