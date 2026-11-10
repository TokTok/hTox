var mx=Object.defineProperty,fx=Object.defineProperties;var px=Object.getOwnPropertyDescriptors;var Yf=Object.getOwnPropertySymbols;var hx=Object.prototype.hasOwnProperty,gx=Object.prototype.propertyIsEnumerable;var Zf=(t,n,e)=>n in t?mx(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,x=(t,n)=>{for(var e in n||={})hx.call(n,e)&&Zf(t,e,n[e]);if(Yf)for(var e of Yf(n))gx.call(n,e)&&Zf(t,e,n[e]);return t},z=(t,n)=>fx(t,px(n));var Zr=[null],ba=[!1],qn=1,bx=null,Te=Symbol("SIGNAL");function C(t){let n=Zr[0];return Zr[0]=t,n}function xa(){return Zr[0]}var Zn={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Dn(t){if(ba[0])throw new Error("");let n=Zr[0];if(n===null)return;n.consumerOnSignalRead(t);let e=n.producersTail;if(e!==void 0&&e.producer===t)return;let i,r=n.recomputing;if(r&&(i=e!==void 0?e.nextProducer:n.producers,i!==void 0&&i.producer===t)){n.producersTail=i,i.lastReadVersion=t.version,i.knownValidAtEpoch=qn;return}let o=t.consumersTail;if(o!==void 0&&o.consumer===n&&(!r||o.knownValidAtEpoch===qn))return;let a=Ui(n),s={producer:t,consumer:n,nextProducer:i,prevConsumer:void 0,knownValidAtEpoch:qn,lastReadVersion:t.version,nextConsumer:void 0};n.producersTail=s,e!==void 0?e.nextProducer=s:n.producers=s,a&&Jf(t,s)}function Kf(){qn++}function Kr(t){if(!(Ui(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===qn)){if(!t.producerMustRecompute(t)&&!zi(t)){_a(t);return}t.producerRecomputeValue(t),_a(t)}}function _c(t){if(t.consumers===void 0)return;let n=ba[0];ba[0]=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||vx(i)}}finally{ba[0]=n}}function xc(){return Zr[0]?.consumerAllowSignalWrites!==!1}function vx(t){t.dirty=!0,_c(t),t.consumerMarkedDirty?.(t)}function _a(t){t.dirty=!1,t.lastCleanEpoch=qn}function En(t){return t&&Qf(t),C(t)}function Qf(t){if(t.producersTail?.knownValidAtEpoch===qn){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Kn(t,n){C(n),t&&Xf(t)}function Xf(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ui(t))do e=wc(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function zi(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Kr(e),i!==e.version))return!0}return!1}function Nn(t){if(Ui(t)){let n=t.producers;for(;n!==void 0;)n=wc(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Jf(t,n){let e=t.consumersTail,i=Ui(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Jf(r.producer,r)}function wc(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ui(n)){let o=n.producers;for(;o!==void 0;)o=wc(o)}return e}function Ui(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function wa(t){bx?.(t)}function Ca(t,n){return Object.is(t,n)}function Qr(t,n){let e=Object.create(yx);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Kr(e),Dn(e),e.value===Yn)throw e.error;return e.value};return i[Te]=e,wa(e),i}var va=Symbol("UNSET"),ya=Symbol("COMPUTING"),Yn=Symbol("ERRORED"),yx=z(x({},Zn),{value:va,dirty:!0,error:null,equal:Ca,kind:"computed",producerMustRecompute(t){return t.value===va||t.value===ya},producerRecomputeValue(t){if(t.value===ya)throw new Error("");let n=t.value;t.value=ya;let e=En(t),i,r=!1;try{i=t.computation(),C(null),r=n!==va&&n!==Yn&&i!==Yn&&t.equal(n,i)}catch(o){i=Yn,t.error=o}finally{Kn(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function _x(){throw new Error}var ep=_x;function tp(t){ep(t)}function Cc(t){ep=t}var xx=null;function Sc(t,n){let e=Object.create($i);e.value=t,n!==void 0&&(e.equal=n);let i=()=>np(e);return i[Te]=e,wa(e),[i,a=>Qn(e,a),a=>Dc(e,a)]}function np(t){return Dn(t),t.value}function Qn(t,n){xc()||tp(t),t.equal(t.value,n)||(t.value=n,wx(t))}function Dc(t,n){xc()||tp(t),Qn(t,n(t.value))}var $i=z(x({},Zn),{equal:Ca,value:void 0,kind:"signal"});function wx(t){t.version++,Kf(),_c(t),xx?.(t)}var Ec=z(x({},Zn),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Nc(t){if(t.dirty=!1,t.version>0&&!zi(t))return;t.version++;let n=En(t);try{t.cleanup(),t.fn()}finally{Kn(t,n)}}var Ic=[void 0];function Sa(){return Ic[0]}function Ht(t){let n=Ic[0];return Ic[0]=t,n}var ip=Symbol("NotFound");function Gi(t){return t===ip||t?.name==="\u0275NotFound"}function rp(t){let n=C(null);try{return t()}finally{C(n)}}function W(t){return typeof t=="function"}function Da(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Ea=Da(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Xn(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var he=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(W(i))try{i()}catch(o){n=o instanceof Ea?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{op(o)}catch(a){n=n??[],a instanceof Ea?n=[...n,...a.errors]:n.push(a)}}if(n)throw new Ea(n)}}add(n){var e;if(n&&n!==this)if(this.closed)op(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Xn(e,n)}remove(n){let{_finalizers:e}=this;e&&Xn(e,n),n instanceof t&&n._removeParent(this)}};he.EMPTY=(()=>{let t=new he;return t.closed=!0,t})();var kc=he.EMPTY;function Na(t){return t instanceof he||t&&"closed"in t&&W(t.remove)&&W(t.add)&&W(t.unsubscribe)}function op(t){W(t)?t():t.unsubscribe()}var kt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Wi={setTimeout(t,n,...e){let{delegate:i}=Wi;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Wi;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Ia(t){Wi.setTimeout(()=>{let{onUnhandledError:n}=kt;if(n)n(t);else throw t})}function Xr(){}var ap=Mc("C",void 0,void 0);function sp(t){return Mc("E",void 0,t)}function lp(t){return Mc("N",t,void 0)}function Mc(t,n,e){return{kind:t,value:n,error:e}}var Jn=null;function qi(t){if(kt.useDeprecatedSynchronousErrorHandling){let n=!Jn;if(n&&(Jn={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Jn;if(Jn=null,e)throw i}}else t()}function cp(t){kt.useDeprecatedSynchronousErrorHandling&&Jn&&(Jn.errorThrown=!0,Jn.error=t)}var ei=class extends he{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Na(n)&&n.add(this)):this.destination=Dx}static create(n,e,i){return new on(n,e,i)}next(n){this.isStopped?Ac(lp(n),this):this._next(n)}error(n){this.isStopped?Ac(sp(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Ac(ap,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Cx=Function.prototype.bind;function Tc(t,n){return Cx.call(t,n)}var Rc=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){ka(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){ka(i)}else ka(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){ka(e)}}},on=class extends ei{constructor(n,e,i){super();let r;if(W(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&kt.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Tc(n.next,o),error:n.error&&Tc(n.error,o),complete:n.complete&&Tc(n.complete,o)}):r=n}this.destination=new Rc(r)}};function ka(t){kt.useDeprecatedSynchronousErrorHandling?cp(t):Ia(t)}function Sx(t){throw t}function Ac(t,n){let{onStoppedNotification:e}=kt;e&&Wi.setTimeout(()=>e(t,n))}var Dx={closed:!0,next:Xr,error:Sx,complete:Xr};var Yi=typeof Symbol=="function"&&Symbol.observable||"@@observable";function an(t){return t}function dp(t){return t.length===0?an:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var L=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=Nx(n)?n:new on(n,e,i);return qi(()=>{let{operator:o,source:a}=this;r.add(o?o.call(r,a):a?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=up(e),new e((i,r)=>{let o=new on({next:a=>{try{n(a)}catch(s){r(s),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[Yi](){return this}pipe(...n){return dp(n)(this)}toPromise(n){return n=up(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};L.create=t=>new L(t);function up(t){var n;return(n=t??kt.Promise)!==null&&n!==void 0?n:Promise}function Ex(t){return t&&W(t.next)&&W(t.error)&&W(t.complete)}function Nx(t){return t&&t instanceof ei||Ex(t)&&Na(t)}function Ix(t){return W(t?.lift)}function ue(t){return n=>{if(Ix(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function me(t,n,e,i,r){return new Oc(t,n,e,i,r)}var Oc=class extends ei{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=r?function(s){try{r(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var mp=Da(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var w=class extends L{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new Ma(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new mp}next(n){qi(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){qi(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){qi(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?kc:(this.currentObservers=null,r.push(n),new he(()=>{this.currentObservers=null,Xn(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new L;return n.source=this,n}};w.create=(t,n)=>new Ma(t,n);var Ma=class extends w{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:kc}};var ti=class extends w{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Jr={now(){return(Jr.delegate||Date).now()},delegate:void 0};var In=class extends w{constructor(n=1/0,e=1/0,i=Jr){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let l=1;l<i.length&&i[l]<=a;l+=2)s=l;s&&i.splice(0,s+1)}}};var Ta=class extends he{constructor(n,e){super()}schedule(n,e=0){return this}};var eo={setInterval(t,n,...e){let{delegate:i}=eo;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=eo;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Aa=class extends Ta{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return eo.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&eo.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Xn(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Zi=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Zi.now=Jr.now;var Ra=class extends Zi{constructor(n,e=Zi.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var to=new Ra(Aa),fp=to;var ni=new L(t=>t.complete());function Oa(t){return t&&W(t.schedule)}function Fc(t){return t[t.length-1]}function Fa(t){return W(Fc(t))?t.pop():void 0}function zt(t){return Oa(Fc(t))?t.pop():void 0}function pp(t,n){return typeof Fc(t)=="number"?t.pop():n}function gp(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(d){try{c(i.next(d))}catch(u){a(u)}}function l(d){try{c(i.throw(d))}catch(u){a(u)}}function c(d){d.done?o(d.value):r(d.value).then(s,l)}c((i=i.apply(t,n||[])).next())})}function hp(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function ii(t){return this instanceof ii?(this.v=t,this):new ii(t)}function bp(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(f){return function(h){return Promise.resolve(h).then(f,u)}}function s(f,h){i[f]&&(r[f]=function(g){return new Promise(function(y,_){o.push([f,g,y,_])>1||l(f,g)})},h&&(r[f]=h(r[f])))}function l(f,h){try{c(i[f](h))}catch(g){p(o[0][3],g)}}function c(f){f.value instanceof ii?Promise.resolve(f.value.v).then(d,u):p(o[0][2],f)}function d(f){l("next",f)}function u(f){l("throw",f)}function p(f,h){f(h),o.shift(),o.length&&l(o[0][0],o[0][1])}}function vp(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof hp=="function"?hp(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),r(s,l,a.done,a.value)})}}function r(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var La=(t=>t&&typeof t.length=="number"&&typeof t!="function");function Pa(t){return W(t?.then)}function Va(t){return W(t[Yi])}function Ba(t){return Symbol.asyncIterator&&W(t?.[Symbol.asyncIterator])}function ja(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function kx(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ha=kx();function za(t){return W(t?.[Ha])}function Ua(t){return bp(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield ii(e.read());if(r)return yield ii(void 0);yield yield ii(i)}}finally{e.releaseLock()}})}function $a(t){return W(t?.getReader)}function ve(t){if(t instanceof L)return t;if(t!=null){if(Va(t))return Mx(t);if(La(t))return Tx(t);if(Pa(t))return Ax(t);if(Ba(t))return yp(t);if(za(t))return Rx(t);if($a(t))return Ox(t)}throw ja(t)}function Mx(t){return new L(n=>{let e=t[Yi]();if(W(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Tx(t){return new L(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Ax(t){return new L(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Ia)})}function Rx(t){return new L(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function yp(t){return new L(n=>{Fx(t,n).catch(e=>n.error(e))})}function Ox(t){return yp(Ua(t))}function Fx(t,n){var e,i,r,o;return gp(this,void 0,void 0,function*(){try{for(e=vp(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Ke(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Ga(t,n=0){return ue((e,i)=>{e.subscribe(me(i,r=>Ke(i,t,()=>i.next(r),n),()=>Ke(i,t,()=>i.complete(),n),r=>Ke(i,t,()=>i.error(r),n)))})}function Wa(t,n=0){return ue((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function _p(t,n){return ve(t).pipe(Wa(n),Ga(n))}function xp(t,n){return ve(t).pipe(Wa(n),Ga(n))}function wp(t,n){return new L(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function Cp(t,n){return new L(e=>{let i;return Ke(e,n,()=>{i=t[Ha](),Ke(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>W(i?.return)&&i.return()})}function qa(t,n){if(!t)throw new Error("Iterable cannot be null");return new L(e=>{Ke(e,n,()=>{let i=t[Symbol.asyncIterator]();Ke(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Sp(t,n){return qa(Ua(t),n)}function Dp(t,n){if(t!=null){if(Va(t))return _p(t,n);if(La(t))return wp(t,n);if(Pa(t))return xp(t,n);if(Ba(t))return qa(t,n);if(za(t))return Cp(t,n);if($a(t))return Sp(t,n)}throw ja(t)}function ht(t,n){return n?Dp(t,n):ve(t)}function no(...t){let n=zt(t);return ht(t,n)}function Ep(t){return t instanceof Date&&!isNaN(t)}function Oe(t,n){return ue((e,i)=>{let r=0;e.subscribe(me(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:Lx}=Array;function Px(t,n){return Lx(n)?t(...n):t(n)}function Ya(t){return Oe(n=>Px(t,n))}var{isArray:Vx}=Array,{getPrototypeOf:Bx,prototype:jx,keys:Hx}=Object;function Za(t){if(t.length===1){let n=t[0];if(Vx(n))return{args:n,keys:null};if(zx(n)){let e=Hx(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function zx(t){return t&&typeof t=="object"&&Bx(t)===jx}function Ka(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Lc(...t){let n=zt(t),e=Fa(t),{args:i,keys:r}=Za(t);if(i.length===0)return ht([],n);let o=new L(Ux(i,n,r?a=>Ka(r,a):an));return e?o.pipe(Ya(e)):o}function Ux(t,n,e=an){return i=>{Np(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let l=0;l<r;l++)Np(n,()=>{let c=ht(t[l],n),d=!1;c.subscribe(me(i,u=>{o[l]=u,d||(d=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function Np(t,n,e){t?Ke(e,t,n):n()}function Ip(t,n,e,i,r,o,a,s){let l=[],c=0,d=0,u=!1,p=()=>{u&&!l.length&&!c&&n.complete()},f=g=>c<i?h(g):l.push(g),h=g=>{o&&n.next(g),c++;let y=!1;ve(e(g,d++)).subscribe(me(n,_=>{r?.(_),o?f(_):n.next(_)},()=>{y=!0},void 0,()=>{if(y)try{for(c--;l.length&&c<i;){let _=l.shift();a?Ke(n,a,()=>h(_)):h(_)}p()}catch(_){n.error(_)}}))};return t.subscribe(me(n,f,()=>{u=!0,p()})),()=>{s?.()}}function Pc(t,n,e=1/0){return W(n)?Pc((i,r)=>Oe((o,a)=>n(i,o,r,a))(ve(t(i,r))),e):(typeof n=="number"&&(e=n),ue((i,r)=>Ip(i,r,t,e)))}function Qa(t=1/0){return Pc(an,t)}function kp(){return Qa(1)}function Ki(...t){return kp()(ht(t,zt(t)))}function io(t){return new L(n=>{ve(t()).subscribe(n)})}function Vc(...t){let n=Fa(t),{args:e,keys:i}=Za(t),r=new L(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let d=0;d<a;d++){let u=!1;ve(e[d]).subscribe(me(o,p=>{u||(u=!0,c--),s[d]=p},()=>l--,void 0,()=>{(!l||!u)&&(c||o.next(i?Ka(i,s):s),o.complete())}))}});return n?r.pipe(Ya(n)):r}function Mp(t=0,n,e=fp){let i=-1;return n!=null&&(Oa(n)?e=n:i=n),new L(r=>{let o=Ep(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function sn(...t){let n=zt(t),e=pp(t,1/0),i=t;return i.length?i.length===1?ve(i[0]):Qa(e)(ht(i,n)):ni}function De(t,n){return ue((e,i)=>{let r=0;e.subscribe(me(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Tp(t){return ue((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(me(e,c=>{i=!0,r=c,o||ve(t(c)).subscribe(o=me(e,s,l))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function Qi(t,n=to){return Tp(()=>Mp(t,n))}function ro(t,n=to){return ue((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=a+t,d=n.now();if(d<c){r=this.schedule(void 0,c-d),i.add(r);return}s()}e.subscribe(me(i,c=>{o=c,a=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function nt(t){return t<=0?()=>ni:ue((n,e)=>{let i=0;n.subscribe(me(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Xa(t,n=an){return t=t??$x,ue((e,i)=>{let r,o=!0;e.subscribe(me(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function $x(t,n){return t===n}function Ja(){return ue((t,n)=>{let e,i=!1;t.subscribe(me(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Ap(t={}){let{connector:n=()=>new w,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,l,c=0,d=!1,u=!1,p=()=>{s?.unsubscribe(),s=void 0},f=()=>{p(),a=l=void 0,d=u=!1},h=()=>{let g=a;f(),g?.unsubscribe()};return ue((g,y)=>{c++,!u&&!d&&p();let _=l=l??n();y.add(()=>{c--,c===0&&!u&&!d&&(s=Bc(h,r))}),_.subscribe(y),!a&&c>0&&(a=new on({next:B=>_.next(B),error:B=>{u=!0,p(),s=Bc(f,e,B),_.error(B)},complete:()=>{d=!0,p(),s=Bc(f,i),_.complete()}}),ve(g).subscribe(a))})(o)}}function Bc(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new on({next:()=>{i.unsubscribe(),t()}});return ve(n(...e)).subscribe(i)}function es(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Ap({connector:()=>new In(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function oo(t){return De((n,e)=>t<=e)}function Ut(...t){let n=zt(t);return ue((e,i)=>{(n?Ki(t,e,n):Ki(t,e)).subscribe(i)})}function $t(t){return ue((n,e)=>{ve(t).subscribe(me(e,()=>e.complete(),Xr)),!e.closed&&n.subscribe(e)})}var td="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",S=class extends Error{code;constructor(n,e){super(tr(n,e)),this.code=n}};function Gx(t){return`NG0${Math.abs(t)}`}function tr(t,n){return`${Gx(t)}${n?": "+n:""}`}function ie(t){for(let n in t)if(t[n]===ie)return n;throw Error("")}function Vp(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function ss(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ss).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function ls(t,n){return t?n?`${t} ${n}`:t:n||""}var Wx=ie({__forward_ref__:ie});function gt(t){return t.__forward_ref__=gt,t}function Ae(t){return nd(t)?t():t}function nd(t){return typeof t=="function"&&Object.hasOwn(t,Wx)&&t.__forward_ref__===gt}function X(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function Fe(t){return{providers:t.providers||[],imports:t.imports||[]}}function cs(t){return qx(t,ds)}function qx(t,n){return Object.hasOwn(t,n)&&t[n]||null}function Yx(t){let n=t?.[ds]??null;return n||null}function Hc(t){return t&&Object.hasOwn(t,ns)?t[ns]:null}var ds=ie({\u0275prov:ie}),ns=ie({\u0275inj:ie}),v=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=X({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function id(t){return t&&!!t.\u0275providers}var fo=ie({\u0275cmp:ie}),po=ie({\u0275dir:ie}),rd=ie({\u0275pipe:ie});var lo=ie({\u0275fac:ie}),ci=ie({__NG_ELEMENT_ID__:ie}),Rp=ie({__NG_ENV_ID__:ie});function Mn(t){return od(t,"@Component"),t[fo]||null}function ho(t){return od(t,"@Directive"),t[po]||null}function Bp(t){return od(t,"@Pipe"),t[rd]||null}function od(t,n){if(t==null)throw new S(-919,!1)}function us(t){return typeof t=="string"?t:t==null?"":String(t)}var jp=ie({ngErrorCode:ie}),Zx=ie({ngErrorMessage:ie}),Kx=ie({ngTokenPath:ie});function ad(t,n){return Hp("",-200,n)}function ms(t,n){throw new S(-201,!1)}function Hp(t,n,e){let i=new S(n,t);return i[jp]=n,i[Zx]=t,e&&(i[Kx]=e),i}function Qx(t){return t[jp]}var zc;function zp(){return zc}function it(t){let n=zc;return zc=t,n}function sd(t,n,e){let i=cs(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;ms(t,"")}var Tn=globalThis;var Xx={},ri=Xx,Jx="__NG_DI_FLAG__",Uc=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=oi(e)||0;try{return this.injector.get(n,i&8?null:ri,i)}catch(r){if(Gi(r))return r;throw r}}};function ew(t,n=0){let e=Sa();if(e===void 0)throw new S(-203,!1);if(e===null)return sd(t,void 0,n);{let i=tw(n),r=e.retrieve(t,i);if(Gi(r)){if(i.optional)return null;throw r}return r}}function j(t,n=0){return(zp()||ew)(Ae(t),n)}function m(t,n){return j(t,oi(n))}function oi(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function tw(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function $c(t){let n=[];for(let e=0;e<t.length;e++){let i=Ae(t[e]);if(Array.isArray(i)){if(i.length===0)throw new S(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],l=nw(s);typeof l=="number"?l===-1?r=s.token:o|=l:r=s}n.push(j(r,o))}else n.push(j(i))}return n}function nw(t){return t[Jx]}function ai(t,n){let e=Object.hasOwn(t,lo);return e?t[lo]:null}function Up(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function $p(t){return t.flat(Number.POSITIVE_INFINITY)}function fs(t,n){t.forEach(e=>Array.isArray(e)?fs(e,n):n(e))}function ld(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function go(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Gp(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Wp(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function ps(t,n,e){let i=nr(t,n);return i>=0?t[i|1]=e:(i=~i,Wp(t,i,n,e)),i}function hs(t,n){let e=nr(t,n);if(e>=0)return t[e|1]}function nr(t,n){return iw(t,n,1)}function iw(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var An={},ze=[],ir=new v(""),cd=new v("",-1),dd=new v(""),Ji=class{get(n,e=ri){if(e===ri){let r=Hp("",-201);throw r.name="\u0275NotFound",r}return e}};function qp(...t){return{\u0275providers:ud(!0,t),\u0275fromNgModule:!0}}function ud(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return fs(n,a=>{let s=a;is(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&Yp(r,o),e}function Yp(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];md(r,o=>{n(o,i)})}}function is(t,n,e,i){if(t=Ae(t),!t)return!1;let r=null,o=Hc(t),a=!o&&Mn(t);if(!o&&!a){let l=t.ngModule;if(o=Hc(l),o)r=l;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)is(c,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let c;fs(o.imports,d=>{is(d,n,e,i)&&(c||=[],c.push(d))}),c!==void 0&&Yp(c,n)}if(!s){let c=ai(r)||(()=>new r);n({provide:r,useFactory:c,deps:ze},r),n({provide:dd,useValue:r,multi:!0},r),n({provide:ir,useValue:()=>j(r),multi:!0},r)}let l=o.providers;if(l!=null&&!s){let c=t;md(l,d=>{n(d,c)})}}else return!1;return r!==t&&t.providers!==void 0}function md(t,n){for(let e of t)id(e)&&(e=e.\u0275providers),Array.isArray(e)?md(e,n):n(e)}var rw=ie({provide:String,useValue:ie});function Zp(t){return t!==null&&typeof t=="object"&&rw in t}function ow(t){return!!(t&&t.useExisting)}function aw(t){return!!(t&&t.useFactory)}function si(t){return typeof t=="function"}function Kp(t){return!!t.useClass}var bo=new v(""),ts={},Op={},jc;function rr(){return jc===void 0&&(jc=new Ji),jc}var Re=class{},li=class extends Re{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Wc(n,a=>this.processProvider(a)),this.records.set(cd,Xi(void 0,this)),r.has("environment")&&this.records.set(Re,Xi(void 0,this));let o=this.records.get(bo);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(dd,ze,{self:!0}))}retrieve(n,e){let i=oi(e)||0;try{return this.get(n,ri,i)}catch(r){if(Gi(r))return r;throw r}}destroy(){ao(this),this._destroyed=!0;let n=C(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),C(n)}}onDestroy(n){return ao(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ao(this);let e=Ht(this),i=it(void 0),r;try{return n()}finally{Ht(e),it(i)}}get(n,e=ri,i){if(ao(this),Object.hasOwn(n,Rp))return n[Rp](this);let r=oi(i),o,a=Ht(this),s=it(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let d=uw(n)&&cs(n);d&&this.injectableDefInScope(d)?c=Xi(Gc(n),ts):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?rr():this.parent;return e=r&8&&e===ri?null:e,l.get(n,e)}catch(l){let c=Qx(l);throw c===-200||c===-201?new S(c,null):l}finally{it(s),Ht(a)}}resolveInjectorInitializers(){let n=C(null),e=Ht(this),i=it(void 0),r;try{let o=this.get(ir,ze,{self:!0});for(let a of o)a()}finally{Ht(e),it(i),C(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Ae(n);let e=si(n)?n:Ae(n&&n.provide),i=lw(n);if(!si(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Xi(void 0,ts,!0),r.factory=()=>$c(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=C(null);try{if(e.value===Op)throw ad("");return e.value===ts&&(e.value=Op,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&dw(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{C(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Ae(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Gc(t){let n=cs(t),e=n!==null?n.factory:ai(t);if(e!==null)return e;if(t instanceof v)throw new S(-204,!1);if(t instanceof Function)return sw(t);throw new S(-204,!1)}function sw(t){if(t.length>0)throw new S(-204,!1);let e=Yx(t);return e!==null?()=>e.factory(t):()=>new t}function lw(t){if(Zp(t))return Xi(void 0,t.useValue);{let n=fd(t);return Xi(n,ts)}}function fd(t,n,e){let i;if(si(t)){let r=Ae(t);return ai(r)||Gc(r)}else if(Zp(t))i=()=>Ae(t.useValue);else if(aw(t))i=()=>t.useFactory(...$c(t.deps||[]));else if(ow(t))i=(r,o)=>j(Ae(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Ae(t&&(t.useClass||t.provide));if(cw(t))i=()=>new r(...$c(t.deps));else return ai(r)||Gc(r)}return i}function ao(t){if(t.destroyed)throw new S(-205,!1)}function Xi(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function cw(t){return!!t.deps}function dw(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function uw(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Wc(t,n){for(let e of t)Array.isArray(e)?Wc(e,n):e&&id(e)?Wc(e.\u0275providers,n):n(e)}function gs(t,n){let e;t instanceof li?(ao(t),e=t):e=new Uc(t);let i,r=Ht(e),o=it(void 0);try{return n()}finally{Ht(r),it(o)}}function Qp(){return zp()!==void 0||Sa()!=null}var Mt=0,E=1,k=2,Ee=3,bt=4,Ve=5,di=6,or=7,we=8,mn=9,Tt=10,ge=11,ar=12,pd=13,Rn=14,Ue=15,On=16,ui=17,Wt=18,vo=19,hd=20,ln=21,bs=22,cn=23,ot=24,mi=25,Xp=26,sr=27,Ne=28,Jp=1,gd=6,fi=7,yo=8,pi=9,Ce=10;function fn(t){return Array.isArray(t)&&typeof t[Jp]=="object"}function vt(t){return Array.isArray(t)&&t[Jp]===!0}function bd(t){return(t.flags&4)!==0}function pn(t){return t.componentOffset>-1}function _o(t){return(t.flags&1)===1}function At(t){return!!t.template}function lr(t){return(t[k]&512)!==0}function hi(t){return(t[k]&256)===256}var ye=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(ye||{}),so,er="svg",vs="math",qc="",Fp="*",Yc=()=>Object.create(null);function mw(){return so||(so=Yc(),kn(ye.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),kn(ye.STYLE,void 0,[["*",["style"]]]),kn(ye.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]]]),kn(ye.URL,vs,[["*",["href","xlink:href"]]]),kn(ye.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),kn(ye.URL,er,[["a",["href","xlink:href"]]]),kn(ye.ATTRIBUTE_NO_BINDING,er,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),kn(ye.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]]]),so)}function kn(t,n,e){let i=n??qc;for(let[r,o]of e){let a=r.toLowerCase();for(let s of o){let l=s.toLowerCase(),c=so[l]??=Yc(),d=c[i]??=Yc();d[a]=t}}}function eh(t,n,e){let r=mw()[n.toLowerCase()];if(!r)return ye.NONE;let o=t.toLowerCase(),a;if(e){let s=r[e];s&&(a=s[o]??s[Fp])}if(a===void 0){let s=r[qc];s&&(a=s[o]??s[Fp])}if(a===void 0&&(!e||e===qc)){let s=r[er];s&&(a=s[o])}return a??ye.NONE}function Be(t){for(;Array.isArray(t);)t=t[Mt];return t}function vd(t,n){return Be(n[t])}function yt(t,n){return Be(n[t.index])}function ys(t,n){return t.data[n]}function th(t,n){return t[n]}function _t(t,n){let e=n[t];return fn(e)?e:e[Mt]}function nh(t){return(t[k]&4)===4}function _s(t){return(t[k]&128)===128}function ih(t){return vt(t[Ee])}function xt(t,n){return n==null?null:t[n]}function yd(t){t[ui]=0}function _d(t){t[k]&1024||(t[k]|=1024,_s(t)&&gi(t))}function rh(t,n){for(;t>0;)n=n[Rn],t--;return n}function xo(t){return!!(t[k]&9216||t[ot]?.dirty)}function xs(t){t[Tt].changeDetectionScheduler?.notify(8),t[k]&64&&(t[k]|=1024),xo(t)&&gi(t)}function gi(t){t[Tt].changeDetectionScheduler?.notify(0);let n=dn(t);for(;n!==null&&!(n[k]&8192||(n[k]|=8192,!_s(n)));)n=dn(n)}function ws(t,n){if(hi(t))throw new S(911,!1);t[ln]===null&&(t[ln]=[]),t[ln].push(n)}function oh(t,n){if(t[ln]===null)return;let e=t[ln].indexOf(n);e!==-1&&t[ln].splice(e,1)}function dn(t){let n=t[Ee];return vt(n)?n[Ee]:n}function xd(t){return t[or]??=[]}function wd(t){return t.cleanup??=[]}function ah(t,n,e,i){let r=xd(n);r.push(e),t.firstCreatePass&&wd(t).push(i,r.length-1)}var H={lFrame:bh(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Zc=!1;function sh(){return H.lFrame.elementDepthCount}function lh(){H.lFrame.elementDepthCount++}function Cd(){H.lFrame.elementDepthCount--}function Sd(){return H.bindingsEnabled}function Dd(){return H.skipHydrationRootTNode!==null}function Ed(t){return H.skipHydrationRootTNode===t}function Nd(){H.skipHydrationRootTNode=null}function F(){return H.lFrame.lView}function Ie(){return H.lFrame.tView}function bi(t){return H.lFrame.contextLView=t,t[we]}function vi(t){return H.lFrame.contextLView=null,t}function Ge(){let t=Id();for(;t!==null&&t.type===64;)t=t.parent;return t}function Id(){return H.lFrame.currentTNode}function ch(){let t=H.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function cr(t,n){let e=H.lFrame;e.currentTNode=t,e.isParent=n}function kd(){return H.lFrame.isParent}function Md(){H.lFrame.isParent=!1}function dh(){return H.lFrame.contextLView}function Td(){return Zc}function co(t){let n=Zc;return Zc=t,n}function uh(t){return H.lFrame.bindingIndex=t}function yi(){return H.lFrame.bindingIndex++}function Ad(t){let n=H.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function mh(){return H.lFrame.inI18n}function fh(t,n){let e=H.lFrame;e.bindingIndex=e.bindingRootIndex=t,Cs(n)}function ph(){return H.lFrame.currentDirectiveIndex}function Cs(t){H.lFrame.currentDirectiveIndex=t}function hh(t){let n=H.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Ss(){return H.lFrame.currentQueryIndex}function wo(t){H.lFrame.currentQueryIndex=t}function fw(t){let n=t[E];return n.type===2?n.declTNode:n.type===1?t[Ve]:null}function Rd(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=fw(o),r===null||(o=o[Rn],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=H.lFrame=gh();return i.currentTNode=n,i.lView=t,!0}function Ds(t){let n=gh(),e=t[E];H.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function gh(){let t=H.lFrame,n=t===null?null:t.child;return n===null?bh(t):n}function bh(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function vh(){let t=H.lFrame;return H.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Od=vh;function Es(){let t=vh();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function yh(t){return(H.lFrame.contextLView=rh(t,H.lFrame.contextLView))[we]}function qt(){return H.lFrame.selectedIndex}function Fn(t){H.lFrame.selectedIndex=t}function Co(){let t=H.lFrame;return ys(t.tView,t.selectedIndex)}function Fd(){return H.lFrame.currentNamespace}var _h=!0;function Ns(){return _h}function Is(t){_h=t}function Kc(t,n=null,e=null,i){let r=xh(t,n,e,i);return r.resolveInjectorInitializers(),r}function xh(t,n=null,e=null,i,r=new Set){let o=[e||ze,qp(t)],a;return new li(o,n||rr(),a||null,r)}var O=class t{static THROW_IF_NOT_FOUND=ri;static NULL=new Ji;static create(n,e){if(Array.isArray(n))return Kc({name:""},e,n,"");{let i=n.name??"";return Kc({name:i},n.parent,n.providers,i)}}static \u0275prov=X({token:t,providedIn:"any",factory:()=>j(cd)});static __NG_ELEMENT_ID__=-1},V=new v(""),rt=class{static __NG_ELEMENT_ID__=pw;static __NG_ENV_ID__=n=>n},rs=class extends rt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return hi(this._lView)}onDestroy(n){let e=this._lView;return ws(e,n),()=>oh(e,n)}};function pw(){return new rs(F())}var wh=!1,Ch=new v(""),dr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new ti(!1);debugTaskTracker=m(Ch,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new L(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=X({token:t,providedIn:"root",factory:()=>new t})}return t})(),Qc=class extends w{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Qp()&&(this.destroyRef=m(rt,{optional:!0})??void 0,this.pendingTasks=m(dr,{optional:!0})??void 0)}emit(n){let e=C(null);try{super.next(n)}finally{C(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof he&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},J=Qc;function os(...t){}function Ld(t){let n,e;function i(){t=os;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function Sh(t){return queueMicrotask(()=>t()),()=>{t=os}}var Pd="isAngularZone",uo=Pd+"_ID",hw=0,D=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new J(!1);onMicrotaskEmpty=new J(!1);onStable=new J(!1);onError=new J(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=wh}=n;if(typeof Zone>"u")throw new S(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,vw(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Pd)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new S(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new S(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,gw,os,os);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},gw={};function Vd(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function bw(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Ld(()=>{t.callbackScheduled=!1,Xc(t),t.isCheckStableRunning=!0,Vd(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Xc(t)}function vw(t){let n=()=>{bw(t)},e=hw++;t._inner=t._inner.fork({name:"angular",properties:{[Pd]:!0,[uo]:e,[uo+e]:!0},onInvokeTask:(i,r,o,a,s,l)=>{if(yw(l))return i.invokeTask(o,a,s,l);try{return Lp(t),i.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Pp(t)}},onInvoke:(i,r,o,a,s,l,c)=>{try{return Lp(t),i.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!_w(l)&&n(),Pp(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,Xc(t),Vd(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function Xc(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Lp(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Pp(t){t._nesting--,Vd(t)}var mo=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new J;onMicrotaskEmpty=new J;onStable=new J;onError=new J;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function yw(t){return Dh(t,"__ignore_ng_zone__")}function _w(t){return Dh(t,"__scheduler_tick__")}function Dh(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Gt=class{_console=console;handleError(n){this._console.error("ERROR",n)}};function Eh(t){return xw(t)?t:new Jc(t)}function xw(t){return t instanceof Error||typeof t=="object"&&t!==null&&typeof t.name=="string"&&typeof t.message=="string"}var Jc=class extends Error{constructor(n){super(String(n),{cause:n}),this.name="ErrorBoundaryWrappedError"}},_i=new v("",{factory:()=>{let t=m(D),n=m(Re),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Gt),e.handleError(i))})}}}),Nh={provide:ir,useValue:()=>{let t=m(Gt,{optional:!0})},multi:!0};function Q(t,n){let[e,i,r]=Sc(t,n?.equal),o=e,a=o[Te];return o.set=i,o.update=r,o.asReadonly=Ih.bind(o),o}function Ih(){let t=this[Te];if(t.readonlyFn===void 0){let n=()=>this();n[Te]=t,t.readonlyFn=n}return t.readonlyFn}var xi=new v("",{factory:()=>ww}),ww="ng";var ks=new v(""),wi=new v("",{providedIn:"platform",factory:()=>"unknown"}),So=new v(""),Ci=new v("",{factory:()=>m(V).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var ur=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Cw}return t})();function Cw(){return new ur(F(),Ge())}var un=class{},Do=new v("",{factory:()=>!0});var Bd=new v(""),Ms=(()=>{class t{static \u0275prov=X({token:t,providedIn:"root",factory:()=>new ed})}return t})(),ed=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},as=class{[Te];constructor(n){this[Te]=n}destroy(){this[Te].destroy()}};function hn(t,n){let e=n?.injector??m(O),i=n?.manualCleanup!==!0?e.get(rt):null,r,o=e.get(ur,null,{optional:!0}),a=e.get(un);return o!==null?(r=Mh(o.view,a,t),i instanceof rs&&i._lView===o.view&&(i=null)):r=Ew(t,e.get(Ms),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new as(r)}var kh=z(x({},Ec),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=co(!1);try{Nc(this)}finally{co(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=C(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],C(t)}}}),Sw=z(x({},kh),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Nn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Dw=z(x({},kh),{consumerMarkedDirty(){this.view[k]|=8192,gi(this.view),this.notifier.notify(13)},destroy(){if(Nn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[cn]?.delete(this)}});function Mh(t,n,e){let i=Object.create(Dw);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.userFn=e,i.fn=Th(i,e),t[cn]??=new Set,t[cn].add(i),i.consumerMarkedDirty(i),i}function Ew(t,n,e){let i=Object.create(Sw);return i.userFn=t,i.fn=Th(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Th(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function gn(t){return typeof t=="function"&&t[Te]!==void 0}var Fu=Symbol("InputSignalNode#UNSET"),vg=z(x({},$i),{transformFn:void 0,applyValueToInputSignal(t,n){Qn(t,n)}});function Fo(t){return{toString:t}.toString()}var Y=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Y||{}),Bs=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function yg(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var _g=null,Qe=(()=>{_g=Ah;let t=()=>Ah;return t.ngInherit=!0,t})();function zw(){return _g}function Ah(t){return t.type.prototype.ngOnChanges&&(t.setInput=$w),Uw}function Uw(){let t=xg(this),n=t?.current;if(n){let e=t.previous;if(e===An)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function $w(t,n,e,i,r){let o=this.declaredInputs[i],a=xg(t)||Gw(t,{previous:An,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new Bs(c&&c.currentValue,e,l===An),yg(t,n,r,e)}var Qd="__ngSimpleChanges__";function xg(t){return Object.hasOwn(t,Qd)&&t[Qd]||null}function Gw(t,n){return t[Qd]=n}var Rh=[];var ee=function(t,n=null,e){for(let i=0;i<Rh.length;i++){let r=Rh[i];r(t,n,e)}};function Ww(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=zw()(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function wg(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:d}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function Fs(t,n,e){Cg(t,n,3,e)}function Ls(t,n,e,i){(t[k]&3)===e&&Cg(t,n,e,i)}function jd(t,n){let e=t[k];(e&3)===n&&(e&=16383,e+=1,t[k]=e)}function Cg(t,n,e,i){let r=i!==void 0?t[ui]&65535:0,o=i??-1,a=n.length-1,s=0;for(let l=r;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],i!=null&&s>=i)break}else n[l]<0&&(t[ui]+=65536),(s<o||o==-1)&&(qw(t,e,n,l),t[ui]=(t[ui]&4294901760)+l+2),l++}function Oh(t,n){ee(Y.LifecycleHookStart,t,n);let e=C(null);try{n.call(t)}finally{C(e),ee(Y.LifecycleHookEnd,t,n)}}function qw(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[k]>>14<t[ui]>>16&&(t[k]&3)===n&&(t[k]+=16384,Oh(s,o)):Oh(s,o)}var pr=-1,Di=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function Yw(t){return(t.flags&8)!==0}function Zw(t){return(t.flags&16)!==0}function Kw(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];Xw(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function Qw(t){return t===3||t===4||t===6}function Xw(t){return t.charCodeAt(0)===64}function hr(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?Fh(t,e,r,null,n[++i]):Fh(t,e,r,null,null))}}return t}function Fh(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Sg(t){return t!==pr}function js(t){return t&32767}function Jw(t){return t>>16}function Hs(t,n){let e=Jw(t),i=n;for(;e>0;)i=i[Rn],e--;return i}var Xd=!0;function Lh(t){let n=Xd;return Xd=t,n}var e0=256,Dg=e0-1,Eg=5,t0=0,Yt={};function n0(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:Object.hasOwn(e,ci)&&(i=e[ci]),i==null&&(i=e[ci]=t0++);let r=i&Dg,o=1<<r;n.data[t+(r>>Eg)]|=o}function zs(t,n){let e=Ng(t,n);if(e!==-1)return e;let i=n[E];i.firstCreatePass&&(t.injectorIndex=n.length,Hd(i.data,t),Hd(n,null),Hd(i.blueprint,null));let r=Lu(t,n),o=t.injectorIndex;if(Sg(r)){let a=js(r),s=Hs(r,n),l=s[E].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=r,o}function Hd(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Ng(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Lu(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=Ag(r),i===null)return pr;if(e++,r=r[Rn],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return pr}function Jd(t,n,e){n0(t,n,e)}function Ig(t,n,e){if(e&8||t!==void 0)return t;ms(n,"NodeInjector")}function kg(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[mn],o=it(void 0);try{return r?r.get(n,i,e&8):sd(n,i,e&8)}finally{it(o)}}return Ig(i,n,e)}function Mg(t,n,e,i=0,r){if(t!==null){if(n[k]&2048&&!(i&2)){let a=a0(t,n,e,i,Yt);if(a!==Yt)return a}let o=Tg(t,n,e,i,Yt);if(o!==Yt)return o}return kg(n,e,i,r)}function Tg(t,n,e,i,r){let o=r0(e);if(typeof o=="function"){if(!Rd(n,t,i))return i&1?Ig(r,e,i):kg(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))ms(e);else return a}finally{Od()}}else if(typeof o=="number"){let a=null,s=Ng(t,n),l=pr,c=i&1?n[Ue][Ve]:null;for((s===-1||i&4)&&(l=s===-1?Lu(t,n):n[s+8],l===pr||!Vh(i,!1)?s=-1:(a=n[E],s=js(l),n=Hs(l,n)));s!==-1;){let d=n[E];if(Ph(o,s,d.data)){let u=i0(s,n,e,a,i,c);if(u!==Yt)return u}l=n[s+8],l!==pr&&Vh(i,n[E].data[s+8]===c)&&Ph(o,s,n)?(a=d,s=js(l),n=Hs(l,n)):s=-1}}return r}function i0(t,n,e,i,r,o){let a=n[E],s=a.data[t+8],l=i==null?pn(s)&&Xd:i!=a&&(s.type&3)!==0,c=r&1&&o===s,d=Ps(s,a,e,l,c);return d!==null?Io(n,a,d,s,r):Yt}function Ps(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,d=o>>20,u=i?s:s+d,p=r?s+d:c;for(let f=u;f<p;f++){let h=a[f];if(f<l&&e===h||f>=l&&h.type===e)return f}if(r){let f=a[l];if(f&&At(f)&&f.type===e)return l}return null}function Io(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof Di){let s=o;if(s.resolving)throw ad("");let l=Lh(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],d,u=s.injectImpl?it(s.injectImpl):null,p=Rd(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&Ww(e,a[e],n)}finally{u!==null&&it(u),Lh(l),s.resolving=!1,Od()}}return o}function r0(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,ci)?t[ci]:void 0;return typeof n=="number"?n>=0?n&Dg:o0:n}function Ph(t,n,e){let i=1<<t;return!!(e[n+(t>>Eg)]&i)}function Vh(t,n){return!(t&2)&&!(t&1&&n)}var bn=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Mg(this._tNode,this._lView,n,oi(i),e)}};function o0(){return new bn(Ge(),F())}function re(t){return Fo(()=>{let n=t.prototype.constructor,e=n[lo]||eu(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[lo]||eu(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function eu(t){return nd(t)?()=>{let n=eu(Ae(t));return n&&n()}:ai(t)}function a0(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[k]&2048&&!lr(a);){let s=Tg(o,a,e,i|2,Yt);if(s!==Yt)return s;i&=-5;let l=o.parent;if(!l){let c=a[hd];if(c){let d=c.get(e,Yt,i);if(d!==Yt)return d}l=Ag(a),a=a[Rn]}o=l}return r}function Ag(t){let n=t[E],e=n.type;return e===2?n.declTNode:e===1?t[Ve]:null}function N(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function s0(){return _r(Ge(),F())}function _r(t,n){return new M(yt(t,n))}var M=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=s0}return t})();function Rg(t){return t instanceof M?t.nativeElement:t}function l0(){return this._results[Symbol.iterator]()}var Us=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new w}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=$p(n);(this._changesDetected=!Up(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=l0};function Og(t){return(t.flags&128)===128}var Pu=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Pu||{}),Fg=new Map,c0=0;function d0(){return c0++}function u0(t){Fg.set(t[vo],t)}function tu(t){Fg.delete(t[vo])}var Bh="__ngContext__";function gr(t,n){fn(n)?(t[Bh]=n[vo],u0(n)):t[Bh]=n}function Lg(t){return Vg(t[ar])}function Pg(t){return Vg(t[bt])}function Vg(t){for(;t!==null&&!vt(t);)t=t[bt];return t}var nu;function Vu(t){nu=t}function Bu(){if(nu!==void 0)return nu;if(typeof document<"u")return document;throw new S(210,!1)}var Bg="r";var jg="di";var Hg=!1,zg=new v("",{factory:()=>Hg});var jh=new WeakMap;function m0(t,n){if(t==null||typeof t!="object")return;let e=jh.get(t);e||(e=new WeakSet,jh.set(t,e)),e.add(n)}var f0=(t,n,e,i)=>{};function p0(t,n,e,i){f0(t,n,e,i)}function Js(t){return(t.flags&32)===32}var h0=()=>null;function Ug(t,n,e=!1){return h0(t,n,e)}function $g(t,n){let e=t.contentQueries;if(e!==null){let i=C(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];wo(o),s.contentQueries(2,n[a],a)}}}finally{C(i)}}}function iu(t,n,e){wo(0);let i=C(null);try{n(t,e)}finally{C(i)}}function Gg(t,n,e){if(bd(n)){let i=C(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{C(i)}}}var Ft=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Ft||{}),vn=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}`}},ru=class extends vn{getTypeName(){return"HTML"}},ou=class extends vn{getTypeName(){return"Style"}},au=class extends vn{getTypeName(){return"Script"}},su=class extends vn{getTypeName(){return"URL"}},lu=class extends vn{getTypeName(){return"ResourceURL"}};function at(t){return t instanceof vn?t.changingThisBreaksApplicationSecurity:t}function yn(t,n){let e=Wg(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${td})`)}return e===n}function Wg(t){return t instanceof vn&&t.getTypeName()||null}function ju(t){return new ru(t)}function Hu(t){return new ou(t)}function zu(t){return new au(t)}function Uu(t){return new su(t)}function $u(t){return new lu(t)}var g0={"http://www.w3.org/2000/svg":er,"http://www.w3.org/1998/Math/MathML":vs},Ts;function b0(){if(Ts===void 0&&(Ts=null,Tn.trustedTypes))try{Ts=Tn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Ts}function el(t){return b0()?.createHTML(t)||t}var As;function qg(){if(As===void 0&&(As=null,Tn.trustedTypes))try{As=Tn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return As}function Hh(t){return qg()?.createHTML(t)||t}function zh(t){return qg()?.createScriptURL(t)||t}function v0(t){let n=new du(t);return y0()?new cu(n):n}var cu=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(el(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},du=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=el(n),e}};function y0(){try{return!!new window.DOMParser().parseFromString(el(""),"text/html")}catch{return!1}}var _0=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function tl(t){return t=String(t),t.match(_0)?t:"unsafe:"+t}function _n(t){let n=Object.create(null);for(let e of t.split(","))n[e]=!0;return n}function Lo(...t){let n=Object.create(null);for(let e of t)for(let i in e)Object.hasOwn(e,i)&&(n[i]=!0);return n}var Yg=_n("area,br,col,hr,img,wbr"),Zg=_n("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Kg=_n("rp,rt"),x0=Lo(Kg,Zg),w0=Lo(Zg,_n("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),C0=Lo(Kg,_n("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Uh=Lo(Yg,w0,C0,x0),Qg=_n("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),S0=_n("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),D0=_n("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),E0=Lo(Qg,S0,D0),N0=_n("script,style,template"),uu=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=M0(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=k0(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=$h(n).toLowerCase();if(!Object.hasOwn(Uh,e))return this.sanitizedSomething=!0,!Object.hasOwn(N0,e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!Object.hasOwn(E0,s)){this.sanitizedSomething=!0;continue}let l=o.value;Qg[s]&&(l=tl(l)),this.buf.push(" ",a,'="',Gh(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=$h(n).toLowerCase();Object.hasOwn(Uh,e)&&!Object.hasOwn(Yg,e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(Gh(n))}};function I0(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function k0(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Xg(n);return n}function M0(t){let n=t.firstChild;if(n&&I0(t,n))throw Xg(n);return n}function $h(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Xg(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var T0=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,A0=/([^\#-~ |!])/g;function Gh(t){return t.replace(/&/g,"&amp;").replace(T0,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(A0,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Rs;function nl(t,n){let e=null;try{Rs=Rs||v0(t);let i=n?String(n):"";e=Rs.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Rs.getInertBodyElement(i)}while(i!==o);let s=new uu().sanitizeChildren(Wh(e)||e);return el(s)}finally{if(e){let i=Wh(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Wh(t){return"content"in t&&R0(t)?t.content:null}function R0(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function O0(t,n){return t.createText(n)}function F0(t,n,e){t.setValue(n,e)}function Jg(t,n,e){return t.createElement(n,e)}function Si(t,n,e,i,r){t.insertBefore(n,e,i,r)}function eb(t,n,e){t.appendChild(n,e)}function qh(t,n,e,i,r){i!==null?Si(t,n,e,i,r):eb(t,n,e)}function tb(t,n,e,i){t.removeChild(null,n,e,i)}function L0(t,n,e){t.setAttribute(n,"style",e)}function P0(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function nb(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&Kw(t,n,i),r!==null&&P0(t,n,r),o!==null&&L0(t,n,o)}function V0(t,n=!0){if(t[0]!=":")return[null,t];let e=t.indexOf(":",1);if(e===-1){if(n)throw new Error(`Unsupported format "${t}" expecting ":namespace:name"`);return[null,t]}return[t.slice(1,e),t.slice(e+1)]}function Gu(t,n,e){if(n!==void 0&&e!==void 0&&B0(n,e)!==ye.HTML)return t;let i=ib();return i?Hh(i.sanitize(ye.HTML,t)||""):yn(t,"HTML")?Hh(at(t)):nl(Bu(),us(t))}function Wu(t){let n=ib();if(n)return zh(n.sanitize(ye.RESOURCE_URL,t)||"");if(yn(t,"ResourceURL"))return zh(at(t));throw new S(-904,!1)}function ib(){let t=F();return t&&t[Tt].sanitizer}function B0(t,n){let[e,i]=j0(t);return eh(i,n,e)}function j0(t){t=t.toLowerCase();let n=V0(t,!1);if(n[0])return n;let i=qt()===-1?null:Co(),r=i?.namespace;if(t==="#host"&&i?.type===2){let o=yt(i,F());if(o.tagName&&(t=o.tagName.toLowerCase()),r==null){let a=o.namespaceURI;r=a&&g0[a]}}return[r,t]}function H0(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var rb="ng-template";function z0(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&H0(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(qu(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function qu(t){return t.type===4&&t.value!==rb}function U0(t,n,e){let i=t.type===4&&!e?rb:t.value;return n===i}function $0(t,n,e){let i=4,r=t.attrs,o=r!==null?q0(r):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!Rt(i)&&!Rt(l))return!1;if(a&&Rt(l))continue;a=!1,i=l|i&1;continue}if(!a)if(i&4){if(i=2|i&1,l!==""&&!U0(t,l,e)||l===""&&n.length===1){if(Rt(i))return!1;a=!0}}else if(i&8){if(r===null||!z0(t,r,l,e)){if(Rt(i))return!1;a=!0}}else{let c=n[++s],d=G0(l,r,qu(t),e);if(d===-1){if(Rt(i))return!1;a=!0;continue}if(c!==""){let u;if(d>o?u="":u=r[d+1].toLowerCase(),i&2&&c!==u){if(Rt(i))return!1;a=!0}}}}return Rt(i)||a}function Rt(t){return(t&1)===0}function G0(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return Y0(n,t)}function ob(t,n,e=!1){for(let i=0;i<n.length;i++)if($0(t,n[i],e))return!0;return!1}function W0(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function q0(t){for(let n=0;n<t.length;n++){let e=t[n];if(Qw(e))return n}return t.length}function Y0(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function Z0(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function Yh(t,n){return t?":not("+n.trim()+")":n}function K0(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!Rt(a)&&(n+=Yh(o,r),r=""),i=a,o=o||!Rt(i);e++}return r!==""&&(n+=Yh(o,r)),n}function Q0(t){return t.map(K0).join(",")}function X0(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Rt(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Pt={},Zt=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Zt||{}),J0;function Yu(t,n){return J0(t,n)}var Zu=new Set;var C1=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var mu=new WeakMap;function ab(t){return t?t[Rn]??t:null}var mr=new WeakSet;function eC(t,n,e){let i=mu.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,a=ab(e);for(let s=i.length-1;s>=0;s--){let{el:l,declarationView:c}=i[s],d=l.parentNode;l===n?(i.splice(s,1),mr.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(i.splice(s,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):d&&r&&d!==r&&(a===null||c===null||a===c)&&(i.splice(s,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function tC(t,n,e){let i=ab(e),r=mu.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):mu.set(t,[{el:n,declarationView:i}])}var il=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(il||{}),Kt=new v(""),Zh=new Set;function Ni(t){Zh.has(t)||(Zh.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var rl=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=X({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ku=[0,1,2,3],Qu=(()=>{class t{ngZone=m(D);scheduler=m(un);errorHandler=m(Gt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){m(Kt,{optional:!0})}execute(){let e=this.sequences.size>0;e&&ee(Y.AfterRenderHooksStart),this.executing=!0;for(let i of Ku)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&ee(Y.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[mi]??=[]).push(e),gi(i),i[k]|=8192):this.addSequence(e)}addSequence(e){this.executing?this.deferredRegistrations.add(e):this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(il.AFTER_NEXT_RENDER,e):e()}static \u0275prov=X({token:t,providedIn:"root",factory:()=>new t})}return t})(),ko=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[mi];n&&(this.view[mi]=n.filter(e=>e!==this))}};function wt(t,n){let e=n?.injector??m(O);return Ni("NgAfterNextRender"),iC(t,e,n,!0)}function nC(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function iC(t,n,e,i){let r=n.get(rl);r.impl??=n.get(Qu);let o=n.get(Kt,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(rt):null,s=n.get(ur,null,{optional:!0}),l=new ko(r.impl,nC(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(l),l}var fu=null,sb=null;function Kh(t,n,e,i){fu?.maybeQueueEnterAnimation(t,n,e,i)}function Qh(t,n,e,i){fu===null?i(!1):fu.runLeaveAnimationsWithCallback(t,n,e,i)}function rC(t){sb?.initViewDetachAnimations(t)}function oC(t){sb?.clearViewDetachAnimations(t)}function fr(t,n,e,i,r,o,a,s){if(r!=null){let l,c=!1;vt(r)?l=r:fn(r)&&(c=!0,r=r[Mt]);let d=Be(r);t===0&&i!==null?(Kh(s,i,o,e),a==null?eb(n,i,d):Si(n,i,d,a||null,!0)):t===1&&i!==null?(Si(n,i,d,a||null,!0),eC(o,d,s),mr.has(d)||Kh(s,i,o,e)):t===2?(s?.[Xp]?.leave?.has(o.index)&&tC(o,d,s),mr.delete(d),Qh(s,o,e,u=>{if(mr.has(d)){mr.delete(d);return}tb(n,d,c,u)})):t===3&&(mr.delete(d),Qh(s,o,e,()=>{n.destroyNode(d)})),l!=null&&gC(n,t,e,l,o,i,a)}}function aC(t,n){lb(t,n),n[Mt]=null,n[Ve]=null}function sC(t,n,e,i,r,o){i[Mt]=r,i[Ve]=n,al(t,i,e,1,r,o)}function lb(t,n){n[Tt].changeDetectionScheduler?.notify(9),al(t,n,n[ge],2,null,null)}function lC(t){let n=t[ar];if(!n)return zd(t[E],t);for(;n;){let e=null;if(fn(n))e=n[ar];else{let i=n[Ce];i&&(e=i)}if(!e){for(;n&&!n[bt]&&n!==t;)fn(n)&&zd(n[E],n),n=n[Ee];n===null&&(n=t),fn(n)&&zd(n[E],n),e=n&&n[bt]}n=e}}function Xu(t,n){let e=t[pi],i=e.indexOf(n);e.splice(i,1)}function ol(t,n){if(hi(n))return;let e=n[ge];e.destroyNode&&al(t,n,e,3,null,null),lC(n)}function zd(t,n){if(hi(n))return;let e=C(null);try{n[k]&=-129,n[k]|=256,n[ot]&&Nn(n[ot]),dC(t,n),cC(t,n),n[E].type===1&&n[ge].destroy();let i=n[On];if(i!==null&&vt(n[Ee])){i!==n[Ee]&&Xu(i,n);let r=n[Wt];r!==null&&r.detachView(t)}tu(n)}finally{C(e)}}function cC(t,n){let e=t.cleanup,i=n[or];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[or]=null);let r=n[ln];if(r!==null){n[ln]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[cn];if(o!==null){n[cn]=null;for(let a of o)a.destroy()}}function dC(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Di)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],l=o[a+1];ee(Y.LifecycleHookStart,s,l);try{l.call(s)}finally{ee(Y.LifecycleHookEnd,s,l)}}else{ee(Y.LifecycleHookStart,r,o);try{o.call(r)}finally{ee(Y.LifecycleHookEnd,r,o)}}}}}function cb(t,n,e){if(n===null)throw new S(510,!1);return uC(t,n.parent,e)}function uC(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Mt];if(pn(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Ft.None||r===Ft.Emulated)return null}return yt(i,e)}function db(t,n,e){return fC(t,n,e)}function mC(t,n,e){return t.type&40?yt(t,e):null}var fC=mC,Xh;function Ju(t,n,e,i){let r=cb(t,i,n),o=n[ge],a=i.parent||n[Ve],s=db(a,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)qh(o,r,e[l],s,!1);else qh(o,r,e,s,!1);Xh!==void 0&&Xh(o,i,n,e,r)}function Eo(t,n){if(n!==null){let e=n.type;if(e&3)return yt(n,t);if(e&4)return pu(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Eo(t,i);{let r=t[n.index];return vt(r)?pu(-1,r):Be(r)}}else{if(e&128)return Eo(t,n.next);if(e&32)return Yu(n,t)()||Be(t[n.index]);{let i=ub(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=dn(t[Ue]);return Eo(r,i)}else return Eo(t,n.next)}}}return null}function ub(t,n){if(n!==null){let i=t[Ue][Ve],r=n.projection;return i.projection[r]}return null}function pu(t,n){let e=Ce+t+1;if(e<n.length){let i=n[e],r=i[E].firstChild;if(r!==null)return Eo(i,r)}return n[fi]}function em(t,n,e,i,r,o,a){for(;e!=null;){let s=i[mn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(a&&n===0&&(l&&gr(Be(l),i),e.flags|=2),!Js(e))if(c&8)em(t,n,e.child,i,r,o,!1),fr(n,t,s,r,l,e,o,i);else if(c&32){let d=Yu(e,i),u;for(;u=d();)fr(n,t,s,r,u,e,o,i);fr(n,t,s,r,l,e,o,i)}else c&16?mb(t,n,i,e,r,o):fr(n,t,s,r,l,e,o,i);e=a?e.projectionNext:e.next}}function al(t,n,e,i,r,o){t.type===3?pC(e,i,n,r,o):em(e,i,t.firstChild,n,r,o,!1)}function pC(t,n,e,i,r){let a=e[E].firstChild,s=a.next,l=Be(e[a.index]),c=Be(e[s.index]),d=s.index+1,u=e[d];if(n===1||n===0)i!==null&&(u&&u.hasChildNodes()?Si(t,i,u,r,!0):(Si(t,i,l,r,!0),Si(t,i,c,r,!0)));else if(n===2){if(u||(u=document.createDocumentFragment(),e[d]=u),l&&l.parentNode===u)return;let p=l;for(;p!==null;){let f=p.nextSibling;if(u.appendChild(p),p===c)break;p=f}}}function hC(t,n,e){let i=n[ge],r=cb(t,e,n),o=e.parent||n[Ve],a=db(o,e,n);mb(i,0,n,e,r,a)}function mb(t,n,e,i,r,o){let a=e[Ue],l=a[Ve].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let d=l[c];fr(n,t,e[mn],r,d,i,o,e)}else{let c=l,d=a[Ee];Og(i)&&(c.flags|=128),em(t,n,c,d,r,o,!0)}}function gC(t,n,e,i,r,o,a){let s=i[fi],l=Be(i);if(s!==l&&fr(n,t,e,o,s,r,a),(i[k]&4)===0)for(let c=Ce;c<i.length;c++){let d=i[c];al(d[E],d,t,n,o,s)}}function bC(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Zt.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Zt.Important),t.setStyle(e,i,r,o))}}function tm(t,n,e,i,r,o,a,s,l,c,d){let u=Ne+i,p=u+r,f=vC(u,p),h=typeof c=="function"?c():c;return f[E]={type:t,blueprint:f,template:e,queries:null,viewQuery:s,declTNode:n,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:h,incompleteFirstPass:!1,ssrId:d}}function vC(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Pt);return e}function yC(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=tm(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function nm(t,n,e,i,r,o,a,s,l,c,d){let u=n.blueprint.slice();return u[Mt]=r,u[k]=i|4|128|8|64|1024,(c!==null||t&&t[k]&2048)&&(u[k]|=2048),yd(u),u[Ee]=u[Rn]=t,u[we]=e,u[Tt]=a||t&&t[Tt],u[ge]=s||t&&t[ge],u[mn]=l||t&&t[mn]||null,u[Ve]=o,u[vo]=d0(),u[di]=d,u[hd]=c,u[Ue]=n.type==2?t[Ue]:u,u}function _C(t,n,e){let i=yt(n,t),r=yC(e),o=t[Tt].rendererFactory,a=im(t,nm(t,r,null,fb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function fb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function pb(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function im(t,n){return t[ar]?t[pd][bt]=n:t[ar]=n,t[pd]=n,n}function G(t=1){hb(Ie(),F(),qt()+t,!1)}function hb(t,n,e,i){if(!i)if((n[k]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Fs(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Ls(n,o,0,e)}Fn(e)}var sl=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(sl||{});function hu(t,n,e,i){let r=C(null);try{let[o,a,s]=t.inputs[e],l=null;(a&sl.SignalBased)!==0&&(l=n[o][Te]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):yg(n,l,o,i)}finally{C(r)}}function gb(t,n,e,i,r){let o=qt(),a=i&2;try{Fn(-1),a&&n.length>Ne&&hb(t,n,Ne,!1);let s=a?Y.TemplateUpdateStart:Y.TemplateCreateStart;ee(s,r,e),e(i,r)}finally{Fn(o);let s=a?Y.TemplateUpdateEnd:Y.TemplateCreateEnd;ee(s,r,e)}}function rm(t,n,e){NC(t,n,e),(e.flags&64)===64&&IC(t,n,e)}function ll(t,n,e=yt){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function xC(t,n,e,i){let o=i.get(zg,Hg)||e===Ft.ShadowDom||e===Ft.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return wC(a),a}function wC(t){CC(t)}var CC=()=>null;function SC(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function DC(t,n,e,i,r,o){let a=n[E];if(om(t,a,n,e,i)){pn(t)&&EC(n,t.index);return}t.type&3&&(e=SC(e)),bb(t,n,e,i,r,o)}function bb(t,n,e,i,r,o){if(t.type&3){let a=yt(t,n);i=o!=null?o(i,t.value||"",e):i,i=vb(e,a,i),r.setProperty(a,e,i)}else t.type&12}function vb(t,n,e,i=!1){return t==="src"&&(n.tagName==="IMG"||n.tagName==="VIDEO")?e==null?i?e:"":at(e):e}function EC(t,n){let e=_t(n,t);e[k]&16||(e[k]|=64)}function NC(t,n,e){let i=e.directiveStart,r=e.directiveEnd;pn(e)&&_C(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||zs(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],l=Io(n,t,a,e);if(gr(l,n),o!==null&&AC(n,a-i,l,s,e,o),At(s)){let c=_t(e.index,n);c[we]=Io(n,t,a,e)}}}function IC(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=ph();try{Fn(o);for(let s=i;s<r;s++){let l=t.data[s],c=n[s];Cs(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&kC(l,c)}}finally{Fn(-1),Cs(a)}}function kC(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function yb(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];ob(n,o.selectors,!1)&&(i??=[],At(o)?i.unshift(o):i.push(o))}return i}function MC(t,n,e,i,r,o){let a=yt(t,n);i=vb(e,a,i,!0),TC(n[ge],a,o,t.value,e,i,r)}function TC(t,n,e,i,r,o,a){if(o==null)a?.(o,i||"",r),t.removeAttribute(n,r,e);else{let s=a==null?us(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function AC(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];hu(i,e,l,c)}}function _b(t,n,e,i,r){let o=Ne+e,a=n[E],s=r(a,n,t,i,e);n[o]=s,cr(t,!0);let l=t.type===2;return l?(nb(n[ge],s,t),(sh()===0||_o(t))&&gr(s,n),lh()):gr(s,n),Ns()&&(!l||!Js(t))&&Ju(a,n,s,t),t}function xb(t){let n=t;return kd()?Md():(n=n.parent,cr(n,!1)),n}function RC(t,n){let e=t[mn];if(!e)return;let i;try{i=e.get(_i,null)}catch{i=null}i?.(n)}function om(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],d=a[l+1],u=n.data[c];hu(u,e[c],d,r),s=!0}if(o)for(let l of o){let c=e[l],d=n.data[l];hu(d,c,i,r),s=!0}return s}function OC(t,n){let e=_t(n,t),i=e[E];FC(i,e);let r=e[Mt];r!==null&&e[di]===null&&(e[di]=Ug(r,e[mn])),ee(Y.ComponentStart);try{am(i,e,e[we])}finally{ee(Y.ComponentEnd,e[we])}}function FC(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function am(t,n,e){Ds(n);try{let i=t.viewQuery;i!==null&&iu(1,i,e);let r=t.template;r!==null&&gb(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Wt]?.finishViewCreation(t),t.staticContentQueries&&$g(t,n),t.staticViewQueries&&iu(2,t.viewQuery,e);let o=t.components;o!==null&&LC(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[k]&=-5,Es()}}function LC(t,n){for(let e=0;e<n.length;e++)OC(t,n[e])}function Po(t,n,e,i){let r=C(null);try{let o=n.tView,s=t[k]&4096?4096:16,l=nm(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[On]=c;let d=t[Wt];return d!==null&&(l[Wt]=d.createEmbeddedView(o)),am(o,l,e),l}finally{C(r)}}function br(t,n){return!n||n.firstChild===null||Og(t)}function Mo(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,a=o.next,s=Be(n[o.index]),l=Be(n[a.index]),c=s;for(;c!==null&&(i.push(c),c!==l);)c=c.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(vt(o)){let s=o[fi];s!==o[Mt]&&i.push(Be(o)),o[k]&4||wb(o,i),i.push(s)}else i.push(Be(o));let a=e.type;if(a&8)Mo(t,n,e.child,i);else if(a&32){let s=Yu(e,n),l;for(;l=s();)i.push(l)}else if(a&16){let s=ub(n,e);if(Array.isArray(s))i.push(...s);else{let l=dn(n[Ue]);Mo(l[E],l,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function wb(t,n){for(let e=Ce;e<t.length;e++){let i=t[e],r=i[E].firstChild;r!==null&&Mo(i[E],i,r,n)}}function Cb(t){if(t[mi]!==null){for(let n of t[mi])n.impl.addSequence(n);t[mi].length=0}}var Sb=[];function PC(t){return t[ot]??VC(t)}function VC(t){let n=Sb.pop()??Object.create(jC);return n.lView=t,n}function BC(t){t.lView[ot]!==t&&(t.lView=null,Sb.push(t))}var jC=z(x({},Zn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{gi(t.lView)},consumerOnSignalRead(){this.lView[ot]=this}});function HC(t){let n=t[ot]??Object.create(zC);return n.lView=t,n}var zC=z(x({},Zn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=dn(t.lView);for(;n&&!Db(n[E]);)n=dn(n);n&&_d(n)},consumerOnSignalRead(){this.lView[ot]=this}});function Db(t){return t.type!==2}function Eb(t){if(t[cn]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[cn])if(i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),t[cn]===null))return;n=e&&!!(t[k]&8192)}}var UC=100;function Nb(t,n=0){let i=t[Tt].rendererFactory,r=!1;r||i.begin?.();try{$C(t,n)}finally{r||i.end?.()}}function $C(t,n){let e=Td();try{co(!0),gu(t,n);let i=0;for(;xo(t);){if(i===UC)throw new S(103,!1);i++,gu(t,1)}}finally{co(e)}}function GC(t,n,e,i){if(hi(n))return;let r=n[k],o=!1,a=!1;Ds(n);let s=!0,l=null,c=null;o||(Db(t)?(c=PC(n),l=En(c)):xa()===null?(s=!1,c=HC(n),l=En(c)):n[ot]&&(Nn(n[ot]),n[ot]=null));try{yd(n),uh(t.bindingStartIndex),e!==null&&gb(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let f=t.preOrderCheckHooks;f!==null&&Fs(n,f,null)}else{let f=t.preOrderHooks;f!==null&&Ls(n,f,0,null),jd(n,0)}if(a||WC(n),Eb(n),kb(n,0),t.contentQueries!==null&&$g(t,n),!o)if(d){let f=t.contentCheckHooks;f!==null&&Fs(n,f)}else{let f=t.contentHooks;f!==null&&Ls(n,f,1),jd(n,1)}YC(t,n);let u=t.components;u!==null&&Tb(n,u,0);let p=t.viewQuery;if(p!==null&&iu(2,p,i),!o)if(d){let f=t.viewCheckHooks;f!==null&&Fs(n,f)}else{let f=t.viewHooks;f!==null&&Ls(n,f,2),jd(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[bs]){for(let f of n[bs])f();n[bs]=null}o||(Cb(n),n[k]&=-73)}catch(d){Ib(n,d,o)}finally{c!==null&&(Kn(c,l),s&&BC(c)),Es()}}function Ib(t,n,e){let i=!1,r=n,o=t,a=C(null);try{for(;o!==null;){if(vt(o)){o=o[Ee];continue}let s=o[sr];if(s)try{let l=t[Ue][we],c=l?.constructor,d={declarationInstance:l,declarationType:c,caughtBy:s},u=C(null);try{s(Eh(r),d)}finally{C(u)}i=!0;break}catch(l){r=l}o=o[Ee]}}finally{C(a)}if(!i)throw e||gi(t),r}function kb(t,n){for(let e=Lg(t);e!==null;e=Pg(e))for(let i=Ce;i<e.length;i++){let r=e[i];Mb(r,n)}}function WC(t){for(let n=Lg(t);n!==null;n=Pg(n)){if(!(n[k]&2))continue;let e=n[pi];for(let i=0;i<e.length;i++){let r=e[i];_d(r)}}}function qC(t,n,e){ee(Y.ComponentStart);let i=_t(n,t);try{Mb(i,e)}finally{ee(Y.ComponentEnd,i[we])}}function Mb(t,n){_s(t)&&gu(t,n)}function gu(t,n){let i=t[E],r=t[k],o=t[ot],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&zi(o)),a||=!1,o&&(o.dirty=!1),t[k]&=-9217,a)GC(i,t,i.template,t[we]);else if(r&8192){let s=C(null);try{try{Eb(t)}catch(c){Ib(t,c,!1)}kb(t,1);let l=i.components;l!==null&&Tb(t,l,1),Cb(t)}finally{C(s)}}}function Tb(t,n,e){for(let i=0;i<n.length;i++)qC(t,n[i],e)}function YC(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Fn(~r);else{let o=r,a=e[++i],s=e[++i];fh(a,o);let l=n[o];ee(Y.HostBindingsUpdateStart,l);try{s(2,l)}finally{ee(Y.HostBindingsUpdateEnd,l)}}}}finally{Fn(-1)}}function sm(t,n){let e=Td()?64:1088;for(t[Tt].changeDetectionScheduler?.notify(n);t;){t[k]|=e;let i=dn(t);if(lr(t)&&!i)return t;t=i}return null}function Ab(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function Rb(t,n){let e=Ce+n;if(e<t.length)return t[e]}function Vo(t,n,e,i=!0){let r=n[E];if(ZC(r,n,t,e),i){let a=pu(e,t),s=n[ge],l=s.parentNode(t[fi]);l!==null&&sC(r,t[Ve],s,n,l,a)}let o=n[di];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Ob(t,n){let e=To(t,n);return e!==void 0&&ol(e[E],e),e}function To(t,n){if(t.length<=Ce)return;let e=Ce+n,i=t[e];if(i){let r=i[On];r!==null&&r!==t&&Xu(r,i),n>0&&(t[e-1][bt]=i[bt]);let o=go(t,Ce+n);aC(i[E],i);let a=o[Wt];a!==null&&a.detachView(o[E]),i[Ee]=null,i[bt]=null,i[k]&=-129}return i}function ZC(t,n,e,i){let r=Ce+i,o=e.length;i>0&&(e[r-1][bt]=n),i<o-Ce?(n[bt]=e[r],ld(e,Ce+i,n)):(e.push(n),n[bt]=null),n[Ee]=e;let a=n[On];a!==null&&e!==a&&Fb(a,n);let s=n[Wt];s!==null&&s.insertView(t),xs(n),n[k]|=128}function Fb(t,n){let e=t[pi],i=n[Ee];if(fn(i))t[k]|=2;else{let r=i[Ee][Ue];n[Ue]!==r&&(t[k]|=2)}e===null?t[pi]=[n]:e.push(n)}var Ln=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[E];return Mo(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[we]}set context(n){this._lView[we]=n}get destroyed(){return hi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Ee];if(vt(n)){let e=n[yo],i=e?e.indexOf(this):-1;i>-1&&(To(n,i),go(e,i))}this._attachedToViewContainer=!1}ol(this._lView[E],this._lView)}onDestroy(n){ws(this._lView,n)}markForCheck(){sm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[k]&=-129}reattach(){xs(this._lView),this._lView[k]|=128}detectChanges(){this._lView[k]|=1024,Nb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new S(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=lr(this._lView),e=this._lView[On];e!==null&&!n&&Xu(e,this._lView),lb(this._lView[E],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new S(902,!1);this._appRef=n;let e=lr(this._lView),i=this._lView[On];i!==null&&!e&&Fb(i,this._lView),xs(this._lView)}};var Lt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=KC;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Po(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Ln(o)}}return t})();function KC(){return cl(Ge(),F())}function cl(t,n){return t.type&4?new Lt(n,t,_r(t,n)):null}function xr(t,n,e,i,r){let o=t.data[n];if(o===null)o=QC(t,n,e,i,r),mh()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=ch();o.injectorIndex=a===null?-1:a.injectorIndex}return cr(o,!0),o}function QC(t,n,e,i,r){let o=Id(),a=kd(),s=a?o:o&&o.parent,l=t.data[n]=JC(t,s,e,n,i,r);return XC(t,l,o,a),l}function XC(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function JC(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return Dd()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:Fd(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function eS(t){let n=t[gd]??[],i=t[Ee][ge],r=[];for(let o of n)o.data[jg]!==void 0?r.push(o):tS(o,i);t[gd]=r}function tS(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[Bg];for(;e<r;){let o=i.nextSibling;tb(n,i,!1),i=o,e++}}}var nS=()=>null,iS=()=>null;function $s(t,n){return nS(t,n)}function Lb(t,n,e){return iS(t,n,e)}var Pb=class{},ke=class{},_e=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>rS()};function rS(){let t=F(),n=Ge(),e=_t(n.index,t);return(fn(e)?e:t)[ge]}var Vb=(()=>{class t{static \u0275prov=X({token:t,providedIn:"root",factory:()=>null})}return t})();function Bb(t){return t.debugInfo?.className||t.type.name||null}var Vs={},Gs=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Vs,i);return r!==Vs||e===Vs?r:this.parentInjector.get(n,e,i)}};function Vn(t,n,e){if(e===Pt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Ud(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&m0(r,o);let a=pn(t)?_t(t.index,n):n;sm(a,5);let s=n[we],l=Jh(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)l=Jh(n,s,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function Jh(t,n,e,i){let r=C(null);try{return ee(Y.OutputStart,n,e),e(i)!==!1}catch(o){return RC(t,o),!1}finally{ee(Y.OutputEnd,n,e),C(r)}}function oS(t,n,e,i,r,o,a,s){let l=_o(t),c=!1,d=null;if(!i&&l&&(d=sS(n,e,o,t.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=a,d.__ngLastListenerFn__=a,c=!0}else{let u=yt(t,e),p=i?i(u):u;p0(e,p,o,s),i||(s.__ngNativeEl__=u);let f=r.listen(p,o,s);if(!aS(o)){let h=i?g=>i(Be(g[t.index])):t.index;jb(h,n,e,o,s,f,!1)}}return c}function aS(t){return t.startsWith("animation")||t.startsWith("transition")}function sS(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[or],l=r[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function jb(t,n,e,i,r,o,a){let s=n.firstCreatePass?wd(n):null,l=xd(e),c=l.length;l.push(r,o),s&&s.push(i,t,c,(c+1)*(a?-1:1))}function eg(t,n,e,i,r,o){let a=n[e],s=n[E],c=s.data[e].outputs[i],u=a[c].subscribe(o);jb(t.index,s,n,r,o,u,!0)}var bu=Symbol("BINDING");var Ii=new v("");function Ws(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=ls(r,s);else if(o==2){let l=s,c=n[++a];i=ls(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function Le(t,n=0){let e=F();if(e===null)return j(t,n);let i=Ge();return Mg(i,e,Ae(t),n)}function Hb(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,l=null,c=null;for(let d of a)if(d.resolveHostDirectives!==null){[s,l,c]=d.resolveHostDirectives(a);break}dS(t,n,e,s,o,l,c)}o!==null&&i!==null&&lS(e,i,o)}function lS(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new S(-301,!1);i.push(n[r],o)}}function cS(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function dS(t,n,e,i,r,o,a){let s=i.length,l=null;for(let p=0;p<s;p++){let f=i[p];l===null&&At(f)&&(l=f,cS(t,e,p)),Jd(zs(e,n),t,f.type)}gS(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let p=0;p<s;p++){let f=i[p];f.providersResolver&&f.providersResolver(f)}let c=!1,d=!1,u=pb(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let p=0;p<s;p++){let f=i[p];if(e.mergedAttrs=hr(e.mergedAttrs,f.hostAttrs),mS(t,e,n,u,f),hS(u,f,r),a!==null&&a.has(f)){let[g,y]=a.get(f);e.directiveToIndex.set(f.type,[u,g+e.directiveStart,y+e.directiveStart])}else(o===null||!o.has(f))&&e.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(e.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(e.flags|=64);let h=f.type.prototype;!c&&(h.ngOnChanges||h.ngOnInit||h.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!d&&(h.ngOnChanges||h.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}uS(t,e,o)}function uS(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))tg(0,n,r,i),tg(1,n,r,i),ig(n,i,!1);else{let o=e.get(r);ng(0,n,o,i),ng(1,n,o,i),ig(n,i,!0)}}}function tg(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),zb(n,o)}}function ng(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),zb(n,a)}}function zb(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function ig(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||qu(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let l=i[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&Object.hasOwn(r,l)){let c=r[l];for(let d of c)if(d===n){a??=[],a.push(l,i[s+1]);break}}else if(e&&Object.hasOwn(o,l)){let c=o[l];for(let d=0;d<c.length;d+=2)if(c[d]===n){a??=[],a.push(c[d+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function mS(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=ai(r.type,!0)),a=new Di(o,At(r),Le,null);t.blueprint[i]=a,e[i]=a,fS(t,n,i,pb(t,e,r.hostVars,Pt),r)}function fS(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;pS(a)!=s&&a.push(s),a.push(e,i,o)}}function pS(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function hS(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;At(n)&&(e[""]=t)}}function gS(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function Ub(t,n,e,i,r,o,a,s){let l=n[E],c=l.consts,d=xt(c,a),u=xr(l,t,e,i,d);return o&&Hb(l,n,u,xt(c,s),r),u.mergedAttrs=hr(u.mergedAttrs,u.attrs),u.attrs!==null&&Ws(u,u.attrs,!1),u.mergedAttrs!==null&&Ws(u,u.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,u),u}function $b(t,n){wg(t,n),bd(n)&&t.queries.elementEnd(n)}function bS(t,n,e,i,r,o){let a=n.consts,s=xt(a,r),l=xr(n,t,e,i,s);if(l.mergedAttrs=hr(l.mergedAttrs,l.attrs),o!=null){let c=xt(a,o);l.localNames=[];for(let d=0;d<c.length;d+=2)l.localNames.push(c[d],-1)}return l.attrs!==null&&Ws(l,l.attrs,!1),l.mergedAttrs!==null&&Ws(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var Gb=typeof ShadowRoot<"u",vS=typeof Document<"u";function yS(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&sl.SignalBased)!==0};return r&&(o.transform=r),o})}function _S(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function xS(t,n,e){let i=n instanceof Re?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Gs(e,i):e}function wS(t){let n=t.get(ke,null);if(n===null)throw new S(407,!1);let e=t.get(Vb,null),i=t.get(un,null),r=t.get(Kt,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function CS(t,n,e){let i=Wb(t);return Jg(n,i,i==="svg"?er:i==="math"?vs:e)}function SS(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new S(905,!1)}function Wb(t){return(t.selectors[0][0]||"div").toLowerCase()}var vr=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=yS(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=_S(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=Q0(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a,s,l){ee(Y.DynamicComponentStart);let c=C(null);try{let d=this.componentDef,u=xS(d,r||this.ngModule,n),p=wS(u),f=p.tracingService;return f&&f.componentCreate?f.componentCreate(Bb(d),()=>this.createComponentRef(p,u,e,i,o,a,s,l)):this.createComponentRef(p,u,e,i,o,a,s,l)}finally{C(c)}}createComponentRef(n,e,i,r,o,a,s,l){let c=this.componentDef,d=DS(r,c,a,o,l),u=n.rendererFactory.createRenderer(null,c),p=r?xC(u,r,c.encapsulation,e):CS(c,u,s??null);SS(p);let f=e.get(Ii,null),h=ES(p,()=>e.get(V,null)??Bu());f&&f.addHost(h);let g=a?.some(rg)||o?.some(B=>typeof B!="function"&&B.bindings.some(rg)),y=nm(null,d,null,512|fb(c),null,null,n,u,e,null,Ug(p,e,!0));f&&Gb&&h instanceof ShadowRoot&&ws(y,()=>{f.removeHost(h)}),y[Ne]=p,Ds(y);let _=null;try{let B=Ub(Ne,y,2,"#host",()=>d.directiveRegistry,!0,0);nb(u,p,B),gr(p,y),rm(d,y,B),Gg(d,B,y),$b(d,B),i!==void 0&&IS(B,this.ngContentSelectors,i),_=_t(B.index,y),y[we]=_[we],am(d,y,null)}catch(B){throw _!==null&&tu(_),tu(y),B}finally{ee(Y.DynamicComponentEnd),Es()}return new qs(this.componentType,y,!!g)}};function DS(t,n,e,i,r){let o=t?["ng-version","22.2.5"]:X0(n.selectors[0]),a=null,s=null,l=0;if(e)for(let u of e)l+=u[bu].requiredVars,u.create&&(u.targetIdx=0,(a??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let p=i[u];if(typeof p!="function")for(let f of p.bindings){l+=f[bu].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(a??=[]).push(f)),f.update&&(f.targetIdx=h,(s??=[]).push(f))}}let c=[n];if(i)for(let u of i){let p=typeof u=="function"?u:u.type,f=ho(p);c.push(f)}return tm(0,null,NS(a,s),1,l,c,null,null,null,[o],null)}function ES(t,n){let e=t.getRootNode?.();return vS&&e instanceof Document?e.head:e&&Gb&&e instanceof ShadowRoot?e:n().head}function NS(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function rg(t){let n=t[bu].kind;return n==="input"||n==="twoWay"}var qs=class extends Pb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=ys(e[E],Ne),this.location=_r(this._tNode,e),this.instance=_t(this._tNode.index,e)[we],this.hostView=this.changeDetectorRef=new Ln(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=om(i,r[E],r,n,e);this.previousInputValues.set(n,e);let a=_t(i.index,r);sm(a,1)}get injector(){return new bn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function IS(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Qt=(()=>{class t{static __NG_ELEMENT_ID__=kS}return t})();function kS(){let t=Ge();return qb(t,F())}var vu=class t extends Qt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return _r(this._hostTNode,this._hostLView)}get injector(){return new bn(this._hostTNode,this._hostLView)}get parentInjector(){let n=Lu(this._hostTNode,this._hostLView);if(Sg(n)){let e=Hs(n,this._hostLView),i=js(n),r=e[E].data[i+8];return new bn(r,e)}else return new bn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=og(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Ce}createEmbeddedView(n,e,i){let r,o,a;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector,a=i.onError);let s=$s(this._lContainer,n.ssrId),l=n.createEmbeddedViewImpl(e||{},o,s);return a&&(l._lView[sr]=a),this.insertImpl(l,r,br(this._hostTNode,s)),l}createComponent(n,e,i,r,o,a,s){let l,c,d=e||{};l=d.index,i=d.injector,r=d.projectableNodes,o=d.environmentInjector||d.ngModuleRef,a=d.directives,s=d.bindings,c=d.onError;let u=new vr(Mn(n)),p=i||this.parentInjector;if(!o&&u.ngModule==null){let B=this.parentInjector.get(Re,null);B&&(o=B)}let f=Mn(u.componentType??{}),h=$s(this._lContainer,f?.id??null),g=h?.firstChild??null,y=u.create(p,r,g,o,a,s,this._getHostElementNamespace());return c&&(y.hostView._lView[sr]=c),this.insertImpl(y.hostView,l,br(this._hostTNode,h)),y}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[Ve];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(ih(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=r[Ee],c=new t(l,l[Ve],l[Ee]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return Vo(a,r,o,i),n.attachToViewContainerRef(),ld($d(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=og(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=To(this._lContainer,e);i&&(go($d(this._lContainer),e),ol(i[E],i))}detach(n){let e=this._adjustIndex(n,-1),i=To(this._lContainer,e);return i&&go($d(this._lContainer),e)!=null?new Ln(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function og(t){return t[yo]}function $d(t){return t[yo]||(t[yo]=[])}function qb(t,n){let e,i=n[t.index];return vt(i)?e=i:(e=Ab(i,n,null,t),n[t.index]=e,im(n,e)),TS(e,n,t,i),new vu(e,t,n)}function MS(t,n){let e=t[ge],i=e.createComment(""),r=yt(n,t),o=e.parentNode(r);return Si(e,o,i,e.nextSibling(r),!1),i}var TS=OS,AS=()=>!1;function RS(t,n,e){return AS(t,n,e)}function OS(t,n,e,i){if(t[fi])return;let r;e.type&8?r=Be(i):r=MS(n,e),t[fi]=r}var yu=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},_u=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)cm(n,e).matches!==null&&this.queries[e].setDirty()}},Ys=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=BS(n):this.predicate=n}},xu=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},wu=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,FS(e,o)),this.matchTNodeWithReadOption(n,e,Ps(e,n,o,!1,!1))}else i===Lt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Ps(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===M||r===Qt||r===O||r===Lt&&e.type&4)this.addMatch(e.index,-2);else{let o=Ps(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function FS(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function LS(t,n){return t.type&11?_r(t,n):t.type&4?cl(t,n):null}function PS(t,n,e,i){return e===-1?LS(n,t):e===-2?VS(t,n,i):Io(t,t[E],e,n)}function VS(t,n,e){if(e===M)return _r(n,t);if(e===Lt)return cl(n,t);if(e===Qt)return qb(n,t);if(e===O)return new bn(n,t)}function Yb(t,n,e,i){let r=n[Wt].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let d=o[c];s.push(PS(n,d,a[l+1],e.metadata.read))}}r.matches=s}return r.matches}function Cu(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=Yb(t,n,r,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)i.push(a[s/2]);else{let c=o[s+1],d=n[-l];for(let u=Ce;u<d.length;u++){let p=d[u];p[On]===p[Ee]&&Cu(p[E],p,c,i)}if(d[pi]!==null){let u=d[pi];for(let p=0;p<u.length;p++){let f=u[p];Cu(f[E],f,c,i)}}}}}return i}function lm(t,n){return t[Wt].queries[n].queryList}function Zb(t,n,e){let i=new Us((e&4)===4);return ah(t,n,i,i.destroy),(n[Wt]??=new _u).queries.push(new yu(i))-1}function Kb(t,n,e){let i=Ie();return i.firstCreatePass&&(Xb(i,new Ys(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Zb(i,F(),n)}function Qb(t,n,e,i){let r=Ie();if(r.firstCreatePass){let o=Ge();Xb(r,new Ys(n,e,i),o.index),jS(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Zb(r,F(),e)}function BS(t){return t.split(",").map(n=>n.trim())}function Xb(t,n,e){t.queries===null&&(t.queries=new xu),t.queries.track(new wu(n,e))}function jS(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function cm(t,n){return t.queries.getByIndex(n)}function Jb(t,n){let e=t[E],i=cm(e,n);return i.crossesNgTemplate?Cu(e,t,n,[]):Yb(e,t,i,n)}function ev(t,n,e){let i,r=Qr(()=>{i._dirtyCounter();let o=HS(i,t);if(n&&o===void 0)throw new S(-951,!1);return o});return i=r[Te],i._dirtyCounter=Q(0),i._flatValue=void 0,r}function dm(t){return ev(!0,!1,t)}function um(t){return ev(!0,!0,t)}function tv(t,n){let e=t[Te];e._lView=F(),e._queryIndex=n,e._queryList=lm(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function HS(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[k]&4)return n?void 0:ze;let r=lm(e,i),o=Jb(e,i);return r.reset(o,Rg),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function ki(t){return!!t&&typeof t.then=="function"}function mm(t){return!!t&&typeof t.subscribe=="function"}var Pn=class{};var Ao=class extends Pn{injector;instance=null;constructor(n){super();let e=new li([...n.providers,{provide:Pn,useValue:this}],n.parent||rr(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function nv(t,n,e=null){return new Ao({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var zS=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=ud(!1,e.type),r=i.length>0?nv([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=X({token:t,providedIn:"environment",factory:()=>new t(j(Re))})}return t})();function T(t){return Fo(()=>{let n=iv(t),e=z(x({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==Pu.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(zS).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Ft.Emulated,styles:t.styles||ze,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Ni("NgStandalone"),rv(e);let i=t.dependencies;return e.directiveDefs=ag(i,US),e.pipeDefs=ag(i,Bp),e.id=WS(e),e})}function US(t){return Mn(t)||ho(t)}function je(t){return Fo(()=>({type:t.type,bootstrap:t.bootstrap||ze,declarations:t.declarations||ze,imports:t.imports||ze,exports:t.exports||ze,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function $S(t,n){if(t==null)return An;let e={};for(let i in t)if(Object.hasOwn(t,i)){let r=t[i],o,a,s,l;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,l=r[3]||null):(o=r,a=r,s=sl.None,l=null),e[o]=[i,s,l],n[o]=a}return e}function GS(t){if(t==null)return An;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function I(t){return Fo(()=>{let n=iv(t);return rv(n),n})}function iv(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||An,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||ze,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:$S(t.inputs,n),outputs:GS(t.outputs),debugInfo:null}}function rv(t){t.features?.forEach(n=>n(t))}function ag(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function WS(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var ov=new v("");var fm=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=m(ov,{optional:!0})??[];injector=m(O);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=gs(this.injector,r);if(ki(o))e.push(o);else if(mm(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function pm(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=qS,e.hostDirectives=i?t.map(Su):[t]):i?e.hostDirectives.unshift(...t.map(Su)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function qS(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let a=t[o];if(a.hostDirectives!==null){let s=n.length;i??=new Map,r??=new Map,av(a,n,i,t),r.set(a,[s,n.length-1])}o===0&&At(a)&&(e=!0,n.push(a))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return i!==null&&i.forEach((o,a)=>{YS(a.declaredInputs,o.inputs)}),[n,i,r]}function av(t,n,e,i){if(t.hostDirectives!==null)for(let r of t.hostDirectives)if(typeof r=="function"){let o=r();for(let a of o)sg(Su(a),n,e,i)}else sg(r,n,e,i)}function sg(t,n,e,i){let r=ho(t.directive);if(av(r,n,e,i),e.has(r)){let o=e.get(r);lg(o,t.inputs,"input"),lg(o,t.outputs,"output")}else i.includes(r)||(e.set(r,t),n.push(r))}function lg(t,n,e){let i=e==="input"?t.inputs:t.outputs;Object.keys(n).forEach(r=>{let o=n[r];(!Object.hasOwn(i,r)||i[r]===o)&&(i[r]=o)})}function Su(t){return typeof t=="function"?{directive:Ae(t),inputs:{},outputs:{}}:{directive:Ae(t.directive),inputs:cg(t.inputs),outputs:cg(t.outputs)}}function cg(t){let n={};if(t!==void 0&&t.length>0)for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function YS(t,n){for(let e in n)if(Object.hasOwn(n,e)){let i=n[e],r=t[e];t[i]=r}}function ZS(t){return Object.getPrototypeOf(t.prototype).constructor}function P(t){let n=ZS(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,fo)?n[fo]:void 0,a=Object.hasOwn(n,po)?n[po]:void 0;if(At(t))r=o??a;else{if(o)throw new S(903,!1);r=a}if(r){if(e){i.push(r);let l=t;l.inputs=Gd(t.inputs),l.declaredInputs=Gd(t.declaredInputs),l.outputs=Gd(t.outputs);let c=r.hostBindings;c&&eD(t,c);let d=r.viewQuery,u=r.contentQueries;if(d&&XS(t,d),u&&JS(t,u),KS(t,r),Vp(t.outputs,r.outputs),At(r)&&r.data.animation){let p=t.data;p.animation=(p.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===P&&(e=!1)}}n=Object.getPrototypeOf(n)}QS(i)}function KS(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function QS(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=hr(r.hostAttrs,e=hr(e,r.hostAttrs))}}function Gd(t){return t===An?{}:t===ze?[]:t}function XS(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function JS(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function eD(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function sv(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=hr(t.mergedAttrs,t.attrs);let d=t.tView=tm(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),cr(t,!1);let l=nD(e,n,t,i);Ns()&&Ju(e,n,l,t),gr(l,n);let c=Ab(l,n,l,t);n[i+Ne]=c,im(n,c),RS(c,t,n)}function tD(t,n,e,i,r,o,a,s,l,c,d){let u=e+Ne,p;return n.firstCreatePass?(p=xr(n,u,4,a||null,s||null),Sd()&&Hb(n,t,p,xt(n.consts,c),yb),wg(n,p)):p=n.data[u],sv(p,t,n,e,i,r,o,l),_o(p)&&rm(n,t,p),c!=null&&ll(t,p,d),p}function Ro(t,n,e,i,r,o,a,s,l,c,d){let u=e+Ne,p;if(n.firstCreatePass){if(p=xr(n,u,4,a||null,s||null),c!=null){let f=xt(n.consts,c);p.localNames=[];for(let h=0;h<f.length;h+=2)p.localNames.push(f[h],-1)}}else p=n.data[u];return sv(p,t,n,e,i,r,o,l),c!=null&&ll(t,p,d),p}function st(t,n,e,i,r,o,a,s){let l=F(),c=Ie(),d=xt(c.consts,o);return tD(l,c,t,n,e,i,r,d,void 0,a,s),st}var nD=iD;function iD(t,n,e,i){return Is(!0),n[ge].createComment("")}var hm=new v("");var gm=new v("");function lv(){Cc(t=>{let n="";throw new S(-600,n)})}var rD=10;var lt=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=m(_i);afterRenderManager=m(rl);zonelessEnabled=m(Do);rootEffectScheduler=m(Ms);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new w;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=m(dr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Oe(e=>!e))}constructor(){m(Kt,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=m(Re);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=O.NULL){return this._injector.get(D).run(()=>{if(ee(Y.BootstrapComponentStart),!this._injector.get(fm).done){let _="";throw new S(405,_)}let s=Mn(e),l=this._injector.get(Pn),c=new vr(s,l);this.componentTypes.push(e);let{hostElement:d,directives:u,bindings:p}=oD(i),f=d||c.selector,h=c.create(r,[],f,l.injector,u,p),g=h.location.nativeElement,y=h.injector.get(hm,null);return y?.registerApplication(g),h.onDestroy(()=>{this.detachView(h.hostView),No(this.components,h),y?.unregisterApplication(g)}),this._loadComponent(h),ee(Y.BootstrapComponentEnd,h),h})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ee(Y.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(il.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ee(Y.ChangeDetectionEnd),new S(101,!1);let e=C(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,C(e),this.afterTick.next(),ee(Y.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ke,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<rD;){ee(Y.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ee(Y.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!xo(r))continue;let o=i&&!this.zonelessEnabled?0:1;Nb(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>xo(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;No(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(gm,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>No(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new S(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function oD(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function No(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function be(t,n,e,i){let r=F(),o=yi();if(Vn(r,o,n)){let a=Ie(),s=Co();MC(s,r,t,n,e,i)}return be}var Du=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Wd(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function aD(t,n,e,i){let r,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){C(i);let c=n.length-1;for(C(null);a<=s&&a<=c;){let d=t.at(a),u=n[a],p=Wd(a,d,a,u,e);if(p!==0){p<0&&t.updateValue(a,u),a++;continue}let f=t.at(s),h=n[c],g=Wd(s,f,c,h,e);if(g!==0){g<0&&t.updateValue(s,h),s--,c--;continue}let y=e(a,d),_=e(s,f),B=e(a,u);if(Object.is(B,_)){let jt=e(c,h);Object.is(jt,y)?(t.swap(a,s),t.updateValue(s,h),c--,s--):t.move(s,a),t.updateValue(a,u),a++;continue}if(r??=new Zs,o??=ug(t,a,s,e),Eu(t,r,a,B))t.updateValue(a,u),a++,s++;else if(o.has(B))r.set(y,t.detach(a)),s--;else{let jt=t.create(a,n[a]);t.attach(a,jt),a++,s++}}for(;a<=c;)dg(t,r,e,a,n[a]),a++}else if(n!=null){C(i);let c=n[Symbol.iterator]();C(null);let d=c.next();for(;!d.done&&a<=s;){let u=t.at(a),p=d.value,f=Wd(a,u,a,p,e);if(f!==0)f<0&&t.updateValue(a,p),a++,d=c.next();else{r??=new Zs,o??=ug(t,a,s,e);let h=e(a,p);if(Eu(t,r,a,h))t.updateValue(a,p),a++,s++,d=c.next();else if(!o.has(h))t.attach(a,t.create(a,p)),a++,s++,d=c.next();else{let g=e(a,u);r.set(g,t.detach(a)),s--}}}for(;!d.done;)dg(t,r,e,t.length,d.value),d=c.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(c=>{t.destroy(c)})}function Eu(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function dg(t,n,e,i,r){if(Eu(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function ug(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Zs=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function fe(t,n,e,i,r,o,a,s){Ni("NgControlFlow");let l=F(),c=Ie(),d=xt(c.consts,o);return Ro(l,c,t,n,e,i,r,d,256,a,s),bm}function bm(t,n,e,i,r,o,a,s){return Ni("NgControlFlow"),sD(t,n,e,i,r,o,a,s),bm}function sD(t,n,e,i,r,o,a,s){let l=F(),c=Ie(),d=xt(c.consts,o);Ro(l,c,t,n,e,i,r,d,512,a,s)}function pe(t,n){Ni("NgControlFlow");let e=F(),i=yi(),r=e[i]!==Pt?e[i]:-1,o=r!==-1?Ks(e,Ne+r):void 0,a=0;if(Vn(e,i,t)){let s=C(null);try{if(o!==void 0&&Ob(o,a),t!==-1){let l=Ne+t,c=Ks(e,l),d=Mu(e[E],l),u=Lb(c,d,e),p=Po(e,d,n,{dehydratedView:u});Vo(c,p,a,br(d,u))}}finally{C(s)}}else if(o!==void 0){let s=Rb(o,a);s!==void 0&&(s[we]=n)}}var Nu=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-Ce}};function vm(t){return t}var Iu=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function dl(t,n,e,i,r,o,a,s,l,c,d,u,p){Ni("NgControlFlow");let f=F(),h=Ie(),g=l!==void 0,y=F(),_=s?a.bind(y[Ue][we]):a,B=new Iu(g,_);y[Ne+t]=B,Ro(f,h,t+1,n,e,i,r,xt(h.consts,o),256),g&&Ro(f,h,t+2,l,c,d,u,xt(h.consts,p),512)}var ku=class extends Du{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-Ce}at(n){return this.getLView(n)[we].$implicit}attach(n,e){let i=e[di];this.needsIndexUpdate||=n!==this.length,Vo(this.lContainer,e,n,br(this.templateTNode,i)),lD(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,cD(this.lContainer,n),dD(this.lContainer,n)}create(n,e){let i=$s(this.lContainer,this.templateTNode.tView.ssrId);return Po(this.hostLView,this.templateTNode,new Nu(this.lContainer,e,n),{dehydratedView:i})}destroy(n){ol(n[E],n)}updateValue(n,e){this.getLView(n)[we].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[we].$index=n}getLView(n){return uD(this.lContainer,n)}};function ul(t){let n=C(null),e=qt();try{let i=F(),r=i[E],o=i[e],a=e+1,s=Ks(i,a);if(o.liveCollection===void 0){let c=Mu(r,a);o.liveCollection=new ku(s,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(aD(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=yi(),d=l.length===0;if(Vn(i,c,d)){let u=e+2,p=Ks(i,u);if(d){let f=Mu(r,u),h=Lb(p,f,i),g=Po(i,f,void 0,{dehydratedView:h});Vo(p,g,0,br(f,h))}else r.firstUpdatePass&&eS(p),Ob(p,0)}}}finally{C(n)}}function Ks(t,n){return t[n]}function lD(t,n){if(t.length<=Ce)return;let e=Ce+n,i=t[e];i&&oC(i)}function cD(t,n){if(t.length<=Ce)return;let e=Ce+n,i=t[e];i&&rC(i)}function dD(t,n){return To(t,n)}function uD(t,n){return Rb(t,n)}function Mu(t,n){return ys(t,n)}function Xe(t,n,e){let i=F(),r=yi();if(Vn(i,r,n)){let o=Ie(),a=Co();DC(a,i,t,n,i[ge],e)}return Xe}function Tu(t,n,e,i,r){om(n,t,e,r?"class":"style",i)}function $(t,n,e,i){let r=F(),o=r[E],a=t+Ne,s=o.firstCreatePass?Ub(a,r,2,n,yb,Sd(),e,i):o.data[a];if(pn(s)){let l=r[Tt].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(Bb(c),()=>(mg(t,n,r,s,i),$))}}return mg(t,n,r,s,i),$}function mg(t,n,e,i,r){if(_b(i,e,t,n,cv),_o(i)){let o=e[E];rm(o,e,i),Gg(o,i,e)}r!=null&&ll(e,i)}function q(){let t=Ie(),n=Ge(),e=xb(n);return t.firstCreatePass&&$b(t,e),Ed(e)&&Nd(),Cd(),e.classesWithoutHost!=null&&Yw(e)&&Tu(t,e,F(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Zw(e)&&Tu(t,e,F(),e.stylesWithoutHost,!1),q}function xn(t,n,e,i){return $(t,n,e,i),q(),xn}function Xt(t,n,e,i){let r=F(),o=r[E],a=t+Ne,s=o.firstCreatePass?bS(a,o,2,n,e,i):o.data[a];return _b(s,r,t,n,cv),i!=null&&ll(r,s),Xt}function Jt(){let t=Ge(),n=xb(t);return Ed(n)&&Nd(),Cd(),Jt}function We(t,n,e,i){return Xt(t,n,e,i),Jt(),We}var cv=(t,n,e,i,r)=>(Is(!0),Jg(n[ge],i,Fd()));function wr(){return F()}function ct(t,n,e){let i=F(),r=yi();if(Vn(i,r,n)){let o=Ie(),a=Co();bb(a,i,t,n,i[ge],e)}return ct}var Bo="en-US";var mD=Bo;function dv(t){typeof t=="string"&&(mD=t.toLowerCase().replace(/_/g,"-"))}function xe(t,n,e){let i=F(),r=Ie(),o=Ge();return fD(r,i,i[ge],o,t,n,e),xe}function fD(t,n,e,i,r,o,a){let s=!0,l=null;if((i.type&3||a)&&(l??=Ud(i,n,o),oS(i,t,n,a,e,r,o,l)&&(s=!1)),s){let c=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let u=0;u<d.length;u+=2){let p=d[u],f=d[u+1];l??=Ud(i,n,o),eg(i,n,p,f,r,l)}if(c&&c.length)for(let u of c)l??=Ud(i,n,o),eg(i,n,u,r,r,l)}}function Se(t=1){return yh(t)}function pD(t,n){let e=null,i=W0(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?ob(t,o,!0):Z0(i,o))return r}return e}function Vt(t){let n=F()[Ue][Ve];if(!n.projection){let e=t?t.length:1,i=n.projection=Gp(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?pD(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function le(t,n=0,e,i,r,o){let a=F(),s=Ie(),l=i?t+1:null;l!==null&&Ro(a,s,l,i,r,o,null,e);let c=xr(s,Ne+t,16,null,e||null);c.projection===null&&(c.projection=n),Md();let u=!a[di]||Dd();a[Ue][Ve].projection[c.projection]===null&&l!==null?hD(a,s,l):u&&!Js(c)&&hC(s,a,c)}function hD(t,n,e){let i=Ne+e,r=n.data[i],o=t[i],a=$s(o,r.tView.ssrId),s=Po(t,r,void 0,{dehydratedView:a});Vo(o,s,0,br(r,a))}function Mi(t,n,e,i){return Qb(t,n,e,i),Mi}function en(t,n,e){return Kb(t,n,e),en}function ce(t){let n=F(),e=Ie(),i=Ss();wo(i+1);let r=cm(e,i);if(t.dirty&&nh(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Jb(n,i);t.reset(o,Rg),t.notifyOnChanges()}return!0}return!1}function de(){return lm(F(),Ss())}function ml(t,n,e,i,r){return tv(n,Qb(t,e,i,r)),ml}function Cr(t,n,e,i){return tv(t,Kb(n,e,i)),Cr}function Sr(t=1){wo(Ss()+t)}function jo(t){let n=dh();return th(n,Ne+t)}function Os(t,n){return t<<17|n<<2}function Ei(t){return t>>17&32767}function gD(t){return(t&2)==2}function bD(t,n){return t&131071|n<<17}function Au(t){return t|2}function yr(t){return(t&131068)>>2}function qd(t,n){return t&-131069|n<<2}function vD(t){return(t&1)===1}function Ru(t){return t|1}function yD(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=Ei(a),l=yr(a);t[i]=e;let c=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||nr(u,d)>0)&&(c=!0)}else d=e;if(r)if(l!==0){let p=Ei(t[s+1]);t[i+1]=Os(p,s),p!==0&&(t[p+1]=qd(t[p+1],i)),t[s+1]=bD(t[s+1],i)}else t[i+1]=Os(s,0),s!==0&&(t[s+1]=qd(t[s+1],i)),s=i;else t[i+1]=Os(l,0),s===0?s=i:t[l+1]=qd(t[l+1],i),l=i;c&&(t[i+1]=Au(t[i+1])),fg(t,d,i,!0),fg(t,d,i,!1),_D(n,d,t,i,o),a=Os(s,l),o?n.classBindings=a:n.styleBindings=a}function _D(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&nr(o,n)>=0&&(e[i+1]=Ru(e[i+1]))}function fg(t,n,e,i){let r=t[e+1],o=n===null,a=i?Ei(r):yr(r),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];xD(l,n)&&(s=!0,t[a+1]=i?Ru(c):Au(c)),a=i?Ei(c):yr(c)}s&&(t[e+1]=i?Au(r):Ru(r))}function xD(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?nr(t,n)>=0:!1}var Ot={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function wD(t){return t.substring(Ot.key,Ot.keyEnd)}function CD(t){return SD(t),uv(t,mv(t,0,Ot.textEnd))}function uv(t,n){let e=Ot.textEnd;return e===n?-1:(n=Ot.keyEnd=DD(t,Ot.key=n,e),mv(t,n,e))}function SD(t){Ot.key=0,Ot.keyEnd=0,Ot.value=0,Ot.valueEnd=0,Ot.textEnd=t.length}function mv(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function DD(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Dr(t,n,e){return fv(t,n,e,!1),Dr}function te(t,n){return fv(t,n,null,!0),te}function ym(t){ND(RD,ED,t,!0)}function ED(t,n){for(let e=CD(n);e>=0;e=uv(n,e))ps(t,wD(n),!0)}function fv(t,n,e,i){let r=F(),o=Ie(),a=Ad(2);if(o.firstUpdatePass&&hv(o,t,a,i),n!==Pt&&Vn(r,a,n)){let s=o.data[qt()];gv(o,s,r,r[ge],t,r[a+1]=FD(n,e),i,a)}}function ND(t,n,e,i){let r=Ie(),o=Ad(2);r.firstUpdatePass&&hv(r,null,o,i);let a=F();if(e!==Pt&&Vn(a,o,e)){let s=r.data[qt()];if(bv(s,i)&&!pv(r,o)){let l=i?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=ls(l,e||"")),Tu(r,s,a,e,i)}else OD(r,s,a,a[ge],a[o+1],a[o+1]=AD(t,n,e),i,o)}}function pv(t,n){return n>=t.expandoStartIndex}function hv(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[qt()],a=pv(t,e);bv(o,i)&&n===null&&!a&&(n=!1),n=ID(r,o,n,i),yD(r,o,n,e,a,i)}}function ID(t,n,e,i){let r=hh(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Yd(null,t,n,e,i),e=Oo(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=Yd(r,t,n,e,i),o===null){let l=kD(t,n,i);l!==void 0&&Array.isArray(l)&&(l=Yd(null,t,n,l[1],i),l=Oo(l,n.attrs,i),MD(t,n,i,l))}else o=TD(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function kD(t,n,e){let i=e?n.classBindings:n.styleBindings;if(yr(i)!==0)return t[Ei(i)]}function MD(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Ei(r)]=i}function TD(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=Oo(i,a,e)}return Oo(i,n.attrs,e)}function Yd(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=Oo(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Oo(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),ps(t,a,e?!0:n[++o]))}return t===void 0?null:t}function AD(t,n,e){if(e==null||e==="")return ze;let i=[],r=at(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function RD(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&ps(t,i,e)}function OD(t,n,e,i,r,o,a,s){r===Pt&&(r=ze);let l=0,c=0,d=0<r.length?r[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let p=l<r.length?r[l+1]:void 0,f=c<o.length?o[c+1]:void 0,h=null,g;d===u?(l+=2,c+=2,p!==f&&(h=u,g=f)):u===null||d!==null&&d<u?(l+=2,h=d):(c+=2,h=u,g=f),h!==null&&gv(t,n,e,i,h,g,a,s),d=l<r.length?r[l]:null,u=c<o.length?o[c]:null}}function gv(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],d=vD(c)?pg(l,n,e,r,yr(c),a):void 0;if(!Qs(d)){Qs(o)||gD(c)&&(o=pg(l,null,e,r,s,a));let u=vd(qt(),e);bC(i,a,u,r,o)}}function pg(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let l=t[r],c=Array.isArray(l),d=c?l[1]:l,u=d===null,p=e[r+1];p===Pt&&(p=u?ze:void 0);let f=u?hs(p,i):d===i?p:void 0;if(c&&!Qs(f)&&(f=hs(l,i)),Qs(f)&&(s=f,a))return s;let h=t[r+1];r=a?Ei(h):yr(h)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=hs(l,i))}return s}function Qs(t){return t!==void 0}function FD(t,n){return t==null||t===""||(typeof n=="string"?t=at(t)+n:typeof t=="object"&&(t=ss(at(t)))),t}function bv(t,n){return(t.flags&(n?8:16))!==0}function dt(t,n=""){let e=F(),i=Ie(),r=t+Ne,o=i.firstCreatePass?xr(i,r,1,n,null):i.data[r],a=LD(i,e,o,n);e[r]=a,Ns()&&Ju(i,e,a,o),cr(o,!1)}var LD=(t,n,e,i)=>(Is(!0),O0(n[ge],i));function PD(t,n,e,i=""){return Vn(t,yi(),e)?n+us(e)+i:Pt}function ut(t){return _m("",t),ut}function _m(t,n,e){let i=F(),r=PD(i,t,n,e);return r!==Pt&&VD(i,qt(),r),_m}function VD(t,n,e){let i=vd(n,t);F0(t[ge],i,e)}function hg(t,n,e){let i=Ie();i.firstCreatePass&&vv(n,i.data,i.blueprint,At(t),e)}function vv(t,n,e,i,r){if(t=Ae(t),Array.isArray(t))for(let o=0;o<t.length;o++)vv(t[o],n,e,i,r);else{let o=Ie(),a=F(),s=Ge(),l=si(t)?t:Ae(t.provide),c=fd(t),d=s.providerIndexes&1048575,u=s.directiveStart,p=s.providerIndexes>>20;if(si(t)||!t.multi){let f=new Di(c,r,Le,null),h=Kd(l,n,r?d:d+p,u);h===-1?(Jd(zs(s,a),o,l),Zd(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(f),a.push(f)):(e[h]=f,a[h]=f)}else{let f=Kd(l,n,d+p,u),h=Kd(l,n,d,d+p),g=f>=0&&e[f],y=h>=0&&e[h];if(r&&!y||!r&&!g){Jd(zs(s,a),o,l);let _=HD(r?jD:BD,e.length,r,i,c,t);!r&&y&&(e[h].providerFactory=_),Zd(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(_),a.push(_)}else{let _=yv(e[r?h:f],c,!r&&i);Zd(o,t,f>-1?f:h,_)}!r&&i&&y&&e[h].componentProviders++}}}function Zd(t,n,e,i){let r=si(n),o=Kp(n);if(r||o){let l=(o?Ae(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=c.indexOf(e);d===-1?c.push(e,[i,l]):c[d+1].push(i,l)}else c.push(e,l)}}}function yv(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Kd(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function BD(t,n,e,i,r){return Ou(this.multi,[])}function jD(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=Io(i,i[E],this.providerFactory.index,r);a=l.slice(0,s),Ou(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],Ou(o,a);return a}function Ou(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function HD(t,n,e,i,r,o){let a=new Di(t,e,Le,null);return a.multi=[],a.index=n,a.componentProviders=0,yv(a,r,i&&!e),a}function mt(t,n){return e=>{e.providersResolver=(i,r)=>hg(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>hg(i,r?r(n):n,!0))}}function fl(t,n){return cl(t,n)}var _v=(()=>{class t{applicationErrorHandler=m(_i);appRef=m(lt);taskService=m(dr);ngZone=m(D);zonelessEnabled=m(Do);tracing=m(Kt,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new he;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(uo):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(m(Bd,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Sh:Ld;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(uo+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function xv(){return[{provide:un,useExisting:_v},{provide:D,useClass:mo},{provide:Do,useValue:!0}]}function zD(){return typeof $localize<"u"&&$localize.locale||Bo}var pl=new v("",{factory:()=>m(pl,{optional:!0,skipSelf:!0})||zD()});function ft(t,n){return Qr(t,n?.equal)}function Me(t){return rp(t)}var wv=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")};function Nv(t,n){let e=Object.create(vg);e.value=t,e.transformFn=n?.transform;function i(){if(Dn(e),e.value===Fu){let r=null;throw new S(-950,r)}return e.value}return i[Te]=e,i}function Cv(t,n){return Nv(t,n)}function lE(t){return Nv(Fu,t)}var Dm=(Cv.required=lE,Cv);function Sv(t,n){return dm(n)}function cE(t,n){return um(n)}var Ti=(Sv.required=cE,Sv);function Dv(t,n){return dm(n)}function dE(t,n){return um(n)}var Iv=(Dv.required=dE,Dv);var uE=1e4;var Qj=uE-1e3;var tn=(()=>{class t{static __NG_ELEMENT_ID__=mE}return t})();function mE(t){return fE(Ge(),F(),(t&16)===16)}function fE(t,n,e){if(pn(t)&&!e){let i=_t(t.index,n);return new Ln(i,i)}else if(t.type&175){let i=n[Ue];return new Ln(i,n)}return null}var wm=new v(""),pE=new v("");function Ho(t){return!t.moduleRef}function hE(t){let n=Ho(t)?t.r3Injector:t.moduleRef.injector,e=n.get(D);return e.run(()=>{Ho(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(_i),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ho(t)){let o=()=>n.destroy(),a=t.platformInjector.get(wm);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(wm);a.add(o),t.moduleRef.onDestroy(()=>{No(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return bE(i,e,()=>{let o=n.get(dr),a=o.add(),s=n.get(fm);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(pl,Bo);if(dv(l||Bo),!n.get(pE,!0))return Ho(t)?n.get(lt):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ho(t)){let d=n.get(lt);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return gE?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var gE;function bE(t,n,e){try{let i=e();return ki(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var hl=null;function vE(t=[],n){return O.create({name:n,providers:[{provide:bo,useValue:"platform"},{provide:wm,useValue:new Set([()=>hl=null])},...t]})}function yE(t=[]){if(hl)return hl;let n=vE(t);return hl=n,lv(),_E(n),n}function _E(t){let n=t.get(ks,null);gs(t,()=>{n?.forEach(e=>e())})}function kv(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;ee(Y.BootstrapApplicationStart);try{let o=r?.injector??yE(i),a=[xv(),Nh,...e||[]],s=new Ao({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return hE({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{ee(Y.BootstrapApplicationEnd)}}function He(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Mv(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var xm=Symbol("NOT_SET"),Tv=new Set,xE=z(x({},$i),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:xm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==xm&&!zi(this))return this.signal;try{for(let r of this.cleanup??Tv)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=En(this),i;try{i=this.userFn.apply(null,n)}finally{Kn(this,e)}return(this.value===xm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Cm=class extends ko{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null,s){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(rt),a),this.scheduler=r;for(let l of Ku){let c=e[l];if(c===void 0)continue;let d=Object.create(xE);d.sequence=this,d.phase=l,d.userFn=c,d.dirty=!0,d.signal=()=>(Dn(d),d.value),d.signal[Te]=d,d.registerCleanupFn=u=>(d.cleanup??=new Set).add(u),this.nodes[l]=d,this.hooks[l]=u=>d.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??Tv)e()}finally{Nn(n)}}};function Em(t,n){let e=n?.injector??m(O),i=e.get(un),r=e.get(rl),o=e.get(Kt,null,{optional:!0});r.impl??=e.get(Qu);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(ur,null,{optional:!0}),l=new Cm(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null),void 0);return r.impl.register(l),l}function Er(t,n){let e=Mn(t),i=n.elementInjector||rr(),o=new vr(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings);return n.onError&&(o.hostView._lView[sr]=n.onError),o}var Av=null;function Ct(){return Av}function Nm(t){Av??=t}var zo=class{},gl=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=X({token:t,factory:()=>m(Rv),providedIn:"platform"})}return t})();var Rv=(()=>{class t extends gl{_location;_history;_doc=m(V);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Ct().getBaseHref(this._doc)}onPopState(e){let i=Ct().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Ct().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=X({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function Lv(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function Ov(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Bn(t){return t&&t[0]!=="?"?`?${t}`:t}var bl=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=X({token:t,factory:()=>m(CE),providedIn:"root"})}return t})(),wE=new v(""),CE=(()=>{class t extends bl{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??m(V).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return Lv(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Bn(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+Bn(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+Bn(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(j(gl),j(wE,8))};static \u0275prov=X({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var vl=(()=>{class t{_subject=new w;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=EE(Ov(Fv(i))),this._locationStrategy.onPopState(r=>{let o={url:this.path(!0),pop:!0,state:r.state,type:r.type};r.hasUAVisualTransition&&(o.hasUAVisualTransition=!0),this._subject.next(o)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Bn(i))}normalize(e){return t.stripTrailingSlash(DE(this._basePath,Fv(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Bn(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Bn(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Bn;static joinWithSlash=Lv;static stripTrailingSlash=Ov;static \u0275fac=function(i){return new(i||t)(j(bl))};static \u0275prov=X({token:t,factory:()=>SE(),providedIn:"root"})}return t})();function SE(){return new vl(j(bl))}function DE(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function Fv(t){return t.replace(/\/index\.html$/,"")}function EE(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Im=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=m(O);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(Le(Qt))};static \u0275dir=I({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Qe]})}return t})();function km(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()!==n)continue;let a=o;try{a=decodeURIComponent(o)}catch{}return a.length>1&&a[0]==='"'&&a[a.length-1]==='"'&&(a=a.slice(1,-1)),a}return null}var Mm="browser";function Pv(t){return t===Mm}var Uo=class{_doc;constructor(n){this._doc=n}manager},yl=(()=>{class t extends Uo{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(j(V))};static \u0275prov=X({token:t,factory:t.\u0275fac})}return t})(),wl=new v(""),Om=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof yl));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof yl);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new S(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(j(wl),j(D))};static \u0275prov=X({token:t,factory:t.\u0275fac})}return t})(),Tm="ng-app-id";function Vv(t){for(let n of t)n.remove()}function Bv(t,n){let e=n.createElement("style");return e.textContent=t,e}function ME(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Tm}="${n}"],link[${Tm}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Tm),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Rm(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Fm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,ME(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,Bv);i?.forEach(r=>this.addUsage(r,this.external,Rm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(Vv(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])Vv(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,Bv(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Rm(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(j(V),j(xi),j(Ci,8),j(wi))};static \u0275prov=X({token:t,factory:t.\u0275fac})}return t})(),Am={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Lm=/%COMP%/g;var Hv="%COMP%",TE=`_nghost-${Hv}`,AE=`_ngcontent-${Hv}`,RE=!0,OE=new v("",{factory:()=>RE}),FE=new v("");function LE(t){return AE.replace(Lm,t)}function PE(t){return TE.replace(Lm,t)}function zv(t,n){return n.map(e=>e.replace(Lm,t))}var Pm=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,a,s,l=null,c=null,d=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.cssVarNamespace=d??"",this.defaultRenderer=new $o(e,a,s,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof xl?r.applyToHost(e):r instanceof Go&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case Ft.Emulated:o=new xl(l,c,i,this.appId,d,a,s,u,this.cssVarNamespace);break;case Ft.ShadowDom:return new _l(l,e,i,a,s,this.nonce,u,this.cssVarNamespace,c);case Ft.ExperimentalIsolatedShadowDom:return new _l(l,e,i,a,s,this.nonce,u,this.cssVarNamespace);default:o=new Go(l,c,i,d,a,s,u,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(j(Om),j(Ii),j(xi),j(OE),j(V),j(D),j(Ci),j(Kt,8),j(FE,8))};static \u0275prov=X({token:t,factory:t.\u0275fac})}return t})(),$o=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Am[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(jv(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){if(n){let r=jv(n)?n.content:n;if(i!=null&&i.parentNode!==r)throw new S(-5106,!1);r.insertBefore(e,i)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new S(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Am[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Am[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(Zt.DashCase|Zt.Important)?n.style.setProperty(e,i,r&Zt.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&Zt.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Ct().getGlobalEventTarget(this.doc,n),!n))throw new S(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function jv(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var _l=class extends $o{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,l,c){super(n,r,o,s,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=i.styles;d=zv(i.id,d).map(p=>p.replace(/%NS%/g,l));for(let p of d){let f=document.createElement("style");a&&f.setAttribute("nonce",a),f.textContent=p,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let p of u){let f=Rm(p,r);a&&f.setAttribute("nonce",a),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Go=class extends $o{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,l,c){super(n,o,a,s,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let d=i.styles,u=c?zv(c,d):d;this.styles=u.map(p=>p.replace(/%NS%/g,l)),this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Zu.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},xl=class extends Go{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,l,c){let d=r+"-"+i.id;super(n,e,i,o,a,s,l,c,d),this.contentAttr=LE(d),this.hostAttr=PE(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Cl=class t extends zo{supportsDOMEvents=!0;static makeCurrent(){Nm(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=VE();return e==null?null:BE(e)}resetBaseElement(){Wo=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return km(document.cookie,n)}},Wo=null;function VE(){return Wo=Wo||document.head.querySelector("base"),Wo?Wo.getAttribute("href"):null}function BE(t){return new URL(t,document.baseURI).pathname}var Uv=["alt","control","meta","shift"],jE={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},HE={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},$v=(()=>{class t extends Uo{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Ct().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),Uv.forEach(c=>{let d=i.indexOf(c);d>-1&&(i.splice(d,1),a+=c+".")}),a+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=a,l}static matchEventFullKeyCode(e,i){let r=jE[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Uv.forEach(a=>{if(a!==r){let s=HE[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(j(V))};static \u0275prov=X({token:t,factory:t.\u0275fac})}return t})();async function Vm(t,n,e){let i=x({rootComponent:t},zE(n,e));return kv(i)}function zE(t,n){return{platformRef:n?.platformRef,appProviders:[...qE,...t?.providers??[]],platformProviders:WE}}function UE(){Cl.makeCurrent()}function $E(){return new Gt}function GE(){return Vu(document),document}var WE=[{provide:wi,useValue:Mm},{provide:ks,useValue:UE,multi:!0},{provide:V,useFactory:GE}];var qE=[{provide:bo,useValue:"root"},{provide:Gt,useFactory:$E},{provide:wl,useClass:yl,multi:!0},{provide:wl,useClass:$v,multi:!0},Pm,{provide:Ii,useClass:Fm},{provide:Fm,useExisting:Ii},Om,{provide:ke,useExisting:Pm},[]];var Sl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=X({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=j(YE),r},providedIn:"root"})}return t})(),YE=(()=>{class t extends Sl{_doc=m(V);sanitize(e,i){if(i==null)return null;switch(e){case ye.NONE:return i;case ye.HTML:return yn(i,"HTML")?at(i):nl(this._doc,String(i)).toString();case ye.STYLE:return yn(i,"Style")?at(i):i;case ye.SCRIPT:if(yn(i,"Script"))return at(i);throw new S(5200,!1);case ye.URL:return yn(i,"URL")?at(i):tl(String(i));case ye.RESOURCE_URL:if(yn(i,"ResourceURL"))return at(i);throw new S(-5201,!1);default:throw new S(5202,!1)}}bypassSecurityTrustHtml(e){return ju(e)}bypassSecurityTrustStyle(e){return Hu(e)}bypassSecurityTrustScript(e){return zu(e)}bypassSecurityTrustUrl(e){return Uu(e)}bypassSecurityTrustResourceUrl(e){return $u(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var ZE=`
registerProcessor('hatter-recorder', class extends AudioWorkletProcessor {
  process([input]) {
    if (input.length > 0) this.port.postMessage(input[0].slice());
    return true;
  }
});`;var KE={1:[[659,.4],[880,.4],[0,2]],2:[[425,1],[0,4]]},QE=.2,XE=t=>Math.max(-32768,Math.min(32767,Math.round(t*32768))),Dl=class{context;end=0;microphone;call=0;toneContext;start(n,e,i){this.stop();let r=++this.call,o=new AudioContext({sampleRate:n,latencyHint:"interactive"});this.context=o,this.end=0,o.resume(),this.record(r,o,e,i).catch(a=>console.warn("Hatter: no microphone:",a))}stop(){this.call++,this.microphone?.getTracks().forEach(n=>n.stop()),this.microphone=void 0,this.context?.close(),this.context=void 0}play(n,e,i){let r=this.context;if(!r||i.length<e)return 0;let o=Math.floor(i.length/e),a=r.createBuffer(e,o,n);for(let l=0;l<e;l++){let c=a.getChannelData(l);for(let d=0;d<o;d++)c[d]=i[d*e+l]/32768}let s=r.createBufferSource();return s.buffer=a,s.connect(r.destination),this.end=Math.max(this.end,r.currentTime),s.start(this.end),this.end+=a.duration,Math.round((this.end-r.currentTime)*1e3)}latency(){let n=this.context;return n?Math.round(((n.outputLatency||0)+n.baseLatency)*1e3):0}tone(n){this.toneContext?.close(),this.toneContext=void 0;let e=KE[n];if(!e)return;let i=new AudioContext;this.toneContext=i;let r=i.sampleRate,o=i.createBuffer(1,Math.round(e.reduce((c,[,d])=>c+d,0)*r),r),a=o.getChannelData(0),s=0;for(let[c,d]of e){let u=Math.round(d*r),p=Math.min(u/2,r/100);for(let f=0;c>0&&f<u&&s+f<a.length;f++)a[s+f]=QE*Math.sin(2*Math.PI*c*f/r)*Math.min(1,f/p,(u-f)/p);s+=u}let l=i.createBufferSource();l.buffer=o,l.loop=!0,l.connect(i.destination),l.start()}async record(n,e,i,r){let o=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:!0,noiseSuppression:!0,autoGainControl:!0}});if(n!==this.call)return o.getTracks().forEach(d=>d.stop());this.microphone=o;let a=URL.createObjectURL(new Blob([ZE],{type:"text/javascript"}));try{await e.audioWorklet.addModule(a)}finally{URL.revokeObjectURL(a)}if(n!==this.call)return;let s=new AudioWorkletNode(e,"hatter-recorder",{numberOfOutputs:0}),l=new Int16Array(i),c=0;s.port.onmessage=({data:d})=>{for(let u of d)l[c++]=XE(u),c===i&&(r(l.slice()),c=0)},e.createMediaStreamSource(o).connect(s)}};var Gv={width:640,height:480};function JE(t,n,e){let i=n>>1,r=e>>1,o=new Uint8Array(n*e+2*i*r);for(let a=0;a<n*e;a++)o[a]=(66*t[4*a]+129*t[4*a+1]+25*t[4*a+2]+128>>8)+16;for(let a=0;a<r;a++)for(let s=0;s<i;s++){let l=0,c=0,d=0;for(let u of[0,1,n,n+1]){let p=4*(2*a*n+2*s+u);l+=t[p],c+=t[p+1],d+=t[p+2]}l/=4,c/=4,d/=4,o[n*e+a*i+s]=(-38*l-74*c+112*d+128>>8)+128,o[n*e+i*r+a*i+s]=(112*l-94*c-18*d+128>>8)+128}return o}var El=class{watching=new Map;camera;filming=0;watch(n,e){e.showStream(n);let i=this.watching.get(n);i||this.watching.set(n,i=new Set),i.add(e)}show(n,e,i,r){let o=this.watching.get(n);if(!o)return;let a=new VideoFrame(r,{format:"I420",codedWidth:e,codedHeight:i,timestamp:0});try{for(let s of o)s.stream!==n?o.delete(s):s.draw(a)}finally{a.close()}}startCamera(n,e){this.stopCamera();let i=this.filming;this.film(i,n,e).catch(r=>console.warn("Hatter: no camera:",r))}stopCamera(){this.filming++,this.camera?.getTracks().forEach(n=>n.stop()),this.camera=void 0}async film(n,e,i){let r=await navigator.mediaDevices.getUserMedia({video:{facingMode:e?"user":"environment",width:{ideal:Gv.width},height:{ideal:Gv.height}}});if(n!==this.filming)return r.getTracks().forEach(c=>c.stop());this.camera=r;let o=document.createElement("video");o.muted=!0,o.playsInline=!0,o.srcObject=r,await o.play();let a=new OffscreenCanvas(1,1),s=a.getContext("2d",{willReadFrequently:!0}),l=()=>{if(n!==this.filming)return;let c=o.videoWidth&-2,d=o.videoHeight&-2;c>0&&d>0&&((a.width!==c||a.height!==d)&&(a.width=c,a.height=d),s.drawImage(o,0,0,c,d),i(c,d,JE(s.getImageData(0,0,c,d).data,c,d))),o.requestVideoFrameCallback(l)};o.requestVideoFrameCallback(l)}};var eN={avif:"image/avif",gif:"image/gif",jpeg:"image/jpeg",jpg:"image/jpeg",m4a:"audio/mp4",mp3:"audio/mpeg",mp4:"video/mp4",oga:"audio/ogg",ogg:"audio/ogg",opus:"audio/ogg",pdf:"application/pdf",png:"image/png",txt:"text/plain; charset=utf-8",wav:"audio/wav",webm:"video/webm",webp:"image/webp"},Nl="/cache",tN=0;function Wv(t,n,e=!1){let i=document.createElement("input");return i.type="file",i.accept=t==="*/*"?"":t,i.multiple=n,e&&(i.capture="environment"),new Promise(r=>{i.addEventListener("change",()=>r([...i.files??[]])),i.addEventListener("cancel",()=>r([])),i.click()})}async function qv(t,n,e,i=!1){let r=[];for(let o of await Wv(n,e,i)){let a=`${Nl}/picked/${tN++}/${o.name.replace(/[^A-Za-z0-9._-]/g,"_")||"file"}`;t.add(a,new Uint8Array(await o.arrayBuffer())),r.push({path:a,name:o.name,size:o.size})}return r}async function Yv(t,n){let[e]=await Wv("image/*",!1);if(!e)return;let i=await createImageBitmap(e).catch(()=>{});if(!i)return;let r=Math.min(i.width,i.height),o=Math.floor((i.width-r)/2),a=Math.floor((i.height-r)/2);try{for(let s of t){let l=new OffscreenCanvas(s,s),c=l.getContext("2d");c.imageSmoothingQuality="high",c.drawImage(i,o,a,r,r,0,0,s,s);let d=await l.convertToBlob({type:"image/png"});if(d.size<=n)return new Uint8Array(await d.arrayBuffer())}return}finally{i.close()}}function Zv(t,n,e,i){let r=t.file(n);r&&(Qv(new Blob([r.data],{type:i}),e),t.remove(n))}function Kv(t,n){let e=t.file(n);if(!e)return;let i=n.slice(n.lastIndexOf("/")+1),r=eN[i.slice(i.lastIndexOf(".")+1).toLowerCase()],o=new Blob([e.data],{type:r??"application/octet-stream"});if(!r)return Qv(o,i);let a=URL.createObjectURL(o);window.open(a,"_blank","noopener"),setTimeout(()=>URL.revokeObjectURL(a),6e4)}function Qv(t,n){let e=URL.createObjectURL(t),i=document.createElement("a");i.href=e,i.download=n,i.click(),setTimeout(()=>URL.revokeObjectURL(e),6e4)}function Xv(t){let n=new DataView(t.buffer,t.byteOffset,t.byteLength),e=(i,r)=>String.fromCharCode(...t.subarray(i,i+r));try{if(e(1,3)==="PNG")return[n.getUint32(16),n.getUint32(20)];if(e(0,4)==="GIF8")return[n.getUint16(6,!0),n.getUint16(8,!0)];if(e(0,4)==="RIFF"&&e(8,4)==="WEBP")return nN(t,n,e(12,4));if(n.getUint16(0)===65496)return iN(t,n)}catch{}}function nN(t,n,e){switch(e){case"VP8 ":return[n.getUint16(26,!0)&16383,n.getUint16(28,!0)&16383];case"VP8L":{let i=n.getUint32(21,!0);return[(i&16383)+1,(i>>>14&16383)+1]}case"VP8X":return[(n.getUint32(24,!0)&16777215)+1,(n.getUint32(26,!0)>>>8)+1]}}function iN(t,n){let e=!1;for(let i=2;i+4<=t.length;){let r=n.getUint16(i),o=n.getUint16(i+2);if(r===65505&&(e||=rN(t.subarray(i+4,i+2+o))),r>=65472&&r<=65487&&![65476,65480,65484].includes(r)){let a=[n.getUint16(i+7),n.getUint16(i+5)];return e?[a[1],a[0]]:a}i+=2+o}}function rN(t){if(String.fromCharCode(...t.subarray(0,4))!=="Exif")return!1;let n=new DataView(t.buffer,t.byteOffset+6,t.byteLength-6),e=n.getUint16(0)===18761,i=n.getUint32(4,e),r=n.getUint16(i,e);for(let o=0;o<r;o++){let a=i+2+o*12;if(n.getUint16(a,e)===274)return n.getUint16(a+8,e)>=5}return!1}var Bm;try{Bm=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Bm=!1}var oe=(()=>{class t{_platformId=m(wi);isBrowser=this._platformId?Pv(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Bm)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Il=new WeakMap,qe=(()=>{class t{_appRef;_injector=m(O);_environmentInjector=m(Re);load(e){let i=this._appRef=this._appRef||this._injector.get(lt),r=Il.get(i);r||(r={loaders:new Set,refs:[]},Il.set(i,r),i.onDestroy(()=>{Il.get(i)?.refs.forEach(o=>o.destroy()),Il.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Er(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function St(t,n=0){return Jv(t)?Number(t):arguments.length===2?n:0}function Jv(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function Ye(t){return t instanceof M?t.nativeElement:t}var ey=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2})}return t})(),oN={passive:!0},ty=(()=>{class t{_platform=m(oe);_ngZone=m(D);_renderer=m(ke).createRenderer(null,null);_styleLoader=m(qe);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return ni;this._styleLoader.load(ey);let i=Ye(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new w,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,oN)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=Ye(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var ny=(()=>{class t{_elementRef=m(M);_platform=m(oe);_ngZone=m(D);_renderer=m(_e);_resizeEvents=new w;_previousValue;_initialHeight;_destroyed=new w;_listenerCleanups;_minRows;_maxRows;_enabled=!0;_previousMinRows=-1;_textareaElement;get minRows(){return this._minRows}set minRows(e){this._minRows=St(e),this._setMinHeight()}get maxRows(){return this._maxRows}set maxRows(e){this._maxRows=St(e),this._setMaxHeight()}get enabled(){return this._enabled}set enabled(e){this._enabled!==e&&((this._enabled=e)?this.resizeToFitContent(!0):this.reset())}get placeholder(){return this._textareaElement.placeholder}set placeholder(e){this._cachedPlaceholderHeight=void 0,e?this._textareaElement.setAttribute("placeholder",e):this._textareaElement.removeAttribute("placeholder"),this._cacheTextareaPlaceholderHeight()}_cachedLineHeight;_cachedPlaceholderHeight;_document=m(V);_hasFocus=!1;_isViewInited=!1;constructor(){m(qe).load(ey),this._textareaElement=this._elementRef.nativeElement}_setMinHeight(){let e=this.minRows&&this._cachedLineHeight?`${this.minRows*this._cachedLineHeight}px`:null;e&&(this._textareaElement.style.minHeight=e)}_setMaxHeight(){let e=this.maxRows&&this._cachedLineHeight?`${this.maxRows*this._cachedLineHeight}px`:null;e&&(this._textareaElement.style.maxHeight=e)}ngAfterViewInit(){this._platform.isBrowser&&(this._initialHeight=this._textareaElement.style.height,this.resizeToFitContent(),this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[this._renderer.listen("window","resize",()=>this._resizeEvents.next()),this._renderer.listen(this._textareaElement,"focus",this._handleFocusEvent),this._renderer.listen(this._textareaElement,"blur",this._handleFocusEvent)],this._resizeEvents.pipe(Qi(16)).subscribe(()=>{this._cachedLineHeight=this._cachedPlaceholderHeight=void 0,this.resizeToFitContent(!0)})}),this._isViewInited=!0,this.resizeToFitContent(!0))}ngOnDestroy(){this._listenerCleanups?.forEach(e=>e()),this._resizeEvents.complete(),this._destroyed.next(),this._destroyed.complete()}_cacheTextareaLineHeight(){if(this._cachedLineHeight)return;let e=this._textareaElement.cloneNode(!1),i=e.style;e.rows=1,i.position="absolute",i.visibility="hidden",i.border="none",i.padding="0",i.height="",i.minHeight="",i.maxHeight="",i.top=i.bottom=i.left=i.right="auto",i.overflow="hidden",this._textareaElement.parentNode.appendChild(e),this._cachedLineHeight=e.clientHeight,e.remove(),this._setMinHeight(),this._setMaxHeight()}_measureScrollHeight(){let e=this._textareaElement,i=e.style.marginBottom||"",r=this._platform.FIREFOX,o=this._hasFocus,a=r?"cdk-textarea-autosize-measuring-firefox":"cdk-textarea-autosize-measuring";o&&(e.style.marginBottom=`${e.clientHeight}px`),e.classList.add(a);let s=e.scrollHeight-4;return e.classList.remove(a),o&&(e.style.marginBottom=i),s}_cacheTextareaPlaceholderHeight(){if(!this._isViewInited||this._cachedPlaceholderHeight!=null)return;if(!this.placeholder){this._cachedPlaceholderHeight=0;return}let e=this._textareaElement.value;this._textareaElement.value=this._textareaElement.placeholder,this._cachedPlaceholderHeight=this._measureScrollHeight(),this._textareaElement.value=e}_handleFocusEvent=e=>{this._hasFocus=e.type==="focus"};ngDoCheck(){this._platform.isBrowser&&this.resizeToFitContent()}resizeToFitContent(e=!1){if(!this._enabled||(this._cacheTextareaLineHeight(),this._cacheTextareaPlaceholderHeight(),!this._cachedLineHeight))return;let i=this._elementRef.nativeElement,r=i.value;if(!e&&this._minRows===this._previousMinRows&&r===this._previousValue)return;let o=this._measureScrollHeight(),a=Math.max(o,this._cachedPlaceholderHeight||0);i.style.height=`${a}px`,this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame<"u"?requestAnimationFrame(()=>this._scrollToCaretPosition(i)):setTimeout(()=>this._scrollToCaretPosition(i))}),this._previousValue=r,this._previousMinRows=this._minRows}reset(){this._initialHeight!==void 0&&(this._textareaElement.style.height=this._initialHeight)}_noopInputHandler(){}_scrollToCaretPosition(e){let{selectionStart:i,selectionEnd:r}=e;!this._destroyed.isStopped&&this._hasFocus&&e.setSelectionRange(i,r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["textarea","cdkTextareaAutosize",""]],hostAttrs:["rows","1",1,"cdk-textarea-autosize"],hostBindings:function(i,r){i&1&&xe("input",function(){return r._noopInputHandler()})},inputs:{minRows:[0,"cdkAutosizeMinRows","minRows"],maxRows:[0,"cdkAutosizeMaxRows","maxRows"],enabled:[2,"cdkTextareaAutosize","enabled",He],placeholder:"placeholder"},exportAs:["cdkTextareaAutosize"]})}return t})(),jm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=je({type:t});static \u0275inj=Fe({})}return t})();function qo(t){return t.buttons===0||t.detail===0}function Yo(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Hm;function iy(){if(Hm==null){let t=typeof document<"u"?document.head:null;Hm=!!(t&&(t.createShadowRoot||t.attachShadow))}return Hm}function zm(t){if(iy()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Nr(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Je(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var Zo;function ry(){if(Zo==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Zo=!0}))}finally{Zo=Zo||!1}return Zo}function Ir(t){return ry()?t:!!t.capture}var oy=new v("cdk-input-modality-detector-options"),ay={ignoreKeys:[18,17,224,91,16]},sy=650,Um={passive:!0,capture:!0},ly=(()=>{class t{_platform=m(oe);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new ti(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Je(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<sy||(this._modality.next(qo(e)?"keyboard":"mouse"),this._mostRecentTarget=Je(e))};_onTouchstart=e=>{if(Yo(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Je(e)};constructor(){let e=m(D),i=m(V),r=m(oy,{optional:!0});if(this._options=x(x({},ay),r),this.modalityDetected=this._modality.pipe(oo(1)),this.modalityChanged=this.modalityDetected.pipe(Xa()),this._platform.isBrowser){let o=m(ke).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Um),o.listen(i,"mousedown",this._onMousedown,Um),o.listen(i,"touchstart",this._onTouchstart,Um)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Ko=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Ko||{}),cy=new v("cdk-focus-monitor-default-options"),kl=Ir({passive:!0,capture:!0}),Qo=(()=>{class t{_ngZone=m(D);_platform=m(oe);_inputModalityDetector=m(ly);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=m(V);_stopInputModalityDetector=new w;constructor(){let e=m(cy,{optional:!0});this._detectionMode=e?.detectionMode||Ko.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Je(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=Ye(e);if(!this._platform.isBrowser||r.nodeType!==1)return no();let o=zm(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new w,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=Ye(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=Ye(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Ko.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Ko.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?sy:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Je(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,kl),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,kl)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe($t(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,kl),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,kl),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var dy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2})}return t})();function Xo(t){return Array.isArray(t)?t:[t]}var uy=new Set,Ai,Ml=(()=>{class t{_platform=m(oe);_nonce=m(Ci,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):lN}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&sN(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function sN(t,n){if(!uy.has(t))try{Ai||(Ai=document.createElement("style"),n&&Ai.setAttribute("nonce",n),Ai.setAttribute("type","text/css"),document.head.appendChild(Ai)),Ai.sheet&&(Ai.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),uy.add(t))}catch(e){console.error(e)}}function lN(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var $m=(()=>{class t{_mediaMatcher=m(Ml);_zone=m(D);_queries=new Map;_destroySubject=new w;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return my(Xo(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=my(Xo(e)).map(a=>this._registerQuery(a).observable),o=Lc(r);return o=Ki(o.pipe(nt(1)),o.pipe(oo(1),ro(0))),o.pipe(Oe(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new L(a=>{let s=l=>this._zone.run(()=>a.next(l));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(Ut(i),Oe(({matches:a})=>({query:e,matches:a})),$t(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function my(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function cN(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var dN=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),uN=(()=>{class t{_mutationObserverFactory=m(dN);_observedElements=new Map;_ngZone=m(D);ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=Ye(e);return new L(r=>{let a=this._observeElement(i).pipe(Oe(s=>s.filter(l=>!cN(l))),De(s=>!!s.length)).subscribe(s=>{this._ngZone.run(()=>{r.next(s)})});return()=>{a.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new w,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),fy=(()=>{class t{_contentObserver=m(uN);_elementRef=m(M);event=new J;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=St(e),this._subscribe()}_debounce;_currentSubscription=null;ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(ro(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",He],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})();var Wm=(()=>{class t{_platform=m(oe);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return fN(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=mN(xN(e));if(i&&(py(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=py(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!yN(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return _N(e)&&!this.isDisabled(e)&&(i?.ignoreInert||!e.closest?.("[inert]"))&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function mN(t){try{return t.frameElement}catch{return null}}function fN(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function pN(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function hN(t){return bN(t)&&t.type=="hidden"}function gN(t){return vN(t)&&t.hasAttribute("href")}function bN(t){return t.nodeName.toLowerCase()=="input"}function vN(t){return t.nodeName.toLowerCase()=="a"}function hy(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function py(t){if(!hy(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function yN(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function _N(t){return hN(t)?!1:pN(t)||gN(t)||t.hasAttribute("contenteditable")||hy(t)}function xN(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Gm=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>{!this.focusLastTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};endAnchorListener=()=>{!this.focusFirstTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){wt(n,{injector:this._injector})}},gy=(()=>{class t{_checker=m(Wm);_ngZone=m(D);_document=m(V);_injector=m(O);constructor(){m(qe).load(dy)}create(e,i=!1){return new Gm(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function kr(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var by=new Map,Ze=class t{_appId=m(xi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=by.get(n);return i===void 0?i=0:i++,by.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})};var Bt=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Bt||{}),Tl,Ri;function Al(){if(Ri==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Ri=!1,Ri;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Ri=!0;else{let t=Element.prototype.scrollTo;t?Ri=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Ri=!1}}return Ri}function Mr(){if(typeof document!="object"||!document)return Bt.NORMAL;if(Tl==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),Tl=Bt.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,Tl=t.scrollLeft===0?Bt.NEGATED:Bt.INVERTED),t.remove()}return Tl}function qm(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Tr,vy=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Ym(){if(Tr)return Tr;if(typeof document!="object"||!document)return Tr=new Set(vy),Tr;let t=document.createElement("input");return Tr=new Set(vy.filter(n=>(t.setAttribute("type",n),t.type===n))),Tr}var Oi={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var wN=new v("MATERIAL_ANIMATIONS"),yy=null;function CN(){return m(wN,{optional:!0})?.animationsDisabled||m(So,{optional:!0})==="NoopAnimations"?"di-disabled":(yy??=m(Ml).matchMedia("(prefers-reduced-motion)").matches,yy?"reduced-motion":"enabled")}function et(){return CN()!=="enabled"}function wn(t){return t==null?"":typeof t=="string"?t:`${t}px`}function Dt(t){return t!=null&&`${t}`!="false"}var Et=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Et||{}),Zm=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Et.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},_y=Ir({passive:!0,capture:!0}),Km=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,_y)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,_y)))}_delegateEventHandler=n=>{let e=Je(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Jo={enterDuration:225,exitDuration:150},SN=800,xy=Ir({passive:!0,capture:!0}),wy=["mousedown","touchstart"],Cy=["mouseup","mouseleave","touchend","touchcancel"],DN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2})}return t})(),Ar=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Km;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=Ye(i)),o&&o.get(qe).load(DN)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=x(x({},Jo),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||EN(n,e,r),s=n-r.left,l=e-r.top,c=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${s-a}px`,d.style.top=`${l-a}px`,d.style.height=`${a*2}px`,d.style.width=`${a*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),p=u.transitionProperty,f=u.transitionDuration,h=p==="none"||f==="0s"||f==="0s, 0s"||r.width===0&&r.height===0,g=new Zm(this,d,i,h);d.style.transform="scale3d(1, 1, 1)",g.state=Et.FADING_IN,i.persistent||(this._mostRecentTransientRipple=g);let y=null;return!h&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let _=()=>{y&&(y.fallbackTimer=null),clearTimeout(jt),this._finishRippleTransition(g)},B=()=>this._destroyRipple(g),jt=setTimeout(B,c+100);d.addEventListener("transitionend",_),d.addEventListener("transitioncancel",B),y={onTransitionEnd:_,onTransitionCancel:B,fallbackTimer:jt}}),this._activeRipples.set(g,y),(h||!c)&&this._finishRippleTransition(g),g}fadeOutRipple(n){if(n.state===Et.FADING_OUT||n.state===Et.HIDDEN)return;let e=n.element,i=x(x({},Jo),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Et.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=Ye(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,wy.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Cy.forEach(e=>{this._triggerElement.addEventListener(e,this,xy)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Et.FADING_IN?this._startFadeOutTransition(n):n.state===Et.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Et.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Et.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=qo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+SN;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Yo(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Et.VISIBLE||n.config.terminateOnPointerUp&&n.state===Et.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(wy.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(Cy.forEach(e=>n.removeEventListener(e,this,xy)),this._pointerUpEventsRegistered=!1))}};function EN(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Rl=new v("mat-ripple-global-options");var NN={capture:!0},IN=["focus","mousedown","mouseenter","touchstart"],Qm="mat-ripple-loader-uninitialized",Xm="mat-ripple-loader-class-name",Sy="mat-ripple-loader-centered",Ol="mat-ripple-loader-disabled",Dy=(()=>{class t{_document=m(V);_animationsDisabled=et();_globalRippleOptions=m(Rl,{optional:!0});_platform=m(oe);_ngZone=m(D);_injector=m(O);_eventCleanups;_hosts=new Map;constructor(){let e=m(ke).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>IN.map(i=>e.listen(this._document,i,this._onInteraction,NN)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Qm,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Xm))&&e.setAttribute(Xm,i.className||""),i.centered&&e.setAttribute(Sy,""),i.disabled&&e.setAttribute(Ol,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Ol,""):e.removeAttribute(Ol)}_onInteraction=e=>{let i=Je(e);if(i instanceof HTMLElement){let r=i.closest(`[${Qm}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Xm)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Jo.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??Jo.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Ol),rippleConfig:{centered:e.hasAttribute(Sy),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Ar(s,this._ngZone,i,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(Qm)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Fl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2})}return t})();var kN=new v("MAT_BUTTON_CONFIG");function Ey(t){return t==null?void 0:Mv(t)}var Ny=(()=>{class t{_elementRef=m(M);_ngZone=m(D);_animationsDisabled=et();_config=m(kN,{optional:!0});_focusMonitor=m(Qo);_cleanupClick;_renderer=m(_e);_rippleLoader=m(Dy);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=Dm(!1,{transform:He});constructor(){m(qe).load(Fl);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(be("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),ym(r.color?"mat-"+r.color:""),te("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",He],disabled:[2,"disabled","disabled",He],ariaDisabled:[2,"aria-disabled","ariaDisabled",He],disabledInteractive:[2,"disabledInteractive","disabledInteractive",He],tabIndex:[2,"tabIndex","tabIndex",Ey],_tabindex:[2,"tabindex","_tabindex",Ey],showProgress:[1,"showProgress"]}})}return t})();var MN=new v("cdk-dir-doc",{providedIn:"root",factory:()=>m(V)}),TN=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function Iy(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?TN.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var nn=(()=>{class t{get value(){return this.valueSignal()}valueSignal=Q("ltr");change=new J;constructor(){let e=m(MN,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(Iy(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var ky=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Ll=(()=>{class t extends Ny{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=AN(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?ky.get(this._appearance):null,o=ky.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""],["",8,"material-symbols-outlined",3,"iconPositionEnd",""],["",8,"material-symbols-rounded",3,"iconPositionEnd",""],["",8,"material-symbols-sharp",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""],["","iconPositionEnd","",8,"material-symbols-outlined"],["","iconPositionEnd","",8,"material-symbols-rounded"],["","iconPositionEnd","",8,"material-symbols-sharp"]],[["","progressIndicator",""]]],i=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd]), .material-symbols-outlined:not([iconPositionEnd]), .material-symbols-rounded:not([iconPositionEnd]), .material-symbols-sharp:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd], .material-symbols-outlined[iconPositionEnd], .material-symbols-rounded[iconPositionEnd], .material-symbols-sharp[iconPositionEnd]","[progressIndicator]"];function r(o,a){o&1&&(Xt(0,"div",2),le(1,3),Jt())}return T({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[P],ngContentSelectors:i,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(a,s){a&1&&(Vt(e),We(0,"span",0),le(1),Xt(2,"span",1),le(3,1),Jt(),le(4,2),fe(5,r,2,0,"div",2),We(6,"span",3)(7,"span",4)),a&2&&(te("mdc-button__ripple",!s._isFab)("mdc-fab__ripple",s._isFab),G(5),pe(s.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2})})()}return t})();function AN(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Ly=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(Le(_e),Le(M))};static \u0275dir=I({type:t})}return t})(),RN=(()=>{class t extends Ly{static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,features:[P]})}return t})(),ON=new v("");var FN={provide:ON,useExisting:gt(()=>Py),multi:!0};function LN(){let t=Ct()?Ct().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var PN=new v(""),Py=(()=>{class t extends Ly{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!LN())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(Le(_e),Le(M),Le(PN,8))};static \u0275dir=I({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&xe("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[mt([FN]),P]})}return t})();function tf(t){return t==null||nf(t)===0}function nf(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var rf=new v(""),Vy=new v(""),VN=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,oa=class{static min(n){return BN(n)}static max(n){return jN(n)}static required(n){return By(n)}static requiredTrue(n){return HN(n)}static email(n){return zN(n)}static minLength(n){return UN(n)}static maxLength(n){return $N(n)}static pattern(n){return GN(n)}static nullValidator(n){return Vl()}static compose(n){return Gy(n)}static composeAsync(n){return Wy(n)}};function BN(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function jN(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function By(t){return tf(t.value)?{required:!0}:null}function HN(t){return t.value===!0?null:{required:!0}}function zN(t){return tf(t.value)||VN.test(t.value)?null:{email:!0}}function UN(t){return n=>{let e=n.value?.length??nf(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function $N(t){return n=>{let e=n.value?.length??nf(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function GN(t){if(!t)return Vl;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(tf(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Vl(t){return null}function jy(t){return t!=null}function Hy(t){return ki(t)?ht(t):t}function zy(t){let n={};return t.forEach(e=>{n=e!=null?x(x({},n),e):n}),Object.keys(n).length===0?null:n}function Uy(t,n){return n.map(e=>e(t))}function WN(t){return!t.validate}function $y(t){return t.map(n=>WN(n)?n:e=>n.validate(e))}function Gy(t){if(!t)return null;let n=t.filter(jy);return n.length==0?null:function(e){return zy(Uy(e,n))}}function of(t){return t!=null?Gy($y(t)):null}function Wy(t){if(!t)return null;let n=t.filter(jy);return n.length==0?null:function(e){let i=Uy(e,n).map(Hy);return Vc(i).pipe(Oe(zy))}}function af(t){return t!=null?Wy($y(t)):null}function My(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function qy(t){return t._rawValidators}function Yy(t){return t._rawAsyncValidators}function Jm(t){return t?Array.isArray(t)?t:[t]:[]}function Bl(t,n){return Array.isArray(t)?t.includes(n):t===n}function Ty(t,n){let e=Jm(n);return Jm(t).forEach(r=>{Bl(e,r)||e.push(r)}),e}function Ay(t,n){return Jm(n).filter(e=>!Bl(t,e))}var jl=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=of(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=af(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Fr=class extends jl{name;get formDirective(){return null}get path(){return null}};var ea="VALID",Pl="INVALID",Rr="PENDING",ta="DISABLED",jn=class{},Hl=class extends jn{value;source;constructor(n,e){super(),this.value=n,this.source=e}},ia=class extends jn{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},ra=class extends jn{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Or=class extends jn{status;source;constructor(n,e){super(),this.status=n,this.source=e}},zl=class extends jn{source;constructor(n){super(),this.source=n}},Lr=class extends jn{source;constructor(n){super(),this.source=n}};function Zy(t){return(Yl(t)?t.validators:t)||null}function qN(t){return Array.isArray(t)?of(t):t||null}function Ky(t,n){return(Yl(n)?n.asyncValidators:t)||null}function YN(t){return Array.isArray(t)?af(t):t||null}function Yl(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function ZN(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new S(1e3,"");if(!Qy(i,e))throw new S(1001,"")}function KN(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new S(-1002,"")})}var Ul=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=Q(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Me(this.statusReactive)}set status(n){Me(()=>this.statusReactive.set(n))}_status=ft(()=>this.statusReactive());statusReactive=Q(void 0);get valid(){return this.status===ea}get invalid(){return this.status===Pl}get pending(){return this.status===Rr}get disabled(){return this.status===ta}get enabled(){return this.status!==ta}errors;get pristine(){return Me(this.pristineReactive)}set pristine(n){Me(()=>this.pristineReactive.set(n))}_pristine=ft(()=>this.pristineReactive());pristineReactive=Q(!0);get dirty(){return!this.pristine}get touched(){return Me(this.touchedReactive)}set touched(n){Me(()=>this.touchedReactive.set(n))}_touched=ft(()=>this.touchedReactive());touchedReactive=Q(!1);get untouched(){return!this.touched}_events=new w;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(Ty(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(Ty(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(Ay(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(Ay(n,this._rawAsyncValidators))}hasValidator(n){return Bl(this._rawValidators,n)}hasAsyncValidator(n){return Bl(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(z(x({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ra(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new ra(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(z(x({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ia(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new ia(!0,i))}markAsPending(n={}){this.status=Rr;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Or(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(z(x({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=ta,this.errors=null,this._forEachChild(r=>{r.disable(z(x({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Hl(this.value,i)),this._events.next(new Or(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(z(x({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=ea,this._forEachChild(i=>{i.enable(z(x({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(z(x({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===ea||this.status===Rr)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Hl(this.value,e)),this._events.next(new Or(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(z(x({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ta:ea}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Rr,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=Hy(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Or(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new J,this.statusChanges=new J}_calculateStatus(){return this._allControlsDisabled()?ta:this.errors?Pl:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Rr)?Rr:this._anyControlsHaveStatus(Pl)?Pl:ea}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new ia(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new ra(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Yl(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=qN(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=YN(this._rawAsyncValidators)}_updateHasRequiredValidator(){Me(()=>this._hasRequired.set(this.hasValidator(oa.required)))}};function Qy(t,n){return Object.hasOwn(t,n)}function QN(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function XN(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var ef=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var JN=(()=>{class t{_validator=Vl;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Vl,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,features:[Qe]})}return t})();var eI={provide:rf,useExisting:gt(()=>Xy),multi:!0};var Xy=(()=>{class t extends JN{required;inputName="required";normalizeInput=He;createValidator=e=>By;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&be("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[mt([eI]),P]})}return t})();var Jy=new v("",{factory:()=>tI}),tI="always";function Ry(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Gl(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function $l(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function e_(t,n){let e=qy(t);n.validator!==null?t.setValidators(My(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=Yy(t);n.asyncValidator!==null?t.setAsyncValidators(My(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();$l(n._rawValidators,r),$l(n._rawAsyncValidators,r)}function Gl(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=qy(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=Yy(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return $l(n._rawValidators,i),$l(n._rawAsyncValidators,i),e}function t_(t,n){t==null,e_(t,n)}function nI(t,n){return Gl(t,n)}function iI(t){return Object.getPrototypeOf(t.constructor)===RN}function n_(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function rI(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Py?e=o:iI(o)?i=o:r=o}),r||i||e||null}function oI(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var Wl=class extends jl{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Lr&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=rI(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;pendingCustomValue=void 0;hasPendingCustomValue=!1;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(rt)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.control,e=this.injector?.get(tn);if(!n||!e)return;let i=e.markForCheck.bind(e);if(this.subscription=new he,this.subscription.add(()=>this.discardPendingCustomValue(n)),typeof n.registerOnChange=="function"){let r=(o,a)=>{this.discardPendingCustomValue(n),a&&this.viewToModelUpdate(o),i()};n.registerOnChange(r),this.subscription.add(()=>n._unregisterOnChange(r))}this.subscription.add(n.valueChanges.subscribe(i)),this.subscription.add(n.statusChanges.subscribe(i)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=n.events.subscribe(r=>{r instanceof Lr&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&n.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{let o=this.control;o&&(o._pendingValue=r,o._pendingChange=!0,o._pendingDirty=!0,this.pendingCustomValue=r,this.hasPendingCustomValue=!0,o.updateOn==="change"&&this.updateControlFromCustom(o))}),n.listenToCustomControlOutput("touch",()=>{let r=this.control;r&&(r._pendingTouched=!0,r.updateOn==="blur"&&r._pendingChange&&this.updateControlFromCustom(r),r.updateOn!=="submit"&&r.markAsTouched())}),this.customControlBindings={},this.isNativeFormElement=QN(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof Xy))}updateControlFromCustom(n){let e=n._pendingValue;n._pendingChange=!1,this.clearPendingCustomValue(),this.customControlBindings&&(this.customControlBindings.value=e),n._pendingDirty&&n.markAsDirty(),n.setValue(e,{emitModelToViewChange:!1}),this.viewToModelUpdate(n.value)}discardPendingCustomValue(n){this.hasPendingCustomValue&&(this.customControlBindings&&(this.customControlBindings.value=this.pendingCustomValue),this.clearPendingCustomValue(),n._pendingValue=n.value,n._pendingChange=!1,n._pendingDirty=!1)}clearPendingCustomValue(){this.pendingCustomValue=void 0,this.hasPendingCustomValue=!1}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;if(!i||!r)return;this.hasPendingCustomValue&&!i._pendingChange&&(r.value=this.pendingCustomValue,this.clearPendingCustomValue()),Object.is(r.value,i.value)||(this.discardPendingCustomValue(i),r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let a=this._convertErrors(o);n.setInputOnDirectives("errors",a)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&XN(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new ef({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=ft(()=>{let r=n();return r.length===0?null:r.reduce((o,a)=>(o[a.kind]=a,o),{})});this.parseErrorsValidator=(()=>e).bind(this),hn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var ql=class extends Ul{constructor(n,e,i){super(Zy(e),Ky(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Me(()=>{KN(this,!0,n),Object.keys(n).forEach(i=>{ZN(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,z(x({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Lr(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return Qy(this.controls,n)?this.controls[n]:null}};var aI={provide:Fr,useExisting:gt(()=>sf)},na=Promise.resolve(),sf=(()=>{class t extends Fr{callSetDisabledState;get submitted(){return Me(this.submittedReactive)}_submitted=ft(()=>this.submittedReactive());submittedReactive=Q(!1);_directives=new Set;form;ngSubmit=new J;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new ql({},of(e),af(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){na.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){na.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){na.then(()=>{let i=this._findContainer(e.path),r=new ql({});t_(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){na.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){na.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),n_(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new zl(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(Le(rf,10),Le(Vy,10),Le(Jy,8))};static \u0275dir=I({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&xe("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[mt([aI]),P]})}return t})();function Oy(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function Fy(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var sI=class extends Ul{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(Zy(e),Ky(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Yl(e)&&(e.nonNullable||e.initialValueIsDefault)&&(Fy(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Me(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Lr(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){Oy(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){Oy(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){Fy(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var lI=t=>t instanceof sI;var cI=(()=>{class t extends Fr{callSetDisabledState;get submitted(){return Me(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=ft(()=>this._submittedReactive());_submittedReactive=Q(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){if(!this.form)throw new S(1052,!1);Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Gl(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);if(!i)throw new S(-1055,!1);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){Ry(e.control||null,e,!1),oI(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,n_(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new zl(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(Ry(i||null,e),lI(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);t_(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&nI(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){e_(this.form,this),this._oldForm&&Gl(this._oldForm,this)}static \u0275fac=function(i){return new(i||t)(Le(rf,10),Le(Vy,10),Le(Jy,8))};static \u0275dir=I({type:t,features:[P,Qe]})}return t})(),dI={provide:Fr,useExisting:gt(()=>lf)},lf=(()=>{class t extends cI{form=null;ngSubmit=new J;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&xe("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[mt([dI]),P]})}return t})();var r_=Symbol("FIELD_TREE");var o_=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),i_=class{reducer;create;brand;[o_];constructor(n,e){this.reducer=n,this.create=e}};function aa(t){return typeof t=="function"&&t[r_]===!0}var a_=new v("");var cf=class{_box;_destroyed=new w;_resizeSubject=new w;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new L(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Oe(e=>e.filter(i=>i.target===n)),De(e=>e.length>0),es({bufferSize:1,refCount:!0}),$t(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},s_=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=m(D);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new cf(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var df=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["mat-label"]]})}return t})(),uI=new v("MatError");var uf=(()=>{class t{align="start";id=m(Ze).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(ct("id",r.id),be("align",null),te("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),mI=new v("MatPrefix");var fI=new v("MatSuffix");var p_=new v("FloatingLabelParent"),l_=(()=>{class t{_elementRef=m(M);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=m(s_);_ngZone=m(D);_parent=m(p_);_resizeSubscription=new he;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return pI(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&te("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function pI(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var c_="mdc-line-ripple--active",Zl="mdc-line-ripple--deactivating",d_=(()=>{class t{_elementRef=m(M);_cleanupTransitionEnd;constructor(){let e=m(D),i=m(_e);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Zl),e.add(c_)}deactivate(){this._elementRef.nativeElement.classList.add(Zl)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Zl);e.propertyName==="opacity"&&r&&i.remove(c_,Zl)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),u_=(()=>{class t{_elementRef=m(M);_ngZone=m(D);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["notch"];return T({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(o,a){if(o&1&&en(e,5),o&2){let s;ce(s=de())&&(a._notch=s.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(o,a){o&2&&te("mdc-notched-outline--notched",a.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:["*"],decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(o,a){o&1&&(Vt(),We(0,"div",1),Xt(1,"div",2,0),le(3),Jt(),We(4,"div",3))},encapsulation:2})})()}return t})(),mf=(()=>{class t{id;ngField=null;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;stateChanges=null;value;static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t})}return t})();var ff=new v("MatFormField"),hI=new v("MAT_FORM_FIELD_DEFAULT_OPTIONS"),m_="fill",gI="auto",f_="fixed",bI="translateY(-50%)",pf=(()=>{class t{_elementRef=m(M);_changeDetectorRef=m(tn);_platform=m(oe);_idGenerator=m(Ze);_ngZone=m(D);_defaults=m(hI,{optional:!0});_currentDirection;_unwrapMaybeSignal(e){return gn(e)?e():e}_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Ti("iconPrefixContainer");_textPrefixContainerSignal=Ti("textPrefixContainer");_iconSuffixContainerSignal=Ti("iconSuffixContainer");_textSuffixContainerSignal=Ti("textSuffixContainer");_prefixSuffixContainers=ft(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Iv(df);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Dt(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||gI}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||m_;this._appearanceSignal.set(i)}_appearanceSignal=Q(m_);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||f_}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||f_}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new w;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=et();constructor(){let e=this._defaults,i=m(nn);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),hn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=aa(this._control.ngField)?null:this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&!aa(this._control.ngField)&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=ft(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges?.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges?.pipe(Ut([void 0,void 0]),Oe(()=>[this._unwrapMaybeSignal(i.errorState),i.userAriaDescribedBy]),Ja(),De(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&!aa(i.ngField)&&(this._valueChanges=i.ngControl.valueChanges.pipe($t(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),sn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._unwrapMaybeSignal(this._control.focused);e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Em({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=ft(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._shouldAlwaysFloat()||this._unwrapMaybeSignal(this._control.shouldLabelFloat):!1}_shouldForward(e){let i=this._control?.ngField||this._control?.ngControl;if(!i)return!1;if(aa(i)){let r=i();return e==="valid"?r.valid():e==="dirty"?r.dirty():e==="touched"?r.touched():e==="pending"?r.pending():e==="untouched"?!r.touched():e==="pristine"?!r.dirty():e==="invalid"?!r.valid():!1}else{let r=i;return e==="valid"?r.valid:e==="dirty"?r.dirty:e==="touched"?r.touched:e==="pending"?r.pending:e==="untouched"?r.untouched:e==="pristine"?r.pristine:e==="invalid"?r.invalid:!1}}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._unwrapMaybeSignal(this._control.errorState)?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${a+s}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,h=`var(--mat-mdc-form-field-label-transform, ${bI} translateX(${f}))`,g=a+s+l+c;return[h,g]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["iconPrefixContainer"],i=["textPrefixContainer"],r=["iconSuffixContainer"],o=["textSuffixContainer"],a=["textField"],s=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],l=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function c(R,ae){R&1&&xn(0,"span",21)}function d(R,ae){if(R&1&&($(0,"label",20),le(1,1),fe(2,c,1,0,"span",21),q()),R&2){let b=Se(2);Xe("floating",b._shouldLabelFloat())("monitorResize",b._hasOutline())("id",b._labelId),be("for",b._control.disableAutomaticLabeling?null:b._control.id),G(2),pe(!b.hideRequiredMarker&&b._unwrapMaybeSignal(b._control.required)?2:-1)}}function u(R,ae){if(R&1&&fe(0,d,3,5,"label",20),R&2){let b=Se();pe(b._hasFloatingLabel()?0:-1)}}function p(R,ae){R&1&&xn(0,"div",7)}function f(R,ae){}function h(R,ae){if(R&1&&st(0,f,0,0,"ng-template",13),R&2){Se(2);let b=jo(1);Xe("ngTemplateOutlet",b)}}function g(R,ae){if(R&1&&($(0,"div",9),fe(1,h,1,1,null,13),q()),R&2){let b=Se();Xe("matFormFieldNotchedOutlineOpen",b._shouldLabelFloat()),G(),pe(b._forceDisplayInfixLabel()?-1:1)}}function y(R,ae){R&1&&($(0,"div",10,2),le(2,2),q())}function _(R,ae){R&1&&($(0,"div",11,3),le(2,3),q())}function B(R,ae){}function jt(R,ae){if(R&1&&st(0,B,0,0,"ng-template",13),R&2){Se();let b=jo(1);Xe("ngTemplateOutlet",b)}}function yc(R,ae){R&1&&($(0,"div",14,4),le(2,4),q())}function ax(R,ae){R&1&&($(0,"div",15,5),le(2,5),q())}function sx(R,ae){R&1&&xn(0,"div",16)}function lx(R,ae){R&1&&($(0,"div",18),le(1,6),q())}function cx(R,ae){if(R&1&&($(0,"mat-hint",22),dt(1),q()),R&2){let b=Se(2);Xe("id",b._hintLabelId),G(),ut(b.hintLabel)}}function dx(R,ae){if(R&1&&($(0,"div",19),fe(1,cx,2,2,"mat-hint",22),le(2,7),xn(3,"div",23),le(4,8),q()),R&2){let b=Se();G(),pe(b.hintLabel?1:-1)}}return T({type:t,selectors:[["mat-form-field"]],contentQueries:function(ae,b,se){if(ae&1&&(ml(se,b._labelChild,df,5),Mi(se,mf,5)(se,mI,5)(se,fI,5)(se,uI,5)(se,uf,5)),ae&2){Sr();let $e;ce($e=de())&&(b._formFieldControl=$e.first),ce($e=de())&&(b._prefixChildren=$e),ce($e=de())&&(b._suffixChildren=$e),ce($e=de())&&(b._errorChildren=$e),ce($e=de())&&(b._hintChildren=$e)}},viewQuery:function(ae,b){if(ae&1&&(Cr(b._iconPrefixContainerSignal,e,5)(b._textPrefixContainerSignal,i,5)(b._iconSuffixContainerSignal,r,5)(b._textSuffixContainerSignal,o,5),en(a,5)(e,5)(i,5)(r,5)(o,5)(l_,5)(u_,5)(d_,5)),ae&2){Sr(4);let se;ce(se=de())&&(b._textField=se.first),ce(se=de())&&(b._iconPrefixContainer=se.first),ce(se=de())&&(b._textPrefixContainer=se.first),ce(se=de())&&(b._iconSuffixContainer=se.first),ce(se=de())&&(b._textSuffixContainer=se.first),ce(se=de())&&(b._floatingLabel=se.first),ce(se=de())&&(b._notchedOutline=se.first),ce(se=de())&&(b._lineRipple=se.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(ae,b){ae&2&&te("mat-mdc-form-field-label-always-float",b._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",b._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",b._hasIconSuffix)("mat-form-field-invalid",b._unwrapMaybeSignal(b._control.errorState))("mat-form-field-disabled",b._unwrapMaybeSignal(b._control.disabled))("mat-form-field-autofilled",b._unwrapMaybeSignal(b._control.autofilled))("mat-form-field-appearance-fill",b.appearance=="fill")("mat-form-field-appearance-outline",b.appearance=="outline")("mat-form-field-hide-placeholder",b._hasFloatingLabel()&&!b._shouldLabelFloat())("mat-primary",b.color!=="accent"&&b.color!=="warn")("mat-accent",b.color==="accent")("mat-warn",b.color==="warn")("ng-untouched",b._shouldForward("untouched"))("ng-touched",b._shouldForward("touched"))("ng-pristine",b._shouldForward("pristine"))("ng-dirty",b._shouldForward("dirty"))("ng-valid",b._shouldForward("valid"))("ng-invalid",b._shouldForward("invalid"))("ng-pending",b._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[mt([{provide:ff,useExisting:t},{provide:p_,useExisting:t}])],ngContentSelectors:l,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(ae,b){if(ae&1&&(Vt(s),st(0,u,1,1,"ng-template",null,0,fl),$(2,"div",6,1),xe("click",function($e){return b._control.onContainerClick($e)}),fe(4,p,1,0,"div",7),$(5,"div",8),fe(6,g,2,2,"div",9),fe(7,y,3,0,"div",10),fe(8,_,3,0,"div",11),$(9,"div",12),fe(10,jt,1,1,null,13),le(11),q(),fe(12,yc,3,0,"div",14),fe(13,ax,3,0,"div",15),q(),fe(14,sx,1,0,"div",16),q(),$(15,"div",17),fe(16,lx,2,0,"div",18)(17,dx,5,1,"div",19),q()),ae&2){let se,$e=b._unwrapMaybeSignal(b._control.disabled);G(2),te("mdc-text-field--filled",!b._hasOutline())("mdc-text-field--outlined",b._hasOutline())("mdc-text-field--no-label",!b._hasFloatingLabel())("mdc-text-field--disabled",$e)("mdc-text-field--invalid",b._unwrapMaybeSignal(b._control.errorState)),G(2),pe(!b._hasOutline()&&!$e?4:-1),G(2),pe(b._hasOutline()?6:-1),G(),pe(b._hasIconPrefix?7:-1),G(),pe(b._hasTextPrefix?8:-1),G(2),pe(!b._hasOutline()||b._forceDisplayInfixLabel()?10:-1),G(2),pe(b._hasTextSuffix?12:-1),G(),pe(b._hasIconSuffix?13:-1),G(),pe(b._hasOutline()?-1:14),G(),te("mat-mdc-form-field-subscript-dynamic-size",b.subscriptSizing==="dynamic");let ux=b._getSubscriptMessageType();G(),pe((se=ux)==="error"?16:se==="hint"?17:-1)}},dependencies:[l_,u_,Im,d_,uf],styles:[`.mdc-text-field {
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
`],encapsulation:2})})()}return t})();var h_=new v("MAT_INPUT_VALUE_ACCESSOR");var g_=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var Kl=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?gn(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var vI=["button","checkbox","file","hidden","image","radio","range","reset","submit"],yI=new v("MAT_INPUT_CONFIG"),b_=(()=>{class t{_elementRef=m(M);_platform=m(oe);ngControl=m(Wl,{optional:!0,self:!0});_autofillMonitor=m(ty);_ngZone=m(D);_formField=m(ff,{optional:!0});_renderer=m(_e);_uid=m(Ze).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=m(yI,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new w;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Dt(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(oa.required)??!1}set required(e){this._required=Dt(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Ym().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Dt(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Ym().has(e));constructor(){let e=m(sf,{optional:!0}),i=m(lf,{optional:!0}),r=m(g_),o=m(h_,{optional:!0,self:!0}),a=m(a_,{optional:!0,self:!0}),s=this._elementRef.nativeElement,l=s.nodeName.toLowerCase();o?gn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Kl(r,a||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&hn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){vI.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&xe("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(ct("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),be("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),te("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",He]},exportAs:["matInput"],features:[mt([{provide:mf,useExisting:t}]),Qe]})}return t})();function _I(t,n){if(t&1){let e=wr();$(0,"textarea",3),xe("input",function(r){bi(e);let o=Se();return vi(o.typed(r))}),q()}if(t&2){let e=Se();Xe("placeholder",e.hint())}}function xI(t,n){if(t&1){let e=wr();$(0,"input",4),xe("input",function(r){bi(e);let o=Se();return vi(o.typed(r))}),q()}if(t&2){let e=Se();Xe("type",e.inputType()===3?"password":"text")("placeholder",e.hint()),be("inputmode",e.inputType()===1?"decimal":null)}}var wI=["canvas"];function CI(t,n){t&1&&We(0,"span",1),t&2&&ct("innerHTML",n,Gu)}function SI(t,n){if(t&1&&We(0,"img",4),t&2){let e=Se();Dr("object-fit",e.fit()),ct("src",e.source())}}function DI(t,n){if(t&1&&We(0,"canvas",null,0),t&2){let e=Se();Dr("object-fit",e.fit())}}var Nt=class{element=m(M).nativeElement;slot=this.element;text=Q("")},gf=class t extends Nt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-text"]],features:[P],decls:1,vars:1,template:function(e,i){e&1&&dt(0),e&2&&ut(i.text())},styles:["[_nghost-%COMP%]{display:block;white-space:pre-wrap;overflow-wrap:anywhere}"]})},bf=class t extends Nt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-button"]],features:[P],decls:2,vars:1,consts:[["matButton","filled"]],template:function(e,i){e&1&&($(0,"button",0),dt(1),q()),e&2&&(G(),ut(i.text()))},dependencies:[Ll],styles:["[_nghost-%COMP%]{display:block}button[_ngcontent-%COMP%]{width:100%}"]})},Hn=class t extends Nt{hint=Q("");inputType=Q(0);onText=()=>{};sent=[];setText(n){let e=this.sent.indexOf(n);if(e>=0){this.sent.splice(0,e+1);return}this.sent.length=0;let i=this.field();i&&i.value!==n&&(i.value=n),this.text.set(n)}focus(){wt(()=>this.field()?.focus(),{injector:this.injector})}typed(n){let e=n.target.value;this.sent.push(e),this.onText(e)}injector=m(O);field(){return this.element.querySelector("input, textarea")}static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-text-input"]],features:[P],decls:3,vars:1,consts:[["appearance","outline","subscriptSizing","dynamic"],["matInput","","cdkTextareaAutosize","",3,"placeholder"],["matInput","",3,"type","placeholder"],["matInput","","cdkTextareaAutosize","",3,"input","placeholder"],["matInput","",3,"input","type","placeholder"]],template:function(e,i){e&1&&($(0,"mat-form-field",0),fe(1,_I,1,1,"textarea",1)(2,xI,1,3,"input",2),q()),e&2&&(G(),pe(i.inputType()===2?1:2))},dependencies:[pf,b_,jm,ny],styles:["[_nghost-%COMP%]{display:block}mat-form-field[_ngcontent-%COMP%]{width:100%}"]})},hf=new Map,Cn=class t extends Nt{sanitizer=m(Sl);source=Q("");fit=Q("contain");svg=Q(null);streaming=Q(!1);canvas=Ti("canvas");shown="";stream;showResource(n){this.stopStream(),this.shown=n,hf.has(n)||hf.set(n,fetch(`res/${n}.svg`).then(e=>e.ok?e.text():"")),hf.get(n).then(e=>{this.shown===n&&this.svg.set(e?this.sanitizer.bypassSecurityTrustHtml(e):null)})}showPicture(n){this.stopStream(),this.shown="",this.svg.set(null),this.source.set(n)}showStream(n){this.shown="",this.svg.set(null),this.source.set(""),this.stream=n,this.streaming.set(!0)}draw(n){let e=this.canvas()?.nativeElement;e&&((e.width!==n.displayWidth||e.height!==n.displayHeight)&&(e.width=n.displayWidth,e.height=n.displayHeight),e.getContext("2d")?.drawImage(n,0,0))}stopStream(){this.stream=void 0,this.streaming.set(!1)}static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-image"]],viewQuery:function(e,i){e&1&&Cr(i.canvas,wI,5),e&2&&Sr()},hostVars:2,hostBindings:function(e,i){e&2&&te("resource",i.svg()!==null)},features:[P],decls:3,vars:1,consts:[["canvas",""],[3,"innerHTML"],["alt","",3,"src","object-fit"],[3,"object-fit"],["alt","",3,"src"]],template:function(e,i){if(e&1&&fe(0,CI,1,1,"span",1)(1,SI,1,3,"img",2)(2,DI,2,2,"canvas",3),e&2){let r;pe((r=i.svg())?0:i.source()?1:i.streaming()?2:-1,r)}},styles:["[_nghost-%COMP%]{display:block}img[_ngcontent-%COMP%], canvas[_ngcontent-%COMP%], span[_ngcontent-%COMP%], span[_ngcontent-%COMP%]     svg{display:block;width:100%;height:100%}.resource[_nghost-%COMP%]:not(.sized)   span[_ngcontent-%COMP%]{width:24px;height:24px;margin:0 auto}"]})},sa=class t extends Nt{sanitizer=m(Sl);url=Q("");load(n){this.url.set(this.sanitizer.bypassSecurityTrustResourceUrl(n))}static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-web-view"]],features:[P],decls:1,vars:1,consts:[[3,"src"]],template:function(e,i){e&1&&We(0,"iframe",0),e&2&&ct("src",i.url(),Wu)},styles:["[_nghost-%COMP%]{display:block}iframe[_ngcontent-%COMP%]{width:100%;height:100%;border:0}"]})},vf=class t extends Nt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-column"]],features:[P],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:flex;flex-direction:column}"]})},yf=class t extends Nt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-row"]],features:[P],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:flex;flex-direction:row;align-items:flex-start}"]})},_f=class t extends Nt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-stack"]],features:[P],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:grid}"]})},la=class extends Nt{constructor(n){super(),this.slot=document.createElement("div"),this.slot.style.display="flex",this.slot.style.flexDirection=n,this.element.appendChild(this.slot)}},xf=class t extends la{constructor(){super("column")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["hatter-scroll-view"]],features:[P],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;overflow-y:auto}"]})},wf=class t extends la{constructor(){super("row")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["hatter-horizontal-scroll-view"]],features:[P],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;overflow-x:auto}"]})},Cf=class t extends la{constructor(){super("column")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["hatter-lazy-column"]],features:[P],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;overflow-y:auto}"]})},Sf=class t extends Nt{static \u0275fac=(()=>{let n;return function(i){return(n||(n=re(t)))(i||t)}})();static \u0275cmp=T({type:t,selectors:[["hatter-map-view"]],features:[P],decls:1,vars:0,template:function(e,i){e&1&&dt(0,"Map")},styles:["[_nghost-%COMP%]{display:grid;place-items:center;background:var(--%NS%mat-sys-surface-variant)}"]})},v_={0:gf,1:bf,2:vf,3:yf,4:Hn,5:xf,6:Cn,7:Sf,8:sa,9:_f,10:wf,11:Cf};var ca=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Pr=class extends ca{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,a){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=a||null}},Vr=class extends ca{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Df=class extends ca{element;constructor(n){super(),this.element=n instanceof M?n.nativeElement:n}},Br=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Pr)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Vr)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Df)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Ql=class extends Br{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Pn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||O.NULL,o=r.get(Re,i.injector);e=Er(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Fi=(()=>{class t extends Br{_moduleRef=m(Pn,{optional:!0});_document=m(V);_viewContainerRef=m(Qt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new J;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._insertContent(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._insertContent(i),this._attachedPortal=e,super.setDisposeFn(()=>r.parentNode?.replaceChild(i,r))};_insertContent(e){let i=this._viewContainerRef.element.nativeElement;i.nodeType===i.ELEMENT_NODE?i.appendChild(e):i.parentNode?.insertBefore(e,i)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[P]})}return t})();var EI=20,y_=(()=>{class t{_ngZone=m(D);_platform=m(oe);_renderer=m(ke).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new w;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=EI){return this._platform.isBrowser?new L(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Qi(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):no()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(De(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=Ye(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),__=(()=>{class t{elementRef=m(M);scrollDispatcher=m(y_);ngZone=m(D);dir=m(nn,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new w;_renderer=m(_e);_cleanupScroll;_elementScrolled=new w;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&Mr()!=Bt.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),Mr()==Bt.INVERTED?e.left=e.right:Mr()==Bt.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Al()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let a=this.dir&&this.dir.value=="rtl";return e=="start"?e=a?r:i:e=="end"&&(e=a?i:r),a&&Mr()==Bt.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:a&&Mr()==Bt.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),NI=20,x_=(()=>{class t{_platform=m(oe);_listeners;_viewportSize=null;_change=new w;_document=m(V);constructor(){let e=m(D),i=m(ke).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=NI){return e>0?this._change.pipe(Qi(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var w_=Al();function Li(t){return new Xl(t.get(x_),t.get(V))}var Xl=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=wn(-this._previousScrollPosition.left),n.style.top=wn(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),w_&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),w_&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};var Jl=class{enable(){}disable(){}attach(){}};var jr=class{positionStrategy;scrollStrategy=new Jl;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var D_=(()=>{class t{_attachedOverlays=[];_document=m(V);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),E_=(()=>{class t extends D_{_ngZone=m(D);_renderer=m(ke).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),N_=(()=>{class t extends D_{_platform=m(oe);_ngZone=m(D);_renderer=m(ke).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Je(e)};_clickListener=e=>{let i=Je(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if(C_(s.overlayElement,i)||C_(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function C_(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var I_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2})}return t})(),kf=(()=>{class t{_platform=m(oe);_containerElement;_document=m(V);_styleLoader=m(qe);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||qm()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),qm()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(I_)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),Nf=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function k_(t){return t&&t.nodeType===1}var Ef=Q([]),Hr=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new w;_attachments=new w;_detachments=new w;_positionStrategy;_scrollStrategy;_locationChanges;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new w;_outsidePointerEvents=new w;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,l,c,d=!1,u,p){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=d,this._injector=u,this._renderer=p,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);if(this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),Me(()=>{Ef.update(i=>i.includes(this)?i:[...i,this])}),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=wt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation===!0||this._config.disposeOnNavigation==="pop-state"){let i=this._location.subscribe(()=>this.dispose());this._locationChanges=()=>i.unsubscribe()}else this._config.disposeOnNavigation==="url-change"&&(this._locationChanges=this._location.onUrlChange(()=>this.dispose()));return this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges?.(),this._outsideClickDispatcher.remove(this),Me(()=>{Ef.update(e=>e.filter(i=>i!==this))}),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges?.(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0,Me(()=>{Ef.update(e=>e.filter(i=>i!==this))})}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=x(x({},this._config),n),this._updateElementSize()}setDirection(n){this._config=z(x({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=wn(this._config.width),n.height=wn(this._config.height),n.minWidth=wn(this._config.minWidth),n.minHeight=wn(this._config.minHeight),n.maxWidth=wn(this._config.maxWidth),n.maxHeight=wn(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;k_(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Nf(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Xo(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=wt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}};var S_="cdk-global-overlay-wrapper";function Pi(t){return new ec}var ec=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(S_),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,l=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),d=this._xPosition,u=this._xOffset,p=this._overlayRef.getConfig().direction==="rtl",f="",h="",g="";l?g="flex-start":d==="center"?(g="center",p?h=u:f=u):p?d==="left"||d==="end"?(g="flex-end",f=u):(d==="right"||d==="start")&&(g="flex-start",h=u):d==="left"||d==="start"?(g="flex-start",f=u):(d==="right"||d==="end")&&(g="flex-end",h=u),n.position=this._cssPosition,n.marginLeft=l?"0":f,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":h,e.justifyContent=g,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(S_),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}};var M_=new v("OVERLAY_DEFAULT_CONFIG");function Mf(t,n){t.get(qe).load(I_);let e=t.get(kf),i=t.get(V),r=t.get(Ze),o=t.get(lt),a=t.get(nn),s=t.get(_e,null,{optional:!0})||t.get(ke).createRenderer(null,null),l=new jr(n),c=t.get(M_,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,!i.body||!("showPopover"in i.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let d=i.createElement("div"),u=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),u.appendChild(d),l.usePopover&&(u.setAttribute("popover","manual"),u.classList.add("cdk-overlay-popover"));let p=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return k_(p)?p.after(u):p?.type==="parent"?p.element.appendChild(u):e.getContainerElement().appendChild(u),new Hr(new Ql(d,o,t),u,d,l,t.get(D),t.get(E_),i,t.get(vl),t.get(N_),n?.disableAnimations??t.get(So,null,{optional:!0})==="NoopAnimations",t.get(Re),s)}var zn=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var da=(()=>{class t extends Br{_elementRef=m(M);_focusTrapFactory=m(gy);_config;_interactivityChecker=m(Wm);_ngZone=m(D);_focusMonitor=m(Qo);_renderer=m(_e);_changeDetectorRef=m(tn);_injector=m(O);_platform=m(oe);_document=m(V);_portalOutlet;_focusTrapped=new w;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=m(zn,{optional:!0})||new zn,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),a(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),a=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||wt(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Nr(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Nr();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Nr()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,r){}return T({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(r,o){if(r&1&&en(Fi,7),r&2){let a;ce(a=de())&&(o._portalOutlet=a.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(r,o){r&2&&be("id",o._config.id||null)("role",o._config.role)("aria-modal",o._config.ariaModal)("aria-labelledby",o._config.ariaLabel?null:o._ariaLabelledByQueue[0])("aria-label",o._config.ariaLabel)("aria-describedby",o._config.ariaDescribedBy||null)},features:[P],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(r,o){r&1&&st(0,e,0,0,"ng-template",0)},dependencies:[Fi],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})})()}return t})(),Vi=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new w;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!kr(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},kI=new v("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=m(O);return()=>Li(t)}}),MI=new v("DialogData"),TI=new v("DefaultDialogConfig");function AI(t){let n=Q(t),e=new J;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var nc=(()=>{class t{_injector=m(O);_defaultOptions=m(TI,{optional:!0});_parentDialog=m(t,{optional:!0,skipSelf:!0});_overlayContainer=m(kf);_idGenerator=m(Ze);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new w;_afterOpenedAtThisLevel=new w;_ariaHiddenElements=new Map;_scrollStrategy=m(kI);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=io(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ut(void 0)));open(e,i){let r=this._defaultOptions||new zn;i=x(x({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),a=Mf(this._injector,o),s=new Vi(a,i),l=this._attachContainer(a,s,i);if(s.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(nt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,s,l,i),this.openDialogs.push(s),s.closed.subscribe(()=>this._removeOpenDialog(s,!0)),this.afterOpened.next(s),s}closeAll(){Tf(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){Tf(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),Tf(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new jr({positionStrategy:e.positionStrategy||Pi().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,a=[{provide:zn,useValue:r},{provide:Vi,useValue:i},{provide:Hr,useValue:e}],s;r.container?typeof r.container=="function"?s=r.container:(s=r.container.type,a.push(...r.container.providers(r))):s=da;let l=new Pr(s,r.viewContainerRef,O.create({parent:o||this._injector,providers:a}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Lt){let a=this._createInjector(o,i,r,void 0),s={$implicit:o.data,dialogRef:i};o.templateContext&&(s=x(x({},s),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Vr(e,null,s,a))}else{let a=this._createInjector(o,i,r,this._injector),s=r.attachComponentPortal(new Pr(e,o.viewContainerRef,a,null,o.bindings));i.componentRef=s,i.componentInstance=s.instance}}_createInjector(e,i,r,o){let a=e.injector||e.viewContainerRef?.injector,s=[{provide:MI,useValue:e.data},{provide:Vi,useValue:i}];return e.providers&&(typeof e.providers=="function"?s.push(...e.providers(i,e,r)):s.push(...e.providers)),e.direction&&(!a||!a.get(nn,null,{optional:!0}))&&s.push({provide:nn,useValue:AI(e.direction)}),O.create({parent:a||o,providers:s})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,a)=>{o?a.setAttribute("aria-hidden",o):a.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();function Tf(t,n){let e=t.length;for(;e--;)n(t[e])}var A_="_mat-bottom-sheet-enter",R_="_mat-bottom-sheet-exit",OI=(()=>{class t extends da{_breakpointSubscription;_animationsDisabled=et();_animationState="void";_animationStateChanged=new J;_destroyed=!1;constructor(){super();let e=m($m);this._breakpointSubscription=e.observe([Oi.Medium,Oi.Large,Oi.XLarge]).subscribe(()=>{let i=this._elementRef.nativeElement.classList;i.toggle("mat-bottom-sheet-container-medium",e.isMatched(Oi.Medium)),i.toggle("mat-bottom-sheet-container-large",e.isMatched(Oi.Large)),i.toggle("mat-bottom-sheet-container-xlarge",e.isMatched(Oi.XLarge))})}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._animationsDisabled&&this._simulateAnimation(A_))}exit(){this._destroyed||(this._elementRef.nativeElement.setAttribute("mat-exit",""),this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._animationsDisabled&&this._simulateAnimation(R_))}ngOnDestroy(){super.ngOnDestroy(),this._breakpointSubscription.unsubscribe(),this._destroyed=!0}_simulateAnimation(e){this._ngZone.run(()=>{this._handleAnimationEvent(!0,e,this._elementRef.nativeElement),setTimeout(()=>this._handleAnimationEvent(!1,e,this._elementRef.nativeElement))})}_trapFocus(){super._trapFocus({preventScroll:!0})}_handleAnimationEvent(e,i,r){if(r===this._elementRef.nativeElement){let o=i===A_;(o||i===R_)&&this._animationStateChanged.emit({toState:o?"visible":"hidden",phase:e?"start":"done"})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,r){}return T({type:t,selectors:[["mat-bottom-sheet-container"]],hostAttrs:["tabindex","-1",1,"mat-bottom-sheet-container"],hostVars:9,hostBindings:function(r,o){r&1&&xe("animationstart",function(s){return o._handleAnimationEvent(!0,s.animationName,s.target)})("animationend",function(s){return o._handleAnimationEvent(!1,s.animationName,s.target)})("animationcancel",function(s){return o._handleAnimationEvent(!1,s.animationName,s.target)}),r&2&&(be("role",o._config.role)("aria-modal",o._config.ariaModal)("aria-label",o._config.ariaLabel),te("mat-bottom-sheet-container-animations-enabled",!o._animationsDisabled)("mat-bottom-sheet-container-enter",o._animationState==="visible")("mat-bottom-sheet-container-exit",o._animationState==="hidden"))},features:[P],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(r,o){r&1&&st(0,e,0,0,"ng-template",0)},dependencies:[Fi],styles:[`@keyframes _mat-bottom-sheet-enter {
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
`],encapsulation:2,changeDetection:1})})()}return t})(),Rf=new v("MatBottomSheetData"),Af=class{viewContainerRef;injector;panelClass;direction;data=null;hasBackdrop=!0;backdropClass;disableClose=!1;ariaLabel=null;ariaModal=!1;closeOnNavigation=!0;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;height="";minHeight;maxHeight;bindings},zr=class{_ref;get instance(){return this._ref.componentInstance}get componentRef(){return this._ref.componentRef}containerInstance;disableClose;_afterOpened=new w;_result;_closeFallbackTimeout;constructor(n,e,i){this._ref=n,this.containerInstance=i,this.disableClose=e.disableClose,i._animationStateChanged.pipe(De(r=>r.phase==="done"&&r.toState==="visible"),nt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(De(r=>r.phase==="done"&&r.toState==="hidden"),nt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._ref.close(this._result)}),n.overlayRef.detachments().subscribe(()=>{this._ref.close(this._result)}),sn(this.backdropClick(),this.keydownEvents().pipe(De(r=>r.keyCode===27))).subscribe(r=>{!this.disableClose&&(r.type!=="keydown"||!kr(r))&&(r.preventDefault(),this.dismiss())})}dismiss(n){this.containerInstance&&(this.containerInstance._animationStateChanged.pipe(De(e=>e.phase==="start"),nt(1)).subscribe(()=>{this._closeFallbackTimeout=setTimeout(()=>this._ref.close(this._result),500),this._ref.overlayRef.detachBackdrop()}),this._result=n,this.containerInstance.exit(),this.containerInstance=null)}afterDismissed(){return this._ref.closed}afterOpened(){return this._afterOpened}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}},FI=new v("mat-bottom-sheet-default-options"),O_=(()=>{class t{_injector=m(O);_parentBottomSheet=m(t,{optional:!0,skipSelf:!0});_animationsDisabled=et();_defaultOptions=m(FI,{optional:!0});_bottomSheetRefAtThisLevel=null;_dialog=m(nc);get _openedBottomSheetRef(){let e=this._parentBottomSheet;return e?e._openedBottomSheetRef:this._bottomSheetRefAtThisLevel}set _openedBottomSheetRef(e){this._parentBottomSheet?this._parentBottomSheet._openedBottomSheetRef=e:this._bottomSheetRefAtThisLevel=e}open(e,i){let r=x(x({},this._defaultOptions||new Af),i),o;return this._dialog.open(e,z(x({},r),{disableClose:!0,closeOnOverlayDetachments:!1,maxWidth:"100%",container:OI,scrollStrategy:r.scrollStrategy||Li(this._injector),positionStrategy:Pi(this._injector).centerHorizontally().bottom("0"),disableAnimations:this._animationsDisabled,templateContext:()=>({bottomSheetRef:o}),providers:(a,s,l)=>(o=new zr(a,r,l),[{provide:zr,useValue:o},{provide:Rf,useValue:r.data}])})),o.afterDismissed().subscribe(()=>{this._openedBottomSheetRef===o&&(this._openedBottomSheetRef=null)}),this._openedBottomSheetRef?(this._openedBottomSheetRef.afterDismissed().subscribe(()=>o.containerInstance?.enter()),this._openedBottomSheetRef.dismiss()):o.containerInstance.enter(),this._openedBottomSheetRef=o,o}dismiss(e){this._openedBottomSheetRef&&this._openedBottomSheetRef.dismiss(e)}ngOnDestroy(){this._bottomSheetRefAtThisLevel&&this._bottomSheetRefAtThisLevel.dismiss()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})();var rc=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},Of="mdc-dialog--open",F_="mdc-dialog--opening",L_="mdc-dialog--closing",LI=150,PI=75,VI=(()=>{class t extends da{_animationStateChanged=new J;_animationsEnabled=!et();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?V_(this._config.enterAnimationDuration)??LI:0;_exitAnimationDuration=this._animationsEnabled?V_(this._config.exitAnimationDuration)??PI:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(P_,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(F_,Of)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Of),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Of),this._animationsEnabled?(this._hostElement.style.setProperty(P_,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(L_)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(F_,L_)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275cmp=(function(){function e(i,r){}return T({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(r,o){r&2&&(ct("id",o._config.id),be("aria-modal",o._config.ariaModal)("role",o._config.role)("aria-labelledby",o._config.ariaLabel?null:o._ariaLabelledByQueue[0])("aria-label",o._config.ariaLabel)("aria-describedby",o._config.ariaDescribedBy||null),te("_mat-animation-noopable",!o._animationsEnabled)("mat-mdc-dialog-container-with-actions",o._actionSectionCount>0))},features:[P],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(r,o){r&1&&($(0,"div",0)(1,"div",1),st(2,e,0,0,"ng-template",2),q()())},dependencies:[Fi],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2,changeDetection:1})})()}return t})(),P_="--mat-dialog-transition-duration";function V_(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?St(t.substring(0,t.length-2)):t.endsWith("s")?St(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var ic=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(ic||{}),ua=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new In(1);_beforeClosed=new In(1);_result;_closeFallbackTimeout;_state=ic.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(De(r=>r.state==="opened"),nt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(De(r=>r.state==="closed"),nt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),sn(this.backdropClick(),this.keydownEvents().pipe(De(r=>r.keyCode===27&&!this.disableClose&&!kr(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),B_(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(De(i=>i.state==="closing"),nt(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=ic.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=ic.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function B_(t,n,e){return t._closeInteractionType=n,t.close(e)}var Ff=new v("MatMdcDialogData"),BI=new v("mat-mdc-dialog-default-options"),jI=new v("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=m(O);return()=>Li(t)}}),oc=(()=>{class t{_defaultOptions=m(BI,{optional:!0});_scrollStrategy=m(jI);_parentDialog=m(t,{optional:!0,skipSelf:!0});_idGenerator=m(Ze);_injector=m(O);_dialog=m(nc);_animationsDisabled=et();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new w;_afterOpenedAtThisLevel=new w;dialogConfigClass=rc;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=io(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Ut(void 0)));constructor(){this._dialogRefConstructor=ua,this._dialogContainerType=VI,this._dialogDataToken=Ff}open(e,i){let r;i=x(x({},this._defaultOptions||new rc),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,z(x({},i),{positionStrategy:Pi(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:zn,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(a,s,l)=>(r=new this._dialogRefConstructor(a,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:s.data},{provide:this._dialogRefConstructor,useValue:r},{provide:Vi,useValue:null}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let a=this.openDialogs.indexOf(r);a>-1&&(this.openDialogs.splice(a,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=N({token:t,factory:t.\u0275fac})}return t})(),j_=(()=>{class t{dialogRef=m(ua,{optional:!0});_elementRef=m(M);_dialog=m(oc);ariaLabel;type="button";dialogResult;_matDialogClose;ngOnInit(){this.dialogRef||(this.dialogRef=G_(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){this._elementRef.nativeElement.getAttribute("aria-disabled")!=="true"&&B_(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&xe("click",function(a){return r._onButtonClick(a)}),i&2&&be("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[Qe]})}return t})(),H_=(()=>{class t{_dialogRef=m(ua,{optional:!0});_elementRef=m(M);_dialog=m(oc);ngOnInit(){this._dialogRef||(this._dialogRef=G_(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t})}return t})(),z_=(()=>{class t extends H_{id=m(Ze).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&ct("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[P]})}return t})(),U_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[pm([__])]})}return t})(),$_=(()=>{class t extends H_{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&te("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[P]})}return t})();function G_(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var HI=new v("ListOption"),zI=(()=>{class t{_elementRef=m(M);static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),UI=(()=>{class t{_elementRef=m(M);static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})(),$I=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return t})(),W_=(()=>{class t{_listOption=m(HI,{optional:!0});_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,hostVars:4,hostBindings:function(i,r){i&2&&te("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),GI=(()=>{class t extends W_{static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[P]})}return t})(),WI=(()=>{class t extends W_{static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275dir=I({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[P]})}return t})(),qI=new v("MAT_LIST_CONFIG"),Lf=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=Dt(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(Dt(e))}_disabled=Q(!1);_defaultOptions=m(qI,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,hostVars:1,hostBindings:function(i,r){i&2&&be("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),YI=(()=>{class t{_elementRef=m(M);_ngZone=m(D);_listBase=m(Lf,{optional:!0});_platform=m(oe);_hostElement;_isButtonElement;_noopAnimations=et();_avatars;_icons;set lines(e){this._explicitLines=St(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=Dt(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(Dt(e))}_disabled=Q(!1);_subscriptions=new he;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){m(qe).load(Fl);let e=m(Rl,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new Ar(this,this._ngZone,this._hostElement,this._platform,m(O)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(sn(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,contentQueries:function(i,r,o){if(i&1&&Mi(o,GI,4)(o,WI,4),i&2){let a;ce(a=de())&&(r._avatars=a),ce(a=de())&&(r._icons=a)}},hostVars:4,hostBindings:function(i,r){i&2&&(be("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),te("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var q_=(()=>{class t extends Lf{_isNonInteractive=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275cmp=(function(){let e=["*"];return T({type:t,selectors:[["mat-action-list"]],hostAttrs:["role","group",1,"mat-mdc-action-list","mat-mdc-list-base","mdc-list"],exportAs:["matActionList"],features:[mt([{provide:Lf,useExisting:t}]),P],ngContentSelectors:e,decls:1,vars:0,template:function(r,o){r&1&&(Vt(),le(0))},styles:[`.mdc-list {
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
`],encapsulation:2})})()}return t})();var Y_=(()=>{class t extends YI{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(e){this._activated=Dt(e)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=re(t)))(r||t)}})();static \u0275cmp=(function(){let e=["unscopedContent"],i=["text"],r=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]];return T({type:t,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(s,l,c){if(s&1&&Mi(c,UI,5)(c,zI,5)(c,$I,5),s&2){let d;ce(d=de())&&(l._lines=d),ce(d=de())&&(l._titles=d),ce(d=de())&&(l._meta=d)}},viewQuery:function(s,l){if(s&1&&en(e,5)(i,5),s&2){let c;ce(c=de())&&(l._unscopedContent=c.first),ce(c=de())&&(l._itemText=c.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(s,l){s&2&&(be("aria-current",l._getAriaCurrent()),te("mdc-list-item--activated",l.activated)("mdc-list-item--with-leading-avatar",l._avatars.length!==0)("mdc-list-item--with-leading-icon",l._icons.length!==0)("mdc-list-item--with-trailing-meta",l._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",l._hasBothLeadingAndTrailing())("_mat-animation-noopable",l._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[P],ngContentSelectors:["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"],decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(s,l){s&1&&(Vt(r),le(0),$(1,"span",1),le(2,1),le(3,2),$(4,"span",2,0),xe("cdkObserveContent",function(){return l._updateItemLines(!0)}),le(6,3),q()(),le(7,4),le(8,5),xn(9,"div",3))},dependencies:[fy],encapsulation:2})})()}return t})();var Z_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=I({type:t,selectors:[["","mat-subheader",""],["","matSubheader",""]],hostAttrs:[1,"mat-mdc-subheader","mdc-list-group__subheader"]})}return t})();var ZI=(t,n)=>n.action;function KI(t,n){if(t&1&&($(0,"h2",0),dt(1),q()),t&2){let e=Se();G(),ut(e.data.title)}}function QI(t,n){if(t&1&&($(0,"mat-dialog-content"),dt(1),q()),t&2){let e=Se();G(),ut(e.data.message)}}function XI(t,n){if(t&1&&($(0,"button",2),dt(1),q()),t&2){let e=n.$implicit;Xe("mat-dialog-close",e.action),G(),ut(e.label)}}function JI(t,n){if(t&1&&($(0,"h3",0),dt(1),q()),t&2){let e=Se();G(),ut(e.data.title)}}function ek(t,n){if(t&1){let e=wr();$(0,"button",2),xe("click",function(){let r=bi(e).$index,o=Se();return vi(o.sheet.dismiss(r))}),dt(1),q()}if(t&2){let e=n.$implicit;G(),ut(e)}}var tk=3,nk=-1,Pf=class t{data=m(Ff);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["hatter-dialog"]],decls:5,vars:2,consts:[["mat-dialog-title",""],["align","end"],["matButton","",3,"mat-dialog-close"]],template:function(e,i){e&1&&(fe(0,KI,2,1,"h2",0),fe(1,QI,2,1,"mat-dialog-content"),$(2,"mat-dialog-actions",1),dl(3,XI,2,2,"button",2,ZI),q()),e&2&&(pe(i.data.title?0:-1),G(),pe(i.data.message?1:-1),G(2),ul(i.data.buttons))},dependencies:[Ll,$_,j_,U_,z_],styles:["mat-dialog-content[_ngcontent-%COMP%]{white-space:pre-wrap}"]})},Vf=class t{data=m(Rf);sheet=m(zr);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["hatter-sheet"]],decls:4,vars:1,consts:[["matSubheader",""],["mat-list-item",""],["mat-list-item","",3,"click"]],template:function(e,i){e&1&&($(0,"mat-action-list"),fe(1,JI,2,1,"h3",0),dl(2,ek,2,1,"button",1,vm),q()),e&2&&(G(),pe(i.data.title?1:-1),G(),ul(i.data.items))},dependencies:[q_,Y_,Z_],encapsulation:2})},ac=class t{dialogs=m(oc);sheets=m(O_);open=0;dialog(n,e,i){let r=i.map((a,s)=>({label:a,action:s})).filter(({label:a})=>a).reverse(),o=this.dialogs.open(Pf,{data:{title:n,message:e,buttons:r},autoFocus:"dialog"});return this.answer(o.afterClosed(),tk)}sheet(n,e){let i=this.sheets.open(Vf,{data:{title:n,items:e},autoFocus:"dialog"});return this.answer(i.afterDismissed(),nk)}close(){return this.open?(this.dialogs.closeAll(),this.sheets.dismiss(),!0):!1}answer(n,e){return this.open++,new Promise(i=>n.subscribe(r=>{this.open--,i(r??e)}))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=X({token:t,factory:t.\u0275fac,providedIn:"root"})};var K_=2147483647,ik=new Set([2,5,11]),rk=new Set([3,10]),Q_=new Set([5,10,11]);function X_(t){return/^#[0-9a-fA-F]{8}$/.test(t)?`#${t.slice(3)}${t.slice(1,3)}`:t}var Ur=class t{appRef=m(lt);injector=m(Re);popups=m(ac);nodes=new Map;nextId=1;root;app;rendering=!1;rootElement;disk;callAudio=new Dl;callVideo=new El;constructor(){globalThis.hatter=this,matchMedia("(prefers-color-scheme: dark)").addEventListener("change",()=>this.redraw())}start(n){this.app=n,history.pushState(null,""),addEventListener("popstate",async()=>{this.popups.close()||await n.back()?history.pushState(null,""):history.back()})}redraw(){this.rendering||!this.app||(this.rendering=!0,requestAnimationFrame(()=>{this.rendering=!1,this.app?.render()}))}dialog(n,e,i,r,o,a){this.popups.dialog(e,i,[r,o,a]).then(s=>this.app?.dialog(n,s))}sheet(n,e,i){this.popups.sheet(e,i.split(`
`)).then(r=>this.app?.sheet(n,r))}pickFiles(n,e,i){this.disk&&qv(this.disk,n,e,i).then(async r=>{for(let{path:o,name:a,size:s}of r)await this.app?.picked(o,a,s)})}pickPicture(n,e){Yv(n,e).then(i=>i&&this.app?.picture(i))}saveFile(n,e,i){this.disk&&Zv(this.disk,n,e,i)}openFile(n){this.disk&&Kv(this.disk,n)}imageSize(n,e,i,r){let o=this.disk?.file(n),a=o&&Xv(o.data);return a?(e.setInt32(i,a[0],!0),e.setInt32(r,a[1],!0),!0):!1}callAudioStart(n,e){this.callAudio.start(n,e,i=>{this.app?.callFrame(i)})}callAudioStop(){this.callAudio.stop()}callAudioPlay(n,e,i){return this.callAudio.play(n,e,i)}callAudioLatency(){return this.callAudio.latency()}callAudioTone(n){this.callAudio.tone(n)}cameraStart(n){this.callVideo.startCamera(n,(e,i,r)=>{this.app?.cameraFrame(e,i,r)})}cameraStop(){this.callVideo.stopCamera()}showFrame(n,e,i,r){this.callVideo.show(n,e,i,r)}createNode(n){let e=v_[n];if(!e)return 0;let i=Er(e,{environmentInjector:this.injector});this.appRef.attachView(i.hostView),i.location.nativeElement.style.boxSizing="border-box";let r=this.nextId++,o={type:n,ref:i,layout:{width:NaN,height:NaN,weight:NaN,alignSelf:NaN,margin:NaN},handlers:new Map};return this.nodes.set(r,o),n===4&&(i.instance.onText=a=>{let s=o.handlers.get(1);s!==void 0&&this.app?.text(s,a)}),n===11&&(o.rows=new Map,o.spare=[]),Q_.has(n)&&i.location.nativeElement.addEventListener("scroll",()=>this.scrolled(o),{passive:!0}),r}setStrProp(n,e,i){let r=this.nodes.get(n);if(!r)return;let o=r.ref.instance,a=o.element.style;switch(e){case 0:o instanceof Hn?o.setText(i):o.text.set(i);break;case 1:a.color=X_(i);break;case 2:o instanceof Hn&&o.hint.set(i);break;case 3:a.backgroundColor=X_(i);break;case 4:o instanceof Cn&&o.showResource(i);break;case 5:{let s=this.disk?.file(i);s&&this.showBytes(r,s.data);break}case 7:o instanceof Cn&&this.callVideo.watch(i,o);break;case 6:o instanceof sa&&o.load(i);break;default:console.warn(`Hatter: no string property ${e} in browsers`)}}setNumProp(n,e,i){let r=this.nodes.get(n);if(!r)return;let o=r.ref.instance,a=o.element,s=a.style,l=c=>`${c}px`;switch(e){case 0:s.fontSize=l(i);break;case 24:s.fontWeight=i&1?"700":"",s.fontFamily=i&2?"monospace":"";break;case 1:s.padding=i?l(i):"";break;case 2:o instanceof Hn&&o.inputType.set(i);break;case 3:{let c=["","center","flex-end"][i]??"";r.type===2?s.alignItems=c:r.type===3?s.justifyContent=c:r.type!==5&&r.type!==9&&(s.textAlign=["","center","end"][i]??"");break}case 4:o instanceof Cn&&o.fit.set(["contain","cover","none"][i]??"contain");break;case 9:case 10:s.setProperty(e===9?"--hatter-x":"--hatter-y",l(i)),s.translate="var(--hatter-x, 0) var(--hatter-y, 0)";break;case 11:o instanceof Hn&&o.focus();break;case 12:s.pointerEvents=i===1?"none":"";break;case 13:case 14:case 15:case 16:case 17:{let c=["width","height","weight","alignSelf","margin"][e-13];r.layout[c]=i,this.applyLayout(r);break}case 18:s.maxWidth=i>=K_?"":l(i);break;case 19:s.borderRadius=i>0?l(i):"",s.overflow=i>0?"hidden":"";break;case 21:i>=K_?s.display=s.webkitLineClamp=s.webkitBoxOrient=s.overflow="":(s.display="-webkit-box",s.webkitLineClamp=String(i),s.webkitBoxOrient="vertical",s.overflow="hidden");break;case 20:i===1&&(r.type===5||r.type===11)&&this.keepAtEnd(r);break;case 22:r.type===11&&(r.count=i,queueMicrotask(()=>this.bindRows(n,r)));break;case 23:Q_.has(r.type)&&a.scrollTo({top:a.scrollHeight,behavior:"smooth"});break;default:console.warn(`Hatter: no number property ${e} in browsers`)}}setImageData(n,e){let i=this.nodes.get(n);i&&this.showBytes(i,e)}showBytes(n,e){n.ref.instance instanceof Cn&&(n.objectUrl&&URL.revokeObjectURL(n.objectUrl),n.objectUrl=URL.createObjectURL(new Blob([e])),n.ref.instance.showPicture(n.objectUrl))}setHandler(n,e,i){let r=this.nodes.get(n);if(!r)return;let o=r.ref.instance.element,a=!r.handlers.has(e);if(r.handlers.set(e,i),!!a)switch(e){case 0:r.type!==1&&(o.style.cursor="pointer"),o.addEventListener("click",s=>{s.stopPropagation(),this.app?.event(r.handlers.get(0))});break;case 2:o.addEventListener("contextmenu",s=>{s.preventDefault(),s.stopPropagation(),this.app?.event(r.handlers.get(2))});break;case 3:r.position=2;break}}addChild(n,e){let i=this.nodes.get(n),r=this.nodes.get(e);!i||!r||(r.parent=i,i.ref.instance.slot.appendChild(r.ref.instance.element),this.applyLayout(r))}removeChild(n,e){let i=this.nodes.get(e);i&&(i.ref.instance.element.remove(),i.parent=void 0)}destroyNode(n){let e=this.nodes.get(n);e&&(this.nodes.delete(n),e.objectUrl&&URL.revokeObjectURL(e.objectUrl),e.ref.instance instanceof Cn&&e.ref.instance.stopStream(),e.ref.instance.element.remove(),e.ref.destroy())}setRoot(n){let e=this.nodes.get(n);if(!e||!this.rootElement)return;this.root?.ref.instance.element.remove(),this.root=e,e.parent=void 0;let i=e.ref.instance.element.style;i.width=i.height="100%",this.rootElement.replaceChildren(e.ref.instance.element)}clear(){for(let n of[...this.nodes.keys()])this.destroyNode(n);this.root=void 0}darkMode(){return matchMedia("(prefers-color-scheme: dark)").matches}clock24(){let n=new Intl.DateTimeFormat(void 0,{hour:"numeric"}).resolvedOptions();return n.hourCycle==="h23"||n.hourCycle==="h24"}copyText(n){navigator.clipboard?.writeText(n)}openUri(n){window.open(n,"_blank","noopener")}applyLayout(n){let e=n.ref.instance.element.style,{width:i,height:r,weight:o,alignSelf:a,margin:s}=n.layout,l=n.parent?.type,c=u=>u===-1?"100%":`${u}px`;e.width=isNaN(i)?"":c(i),e.height=isNaN(r)?"":c(r),n.ref.instance.element.classList.toggle("sized",!isNaN(i)||!isNaN(r)),e.flexGrow=isNaN(o)?"":String(o),e.flexShrink=isNaN(o)?"":"1000",e.minWidth=e.minHeight=isNaN(o)?"":"0",e.margin=isNaN(s)?"":`${s}px`,e.alignSelf=e.justifySelf="",e.gridArea=l===9?"1 / 1":"";let d=["flex-start","center","flex-end","stretch"];if(l!==void 0&&ik.has(l))e.alignSelf=isNaN(a)?isNaN(i)?"stretch":"":d[a];else if(l!==void 0&&rk.has(l))e.alignSelf=isNaN(a)?"":d[a];else if(l===9){let u=isNaN(a)?"stretch":["start","center","end","stretch"][a];e.alignSelf=e.justifySelf=u}}keepAtEnd(n){if(n.pinned!==void 0)return;let e=n.ref.instance.element,i=n.ref.instance.slot;i.style.minHeight="100%",i.style.justifyContent="flex-end",n.pinned=!0,e.addEventListener("scroll",()=>{n.pinned=e.scrollTop+e.clientHeight>=e.scrollHeight-1},{passive:!0}),new ResizeObserver(()=>{n.pinned&&(e.scrollTop=e.scrollHeight)}).observe(i)}scrolled(n){let e=n.handlers.get(3);if(e===void 0)return;let i=n.ref.instance.element,r=i.scrollTop+i.clientHeight>=i.scrollHeight-1?2:i.scrollTop<=0?0:1;r!==n.position&&(n.position=r,this.app?.scroll(e,r))}bindRows(n,e){let i=this.app;if(!i||!this.nodes.has(n)||e.count===void 0||!e.rows||!e.spare)return;let r=Array.from({length:e.count},(c,d)=>i.key(n,d)),o=new Set(r),a=[...e.spare,...[...e.rows].filter(([c])=>!o.has(c)).map(([,c])=>c)],s=new Map,l=[];r.forEach((c,d)=>{let u=i.bind(n,e.rows.get(c)??a.pop()??0,d),p=this.nodes.get(u);p&&(s.set(c,u),p.parent=e,this.applyLayout(p),l.push(p.ref.instance.element))}),e.rows=s,e.spare=a,e.ref.instance.slot.replaceChildren(...l)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=X({token:t,factory:t.\u0275fac,providedIn:"root"})};var sc=class t{constructor(){m(Ur).rootElement=m(M).nativeElement}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["hatter-root"]],decls:0,vars:0,template:function(e,i){},styles:["[_nghost-%COMP%]{display:block;height:100%}"]})};var ma=class t{static read_bytes(n,e){let i=new t;return i.buf=n.getUint32(e,!0),i.buf_len=n.getUint32(e+4,!0),i}static read_bytes_array(n,e,i){let r=[];for(let o=0;o<i;o++)r.push(t.read_bytes(n,e+8*o));return r}},fa=class t{static read_bytes(n,e){let i=new t;return i.buf=n.getUint32(e,!0),i.buf_len=n.getUint32(e+4,!0),i}static read_bytes_array(n,e,i){let r=[];for(let o=0;o<i;o++)r.push(t.read_bytes(n,e+8*o));return r}},jf=0,Hf=1,uc=2;var zf=2,pt=3,Gr=4;var $r=class{head_length(){return 24}name_length(){return this.dir_name.byteLength}write_head_bytes(n,e){n.setBigUint64(e,this.d_next,!0),n.setBigUint64(e+8,this.d_ino,!0),n.setUint32(e+16,this.dir_name.length,!0),n.setUint8(e+20,this.d_type)}write_name_bytes(n,e,i){n.set(this.dir_name.slice(0,Math.min(this.dir_name.byteLength,i)),e)}constructor(n,e,i,r){let o=new TextEncoder().encode(i);this.d_next=n,this.d_ino=e,this.d_namlen=o.byteLength,this.d_type=r,this.dir_name=o}};var Uf=1;var Bi=class{write_bytes(n,e){n.setUint8(e,this.fs_filetype),n.setUint16(e+2,this.fs_flags,!0),n.setBigUint64(e+8,this.fs_rights_base,!0),n.setBigUint64(e+16,this.fs_rights_inherited,!0)}constructor(n,e){this.fs_rights_base=0n,this.fs_rights_inherited=0n,this.fs_filetype=n,this.fs_flags=e}};var mc=1,Wr=2,$f=4,fc=8,ji=class{write_bytes(n,e){n.setBigUint64(e,this.dev,!0),n.setBigUint64(e+8,this.ino,!0),n.setUint8(e+16,this.filetype),n.setBigUint64(e+24,this.nlink,!0),n.setBigUint64(e+32,this.size,!0),n.setBigUint64(e+38,this.atim,!0),n.setBigUint64(e+46,this.mtim,!0),n.setBigUint64(e+52,this.ctim,!0)}constructor(n,e,i){this.dev=0n,this.nlink=0n,this.atim=0n,this.mtim=0n,this.ctim=0n,this.ino=n,this.filetype=e,this.size=i}},J_=0;var ex=1,lc=class t{static read_bytes(n,e){return new t(n.getBigUint64(e,!0),n.getUint8(e+8),n.getUint32(e+16,!0),n.getBigUint64(e+24,!0),n.getUint16(e+36,!0))}constructor(n,e,i,r,o){this.userdata=n,this.eventtype=e,this.clockid=i,this.timeout=r,this.flags=o}},cc=class{write_bytes(n,e){n.setBigUint64(e,this.userdata,!0),n.setUint16(e+8,this.error,!0),n.setUint8(e+10,this.eventtype)}constructor(n,e,i){this.userdata=n,this.error=e,this.eventtype=i}};var ok=0,Bf=class{write_bytes(n,e){n.setUint32(e,this.pr_name.byteLength,!0)}constructor(n){this.pr_name=new TextEncoder().encode(n)}},dc=class t{static dir(n){let e=new t;return e.tag=ok,e.inner=new Bf(n),e}write_bytes(n,e){n.setUint32(e,this.tag,!0),this.inner.write_bytes(n,e+4)}};var ak=class{enable(n){this.log=sk(n===void 0?!0:n,this.prefix)}get enabled(){return this.isEnabled}constructor(n){this.isEnabled=n,this.prefix="wasi:",this.enable(n)}};function sk(t,n){return t?console.log.bind(console,"%c%s","color: #265BA0",n):()=>{}}var Pe=new ak(!1);var ha=class extends Error{constructor(n){super("exit with exit code "+n),this.code=n}},qf=class{start(n){this.inst=n;try{return n.exports._start(),0}catch(e){if(e instanceof ha)return e.code;throw e}}initialize(n){this.inst=n,n.exports._initialize&&n.exports._initialize()}constructor(n,e,i,r={}){this.args=[],this.env=[],this.fds=[],Pe.enable(r.debug),this.args=n,this.env=e,this.fds=i;let o=this;this.wasiImport={args_sizes_get(a,s){let l=new DataView(o.inst.exports.memory.buffer);l.setUint32(a,o.args.length,!0);let c=0;for(let d of o.args)c+=d.length+1;return l.setUint32(s,c,!0),Pe.log(l.getUint32(a,!0),l.getUint32(s,!0)),0},args_get(a,s){let l=new DataView(o.inst.exports.memory.buffer),c=new Uint8Array(o.inst.exports.memory.buffer),d=s;for(let u=0;u<o.args.length;u++){l.setUint32(a,s,!0),a+=4;let p=new TextEncoder().encode(o.args[u]);c.set(p,s),l.setUint8(s+p.length,0),s+=p.length+1}return Pe.enabled&&Pe.log(new TextDecoder("utf-8").decode(c.slice(d,s))),0},environ_sizes_get(a,s){let l=new DataView(o.inst.exports.memory.buffer);l.setUint32(a,o.env.length,!0);let c=0;for(let d of o.env)c+=new TextEncoder().encode(d).length+1;return l.setUint32(s,c,!0),Pe.log(l.getUint32(a,!0),l.getUint32(s,!0)),0},environ_get(a,s){let l=new DataView(o.inst.exports.memory.buffer),c=new Uint8Array(o.inst.exports.memory.buffer),d=s;for(let u=0;u<o.env.length;u++){l.setUint32(a,s,!0),a+=4;let p=new TextEncoder().encode(o.env[u]);c.set(p,s),l.setUint8(s+p.length,0),s+=p.length+1}return Pe.enabled&&Pe.log(new TextDecoder("utf-8").decode(c.slice(d,s))),0},clock_res_get(a,s){let l;switch(a){case 1:{l=5000n;break}case 0:{l=1000000n;break}default:return 52}return new DataView(o.inst.exports.memory.buffer).setBigUint64(s,l,!0),0},clock_time_get(a,s,l){let c=new DataView(o.inst.exports.memory.buffer);if(a===0)c.setBigUint64(l,BigInt(new Date().getTime())*1000000n,!0);else if(a==1){let d;try{d=BigInt(Math.round(performance.now()*1e6))}catch{d=0n}c.setBigUint64(l,d,!0)}else c.setBigUint64(l,0n,!0);return 0},fd_advise(a,s,l,c){return o.fds[a]!=null?0:8},fd_allocate(a,s,l){return o.fds[a]!=null?o.fds[a].fd_allocate(s,l):8},fd_close(a){if(o.fds[a]!=null){let s=o.fds[a].fd_close();return o.fds[a]=void 0,s}else return 8},fd_datasync(a){return o.fds[a]!=null?o.fds[a].fd_sync():8},fd_fdstat_get(a,s){if(o.fds[a]!=null){let{ret:l,fdstat:c}=o.fds[a].fd_fdstat_get();return c?.write_bytes(new DataView(o.inst.exports.memory.buffer),s),l}else return 8},fd_fdstat_set_flags(a,s){return o.fds[a]!=null?o.fds[a].fd_fdstat_set_flags(s):8},fd_fdstat_set_rights(a,s,l){return o.fds[a]!=null?o.fds[a].fd_fdstat_set_rights(s,l):8},fd_filestat_get(a,s){if(o.fds[a]!=null){let{ret:l,filestat:c}=o.fds[a].fd_filestat_get();return c?.write_bytes(new DataView(o.inst.exports.memory.buffer),s),l}else return 8},fd_filestat_set_size(a,s){return o.fds[a]!=null?o.fds[a].fd_filestat_set_size(s):8},fd_filestat_set_times(a,s,l,c){return o.fds[a]!=null?o.fds[a].fd_filestat_set_times(s,l,c):8},fd_pread(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),p=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=ma.read_bytes_array(u,s,l),h=0;for(let g of f){let{ret:y,data:_}=o.fds[a].fd_pread(g.buf_len,c);if(y!=0)return u.setUint32(d,h,!0),y;if(p.set(_,g.buf),h+=_.length,c+=BigInt(_.length),_.length!=g.buf_len)break}return u.setUint32(d,h,!0),0}else return 8},fd_prestat_get(a,s){let l=new DataView(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let{ret:c,prestat:d}=o.fds[a].fd_prestat_get();return d?.write_bytes(l,s),c}else return 8},fd_prestat_dir_name(a,s,l){if(o.fds[a]!=null){let{ret:c,prestat:d}=o.fds[a].fd_prestat_get();if(d==null)return c;let u=d.inner.pr_name;return new Uint8Array(o.inst.exports.memory.buffer).set(u.slice(0,l),s),u.byteLength>l?37:0}else return 8},fd_pwrite(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),p=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=fa.read_bytes_array(u,s,l),h=0;for(let g of f){let y=p.slice(g.buf,g.buf+g.buf_len),{ret:_,nwritten:B}=o.fds[a].fd_pwrite(y,c);if(_!=0)return u.setUint32(d,h,!0),_;if(h+=B,c+=BigInt(B),B!=y.byteLength)break}return u.setUint32(d,h,!0),0}else return 8},fd_read(a,s,l,c){let d=new DataView(o.inst.exports.memory.buffer),u=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let p=ma.read_bytes_array(d,s,l),f=0;for(let h of p){let{ret:g,data:y}=o.fds[a].fd_read(h.buf_len);if(g!=0)return d.setUint32(c,f,!0),g;if(u.set(y,h.buf),f+=y.length,y.length!=h.buf_len)break}return d.setUint32(c,f,!0),0}else return 8},fd_readdir(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),p=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=0;for(;;){let{ret:h,dirent:g}=o.fds[a].fd_readdir_single(c);if(h!=0)return u.setUint32(d,f,!0),h;if(g==null)break;if(l-f<g.head_length()){f=l;break}let y=new ArrayBuffer(g.head_length());if(g.write_head_bytes(new DataView(y),0),p.set(new Uint8Array(y).slice(0,Math.min(y.byteLength,l-f)),s),s+=g.head_length(),f+=g.head_length(),l-f<g.name_length()){f=l;break}g.write_name_bytes(p,s,l-f),s+=g.name_length(),f+=g.name_length(),c=g.d_next}return u.setUint32(d,f,!0),0}else return 8},fd_renumber(a,s){if(o.fds[a]!=null&&o.fds[s]!=null){let l=o.fds[s].fd_close();return l!=0?l:(o.fds[s]=o.fds[a],o.fds[a]=void 0,0)}else return 8},fd_seek(a,s,l,c){let d=new DataView(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let{ret:u,offset:p}=o.fds[a].fd_seek(s,l);return d.setBigInt64(c,p,!0),u}else return 8},fd_sync(a){return o.fds[a]!=null?o.fds[a].fd_sync():8},fd_tell(a,s){let l=new DataView(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let{ret:c,offset:d}=o.fds[a].fd_tell();return l.setBigUint64(s,d,!0),c}else return 8},fd_write(a,s,l,c){let d=new DataView(o.inst.exports.memory.buffer),u=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let p=fa.read_bytes_array(d,s,l),f=0;for(let h of p){let g=u.slice(h.buf,h.buf+h.buf_len),{ret:y,nwritten:_}=o.fds[a].fd_write(g);if(y!=0)return d.setUint32(c,f,!0),y;if(f+=_,_!=g.byteLength)break}return d.setUint32(c,f,!0),0}else return 8},path_create_directory(a,s,l){let c=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let d=new TextDecoder("utf-8").decode(c.slice(s,s+l));return o.fds[a].path_create_directory(d)}else return 8},path_filestat_get(a,s,l,c,d){let u=new DataView(o.inst.exports.memory.buffer),p=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let f=new TextDecoder("utf-8").decode(p.slice(l,l+c)),{ret:h,filestat:g}=o.fds[a].path_filestat_get(s,f);return g?.write_bytes(u,d),h}else return 8},path_filestat_set_times(a,s,l,c,d,u,p){let f=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let h=new TextDecoder("utf-8").decode(f.slice(l,l+c));return o.fds[a].path_filestat_set_times(s,h,d,u,p)}else return 8},path_link(a,s,l,c,d,u,p){let f=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null&&o.fds[d]!=null){let h=new TextDecoder("utf-8").decode(f.slice(l,l+c)),g=new TextDecoder("utf-8").decode(f.slice(u,u+p)),{ret:y,inode_obj:_}=o.fds[a].path_lookup(h,s);return _==null?y:o.fds[d].path_link(g,_,!1)}else return 8},path_open(a,s,l,c,d,u,p,f,h){let g=new DataView(o.inst.exports.memory.buffer),y=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let _=new TextDecoder("utf-8").decode(y.slice(l,l+c));Pe.log(_);let{ret:B,fd_obj:jt}=o.fds[a].path_open(s,_,d,u,p,f);if(B!=0)return B;o.fds.push(jt);let yc=o.fds.length-1;return g.setUint32(h,yc,!0),0}else return 8},path_readlink(a,s,l,c,d,u){let p=new DataView(o.inst.exports.memory.buffer),f=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let h=new TextDecoder("utf-8").decode(f.slice(s,s+l));Pe.log(h);let{ret:g,data:y}=o.fds[a].path_readlink(h);if(y!=null){let _=new TextEncoder().encode(y);if(_.length>d)return p.setUint32(u,0,!0),8;f.set(_,c),p.setUint32(u,_.length,!0)}return g}else return 8},path_remove_directory(a,s,l){let c=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let d=new TextDecoder("utf-8").decode(c.slice(s,s+l));return o.fds[a].path_remove_directory(d)}else return 8},path_rename(a,s,l,c,d,u){let p=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null&&o.fds[c]!=null){let f=new TextDecoder("utf-8").decode(p.slice(s,s+l)),h=new TextDecoder("utf-8").decode(p.slice(d,d+u)),{ret:g,inode_obj:y}=o.fds[a].path_unlink(f);if(y==null)return g;if(g=o.fds[c].path_link(h,y,!0),g!=0&&o.fds[a].path_link(f,y,!0)!=0)throw"path_link should always return success when relinking an inode back to the original place";return g}else return 8},path_symlink(a,s,l,c,d){let u=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[l]!=null){let p=new TextDecoder("utf-8").decode(u.slice(a,a+s)),f=new TextDecoder("utf-8").decode(u.slice(c,c+d));return 58}else return 8},path_unlink_file(a,s,l){let c=new Uint8Array(o.inst.exports.memory.buffer);if(o.fds[a]!=null){let d=new TextDecoder("utf-8").decode(c.slice(s,s+l));return o.fds[a].path_unlink_file(d)}else return 8},poll_oneoff(a,s,l){if(l===0)return 28;if(l>1)return Pe.log("poll_oneoff: only a single subscription is supported"),58;let c=new DataView(o.inst.exports.memory.buffer),d=lc.read_bytes(c,a),u=d.eventtype,p=d.clockid,f=d.timeout;if(u!==J_)return Pe.log("poll_oneoff: only clock subscriptions are supported"),58;let h;if(p===1)h=()=>BigInt(Math.round(performance.now()*1e6));else if(p===0)h=()=>BigInt(new Date().getTime())*1000000n;else return 28;let g=(d.flags&ex)!==0?f:h()+f;for(;g>h(););return new cc(d.userdata,0,u).write_bytes(c,s),0},proc_exit(a){throw new ha(a)},proc_raise(a){throw"raised signal "+a},sched_yield(){},random_get(a,s){let l=new Uint8Array(o.inst.exports.memory.buffer).subarray(a,a+s);if("crypto"in globalThis&&(typeof SharedArrayBuffer>"u"||!(o.inst.exports.memory.buffer instanceof SharedArrayBuffer)))for(let c=0;c<s;c+=65536)crypto.getRandomValues(l.subarray(c,c+65536));else for(let c=0;c<s;c++)l[c]=Math.random()*256|0},sock_recv(a,s,l){throw"sockets not supported"},sock_send(a,s,l){throw"sockets not supported"},sock_shutdown(a,s){throw"sockets not supported"},sock_accept(a,s){throw"sockets not supported"}}}};var $n=class{fd_allocate(n,e){return 58}fd_close(){return 0}fd_fdstat_get(){return{ret:58,fdstat:null}}fd_fdstat_set_flags(n){return 58}fd_fdstat_set_rights(n,e){return 58}fd_filestat_get(){return{ret:58,filestat:null}}fd_filestat_set_size(n){return 58}fd_filestat_set_times(n,e,i){return 58}fd_pread(n,e){return{ret:58,data:new Uint8Array}}fd_prestat_get(){return{ret:58,prestat:null}}fd_pwrite(n,e){return{ret:58,nwritten:0}}fd_read(n){return{ret:58,data:new Uint8Array}}fd_readdir_single(n){return{ret:58,dirent:null}}fd_seek(n,e){return{ret:58,offset:0n}}fd_sync(){return 0}fd_tell(){return{ret:58,offset:0n}}fd_write(n){return{ret:58,nwritten:0}}path_create_directory(n){return 58}path_filestat_get(n,e){return{ret:58,filestat:null}}path_filestat_set_times(n,e,i,r,o){return 58}path_link(n,e,i){return 58}path_unlink(n){return{ret:58,inode_obj:null}}path_lookup(n,e){return{ret:58,inode_obj:null}}path_open(n,e,i,r,o,a){return{ret:54,fd_obj:null}}path_readlink(n){return{ret:58,data:null}}path_remove_directory(n){return 58}path_rename(n,e,i){return 58}path_unlink_file(n){return 58}},rn=class t{static issue_ino(){return t.next_ino++}static root_ino(){return 0n}constructor(){this.ino=t.issue_ino()}};rn.next_ino=1n;var Wn=class extends $n{fd_allocate(n,e){if(!(this.file.size>n+e)){let i=new Uint8Array(Number(n+e));i.set(this.file.data,0),this.file.data=i}return 0}fd_fdstat_get(){return{ret:0,fdstat:new Bi(Gr,0)}}fd_filestat_set_size(n){if(this.file.size>n)this.file.data=new Uint8Array(this.file.data.buffer.slice(0,Number(n)));else{let e=new Uint8Array(Number(n));e.set(this.file.data,0),this.file.data=e}return 0}fd_read(n){let e=this.file.data.slice(Number(this.file_pos),Number(this.file_pos+BigInt(n)));return this.file_pos+=BigInt(e.length),{ret:0,data:e}}fd_pread(n,e){return{ret:0,data:this.file.data.slice(Number(e),Number(e+BigInt(n)))}}fd_seek(n,e){let i;switch(e){case jf:i=n;break;case Hf:i=this.file_pos+n;break;case uc:i=BigInt(this.file.data.byteLength)+n;break;default:return{ret:28,offset:0n}}return i<0?{ret:28,offset:0n}:(this.file_pos=i,{ret:0,offset:this.file_pos})}fd_tell(){return{ret:0,offset:this.file_pos}}fd_write(n){if(this.file.readonly)return{ret:8,nwritten:0};if(this.file_pos+BigInt(n.byteLength)>this.file.size){let e=this.file.data;this.file.data=new Uint8Array(Number(this.file_pos+BigInt(n.byteLength))),this.file.data.set(e)}return this.file.data.set(n,Number(this.file_pos)),this.file_pos+=BigInt(n.byteLength),{ret:0,nwritten:n.byteLength}}fd_pwrite(n,e){if(this.file.readonly)return{ret:8,nwritten:0};if(e+BigInt(n.byteLength)>this.file.size){let i=this.file.data;this.file.data=new Uint8Array(Number(e+BigInt(n.byteLength))),this.file.data.set(i)}return this.file.data.set(n,Number(e)),{ret:0,nwritten:n.byteLength}}fd_filestat_get(){return{ret:0,filestat:this.file.stat()}}constructor(n){super(),this.file_pos=0n,this.file=n}},ga=class extends $n{fd_seek(n,e){return{ret:8,offset:0n}}fd_tell(){return{ret:8,offset:0n}}fd_allocate(n,e){return 8}fd_fdstat_get(){return{ret:0,fdstat:new Bi(pt,0)}}fd_readdir_single(n){if(Pe.enabled&&(Pe.log("readdir_single",n),Pe.log(n,this.dir.contents.keys())),n==0n)return{ret:0,dirent:new $r(1n,this.dir.ino,".",pt)};if(n==1n)return{ret:0,dirent:new $r(2n,this.dir.parent_ino(),"..",pt)};if(n>=BigInt(this.dir.contents.size)+2n)return{ret:0,dirent:null};let[e,i]=Array.from(this.dir.contents.entries())[Number(n-2n)];return{ret:0,dirent:new $r(n+1n,i.ino,e,i.stat().filetype)}}path_filestat_get(n,e){let{ret:i,path:r}=Gn.from(e);if(r==null)return{ret:i,filestat:null};let{ret:o,entry:a}=this.dir.get_entry_for_path(r);return a==null?{ret:o,filestat:null}:{ret:0,filestat:a.stat()}}path_lookup(n,e){let{ret:i,path:r}=Gn.from(n);if(r==null)return{ret:i,inode_obj:null};let{ret:o,entry:a}=this.dir.get_entry_for_path(r);return a==null?{ret:o,inode_obj:null}:{ret:0,inode_obj:a}}path_open(n,e,i,r,o,a){let{ret:s,path:l}=Gn.from(e);if(l==null)return{ret:s,fd_obj:null};let{ret:c,entry:d}=this.dir.get_entry_for_path(l);if(d==null){if(c!=44)return{ret:c,fd_obj:null};if((i&mc)==mc){let{ret:u,entry:p}=this.dir.create_entry_for_path(e,(i&Wr)==Wr);if(p==null)return{ret:u,fd_obj:null};d=p}else return{ret:44,fd_obj:null}}else if((i&$f)==$f)return{ret:20,fd_obj:null};return(i&Wr)==Wr&&d.stat().filetype!==pt?{ret:54,fd_obj:null}:d.path_open(i,r,a)}path_create_directory(n){return this.path_open(0,n,mc|Wr,0n,0n,0).ret}path_link(n,e,i){let{ret:r,path:o}=Gn.from(n);if(o==null)return r;if(o.is_dir)return 44;let{ret:a,parent_entry:s,filename:l,entry:c}=this.dir.get_parent_dir_and_entry_for_path(o,!0);if(s==null||l==null)return a;if(c!=null){let d=e.stat().filetype==pt,u=c.stat().filetype==pt;if(d&&u)if(i&&c instanceof tt){if(c.contents.size!=0)return 55}else return 20;else{if(d&&!u)return 54;if(!d&&u)return 31;if(!(e.stat().filetype==Gr&&c.stat().filetype==Gr))return 20}}return!i&&e.stat().filetype==pt?63:(s.contents.set(l,e),0)}path_unlink(n){let{ret:e,path:i}=Gn.from(n);if(i==null)return{ret:e,inode_obj:null};let{ret:r,parent_entry:o,filename:a,entry:s}=this.dir.get_parent_dir_and_entry_for_path(i,!0);return o==null||a==null?{ret:r,inode_obj:null}:s==null?{ret:44,inode_obj:null}:(o.contents.delete(a),{ret:0,inode_obj:s})}path_unlink_file(n){let{ret:e,path:i}=Gn.from(n);if(i==null)return e;let{ret:r,parent_entry:o,filename:a,entry:s}=this.dir.get_parent_dir_and_entry_for_path(i,!1);return o==null||a==null||s==null?r:s.stat().filetype===pt?31:(o.contents.delete(a),0)}path_remove_directory(n){let{ret:e,path:i}=Gn.from(n);if(i==null)return e;let{ret:r,parent_entry:o,filename:a,entry:s}=this.dir.get_parent_dir_and_entry_for_path(i,!1);return o==null||a==null||s==null?r:!(s instanceof tt)||s.stat().filetype!==pt?54:s.contents.size!==0?55:o.contents.delete(a)?0:44}fd_filestat_get(){return{ret:0,filestat:this.dir.stat()}}fd_filestat_set_size(n){return 8}fd_read(n){return{ret:8,data:new Uint8Array}}fd_pread(n,e){return{ret:8,data:new Uint8Array}}fd_write(n){return{ret:8,nwritten:0}}fd_pwrite(n,e){return{ret:8,nwritten:0}}constructor(n){super(),this.dir=n}},qr=class extends ga{fd_prestat_get(){return{ret:0,prestat:dc.dir(this.prestat_name)}}constructor(n,e){super(new tt(e)),this.prestat_name=n}},It=class extends rn{path_open(n,e,i){if(this.readonly&&(e&BigInt(64))==BigInt(64))return{ret:63,fd_obj:null};if((n&fc)==fc){if(this.readonly)return{ret:63,fd_obj:null};this.data=new Uint8Array([])}let r=new Wn(this);return i&Uf&&r.fd_seek(0n,uc),{ret:0,fd_obj:r}}get size(){return BigInt(this.data.byteLength)}stat(){return new ji(this.ino,Gr,this.size)}constructor(n,e){super(),this.data=new Uint8Array(n),this.readonly=!!e?.readonly}},Gn=class rx{static from(n){let e=new rx;if(e.is_dir=n.endsWith("/"),n.startsWith("/"))return{ret:76,path:null};if(n.includes("\0"))return{ret:28,path:null};for(let i of n.split("/"))if(!(i===""||i===".")){if(i===".."){if(e.parts.pop()==null)return{ret:76,path:null};continue}e.parts.push(i)}return{ret:0,path:e}}to_path_string(){let n=this.parts.join("/");return this.is_dir&&(n+="/"),n}constructor(){this.parts=[],this.is_dir=!1}},tt=class t extends rn{parent_ino(){return this.parent==null?rn.root_ino():this.parent.ino}path_open(n,e,i){return{ret:0,fd_obj:new ga(this)}}stat(){return new ji(this.ino,pt,0n)}get_entry_for_path(n){let e=this;for(let i of n.parts){if(!(e instanceof t))return{ret:54,entry:null};let r=e.contents.get(i);if(r!==void 0)e=r;else return Pe.log(i),{ret:44,entry:null}}return n.is_dir&&e.stat().filetype!=pt?{ret:54,entry:null}:{ret:0,entry:e}}get_parent_dir_and_entry_for_path(n,e){let i=n.parts.pop();if(i===void 0)return{ret:28,parent_entry:null,filename:null,entry:null};let{ret:r,entry:o}=this.get_entry_for_path(n);if(o==null)return{ret:r,parent_entry:null,filename:null,entry:null};if(!(o instanceof t))return{ret:54,parent_entry:null,filename:null,entry:null};let a=o.contents.get(i);return a===void 0?e?{ret:0,parent_entry:o,filename:i,entry:null}:{ret:44,parent_entry:null,filename:null,entry:null}:n.is_dir&&a.stat().filetype!=pt?{ret:54,parent_entry:null,filename:null,entry:null}:{ret:0,parent_entry:o,filename:i,entry:a}}create_entry_for_path(n,e){let{ret:i,path:r}=Gn.from(n);if(r==null)return{ret:i,entry:null};let{ret:o,parent_entry:a,filename:s,entry:l}=this.get_parent_dir_and_entry_for_path(r,!0);if(a==null||s==null)return{ret:o,entry:null};if(l!=null)return{ret:20,entry:null};Pe.log("create",r);let c;return e?c=new t(new Map):c=new It(new ArrayBuffer(0)),a.contents.set(s,c),l=c,{ret:0,entry:l}}constructor(n){super(),this.parent=null,n instanceof Array?this.contents=new Map(n):this.contents=n;for(let e of this.contents.values())e instanceof t&&(e.parent=this)}},Yr=class t extends $n{fd_filestat_get(){return{ret:0,filestat:new ji(this.ino,zf,BigInt(0))}}fd_fdstat_get(){let n=new Bi(zf,0);return n.fs_rights_base=BigInt(64),{ret:0,fdstat:n}}fd_write(n){return this.write(n),{ret:0,nwritten:n.byteLength}}static lineBuffered(n){let e=new TextDecoder("utf-8",{fatal:!1}),i="";return new t(r=>{i+=e.decode(r,{stream:!0});let o=i.split(`
`);for(let[a,s]of o.entries())a<o.length-1?n(s):i=s})}constructor(n){super(),this.ino=rn.issue_ino(),this.write=n}};var bc=class{constructor(n,e){this.roots=n;this.changed=e}roots;changed;file(n){let[e,i]=this.place(n)??[],r=i===void 0?void 0:e?.get(i);return r instanceof It?r:void 0}add(n,e){let[i,r]=this.place(n,!0)??[];if(!i||r===void 0)throw new Error(`No directory for ${n}`);i.set(r,new It(e)),this.changed()}remove(n){let[e,i]=this.place(n)??[];i!==void 0&&e?.delete(i)&&this.changed()}place(n,e=!1){for(let[i,r]of this.roots){if(!n.startsWith(i+"/"))continue;let o=n.slice(i.length+1).split("/"),a=o.pop(),s=r;for(let l of o){let c=s.get(l);if(!(c instanceof tt)&&e&&(c=new tt(new Map),s.set(l,c)),!(c instanceof tt))return;s=c.contents}return[s,a]}}};var dk=1e3,uk=["fd_write","fd_pwrite","fd_allocate","fd_filestat_set_size"],mk=["path_open","path_unlink_file","path_remove_directory","path_rename","path_create_directory","path_link","path_symlink"],vc=class t{constructor(n){this.dir=n}dir;root=new Map;changed=new Set;saved=new Map;next=0;timer;saving=Promise.resolve();static async open(n){let i=await(await navigator.storage.getDirectory()).getDirectoryHandle("hatter",{create:!0}),r=new t(await i.getDirectoryHandle(n,{create:!0}));return await r.load(),navigator.storage.persist?.(),r}watch(n){let e=n.wasiImport;for(let i of uk){let r=e[i];e[i]=(...o)=>{let a=n.fds[o[0]];return a instanceof Wn&&this.changed.add(a.file),this.saveSoon(),r(...o)}}for(let i of mk){let r=e[i];e[i]=(...o)=>(this.saveSoon(),r(...o))}addEventListener("visibilitychange",()=>{document.visibilityState==="hidden"&&this.now()}),addEventListener("pagehide",()=>this.now())}saveSoon(){this.timer??=setTimeout(()=>this.now(),dk)}now(){clearTimeout(this.timer),this.timer=void 0,this.saving=this.saving.then(()=>this.save()).catch(n=>console.error("Saving files failed:",n))}async load(){let n=await this.readManifest();if(n){this.next=n.next;for(let e of n.dirs)this.directory(e.split("/"));for(let[e,i]of Object.entries(n.files)){let r=await this.dir.getFileHandle(i),o=new Uint8Array(await(await r.getFile()).arrayBuffer()),a=e.split("/"),s=new It(o);this.directory(a.slice(0,-1)).set(a[a.length-1],s),this.saved.set(e,{file:s,size:o.length,blob:i})}}}async readManifest(){try{let n=await this.dir.getFileHandle("manifest.json");return JSON.parse(await(await n.getFile()).text())}catch{return}}directory(n){let e=this.root;for(let i of n){let r=e.get(i);r instanceof tt||(r=new tt(new Map),e.set(i,r)),e=r.contents}return e}walk(){let n=new Map,e=[],i=(r,o)=>{for(let[a,s]of r){let l=o+a;s instanceof It?n.set(l,s):s instanceof tt&&(e.push(l),i(s.contents,l+"/"))}};return i(this.root,""),{files:n,dirs:e}}async save(){let{files:n,dirs:e}=this.walk(),i=[];for(let[l,c]of n){let d=this.saved.get(l);(!d||d.file!==c||d.size!==c.data.length||this.changed.has(c))&&i.push([l,c,c.data.slice()])}this.changed.clear();let r=new Set(i.map(([l])=>l)),o=new Map;for(let l of n.keys()){let c=this.saved.get(l);c&&!r.has(l)&&o.set(l,c)}for(let[l,c,d]of i){let u=String(this.next++);await this.write(u,d),o.set(l,{file:c,size:d.length,blob:u})}let a={next:this.next,files:Object.fromEntries([...o].map(([l,{blob:c}])=>[l,c])),dirs:e};await this.write("manifest.json",new TextEncoder().encode(JSON.stringify(a)));let s=new Set(Object.values(a.files));for(let{blob:l}of this.saved.values())s.has(l)||await this.dir.removeEntry(l).catch(()=>{});this.saved=o}async write(n,e){let r=await(await this.dir.getFileHandle(n,{create:!0})).createWritable();await r.write(e),await r.close()}};function fk(){return new URL(document.baseURI).pathname.split("/").filter(Boolean).pop()??"app"}function pk(t){return new Promise(n=>{navigator.locks.request(`hatter:${t}`,{ifAvailable:!0},e=>(n(e!==null),e===null?void 0:new Promise(()=>{})))})}async function ox(t){let n=fk();if(!await pk(n)){let l=document.querySelector("hatter-root");l&&(l.textContent="This is open in another tab already. Use that one, or close it and reload this one.");return}let e=await vc.open(n),i=new Map;t.injector.get(Ur).disk=new bc(new Map([["/data",e.root],[Nl,i]]),()=>e.saveSoon());let r=new qf([],[],[new Wn(new It([])),Yr.lineBuffered(l=>console.log(l)),Yr.lineBuffered(l=>console.warn(l)),new qr("/data",e.root),new qr(Nl,i)],{debug:!1});e.watch(r);let o=await import(new URL("ghc_wasm_jsffi.js",document.baseURI).href),a={},{instance:s}=await WebAssembly.instantiateStreaming(fetch("app.wasm"),{wasi_snapshot_preview1:r.wasiImport,ghc_wasm_jsffi:o.default(a)});Object.assign(a,s.exports),r.initialize(s),a.hs_init(0,0),await a.start()}Vm(sc).then(ox).catch(t=>console.error(t));
