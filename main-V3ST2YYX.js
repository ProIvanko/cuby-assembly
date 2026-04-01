var tC=Object.defineProperty,nC=Object.defineProperties;var iC=Object.getOwnPropertyDescriptors;var j0=Object.getOwnPropertySymbols;var rC=Object.prototype.hasOwnProperty,oC=Object.prototype.propertyIsEnumerable;var W0=(n,e,t)=>e in n?tC(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,ae=(n,e)=>{for(var t in e||={})rC.call(e,t)&&W0(n,t,e[t]);if(j0)for(var t of j0(e))oC.call(e,t)&&W0(n,t,e[t]);return n},it=(n,e)=>nC(n,iC(e));var mn=null,Ul=!1,Rp=1,sC=null,Sn=Symbol("SIGNAL");function Ne(n){let e=mn;return mn=n,e}function zl(){return mn}var xo={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Es(n){if(Ul)throw new Error("");if(mn===null)return;mn.consumerOnSignalRead(n);let e=mn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=mn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:mn.producers,t!==void 0&&t.producer===n)){mn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===mn&&(!i||cC(r,mn)))return;let o=Ms(mn),s={producer:n,consumer:mn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};mn.producersTail=s,e!==void 0?e.nextProducer=s:mn.producers=s,o&&Y0(n,s)}function $0(){Rp++}function Gl(n){if(!(Ms(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Rp)){if(!n.producerMustRecompute(n)&&!Wa(n)){Hl(n);return}n.producerRecomputeValue(n),Hl(n)}}function Np(n){if(n.consumers===void 0)return;let e=Ul;Ul=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||aC(i)}}finally{Ul=e}}function Pp(){return mn?.consumerAllowSignalWrites!==!1}function aC(n){n.dirty=!0,Np(n),n.consumerMarkedDirty?.(n)}function Hl(n){n.dirty=!1,n.lastCleanEpoch=Rp}function Eo(n){return n&&q0(n),Ne(n)}function q0(n){n.producersTail=void 0,n.recomputing=!0}function bs(n,e){Ne(e),n&&X0(n)}function X0(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(Ms(n))do t=Op(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Wa(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(Gl(t),i!==t.version))return!0}return!1}function bo(n){if(Ms(n)){let e=n.producers;for(;e!==void 0;)e=Op(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Y0(n,e){let t=n.consumersTail,i=Ms(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Y0(r.producer,r)}function Op(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!Ms(e)){let o=e.producers;for(;o!==void 0;)o=Op(o)}return t}function Ms(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function jl(n){sC?.(n)}function cC(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function Wl(n,e){return Object.is(n,e)}function $l(n,e){let t=Object.create(lC);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(Gl(t),Es(t),t.value===ja)throw t.error;return t.value};return i[Sn]=t,jl(t),i}var Bl=Symbol("UNSET"),Vl=Symbol("COMPUTING"),ja=Symbol("ERRORED"),lC=it(ae({},xo),{value:Bl,dirty:!0,error:null,equal:Wl,kind:"computed",producerMustRecompute(n){return n.value===Bl||n.value===Vl},producerRecomputeValue(n){if(n.value===Vl)throw new Error("");let e=n.value;n.value=Vl;let t=Eo(n),i,r=!1;try{i=n.computation(),Ne(null),r=e!==Bl&&e!==ja&&i!==ja&&n.equal(e,i)}catch(o){i=ja,n.error=o}finally{bs(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function uC(){throw new Error}var Z0=uC;function J0(n){Z0(n)}function Lp(n){Z0=n}var dC=null;function Fp(n,e){let t=Object.create(ql);t.value=n,e!==void 0&&(t.equal=e);let i=()=>K0(t);return i[Sn]=t,jl(t),[i,s=>Ss(t,s),s=>kp(t,s)]}function K0(n){return Es(n),n.value}function Ss(n,e){Pp()||J0(n),n.equal(n.value,e)||(n.value=e,fC(n))}function kp(n,e){Pp()||J0(n),Ss(n,e(n.value))}var ql=it(ae({},xo),{equal:Wl,value:void 0,kind:"signal"});function fC(n){n.version++,$0(),Np(n),dC?.(n)}var Up=it(ae({},xo),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Bp(n){if(n.dirty=!1,n.version>0&&!Wa(n))return;n.version++;let e=Eo(n);try{n.cleanup(),n.fn()}finally{bs(n,e)}}function Ge(n){return typeof n=="function"}function ws(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Xl=ws(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function $a(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var cn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Ge(i))try{i()}catch(o){e=o instanceof Xl?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Q0(o)}catch(s){e=e??[],s instanceof Xl?e=[...e,...s.errors]:e.push(s)}}if(e)throw new Xl(e)}}add(e){var t;if(e&&e!==this)if(this.closed)Q0(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&$a(t,e)}remove(e){let{_finalizers:t}=this;t&&$a(t,e),e instanceof n&&e._removeParent(this)}};cn.EMPTY=(()=>{let n=new cn;return n.closed=!0,n})();var Vp=cn.EMPTY;function Yl(n){return n instanceof cn||n&&"closed"in n&&Ge(n.remove)&&Ge(n.add)&&Ge(n.unsubscribe)}function Q0(n){Ge(n)?n():n.unsubscribe()}var pi={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Cs={setTimeout(n,e,...t){let{delegate:i}=Cs;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Cs;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Zl(n){Cs.setTimeout(()=>{let{onUnhandledError:e}=pi;if(e)e(n);else throw n})}function Mo(){}var e_=Hp("C",void 0,void 0);function t_(n){return Hp("E",void 0,n)}function n_(n){return Hp("N",n,void 0)}function Hp(n,e,t){return{kind:n,value:e,error:t}}var So=null;function Ts(n){if(pi.useDeprecatedSynchronousErrorHandling){let e=!So;if(e&&(So={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=So;if(So=null,t)throw i}}else n()}function i_(n){pi.useDeprecatedSynchronousErrorHandling&&So&&(So.errorThrown=!0,So.error=n)}var wo=class extends cn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Yl(e)&&e.add(this)):this.destination=mC}static create(e,t,i){return new Ds(e,t,i)}next(e){this.isStopped?Gp(n_(e),this):this._next(e)}error(e){this.isStopped?Gp(t_(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Gp(e_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},hC=Function.prototype.bind;function zp(n,e){return hC.call(n,e)}var jp=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Jl(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Jl(i)}else Jl(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Jl(t)}}},Ds=class extends wo{constructor(e,t,i){super();let r;if(Ge(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let o;this&&pi.useDeprecatedNextContext?(o=Object.create(e),o.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&zp(e.next,o),error:e.error&&zp(e.error,o),complete:e.complete&&zp(e.complete,o)}):r=e}this.destination=new jp(r)}};function Jl(n){pi.useDeprecatedSynchronousErrorHandling?i_(n):Zl(n)}function pC(n){throw n}function Gp(n,e){let{onStoppedNotification:t}=pi;t&&Cs.setTimeout(()=>t(n,e))}var mC={closed:!0,next:Mo,error:pC,complete:Mo};var Is=typeof Symbol=="function"&&Symbol.observable||"@@observable";function mi(n){return n}function Wp(...n){return $p(n)}function $p(n){return n.length===0?mi:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var rt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let o=vC(t)?t:new Ds(t,i,r);return Ts(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=r_(i),new i((r,o)=>{let s=new Ds({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Is](){return this}pipe(...t){return $p(t)(this)}toPromise(t){return t=r_(t),new t((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return n.create=e=>new n(e),n})();function r_(n){var e;return(e=n??pi.Promise)!==null&&e!==void 0?e:Promise}function gC(n){return n&&Ge(n.next)&&Ge(n.error)&&Ge(n.complete)}function vC(n){return n&&n instanceof wo||gC(n)&&Yl(n)}function yC(n){return Ge(n?.lift)}function vt(n){return e=>{if(yC(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Ct(n,e,t,i,r){return new qp(n,e,t,i,r)}var qp=class extends wo{constructor(e,t,i,r,o,s){super(e),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var o_=ws(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Ht=(()=>{class n extends rt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Kl(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new o_}next(t){Ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Vp:(this.currentObservers=null,o.push(t),new cn(()=>{this.currentObservers=null,$a(o,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:o}=this;i?t.error(r):o&&t.complete()}asObservable(){let t=new rt;return t.source=this,t}}return n.create=(e,t)=>new Kl(e,t),n})(),Kl=class extends Ht{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Vp}};var ln=class extends Ht{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var un=new rt(n=>n.complete());function s_(n){return n&&Ge(n.schedule)}function a_(n){return n[n.length-1]}function c_(n){return Ge(a_(n))?n.pop():void 0}function Pr(n){return s_(a_(n))?n.pop():void 0}function u_(n,e,t,i){function r(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(u){try{l(i.next(u))}catch(d){s(d)}}function c(u){try{l(i.throw(u))}catch(d){s(d)}}function l(u){u.done?o(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function l_(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Co(n){return this instanceof Co?(this.v=n,this):new Co(n)}function d_(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(h){return function(p){return Promise.resolve(p).then(h,d)}}function a(h,p){i[h]&&(r[h]=function(y){return new Promise(function(g,m){o.push([h,y,g,m])>1||c(h,y)})},p&&(r[h]=p(r[h])))}function c(h,p){try{l(i[h](p))}catch(y){f(o[0][3],y)}}function l(h){h.value instanceof Co?Promise.resolve(h.value.v).then(u,d):f(o[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,p){h(p),o.shift(),o.length&&c(o[0][0],o[0][1])}}function f_(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof l_=="function"?l_(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(o){t[o]=n[o]&&function(s){return new Promise(function(a,c){s=n[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Ql=n=>n&&typeof n.length=="number"&&typeof n!="function";function eu(n){return Ge(n?.then)}function tu(n){return Ge(n[Is])}function nu(n){return Symbol.asyncIterator&&Ge(n?.[Symbol.asyncIterator])}function iu(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function _C(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ru=_C();function ou(n){return Ge(n?.[ru])}function su(n){return d_(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Co(t.read());if(r)return yield Co(void 0);yield yield Co(i)}}finally{t.releaseLock()}})}function au(n){return Ge(n?.getReader)}function nn(n){if(n instanceof rt)return n;if(n!=null){if(tu(n))return xC(n);if(Ql(n))return EC(n);if(eu(n))return bC(n);if(nu(n))return h_(n);if(ou(n))return MC(n);if(au(n))return SC(n)}throw iu(n)}function xC(n){return new rt(e=>{let t=n[Is]();if(Ge(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function EC(n){return new rt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function bC(n){return new rt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,Zl)})}function MC(n){return new rt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function h_(n){return new rt(e=>{wC(n,e).catch(t=>e.error(t))})}function SC(n){return h_(su(n))}function wC(n,e){var t,i,r,o;return u_(this,void 0,void 0,function*(){try{for(t=f_(n);i=yield t.next(),!i.done;){let s=i.value;if(e.next(s),e.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=t.return)&&(yield o.call(t))}finally{if(r)throw r.error}}e.complete()})}function Un(n,e,t,i=0,r=!1){let o=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(o),!r)return o}function cu(n,e=0){return vt((t,i)=>{t.subscribe(Ct(i,r=>Un(i,n,()=>i.next(r),e),()=>Un(i,n,()=>i.complete(),e),r=>Un(i,n,()=>i.error(r),e)))})}function lu(n,e=0){return vt((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function p_(n,e){return nn(n).pipe(lu(e),cu(e))}function m_(n,e){return nn(n).pipe(lu(e),cu(e))}function g_(n,e){return new rt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function v_(n,e){return new rt(t=>{let i;return Un(t,e,()=>{i=n[ru](),Un(t,e,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){t.error(s);return}o?t.complete():t.next(r)},0,!0)}),()=>Ge(i?.return)&&i.return()})}function uu(n,e){if(!n)throw new Error("Iterable cannot be null");return new rt(t=>{Un(t,e,()=>{let i=n[Symbol.asyncIterator]();Un(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function y_(n,e){return uu(su(n),e)}function __(n,e){if(n!=null){if(tu(n))return p_(n,e);if(Ql(n))return g_(n,e);if(eu(n))return m_(n,e);if(nu(n))return uu(n,e);if(ou(n))return v_(n,e);if(au(n))return y_(n,e)}throw iu(n)}function Wt(n,e){return e?__(n,e):nn(n)}function st(...n){let e=Pr(n);return Wt(n,e)}function Xp(n,e){let t=Ge(n)?n:()=>n,i=r=>r.error(t());return new rt(e?r=>e.schedule(i,0,r):i)}function du(n){return!!n&&(n instanceof rt||Ge(n.lift)&&Ge(n.subscribe))}var To=ws(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function Mt(n,e){return vt((t,i)=>{let r=0;t.subscribe(Ct(i,o=>{i.next(n.call(e,o,r++))}))})}var{isArray:CC}=Array;function TC(n,e){return CC(e)?n(...e):n(e)}function x_(n){return Mt(e=>TC(n,e))}var{isArray:DC}=Array,{getPrototypeOf:IC,prototype:AC,keys:RC}=Object;function E_(n){if(n.length===1){let e=n[0];if(DC(e))return{args:e,keys:null};if(NC(e)){let t=RC(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function NC(n){return n&&typeof n=="object"&&IC(n)===AC}function b_(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Yp(...n){let e=Pr(n),t=c_(n),{args:i,keys:r}=E_(n);if(i.length===0)return Wt([],e);let o=new rt(PC(i,e,r?s=>b_(r,s):mi));return t?o.pipe(x_(t)):o}function PC(n,e,t=mi){return i=>{M_(e,()=>{let{length:r}=n,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)M_(e,()=>{let l=Wt(n[c],e),u=!1;l.subscribe(Ct(i,d=>{o[c]=d,u||(u=!0,a--),a||i.next(t(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function M_(n,e,t){n?Un(t,n,e):e()}function S_(n,e,t,i,r,o,s,a){let c=[],l=0,u=0,d=!1,f=()=>{d&&!c.length&&!l&&e.complete()},h=y=>l<i?p(y):c.push(y),p=y=>{o&&e.next(y),l++;let g=!1;nn(t(y,u++)).subscribe(Ct(e,m=>{r?.(m),o?h(m):e.next(m)},()=>{g=!0},void 0,()=>{if(g)try{for(l--;c.length&&l<i;){let m=c.shift();s?Un(e,s,()=>p(m)):p(m)}f()}catch(m){e.error(m)}}))};return n.subscribe(Ct(e,h,()=>{d=!0,f()})),()=>{a?.()}}function wn(n,e,t=1/0){return Ge(e)?wn((i,r)=>Mt((o,s)=>e(i,o,r,s))(nn(n(i,r))),t):(typeof e=="number"&&(t=e),vt((i,r)=>S_(i,r,n,t)))}function w_(n=1/0){return wn(mi,n)}function C_(){return w_(1)}function As(...n){return C_()(Wt(n,Pr(n)))}function qa(n){return new rt(e=>{nn(n()).subscribe(e)})}var or=new rt(Mo);function ri(n,e){return vt((t,i)=>{let r=0;t.subscribe(Ct(i,o=>n.call(e,o,r++)&&i.next(o)))})}function Xa(n){return vt((e,t)=>{let i=null,r=!1,o;i=e.subscribe(Ct(t,void 0,void 0,s=>{o=nn(n(s,Xa(n)(e))),i?(i.unsubscribe(),i=null,o.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(t))})}function fu(n,e){return Ge(e)?wn(n,e,1):wn(n,1)}function T_(n){return vt((e,t)=>{let i=!1;e.subscribe(Ct(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function Cn(n){return n<=0?()=>un:vt((e,t)=>{let i=0;e.subscribe(Ct(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function D_(n=OC){return vt((e,t)=>{let i=!1;e.subscribe(Ct(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function OC(){return new To}function Zp(n){return vt((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function sr(n,e){let t=arguments.length>=2;return i=>i.pipe(n?ri((r,o)=>n(r,o,i)):mi,Cn(1),t?T_(e):D_(()=>new To))}function hu(n){return n<=0?()=>un:vt((e,t)=>{let i=[];e.subscribe(Ct(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Jp(...n){let e=Pr(n);return vt((t,i)=>{(e?As(n,t,e):As(n,t)).subscribe(i)})}function gn(n,e){return vt((t,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();t.subscribe(Ct(i,c=>{r?.unsubscribe();let l=0,u=o++;nn(n(c,u)).subscribe(r=Ct(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ya(n){return vt((e,t)=>{nn(n).subscribe(Ct(t,()=>t.complete(),Mo)),!t.closed&&e.subscribe(t)})}function oi(n,e,t){let i=Ge(n)||e||t?{next:n,error:e,complete:t}:n;return i?vt((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(Ct(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):mi}var Kp;function pu(){return Kp}function Oi(n){let e=Kp;return Kp=n,e}var I_=Symbol("NotFound");function Rs(n){return n===I_||n?.name==="\u0275NotFound"}function A_(n){let e=Ne(null);try{return n()}finally{Ne(e)}}var bu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Te=class extends Error{code;constructor(e,t){super(No(e,t)),this.code=e}};function LC(n){return`NG0${Math.abs(n)}`}function No(n,e){return`${LC(n)}${e?": "+e:""}`}function yt(n){for(let e in n)if(n[e]===yt)return e;throw Error("")}function nc(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(nc).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Mu(n,e){return n?e?`${n} ${e}`:n:e||""}var FC=yt({__forward_ref__:yt});function Su(n){return n.__forward_ref__=Su,n}function Bn(n){return dm(n)?n():n}function dm(n){return typeof n=="function"&&n.hasOwnProperty(FC)&&n.__forward_ref__===Su}function Pe(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Po(n){return{providers:n.providers||[],imports:n.imports||[]}}function ic(n){return kC(n,wu)}function fm(n){return ic(n)!==null}function kC(n,e){return n.hasOwnProperty(e)&&n[e]||null}function UC(n){let e=n?.[wu]??null;return e||null}function em(n){return n&&n.hasOwnProperty(gu)?n[gu]:null}var wu=yt({\u0275prov:yt}),gu=yt({\u0275inj:yt}),Le=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Pe({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function hm(n){return n&&!!n.\u0275providers}var pm=yt({\u0275cmp:yt}),mm=yt({\u0275dir:yt}),gm=yt({\u0275pipe:yt}),vm=yt({\u0275mod:yt}),Ja=yt({\u0275fac:yt}),Oo=yt({__NG_ELEMENT_ID__:yt}),R_=yt({__NG_ENV_ID__:yt});function ym(n){return Cu(n,"@NgModule"),n[vm]||null}function kr(n){return Cu(n,"@Component"),n[pm]||null}function _m(n){return Cu(n,"@Directive"),n[mm]||null}function L_(n){return Cu(n,"@Pipe"),n[gm]||null}function Cu(n,e){if(n==null)throw new Te(-919,!1)}function Ps(n){return typeof n=="string"?n:n==null?"":String(n)}var F_=yt({ngErrorCode:yt}),BC=yt({ngErrorMessage:yt}),VC=yt({ngTokenPath:yt});function xm(n,e){return k_("",-200,e)}function Tu(n,e){throw new Te(-201,!1)}function k_(n,e,t){let i=new Te(e,n);return i[F_]=e,i[BC]=n,t&&(i[VC]=t),i}function HC(n){return n[F_]}var tm;function U_(){return tm}function Tn(n){let e=tm;return tm=n,e}function Em(n,e,t){let i=ic(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Tu(n,"")}var zC={},Do=zC,GC="__NG_DI_FLAG__",nm=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Io(t)||0;try{return this.injector.get(e,i&8?null:Do,i)}catch(r){if(Rs(r))return r;throw r}}};function jC(n,e=0){let t=pu();if(t===void 0)throw new Te(-203,!1);if(t===null)return Em(n,void 0,e);{let i=WC(e),r=t.retrieve(n,i);if(Rs(r)){if(i.optional)return null;throw r}return r}}function je(n,e=0){return(U_()||jC)(Bn(n),e)}function J(n,e){return je(n,Io(e))}function Io(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function WC(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function im(n){let e=[];for(let t=0;t<n.length;t++){let i=Bn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Te(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=$C(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}e.push(je(r,o))}else e.push(je(i))}return e}function $C(n){return n[GC]}function Or(n,e){let t=n.hasOwnProperty(Ja);return t?n[Ja]:null}function B_(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],o=e[i];if(t&&(r=t(r),o=t(o)),o!==r)return!1}return!0}function V_(n){return n.flat(Number.POSITIVE_INFINITY)}function Du(n,e){n.forEach(t=>Array.isArray(t)?Du(t,e):e(t))}function bm(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function rc(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function H_(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let o=r-2;n[r]=n[o],r--}n[e]=t,n[e+1]=i}}function Iu(n,e,t){let i=Os(n,e);return i>=0?n[i|1]=t:(i=~i,H_(n,i,e,t)),i}function Au(n,e){let t=Os(n,e);if(t>=0)return n[t|1]}function Os(n,e){return qC(n,e,1)}function qC(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let o=i+(r-i>>1),s=n[o<<t];if(e===s)return o<<t;s>e?r=o:i=o+1}return~(r<<t)}var Lo={},Vn=[],Fo=new Le(""),Mm=new Le("",-1),Sm=new Le(""),Ka=class{get(e,t=Do){if(t===Do){let r=k_("",-201);throw r.name="\u0275NotFound",r}return t}};function lr(n){return{\u0275providers:n}}function z_(n){return lr([{provide:Fo,multi:!0,useValue:n}])}function G_(...n){return{\u0275providers:wm(!0,n),\u0275fromNgModule:!0}}function wm(n,...e){let t=[],i=new Set,r,o=s=>{t.push(s)};return Du(e,s=>{let a=s;vu(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&j_(r,o),t}function j_(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Cm(r,o=>{e(o,i)})}}function vu(n,e,t,i){if(n=Bn(n),!n)return!1;let r=null,o=em(n),s=!o&&kr(n);if(!o&&!s){let c=n.ngModule;if(o=em(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=n}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)vu(l,e,t,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;Du(o.imports,u=>{vu(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&j_(l,e)}if(!a){let l=Or(r)||(()=>new r);e({provide:r,useFactory:l,deps:Vn},r),e({provide:Sm,useValue:r,multi:!0},r),e({provide:Fo,useValue:()=>je(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=n;Cm(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Cm(n,e){for(let t of n)hm(t)&&(t=t.\u0275providers),Array.isArray(t)?Cm(t,e):e(t)}var XC=yt({provide:String,useValue:yt});function W_(n){return n!==null&&typeof n=="object"&&XC in n}function YC(n){return!!(n&&n.useExisting)}function ZC(n){return!!(n&&n.useFactory)}function yu(n){return typeof n=="function"}var oc=new Le(""),mu={},N_={},Qp;function sc(){return Qp===void 0&&(Qp=new Ka),Qp}var Yt=class{},Ao=class extends Yt{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,om(e,s=>this.processProvider(s)),this.records.set(Mm,Ns(void 0,this)),r.has("environment")&&this.records.set(Yt,Ns(void 0,this));let o=this.records.get(oc);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Sm,Vn,{self:!0}))}retrieve(e,t){let i=Io(t)||0;try{return this.get(e,Do,i)}catch(r){if(Rs(r))return r;throw r}}destroy(){Za(this),this._destroyed=!0;let e=Ne(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ne(e)}}onDestroy(e){return Za(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Za(this);let t=Oi(this),i=Tn(void 0),r;try{return e()}finally{Oi(t),Tn(i)}}get(e,t=Do,i){if(Za(this),e.hasOwnProperty(R_))return e[R_](this);let r=Io(i),o,s=Oi(this),a=Tn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=tT(e)&&ic(e);u&&this.injectableDefInScope(u)?l=Ns(rm(e),mu):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?sc():this.parent;return t=r&8&&t===Do?null:t,c.get(e,t)}catch(c){let l=HC(c);throw l===-200||l===-201?new Te(l,null):c}finally{Tn(a),Oi(s)}}resolveInjectorInitializers(){let e=Ne(null),t=Oi(this),i=Tn(void 0),r;try{let o=this.get(Fo,Vn,{self:!0});for(let s of o)s()}finally{Oi(t),Tn(i),Ne(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=Bn(e);let t=yu(e)?e:Bn(e&&e.provide),i=KC(e);if(!yu(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Ns(void 0,mu,!0),r.factory=()=>im(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ne(null);try{if(t.value===N_)throw xm("");return t.value===mu&&(t.value=N_,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&eT(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ne(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Bn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function rm(n){let e=ic(n),t=e!==null?e.factory:Or(n);if(t!==null)return t;if(n instanceof Le)throw new Te(-204,!1);if(n instanceof Function)return JC(n);throw new Te(-204,!1)}function JC(n){if(n.length>0)throw new Te(-204,!1);let t=UC(n);return t!==null?()=>t.factory(n):()=>new n}function KC(n){if(W_(n))return Ns(void 0,n.useValue);{let e=$_(n);return Ns(e,mu)}}function $_(n,e,t){let i;if(yu(n)){let r=Bn(n);return Or(r)||rm(r)}else if(W_(n))i=()=>Bn(n.useValue);else if(ZC(n))i=()=>n.useFactory(...im(n.deps||[]));else if(YC(n))i=(r,o)=>je(Bn(n.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Bn(n&&(n.useClass||n.provide));if(QC(n))i=()=>new r(...im(n.deps));else return Or(r)||rm(r)}return i}function Za(n){if(n.destroyed)throw new Te(-205,!1)}function Ns(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function QC(n){return!!n.deps}function eT(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function tT(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function om(n,e){for(let t of n)Array.isArray(t)?om(t,e):t&&hm(t)?om(t.\u0275providers,e):e(t)}function vn(n,e){let t;n instanceof Ao?(Za(n),t=n):t=new nm(n);let i,r=Oi(t),o=Tn(void 0);try{return e()}finally{Oi(r),Tn(o)}}function q_(){return U_()!==void 0||pu()!=null}var gi=0,Ve=1,We=2,Zt=3,si=4,ai=5,Ls=6,Fs=7,zt=8,ur=9,vi=10,Gt=11,ks=12,Tm=13,ko=14,Xn=15,Ur=16,Uo=17,Li=18,dr=19,Dm=20,ar=21,Ru=22,Lr=23,Yn=24,Nu=25,Br=26,$t=27,X_=1,Im=6,Vr=7,ac=8,Bo=9,kt=10;function Hr(n){return Array.isArray(n)&&typeof n[X_]=="object"}function yi(n){return Array.isArray(n)&&n[X_]===!0}function Am(n){return(n.flags&4)!==0}function zr(n){return n.componentOffset>-1}function Pu(n){return(n.flags&1)===1}function Vo(n){return!!n.template}function Us(n){return(n[We]&512)!==0}function Ho(n){return(n[We]&256)===256}var Rm="svg",Y_="math";function ci(n){for(;Array.isArray(n);)n=n[gi];return n}function Nm(n,e){return ci(e[n])}function _i(n,e){return ci(e[n.index])}function Ou(n,e){return n.data[e]}function Pm(n,e){return n[e]}function Om(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function Fi(n,e){let t=e[n];return Hr(t)?t:t[gi]}function Z_(n){return(n[We]&4)===4}function Lu(n){return(n[We]&128)===128}function J_(n){return yi(n[Zt])}function ki(n,e){return e==null?null:n[e]}function Lm(n){n[Uo]=0}function Fm(n){n[We]&1024||(n[We]|=1024,Lu(n)&&Bs(n))}function K_(n,e){for(;n>0;)e=e[ko],n--;return e}function cc(n){return!!(n[We]&9216||n[Yn]?.dirty)}function Fu(n){n[vi].changeDetectionScheduler?.notify(8),n[We]&64&&(n[We]|=1024),cc(n)&&Bs(n)}function Bs(n){n[vi].changeDetectionScheduler?.notify(0);let e=Fr(n);for(;e!==null&&!(e[We]&8192||(e[We]|=8192,!Lu(e)));)e=Fr(e)}function km(n,e){if(Ho(n))throw new Te(911,!1);n[ar]===null&&(n[ar]=[]),n[ar].push(e)}function Q_(n,e){if(n[ar]===null)return;let t=n[ar].indexOf(e);t!==-1&&n[ar].splice(t,1)}function Fr(n){let e=n[Zt];return yi(e)?e[Zt]:e}function Um(n){return n[Fs]??=[]}function Bm(n){return n.cleanup??=[]}function ex(n,e,t,i){let r=Um(e);r.push(t),n.firstCreatePass&&Bm(n).push(i,r.length-1)}var Xe={lFrame:gx(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var sm=!1;function tx(){return Xe.lFrame.elementDepthCount}function nx(){Xe.lFrame.elementDepthCount++}function Vm(){Xe.lFrame.elementDepthCount--}function ix(){return Xe.bindingsEnabled}function rx(){return Xe.skipHydrationRootTNode!==null}function Hm(n){return Xe.skipHydrationRootTNode===n}function zm(){Xe.skipHydrationRootTNode=null}function ot(){return Xe.lFrame.lView}function Rn(){return Xe.lFrame.tView}function ht(n){return Xe.lFrame.contextLView=n,n[zt]}function pt(n){return Xe.lFrame.contextLView=null,n}function li(){let n=Gm();for(;n!==null&&n.type===64;)n=n.parent;return n}function Gm(){return Xe.lFrame.currentTNode}function ox(){let n=Xe.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Vs(n,e){let t=Xe.lFrame;t.currentTNode=n,t.isParent=e}function jm(){return Xe.lFrame.isParent}function sx(){Xe.lFrame.isParent=!1}function ax(){return Xe.lFrame.contextLView}function Wm(){return sm}function Qa(n){let e=sm;return sm=n,e}function cx(){let n=Xe.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function lx(){return Xe.lFrame.bindingIndex}function ux(n){return Xe.lFrame.bindingIndex=n}function Hs(){return Xe.lFrame.bindingIndex++}function ku(n){let e=Xe.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function dx(){return Xe.lFrame.inI18n}function fx(n,e){let t=Xe.lFrame;t.bindingIndex=t.bindingRootIndex=n,Uu(e)}function hx(){return Xe.lFrame.currentDirectiveIndex}function Uu(n){Xe.lFrame.currentDirectiveIndex=n}function px(n){let e=Xe.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function $m(){return Xe.lFrame.currentQueryIndex}function Bu(n){Xe.lFrame.currentQueryIndex=n}function nT(n){let e=n[Ve];return e.type===2?e.declTNode:e.type===1?n[ai]:null}function qm(n,e,t){if(t&4){let r=e,o=n;for(;r=r.parent,r===null&&!(t&1);)if(r=nT(o),r===null||(o=o[ko],r.type&10))break;if(r===null)return!1;e=r,n=o}let i=Xe.lFrame=mx();return i.currentTNode=e,i.lView=n,!0}function Vu(n){let e=mx(),t=n[Ve];Xe.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function mx(){let n=Xe.lFrame,e=n===null?null:n.child;return e===null?gx(n):e}function gx(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function vx(){let n=Xe.lFrame;return Xe.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Xm=vx;function Hu(){let n=vx();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function yx(n){return(Xe.lFrame.contextLView=K_(n,Xe.lFrame.contextLView))[zt]}function Ui(){return Xe.lFrame.selectedIndex}function Gr(n){Xe.lFrame.selectedIndex=n}function Ym(){let n=Xe.lFrame;return Ou(n.tView,n.selectedIndex)}function zu(){Xe.lFrame.currentNamespace=Rm}function Gu(){iT()}function iT(){Xe.lFrame.currentNamespace=null}function _x(){return Xe.lFrame.currentNamespace}var xx=!0;function ju(){return xx}function Wu(n){xx=n}function am(n,e=null,t=null,i){let r=Zm(n,e,t,i);return r.resolveInjectorInitializers(),r}function Zm(n,e=null,t=null,i,r=new Set){let o=[t||Vn,G_(n)],s;return new Ao(o,e||sc(),s||null,r)}var In=class n{static THROW_IF_NOT_FOUND=Do;static NULL=new Ka;static create(e,t){if(Array.isArray(e))return am({name:""},t,e,"");{let i=e.name??"";return am({name:i},e.parent,e.providers,i)}}static \u0275prov=Pe({token:n,providedIn:"any",factory:()=>je(Mm)});static __NG_ELEMENT_ID__=-1},Jt=new Le(""),fr=(()=>{class n{static __NG_ELEMENT_ID__=rT;static __NG_ENV_ID__=t=>t}return n})(),_u=class extends fr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Ho(this._lView)}onDestroy(e){let t=this._lView;return km(t,e),()=>Q_(t,e)}};function rT(){return new _u(ot())}var Ex=!1,bx=new Le(""),jr=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new ln(!1);debugTaskTracker=J(bx,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new rt(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Pe({token:n,providedIn:"root",factory:()=>new n})}return n})(),cm=class extends Ht{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,q_()&&(this.destroyRef=J(fr,{optional:!0})??void 0,this.pendingTasks=J(jr,{optional:!0})??void 0)}emit(e){let t=Ne(null);try{super.next(e)}finally{Ne(t)}}subscribe(e,t,i){let r=e,o=t||(()=>null),s=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return e instanceof cn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Dn=cm;function xu(...n){}function Jm(n){let e,t;function i(){n=xu;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Mx(n){return queueMicrotask(()=>n()),()=>{n=xu}}var Km="isAngularZone",ec=Km+"_ID",oT=0,An=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Dn(!1);onMicrotaskEmpty=new Dn(!1);onStable=new Dn(!1);onError=new Dn(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Ex}=e;if(typeof Zone>"u")throw new Te(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,cT(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Km)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Te(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Te(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,e,sT,xu,xu);try{return o.runTask(s,t,i)}finally{o.cancelTask(s)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},sT={};function Qm(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function aT(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Jm(()=>{n.callbackScheduled=!1,lm(n),n.isCheckStableRunning=!0,Qm(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),lm(n)}function cT(n){let e=()=>{aT(n)},t=oT++;n._inner=n._inner.fork({name:"angular",properties:{[Km]:!0,[ec]:t,[ec+t]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(lT(c))return i.invokeTask(o,s,a,c);try{return P_(n),i.invokeTask(o,s,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),O_(n)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return P_(n),i.invoke(o,s,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!uT(c)&&e(),O_(n)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(n._hasPendingMicrotasks=s.microTask,lm(n),Qm(n)):s.change=="macroTask"&&(n.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),n.runOutsideAngular(()=>n.onError.emit(s)),!1)})}function lm(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function P_(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function O_(n){n._nesting--,Qm(n)}var tc=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Dn;onMicrotaskEmpty=new Dn;onStable=new Dn;onError=new Dn;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function lT(n){return Sx(n,"__ignore_ng_zone__")}function uT(n){return Sx(n,"__scheduler_tick__")}function Sx(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var cr=class{_console=console;handleError(e){this._console.error("ERROR",e)}},Bi=new Le("",{factory:()=>{let n=J(An),e=J(Yt),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(cr),t.handleError(i))})}}}),wx={provide:Fo,useValue:()=>{let n=J(cr,{optional:!0})},multi:!0},dT=new Le("",{factory:()=>{let n=J(Jt).defaultView;if(!n)return;let e=J(Bi),t=o=>{e(o.reason),o.preventDefault()},i=o=>{o.error?e(o.error):e(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),J(fr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function eg(){return lr([z_(()=>{J(dT)})])}function Hn(n,e){let[t,i,r]=Fp(n,e?.equal),o=t,s=o[Sn];return o.set=i,o.update=r,o.asReadonly=Cx.bind(o),o}function Cx(){let n=this[Sn];if(n.readonlyFn===void 0){let e=()=>this();e[Sn]=n,n.readonlyFn=e}return n.readonlyFn}var $u=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=fT}return n})();function fT(){return new $u(ot(),li())}var Ro=class{},lc=new Le("",{factory:()=>!0});var tg=new Le("");var qu=(()=>{class n{static \u0275prov=Pe({token:n,providedIn:"root",factory:()=>new um})}return n})(),um=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Eu=class{[Sn];constructor(e){this[Sn]=e}destroy(){this[Sn].destroy()}};function Xu(n,e){let t=e?.injector??J(In),i=e?.manualCleanup!==!0?t.get(fr):null,r,o=t.get($u,null,{optional:!0}),s=t.get(Ro);return o!==null?(r=mT(o.view,s,n),i instanceof _u&&i._lView===o.view&&(i=null)):r=gT(n,t.get(qu),s),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Eu(r)}var Tx=it(ae({},Up),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=Qa(!1);try{Bp(this)}finally{Qa(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=Ne(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],Ne(n)}}}),hT=it(ae({},Tx),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(bo(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),pT=it(ae({},Tx),{consumerMarkedDirty(){this.view[We]|=8192,Bs(this.view),this.notifier.notify(13)},destroy(){if(bo(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[Lr]?.delete(this)}});function mT(n,e,t){let i=Object.create(pT);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.fn=Dx(i,t),n[Lr]??=new Set,n[Lr].add(i),i.consumerMarkedDirty(i),i}function gT(n,e,t){let i=Object.create(hT);return i.fn=Dx(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Dx(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function _c(n){return{toString:n}.toString()}function MT(n){return typeof n=="function"}function tE(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var ed=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},yd=(()=>{let n=()=>nE;return n.ngInherit=!0,n})();function nE(n){return n.type.prototype.ngOnChanges&&(n.setInput=wT),ST}function ST(){let n=rE(this),e=n?.current;if(e){let t=n.previous;if(t===Lo)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function wT(n,e,t,i,r){let o=this.declaredInputs[i],s=rE(n)||CT(n,{previous:Lo,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new ed(l&&l.currentValue,t,c===Lo),tE(n,e,r,t)}var iE="__ngSimpleChanges__";function rE(n){return n[iE]||null}function CT(n,e){return n[iE]=e}var Ix=[];var Tt=function(n,e=null,t){for(let i=0;i<Ix.length;i++){let r=Ix[i];r(n,e,t)}},mt=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(mt||{});function TT(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=e.type.prototype;if(i){let s=nE(e);(t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s)}r&&(t.preOrderHooks??=[]).push(0-n,r),o&&((t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o))}function DT(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let o=n.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=o;s&&(n.contentHooks??=[]).push(-t,s),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function Zu(n,e,t){oE(n,e,3,t)}function Ju(n,e,t,i){(n[We]&3)===t&&oE(n,e,t,i)}function ng(n,e){let t=n[We];(t&3)===e&&(t&=16383,t+=1,n[We]=t)}function oE(n,e,t,i){let r=i!==void 0?n[Uo]&65535:0,o=i??-1,s=e.length-1,a=0;for(let c=r;c<s;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Uo]+=65536),(a<o||o==-1)&&(IT(n,t,e,c),n[Uo]=(n[Uo]&4294901760)+c+2),c++}function Ax(n,e){Tt(mt.LifecycleHookStart,n,e);let t=Ne(null);try{e.call(n)}finally{Ne(t),Tt(mt.LifecycleHookEnd,n,e)}}function IT(n,e,t,i){let r=t[i]<0,o=t[i+1],s=r?-t[i]:t[i],a=n[s];r?n[We]>>14<n[Uo]>>16&&(n[We]&3)===e&&(n[We]+=16384,Ax(a,o)):Ax(a,o)}var Gs=-1,hc=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function AT(n){return(n.flags&8)!==0}function RT(n){return(n.flags&16)!==0}function NT(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let o=t[i++],s=t[i++],a=t[i++];n.setAttribute(e,s,a,o)}else{let o=r,s=t[++i];OT(o)?n.setProperty(e,o,s):n.setAttribute(e,o,s),i++}}return i}function PT(n){return n===3||n===4||n===6}function OT(n){return n.charCodeAt(0)===64}function _d(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?Rx(n,t,r,null,e[++i]):Rx(n,t,r,null,null))}}return n}function Rx(n,e,t,i,r){let o=0,s=n.length;if(e===-1)s=-1;else for(;o<n.length;){let a=n[o++];if(typeof a=="number"){if(a===e){s=-1;break}else if(a>e){s=o-1;break}}}for(;o<n.length;){let a=n[o];if(typeof a=="number")break;if(a===t){r!==null&&(n[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(n.splice(s,0,e),o=s+1),n.splice(o++,0,t),r!==null&&n.splice(o++,0,r)}function sE(n){return n!==Gs}function td(n){return n&32767}function LT(n){return n>>16}function nd(n,e){let t=LT(n),i=e;for(;t>0;)i=i[ko],t--;return i}var lg=!0;function id(n){let e=lg;return lg=n,e}var FT=256,aE=FT-1,cE=5,kT=0,Vi={};function UT(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Oo)&&(i=t[Oo]),i==null&&(i=t[Oo]=kT++);let r=i&aE,o=1<<r;e.data[n+(r>>cE)]|=o}function lE(n,e){let t=uE(n,e);if(t!==-1)return t;let i=e[Ve];i.firstCreatePass&&(n.injectorIndex=e.length,ig(i.data,n),ig(e,null),ig(i.blueprint,null));let r=kg(n,e),o=n.injectorIndex;if(sE(r)){let s=td(r),a=nd(r,e),c=a[Ve].data;for(let l=0;l<8;l++)e[o+l]=a[s+l]|c[s+l]}return e[o+8]=r,o}function ig(n,e){n.push(0,0,0,0,0,0,0,0,e)}function uE(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function kg(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=mE(r),i===null)return Gs;if(t++,r=r[ko],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Gs}function BT(n,e,t){UT(n,e,t)}function dE(n,e,t){if(t&8||n!==void 0)return n;Tu(e,"NodeInjector")}function fE(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[ur],o=Tn(void 0);try{return r?r.get(e,i,t&8):Em(e,i,t&8)}finally{Tn(o)}}return dE(i,e,t)}function hE(n,e,t,i=0,r){if(n!==null){if(e[We]&2048&&!(i&2)){let s=GT(n,e,t,i,Vi);if(s!==Vi)return s}let o=pE(n,e,t,i,Vi);if(o!==Vi)return o}return fE(e,t,i,r)}function pE(n,e,t,i,r){let o=HT(t);if(typeof o=="function"){if(!qm(e,n,i))return i&1?dE(r,t,i):fE(e,t,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Tu(t);else return s}finally{Xm()}}else if(typeof o=="number"){let s=null,a=uE(n,e),c=Gs,l=i&1?e[Xn][ai]:null;for((a===-1||i&4)&&(c=a===-1?kg(n,e):e[a+8],c===Gs||!Px(i,!1)?a=-1:(s=e[Ve],a=td(c),e=nd(c,e)));a!==-1;){let u=e[Ve];if(Nx(o,a,u.data)){let d=VT(a,e,t,s,i,l);if(d!==Vi)return d}c=e[a+8],c!==Gs&&Px(i,e[Ve].data[a+8]===l)&&Nx(o,a,e)?(s=u,a=td(c),e=nd(c,e)):a=-1}}return r}function VT(n,e,t,i,r,o){let s=e[Ve],a=s.data[n+8],c=i==null?zr(a)&&lg:i!=s&&(a.type&3)!==0,l=r&1&&o===a,u=Ku(a,s,t,c,l);return u!==null?rd(e,s,u,a,r):Vi}function Ku(n,e,t,i,r){let o=n.providerIndexes,s=e.data,a=o&1048575,c=n.directiveStart,l=n.directiveEnd,u=o>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let p=s[h];if(h<c&&t===p||h>=c&&p.type===t)return h}if(r){let h=s[c];if(h&&Vo(h)&&h.type===t)return c}return null}function rd(n,e,t,i,r){let o=n[t],s=e.data;if(o instanceof hc){let a=o;if(a.resolving)throw xm("");let c=id(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],u,d=a.injectImpl?Tn(a.injectImpl):null,f=qm(n,i,0);try{o=n[t]=a.factory(void 0,r,s,n,i),e.firstCreatePass&&t>=i.directiveStart&&TT(t,s[t],e)}finally{d!==null&&Tn(d),id(c),a.resolving=!1,Xm()}}return o}function HT(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Oo)?n[Oo]:void 0;return typeof e=="number"?e>=0?e&aE:zT:e}function Nx(n,e,t){let i=1<<n;return!!(t[e+(n>>cE)]&i)}function Px(n,e){return!(n&2)&&!(n&1&&e)}var zo=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return hE(this._tNode,this._lView,e,Io(i),t)}};function zT(){return new zo(li(),ot())}function xc(n){return _c(()=>{let e=n.prototype.constructor,t=e[Ja]||ug(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let o=r[Ja]||ug(r);if(o&&o!==t)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function ug(n){return dm(n)?()=>{let e=ug(Bn(n));return e&&e()}:Or(n)}function GT(n,e,t,i,r){let o=n,s=e;for(;o!==null&&s!==null&&s[We]&2048&&!Us(s);){let a=pE(o,s,t,i|2,Vi);if(a!==Vi)return a;let c=o.parent;if(!c){let l=s[Dm];if(l){let u=l.get(t,Vi,i&-5);if(u!==Vi)return u}c=mE(s),s=s[ko]}o=c}return r}function mE(n){let e=n[Ve],t=e.type;return t===2?e.declTNode:t===1?n[ai]:null}function jT(){return Ys(li(),ot())}function Ys(n,e){return new Zs(_i(n,e))}var Zs=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=jT}return n})();function WT(n){return n instanceof Zs?n.nativeElement:n}function $T(){return this._results[Symbol.iterator]()}var od=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Ht}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=V_(e);(this._changesDetected=!B_(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=$T};function gE(n){return(n.flags&128)===128}var Ug=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Ug||{}),vE=new Map,qT=0;function XT(){return qT++}function YT(n){vE.set(n[dr],n)}function dg(n){vE.delete(n[dr])}var Ox="__ngContext__";function js(n,e){Hr(e)?(n[Ox]=e[dr],YT(e)):n[Ox]=e}function yE(n){return xE(n[ks])}function _E(n){return xE(n[si])}function xE(n){for(;n!==null&&!yi(n);)n=n[si];return n}var ZT;function Bg(n){ZT=n}var xd=new Le("",{factory:()=>JT}),JT="ng";var Ed=new Le(""),Ec=new Le("",{providedIn:"platform",factory:()=>"unknown"});var bd=new Le("",{factory:()=>J(Jt).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var EE="r";var bE="di";var ME=!1,SE=new Le("",{factory:()=>ME});var KT=(n,e,t,i)=>{};function QT(n,e,t,i){KT(n,e,t,i)}function Vg(n){return(n.flags&32)===32}var eD=()=>null;function wE(n,e,t=!1){return eD(n,e,t)}function CE(n,e){let t=n.contentQueries;if(t!==null){let i=Ne(null);try{for(let r=0;r<t.length;r+=2){let o=t[r],s=t[r+1];if(s!==-1){let a=n.data[s];Bu(o),a.contentQueries(2,e[s],s)}}}finally{Ne(i)}}}function fg(n,e,t){Bu(0);let i=Ne(null);try{e(n,t)}finally{Ne(i)}}function TE(n,e,t){if(Am(e)){let i=Ne(null);try{let r=e.directiveStart,o=e.directiveEnd;for(let s=r;s<o;s++){let a=n.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{Ne(i)}}}var bi=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(bi||{});var sd=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${bu})`}};function Md(n){return n instanceof sd?n.changingThisBreaksApplicationSecurity:n}function DE(n,e){let t=IE(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${bu})`)}return t===e}function IE(n){return n instanceof sd&&n.getTypeName()||null}var tD=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function AE(n){return n=String(n),n.match(tD)?n:"unsafe:"+n}function nD(n,e){return n.createText(e)}function iD(n,e,t){n.setValue(e,t)}function RE(n,e,t){return n.createElement(e,t)}function ad(n,e,t,i,r){n.insertBefore(e,t,i,r)}function NE(n,e,t){n.appendChild(e,t)}function Lx(n,e,t,i,r){i!==null?ad(n,e,t,i,r):NE(n,e,t)}function PE(n,e,t,i){n.removeChild(null,e,t,i)}function rD(n,e,t){n.setAttribute(e,"style",t)}function oD(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function OE(n,e,t){let{mergedAttrs:i,classes:r,styles:o}=t;i!==null&&NT(n,e,i),r!==null&&oD(n,e,r),o!==null&&rD(n,e,o)}var Hg=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n})(Hg||{});function bc(n){let e=sD();return e?e.sanitize(Hg.URL,n)||"":DE(n,"URL")?Md(n):AE(Ps(n))}function sD(){let n=ot();return n&&n[vi].sanitizer}function LE(n){return n instanceof Function?n():n}function aD(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let o=e.length;if(r+o===i||n.charCodeAt(r+o)<=32)return r}t=r+1}}var FE="ng-template";function cD(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&aD(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(zg(n))return!1;if(r=e.indexOf(1,r),r>-1){let o;for(;++r<e.length&&typeof(o=e[r])=="string";)if(o.toLowerCase()===t)return!0}return!1}function zg(n){return n.type===4&&n.value!==FE}function lD(n,e,t){let i=n.type===4&&!t?FE:n.value;return e===i}function uD(n,e,t){let i=4,r=n.attrs,o=r!==null?hD(r):0,s=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!s&&!xi(i)&&!xi(c))return!1;if(s&&xi(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!lD(n,c,t)||c===""&&e.length===1){if(xi(i))return!1;s=!0}}else if(i&8){if(r===null||!cD(n,r,c,t)){if(xi(i))return!1;s=!0}}else{let l=e[++a],u=dD(c,r,zg(n),t);if(u===-1){if(xi(i))return!1;s=!0;continue}if(l!==""){let d;if(u>o?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(xi(i))return!1;s=!0}}}}return xi(i)||s}function xi(n){return(n&1)===0}function dD(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let o=!1;for(;r<e.length;){let s=e[r];if(s===n)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return pD(e,n)}function fD(n,e,t=!1){for(let i=0;i<e.length;i++)if(uD(n,e[i],t))return!0;return!1}function hD(n){for(let e=0;e<n.length;e++){let t=n[e];if(PT(t))return e}return n.length}function pD(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function Fx(n,e){return n?":not("+e.trim()+")":e}function mD(n){let e=n[0],t=1,i=2,r="",o=!1;for(;t<n.length;){let s=n[t];if(typeof s=="string")if(i&2){let a=n[++t];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!xi(s)&&(e+=Fx(o,r),r=""),i=s,o=o||!xi(i);t++}return r!==""&&(e+=Fx(o,r)),e}function gD(n){return n.map(mD).join(",")}function vD(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let o=n[i];if(typeof o=="string")r===2?o!==""&&e.push(o,n[++i]):r===8&&t.push(o);else{if(!xi(r))break;r=o}i++}return t.length&&e.push(1,...t),e}var zn={};function Gg(n,e,t,i,r,o,s,a,c,l,u){let d=$t+i,f=d+r,h=yD(d,f),p=typeof l=="function"?l():l;return h[Ve]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:p,incompleteFirstPass:!1,ssrId:u}}function yD(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:zn);return t}function _D(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Gg(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function jg(n,e,t,i,r,o,s,a,c,l,u){let d=e.blueprint.slice();return d[gi]=r,d[We]=i|4|128|8|64|1024,(l!==null||n&&n[We]&2048)&&(d[We]|=2048),Lm(d),d[Zt]=d[ko]=n,d[zt]=t,d[vi]=s||n&&n[vi],d[Gt]=a||n&&n[Gt],d[ur]=c||n&&n[ur]||null,d[ai]=o,d[dr]=XT(),d[Ls]=u,d[Dm]=l,d[Xn]=e.type==2?n[Xn]:d,d}function xD(n,e,t){let i=_i(e,n),r=_D(t),o=n[vi].rendererFactory,s=Wg(n,jg(n,r,null,kE(t),i,e,null,o.createRenderer(i,t),null,null,null));return n[e.index]=s}function kE(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function UE(n,e,t,i){if(t===0)return-1;let r=e.length;for(let o=0;o<t;o++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Wg(n,e){return n[ks]?n[Tm][si]=e:n[ks]=e,n[Tm]=e,e}function Oe(n=1){BE(Rn(),ot(),Ui()+n,!1)}function BE(n,e,t,i){if(!i)if((e[We]&3)===3){let o=n.preOrderCheckHooks;o!==null&&Zu(e,o,t)}else{let o=n.preOrderHooks;o!==null&&Ju(e,o,0,t)}Gr(t)}var Sd=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Sd||{});function hg(n,e,t,i){let r=Ne(null);try{let[o,s,a]=n.inputs[t],c=null;(s&Sd.SignalBased)!==0&&(c=e[o][Sn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,o):tE(e,c,o,i)}finally{Ne(r)}}var hr=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(hr||{}),ED;function $g(n,e){return ED(n,e)}var Iz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var pg=new WeakMap,uc=new WeakSet;function bD(n,e){let t=pg.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let o=t.length-1;o>=0;o--){let s=t[o],a=s.parentNode;s===e?(t.splice(o,1),uc.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(t.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function MD(n,e){let t=pg.get(n);t?t.includes(e)||t.push(e):pg.set(n,[e])}var Go=new Set,qg=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(qg||{}),Js=new Le(""),kx=new Set;function qo(n){kx.has(n)||(kx.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var VE=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Pe({token:n,providedIn:"root",factory:()=>new n})}return n})();var HE=new Le("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:J(Yt)})});function zE(n,e,t){let i=n.get(HE);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function SD(n,e){let t=n.get(HE);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function wD(n,e){for(let[t,i]of e)zE(n,i.animateFns)}function Ux(n,e,t,i){let r=n?.[Br]?.enter;e!==null&&r&&r.has(t.index)&&wD(i,r)}function zs(n,e,t,i,r,o,s,a){if(r!=null){let c,l=!1;yi(r)?c=r:Hr(r)&&(l=!0,r=r[gi]);let u=ci(r);n===0&&i!==null?(Ux(a,i,o,t),s==null?NE(e,i,u):ad(e,i,u,s||null,!0)):n===1&&i!==null?(Ux(a,i,o,t),ad(e,i,u,s||null,!0),bD(o,u)):n===2?(a?.[Br]?.leave?.has(o.index)&&MD(o,u),uc.delete(u),Bx(a,o,t,d=>{if(uc.has(u)){uc.delete(u);return}PE(e,u,l,d)})):n===3&&(uc.delete(u),Bx(a,o,t,()=>{e.destroyNode(u)})),c!=null&&UD(e,n,t,c,o,i,s)}}function CD(n,e){GE(n,e),e[gi]=null,e[ai]=null}function TD(n,e,t,i,r,o){i[gi]=r,i[ai]=e,Cd(n,i,t,1,r,o)}function GE(n,e){e[vi].changeDetectionScheduler?.notify(9),Cd(n,e,e[Gt],2,null,null)}function DD(n){let e=n[ks];if(!e)return rg(n[Ve],n);for(;e;){let t=null;if(Hr(e))t=e[ks];else{let i=e[kt];i&&(t=i)}if(!t){for(;e&&!e[si]&&e!==n;)Hr(e)&&rg(e[Ve],e),e=e[Zt];e===null&&(e=n),Hr(e)&&rg(e[Ve],e),t=e&&e[si]}e=t}}function Xg(n,e){let t=n[Bo],i=t.indexOf(e);t.splice(i,1)}function wd(n,e){if(Ho(e))return;let t=e[Gt];t.destroyNode&&Cd(n,e,t,3,null,null),DD(e)}function rg(n,e){if(Ho(e))return;let t=Ne(null);try{e[We]&=-129,e[We]|=256,e[Yn]&&bo(e[Yn]),RD(n,e),AD(n,e),e[Ve].type===1&&e[Gt].destroy();let i=e[Ur];if(i!==null&&yi(e[Zt])){i!==e[Zt]&&Xg(i,e);let r=e[Li];r!==null&&r.detachView(n)}dg(e)}finally{Ne(t)}}function Bx(n,e,t,i){let r=n?.[Br];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&Go.add(n[dr]),zE(t,()=>{if(r.leave&&r.leave.has(e.index)){let s=r.leave.get(e.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),ID(n,i)}else n&&Go.delete(n[dr]),i(!1)},r)}function ID(n,e){let t=n[Br]?.running;if(t){t.then(()=>{n[Br].running=void 0,Go.delete(n[dr]),e(!0)});return}e(!1)}function AD(n,e){let t=n.cleanup,i=e[Fs];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[t[s+1]];t[s].call(a)}i!==null&&(e[Fs]=null);let r=e[ar];if(r!==null){e[ar]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=e[Lr];if(o!==null){e[Lr]=null;for(let s of o)s.destroy()}}function RD(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof hc)){let o=t[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];Tt(mt.LifecycleHookStart,a,c);try{c.call(a)}finally{Tt(mt.LifecycleHookEnd,a,c)}}else{Tt(mt.LifecycleHookStart,r,o);try{o.call(r)}finally{Tt(mt.LifecycleHookEnd,r,o)}}}}}function ND(n,e,t){return PD(n,e.parent,t)}function PD(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[gi];if(zr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===bi.None||r===bi.Emulated)return null}return _i(i,t)}function OD(n,e,t){return FD(n,e,t)}function LD(n,e,t){return n.type&40?_i(n,t):null}var FD=LD,Vx;function Yg(n,e,t,i){let r=ND(n,i,e),o=e[Gt],s=i.parent||e[ai],a=OD(s,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Lx(o,r,t[c],a,!1);else Lx(o,r,t,a,!1);Vx!==void 0&&Vx(o,i,e,t,r)}function dc(n,e){if(e!==null){let t=e.type;if(t&3)return _i(e,n);if(t&4)return mg(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return dc(n,i);{let r=n[e.index];return yi(r)?mg(-1,r):ci(r)}}else{if(t&128)return dc(n,e.next);if(t&32)return $g(e,n)()||ci(n[e.index]);{let i=jE(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Fr(n[Xn]);return dc(r,i)}else return dc(n,e.next)}}}return null}function jE(n,e){if(e!==null){let i=n[Xn][ai],r=e.projection;return i.projection[r]}return null}function mg(n,e){let t=kt+n+1;if(t<e.length){let i=e[t],r=i[Ve].firstChild;if(r!==null)return dc(i,r)}return e[Vr]}function Zg(n,e,t,i,r,o,s){for(;t!=null;){let a=i[ur];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(s&&e===0&&(c&&js(ci(c),i),t.flags|=2),!Vg(t))if(l&8)Zg(n,e,t.child,i,r,o,!1),zs(e,n,a,r,c,t,o,i);else if(l&32){let u=$g(t,i),d;for(;d=u();)zs(e,n,a,r,d,t,o,i);zs(e,n,a,r,c,t,o,i)}else l&16?kD(n,e,i,t,r,o):zs(e,n,a,r,c,t,o,i);t=s?t.projectionNext:t.next}}function Cd(n,e,t,i,r,o){Zg(t,i,n.firstChild,e,r,o,!1)}function kD(n,e,t,i,r,o){let s=t[Xn],c=s[ai].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];zs(e,n,t[ur],r,u,i,o,t)}else{let l=c,u=s[Zt];gE(i)&&(l.flags|=128),Zg(n,e,l,u,r,o,!0)}}function UD(n,e,t,i,r,o,s){let a=i[Vr],c=ci(i);a!==c&&zs(e,n,t,o,a,r,s);for(let l=kt;l<i.length;l++){let u=i[l];Cd(u[Ve],u,n,e,o,a)}}function BD(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let o=i.indexOf("-")===-1?void 0:hr.DashCase;r==null?n.removeStyle(t,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=hr.Important),n.setStyle(t,i,r,o))}}function WE(n,e,t,i,r){let o=Ui(),s=i&2;try{Gr(-1),s&&e.length>$t&&BE(n,e,$t,!1);let a=s?mt.TemplateUpdateStart:mt.TemplateCreateStart;Tt(a,r,t),t(i,r)}finally{Gr(o);let a=s?mt.TemplateUpdateEnd:mt.TemplateCreateEnd;Tt(a,r,t)}}function $E(n,e,t){jD(n,e,t),(t.flags&64)===64&&WD(n,e,t)}function Jg(n,e,t=_i){let i=e.localNames;if(i!==null){let r=e.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?t(e,n):n[s];n[r++]=a}}}function VD(n,e,t,i){let o=i.get(SE,ME)||t===bi.ShadowDom||t===bi.ExperimentalIsolatedShadowDom,s=n.selectRootElement(e,o);return HD(s),s}function HD(n){zD(n)}var zD=()=>null;function GD(n,e,t,i,r,o){if(n.type&3){let s=_i(n,e);i=o!=null?o(i,n.value||"",t):i,r.setProperty(s,t,i)}else n.type&12}function jD(n,e,t){let i=t.directiveStart,r=t.directiveEnd;zr(t)&&xD(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||lE(t,e);let o=t.initialInputs;for(let s=i;s<r;s++){let a=n.data[s],c=rd(e,n,s,t);if(js(c,e),o!==null&&ZD(e,s-i,c,a,t,o),Vo(a)){let l=Fi(t.index,e);l[zt]=rd(e,n,s,t)}}}function WD(n,e,t){let i=t.directiveStart,r=t.directiveEnd,o=t.index,s=hx();try{Gr(o);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Uu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&$D(c,l)}}finally{Gr(-1),Uu(s)}}function $D(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function qD(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let o=t[r];fD(e,o.selectors,!1)&&(i??=[],Vo(o)?i.unshift(o):i.push(o))}return i}function XD(n,e,t,i,r,o){let s=_i(n,e);YD(e[Gt],s,o,n.value,t,i,r)}function YD(n,e,t,i,r,o,s){if(o==null)n.removeAttribute(e,r,t);else{let a=s==null?Ps(o):s(o,i||"",r);n.setAttribute(e,r,a,t)}}function ZD(n,e,t,i,r,o){let s=o[e];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];hg(i,t,c,l)}}function qE(n,e,t,i,r){let o=$t+t,s=e[Ve],a=r(s,e,n,i,t);e[o]=a,Vs(n,!0);let c=n.type===2;return c?(OE(e[Gt],a,n),(tx()===0||Pu(n))&&js(a,e),nx()):js(a,e),ju()&&(!c||!Vg(n))&&Yg(s,e,a,n),n}function XE(n){let e=n;return jm()?sx():(e=e.parent,Vs(e,!1)),e}function JD(n,e){let t=n[ur];if(!t)return;let i;try{i=t.get(Bi,null)}catch{i=null}i?.(e)}function YE(n,e,t,i,r){let o=n.inputs?.[i],s=n.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],u=s[c+1],d=e.data[l];hg(d,t[l],u,r),a=!0}if(o)for(let c of o){let l=t[c],u=e.data[c];hg(u,l,i,r),a=!0}return a}function KD(n,e){let t=Fi(e,n),i=t[Ve];QD(i,t);let r=t[gi];r!==null&&t[Ls]===null&&(t[Ls]=wE(r,t[ur])),Tt(mt.ComponentStart);try{Kg(i,t,t[zt])}finally{Tt(mt.ComponentEnd,t[zt])}}function QD(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Kg(n,e,t){Vu(e);try{let i=n.viewQuery;i!==null&&fg(1,i,t);let r=n.template;r!==null&&WE(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Li]?.finishViewCreation(n),n.staticContentQueries&&CE(n,e),n.staticViewQueries&&fg(2,n.viewQuery,t);let o=n.components;o!==null&&eI(e,o)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[We]&=-5,Hu()}}function eI(n,e){for(let t=0;t<e.length;t++)KD(n,e[t])}function Td(n,e,t,i){let r=Ne(null);try{let o=e.tView,a=n[We]&4096?4096:16,c=jg(n,o,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Ur]=l;let u=n[Li];return u!==null&&(c[Li]=u.createEmbeddedView(o)),Kg(o,c,t),c}finally{Ne(r)}}function pc(n,e){return!e||e.firstChild===null||gE(n)}function mc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let o=e[t.index];o!==null&&i.push(ci(o)),yi(o)&&ZE(o,i);let s=t.type;if(s&8)mc(n,e,t.child,i);else if(s&32){let a=$g(t,e),c;for(;c=a();)i.push(c)}else if(s&16){let a=jE(e,t);if(Array.isArray(a))i.push(...a);else{let c=Fr(e[Xn]);mc(c[Ve],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function ZE(n,e){for(let t=kt;t<n.length;t++){let i=n[t],r=i[Ve].firstChild;r!==null&&mc(i[Ve],i,r,e)}n[Vr]!==n[gi]&&e.push(n[Vr])}function JE(n){if(n[Nu]!==null){for(let e of n[Nu])e.impl.addSequence(e);n[Nu].length=0}}var KE=[];function tI(n){return n[Yn]??nI(n)}function nI(n){let e=KE.pop()??Object.create(rI);return e.lView=n,e}function iI(n){n.lView[Yn]!==n&&(n.lView=null,KE.push(n))}var rI=it(ae({},xo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Bs(n.lView)},consumerOnSignalRead(){this.lView[Yn]=this}});function oI(n){let e=n[Yn]??Object.create(sI);return e.lView=n,e}var sI=it(ae({},xo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Fr(n.lView);for(;e&&!QE(e[Ve]);)e=Fr(e);e&&Fm(e)},consumerOnSignalRead(){this.lView[Yn]=this}});function QE(n){return n.type!==2}function eb(n){if(n[Lr]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Lr])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[We]&8192)}}var aI=100;function tb(n,e=0){let i=n[vi].rendererFactory,r=!1;r||i.begin?.();try{cI(n,e)}finally{r||i.end?.()}}function cI(n,e){let t=Wm();try{Qa(!0),gg(n,e);let i=0;for(;cc(n);){if(i===aI)throw new Te(103,!1);i++,gg(n,1)}}finally{Qa(t)}}function lI(n,e,t,i){if(Ho(e))return;let r=e[We],o=!1,s=!1;Vu(e);let a=!0,c=null,l=null;o||(QE(n)?(l=tI(e),c=Eo(l)):zl()===null?(a=!1,l=oI(e),c=Eo(l)):e[Yn]&&(bo(e[Yn]),e[Yn]=null));try{Lm(e),ux(n.bindingStartIndex),t!==null&&WE(n,e,t,2,i);let u=(r&3)===3;if(!o)if(u){let h=n.preOrderCheckHooks;h!==null&&Zu(e,h,null)}else{let h=n.preOrderHooks;h!==null&&Ju(e,h,0,null),ng(e,0)}if(s||uI(e),eb(e),nb(e,0),n.contentQueries!==null&&CE(n,e),!o)if(u){let h=n.contentCheckHooks;h!==null&&Zu(e,h)}else{let h=n.contentHooks;h!==null&&Ju(e,h,1),ng(e,1)}fI(n,e);let d=n.components;d!==null&&rb(e,d,0);let f=n.viewQuery;if(f!==null&&fg(2,f,i),!o)if(u){let h=n.viewCheckHooks;h!==null&&Zu(e,h)}else{let h=n.viewHooks;h!==null&&Ju(e,h,2),ng(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Ru]){for(let h of e[Ru])h();e[Ru]=null}o||(JE(e),e[We]&=-73)}catch(u){throw o||Bs(e),u}finally{l!==null&&(bs(l,c),a&&iI(l)),Hu()}}function nb(n,e){for(let t=yE(n);t!==null;t=_E(t))for(let i=kt;i<t.length;i++){let r=t[i];ib(r,e)}}function uI(n){for(let e=yE(n);e!==null;e=_E(e)){if(!(e[We]&2))continue;let t=e[Bo];for(let i=0;i<t.length;i++){let r=t[i];Fm(r)}}}function dI(n,e,t){Tt(mt.ComponentStart);let i=Fi(e,n);try{ib(i,t)}finally{Tt(mt.ComponentEnd,i[zt])}}function ib(n,e){Lu(n)&&gg(n,e)}function gg(n,e){let i=n[Ve],r=n[We],o=n[Yn],s=!!(e===0&&r&16);if(s||=!!(r&64&&e===0),s||=!!(r&1024),s||=!!(o?.dirty&&Wa(o)),s||=!1,o&&(o.dirty=!1),n[We]&=-9217,s)lI(i,n,i.template,n[zt]);else if(r&8192){let a=Ne(null);try{eb(n),nb(n,1);let c=i.components;c!==null&&rb(n,c,1),JE(n)}finally{Ne(a)}}}function rb(n,e,t){for(let i=0;i<e.length;i++)dI(n,e[i],t)}function fI(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Gr(~r);else{let o=r,s=t[++i],a=t[++i];fx(s,o);let c=e[o];Tt(mt.HostBindingsUpdateStart,c);try{a(2,c)}finally{Tt(mt.HostBindingsUpdateEnd,c)}}}}finally{Gr(-1)}}function Qg(n,e){let t=Wm()?64:1088;for(n[vi].changeDetectionScheduler?.notify(e);n;){n[We]|=t;let i=Fr(n);if(Us(n)&&!i)return n;n=i}return null}function ob(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function sb(n,e){let t=kt+e;if(t<n.length)return n[t]}function Dd(n,e,t,i=!0){let r=e[Ve];if(hI(r,e,n,t),i){let s=mg(t,n),a=e[Gt],c=a.parentNode(n[Vr]);c!==null&&TD(r,n[ai],a,e,c,s)}let o=e[Ls];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function ab(n,e){let t=gc(n,e);return t!==void 0&&wd(t[Ve],t),t}function gc(n,e){if(n.length<=kt)return;let t=kt+e,i=n[t];if(i){let r=i[Ur];r!==null&&r!==n&&Xg(r,i),e>0&&(n[t-1][si]=i[si]);let o=rc(n,kt+e);CD(i[Ve],i);let s=o[Li];s!==null&&s.detachView(o[Ve]),i[Zt]=null,i[si]=null,i[We]&=-129}return i}function hI(n,e,t,i){let r=kt+i,o=t.length;i>0&&(t[r-1][si]=e),i<o-kt?(e[si]=t[r],bm(t,kt+i,e)):(t.push(e),e[si]=null),e[Zt]=t;let s=e[Ur];s!==null&&t!==s&&cb(s,e);let a=e[Li];a!==null&&a.insertView(n),Fu(e),e[We]|=128}function cb(n,e){let t=n[Bo],i=e[Zt];if(Hr(i))n[We]|=2;else{let r=i[Zt][Xn];e[Xn]!==r&&(n[We]|=2)}t===null?n[Bo]=[e]:t.push(e)}var Wr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Ve];return mc(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[zt]}set context(e){this._lView[zt]=e}get destroyed(){return Ho(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Zt];if(yi(e)){let t=e[ac],i=t?t.indexOf(this):-1;i>-1&&(gc(e,i),rc(t,i))}this._attachedToViewContainer=!1}wd(this._lView[Ve],this._lView)}onDestroy(e){km(this._lView,e)}markForCheck(){Qg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[We]&=-129}reattach(){Fu(this._lView),this._lView[We]|=128}detectChanges(){this._lView[We]|=1024,tb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Te(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Us(this._lView),t=this._lView[Ur];t!==null&&!e&&Xg(t,this._lView),GE(this._lView[Ve],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Te(902,!1);this._appRef=e;let t=Us(this._lView),i=this._lView[Ur];i!==null&&!t&&cb(i,this._lView),Fu(this._lView)}};var Ws=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=pI;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let o=Td(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Wr(o)}}return n})();function pI(){return ev(li(),ot())}function ev(n,e){return n.type&4?new Ws(e,n,Ys(n,e)):null}function Id(n,e,t,i,r){let o=n.data[e];if(o===null)o=mI(n,e,t,i,r),dx()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=i,o.attrs=r;let s=ox();o.injectorIndex=s===null?-1:s.injectorIndex}return Vs(o,!0),o}function mI(n,e,t,i,r){let o=Gm(),s=jm(),a=s?o:o&&o.parent,c=n.data[e]=vI(n,a,t,e,i,r);return gI(n,c,o,s),c}function gI(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function vI(n,e,t,i,r,o){let s=e?e.injectorIndex:-1,a=0;return rx()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function yI(n){let e=n[Im]??[],i=n[Zt][Gt],r=[];for(let o of e)o.data[bE]!==void 0?r.push(o):_I(o,i);n[Im]=r}function _I(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[EE];for(;t<r;){let o=i.nextSibling;PE(e,i,!1),i=o,t++}}}var xI=()=>null,EI=()=>null;function vg(n,e){return xI(n,e)}function lb(n,e,t){return EI(n,e,t)}var ub=class{},Ad=class{},yg=class{resolveComponentFactory(e){throw new Te(917,!1)}},Mc=class{static NULL=new yg},jo=class{};var db=(()=>{class n{static \u0275prov=Pe({token:n,providedIn:"root",factory:()=>null})}return n})();var Qu={},_g=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Qu,i);return r!==Qu||t===Qu?r:this.parentInjector.get(e,t,i)}};function cd(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,o=0;if(e!==null)for(let s=0;s<e.length;s++){let a=e[s];if(typeof a=="number")o=a;else if(o==1)r=Mu(r,a);else if(o==2){let c=a,l=e[++s];i=Mu(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Rd(n,e=0){let t=ot();if(t===null)return je(n,e);let i=li();return hE(i,t,Bn(n),e)}function bI(n,e,t,i,r){let o=i===null?null:{"":-1},s=r(n,t);if(s!==null){let a=s,c=null,l=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(s);break}wI(n,e,t,a,o,c,l)}o!==null&&i!==null&&MI(t,i,o)}function MI(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let o=t[e[r+1]];if(o==null)throw new Te(-301,!1);i.push(e[r],o)}}function SI(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function wI(n,e,t,i,r,o,s){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&Vo(h)&&(c=h,SI(n,t,f)),BT(lE(t,e),n,h.type)}RI(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=UE(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=_d(t.mergedAttrs,h.hostAttrs),TI(n,t,e,d,h),AI(d,h,r),s!==null&&s.has(h)){let[y,g]=s.get(h);t.directiveToIndex.set(h.type,[d,y+t.directiveStart,g+t.directiveStart])}else(o===null||!o.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let p=h.type.prototype;!l&&(p.ngOnChanges||p.ngOnInit||p.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(p.ngOnChanges||p.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}CI(n,t,o)}function CI(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Hx(0,e,r,i),Hx(1,e,r,i),Gx(e,i,!1);else{let o=t.get(r);zx(0,e,o,i),zx(1,e,o,i),Gx(e,i,!0)}}}function Hx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;n===0?s=e.inputs??={}:s=e.outputs??={},s[o]??=[],s[o].push(i),fb(e,o)}}function zx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),fb(e,s)}}function fb(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function Gx(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:o}=n;if(i===null||!t&&r===null||t&&o===null||zg(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){s??=[],s.push(c,i[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){s??=[],s.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(s)}function TI(n,e,t,i,r){n.data[i]=r;let o=r.factory||(r.factory=Or(r.type,!0)),s=new hc(o,Vo(r),Rd,null);n.blueprint[i]=s,t[i]=s,DI(n,e,i,UE(n,t,r.hostVars,zn),r)}function DI(n,e,t,i,r){let o=r.hostBindings;if(o){let s=n.hostBindingOpCodes;s===null&&(s=n.hostBindingOpCodes=[]);let a=~e.index;II(s)!=a&&s.push(a),s.push(t,i,o)}}function II(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function AI(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Vo(e)&&(t[""]=n)}}function RI(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function hb(n,e,t,i,r,o,s,a){let c=e[Ve],l=c.consts,u=ki(l,s),d=Id(c,n,t,i,u);return o&&bI(c,e,d,ki(l,a),r),d.mergedAttrs=_d(d.mergedAttrs,d.attrs),d.attrs!==null&&cd(d,d.attrs,!1),d.mergedAttrs!==null&&cd(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function pb(n,e){DT(n,e),Am(e)&&n.queries.elementEnd(e)}function NI(n,e,t,i,r,o){let s=e.consts,a=ki(s,r),c=Id(e,n,t,i,a);if(c.mergedAttrs=_d(c.mergedAttrs,c.attrs),o!=null){let l=ki(s,o);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&cd(c,c.attrs,!1),c.mergedAttrs!==null&&cd(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function PI(n,e,t){return n[e]=t}function Hi(n,e,t){if(t===zn)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function OI(n,e,t,i){let r=Hi(n,e,t);return Hi(n,e+1,i)||r}function LI(n,e,t){return function i(r){let o=zr(n)?Fi(n.index,e):e;Qg(o,5);let s=e[zt],a=jx(e,s,t,r),c=i.__ngNextListenerFn__;for(;c;)a=jx(e,s,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function jx(n,e,t,i){let r=Ne(null);try{return Tt(mt.OutputStart,e,t),t(i)!==!1}catch(o){return JD(n,o),!1}finally{Tt(mt.OutputEnd,e,t),Ne(r)}}function FI(n,e,t,i,r,o,s,a){let c=Pu(n),l=!1,u=null;if(!i&&c&&(u=UI(e,t,o,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,l=!0}else{let d=_i(n,t),f=i?i(d):d;QT(t,f,o,a);let h=r.listen(f,o,a);if(!kI(o)){let p=i?y=>i(ci(y[n.index])):n.index;BI(p,e,t,o,a,h,!1)}}return l}function kI(n){return n.startsWith("animation")||n.startsWith("transition")}function UI(n,e,t,i){let r=n.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===t&&r[o+1]===i){let a=e[Fs],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function BI(n,e,t,i,r,o,s){let a=e.firstCreatePass?Bm(e):null,c=Um(t),l=c.length;c.push(r,o),a&&a.push(i,n,l,(l+1)*(s?-1:1))}var xg=Symbol("BINDING");function mb(n){return n.debugInfo?.className||n.type.name||null}var ld=class extends Mc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=kr(e);return new $s(t,this.ngModule)}};function VI(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],o={propName:t,templateName:e,isSignal:(i&Sd.SignalBased)!==0};return r&&(o.transform=r),o})}function HI(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function zI(n,e,t){let i=e instanceof Yt?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new _g(t,i):t}function GI(n){let e=n.get(jo,null);if(e===null)throw new Te(407,!1);let t=n.get(db,null),i=n.get(Ro,null),r=n.get(Js,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function jI(n,e){let t=gb(n);return RE(e,t,t==="svg"?Rm:t==="math"?Y_:null)}function gb(n){return(n.selectors[0][0]||"div").toLowerCase()}var $s=class extends Ad{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=VI(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=HI(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=gD(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,o,s){Tt(mt.DynamicComponentStart);let a=Ne(null);try{let c=this.componentDef,l=zI(c,r||this.ngModule,e),u=GI(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(mb(c),()=>this.createComponentRef(u,l,t,i,o,s)):this.createComponentRef(u,l,t,i,o,s)}finally{Ne(a)}}createComponentRef(e,t,i,r,o,s){let a=this.componentDef,c=WI(r,a,s,o),l=e.rendererFactory.createRenderer(null,a),u=r?VD(l,r,a.encapsulation,t):jI(a,l),d=s?.some(Wx)||o?.some(p=>typeof p!="function"&&p.bindings.some(Wx)),f=jg(null,c,null,512|kE(a),null,null,e,l,t,null,wE(u,t,!0));f[$t]=u,Vu(f);let h=null;try{let p=hb($t,f,2,"#host",()=>c.directiveRegistry,!0,0);OE(l,u,p),js(u,f),$E(c,f,p),TE(c,p,f),pb(c,p),i!==void 0&&qI(p,this.ngContentSelectors,i),h=Fi(p.index,f),f[zt]=h[zt],Kg(c,f,null)}catch(p){throw h!==null&&dg(h),dg(f),p}finally{Tt(mt.DynamicComponentEnd),Hu()}return new ud(this.componentType,f,!!d)}};function WI(n,e,t,i){let r=n?["ng-version","21.2.6"]:vD(e.selectors[0]),o=null,s=null,a=0;if(t)for(let u of t)a+=u[xg].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let f of d.bindings){a+=f[xg].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(o??=[]).push(f)),f.update&&(f.targetIdx=h,(s??=[]).push(f))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,f=_m(d);c.push(f)}return Gg(0,null,$I(o,s),1,a,c,null,null,null,[r],null)}function $I(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function Wx(n){let e=n[xg].kind;return e==="input"||e==="twoWay"}var ud=class extends ub{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Ou(t[Ve],$t),this.location=Ys(this._tNode,t),this.instance=Fi(this._tNode.index,t)[zt],this.hostView=this.changeDetectorRef=new Wr(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,o=YE(i,r[Ve],r,e,t);this.previousInputValues.set(e,t);let s=Fi(i.index,r);Qg(s,1)}get injector(){return new zo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function qI(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let o=t[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Xo=(()=>{class n{static __NG_ELEMENT_ID__=XI}return n})();function XI(){let n=li();return vb(n,ot())}var Eg=class n extends Xo{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Ys(this._hostTNode,this._hostLView)}get injector(){return new zo(this._hostTNode,this._hostLView)}get parentInjector(){let e=kg(this._hostTNode,this._hostLView);if(sE(e)){let t=nd(e,this._hostLView),i=td(e),r=t[Ve].data[i+8];return new zo(r,t)}else return new zo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=$x(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-kt}createEmbeddedView(e,t,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=vg(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,r,pc(this._hostTNode,s)),a}createComponent(e,t,i,r,o,s,a){let c=e&&!MT(e),l;if(c)l=t;else{let g=t||{};l=g.index,i=g.injector,r=g.projectableNodes,o=g.environmentInjector||g.ngModuleRef,s=g.directives,a=g.bindings}let u=c?e:new $s(kr(e)),d=i||this.parentInjector;if(!o&&u.ngModule==null){let m=(c?d:this.parentInjector).get(Yt,null);m&&(o=m)}let f=kr(u.componentType??{}),h=vg(this._lContainer,f?.id??null),p=h?.firstChild??null,y=u.create(d,r,p,o,s,a);return this.insertImpl(y.hostView,l,pc(this._hostTNode,h)),y}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(J_(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Zt],l=new n(c,c[ai],c[Zt]);l.detach(l.indexOf(e))}}let o=this._adjustIndex(t),s=this._lContainer;return Dd(s,r,o,i),e.attachToViewContainerRef(),bm(og(s),o,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=$x(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=gc(this._lContainer,t);i&&(rc(og(this._lContainer),t),wd(i[Ve],i))}detach(e){let t=this._adjustIndex(e,-1),i=gc(this._lContainer,t);return i&&rc(og(this._lContainer),t)!=null?new Wr(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function $x(n){return n[ac]}function og(n){return n[ac]||(n[ac]=[])}function vb(n,e){let t,i=e[n.index];return yi(i)?t=i:(t=ob(i,e,null,n),e[n.index]=t,Wg(e,t)),ZI(t,e,n,i),new Eg(t,n,e)}function YI(n,e){let t=n[Gt],i=t.createComment(""),r=_i(e,n),o=t.parentNode(r);return ad(t,o,i,t.nextSibling(r),!1),i}var ZI=QI,JI=()=>!1;function KI(n,e,t){return JI(n,e,t)}function QI(n,e,t,i){if(n[Vr])return;let r;t.type&8?r=ci(i):r=YI(e,t),n[Vr]=r}var bg=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Mg=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let o=0;o<i;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)tv(e,t).matches!==null&&this.queries[t].setDirty()}},Sg=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=aA(e):this.predicate=e}},wg=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,o=this.getByIndex(i).embeddedTView(e,r);o&&(o.indexInDeclarationView=i,t!==null?t.push(o):t=[o])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Cg=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(e,t,eA(t,o)),this.matchTNodeWithReadOption(e,t,Ku(t,e,o,!1,!1))}else i===Ws?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Ku(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Zs||r===Xo||r===Ws&&t.type&4)this.addMatch(t.index,-2);else{let o=Ku(t,e,r,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function eA(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function tA(n,e){return n.type&11?Ys(n,e):n.type&4?ev(n,e):null}function nA(n,e,t,i){return t===-1?tA(e,n):t===-2?iA(n,e,i):rd(n,n[Ve],t,e)}function iA(n,e,t){if(t===Zs)return Ys(e,n);if(t===Ws)return ev(e,n);if(t===Xo)return vb(e,n)}function yb(n,e,t,i){let r=e[Li].queries[i];if(r.matches===null){let o=n.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let u=o[l];a.push(nA(e,u,s[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Tg(n,e,t,i){let r=n.queries.getByIndex(t),o=r.matches;if(o!==null){let s=yb(n,e,r,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],u=e[-c];for(let d=kt;d<u.length;d++){let f=u[d];f[Ur]===f[Zt]&&Tg(f[Ve],f,l,i)}if(u[Bo]!==null){let d=u[Bo];for(let f=0;f<d.length;f++){let h=d[f];Tg(h[Ve],h,l,i)}}}}}return i}function rA(n,e){return n[Li].queries[e].queryList}function oA(n,e,t){let i=new od((t&4)===4);return ex(n,e,i,i.destroy),(e[Li]??=new Mg).queries.push(new bg(i))-1}function sA(n,e,t){let i=Rn();return i.firstCreatePass&&(cA(i,new Sg(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),oA(i,ot(),e)}function aA(n){return n.split(",").map(e=>e.trim())}function cA(n,e,t){n.queries===null&&(n.queries=new wg),n.queries.track(new Cg(e,t))}function tv(n,e){return n.queries.getByIndex(e)}function lA(n,e){let t=n[Ve],i=tv(t,e);return i.crossesNgTemplate?Tg(t,n,e,[]):yb(t,n,i,e)}var qs=class{},Nd=class{};var dd=class extends qs{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new ld(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let o=ym(e);this._bootstrapComponents=LE(o.bootstrap),this._r3Injector=Zm(e,t,[{provide:qs,useValue:this},{provide:Mc,useValue:this.componentFactoryResolver},...i],nc(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},fd=class extends Nd{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new dd(this.moduleType,e,[])}};var vc=class extends qs{injector;componentFactoryResolver=new ld(this);instance=null;constructor(e){super();let t=new Ao([...e.providers,{provide:qs,useValue:this},{provide:Mc,useValue:this.componentFactoryResolver}],e.parent||sc(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Sc(n,e,t=null){return new vc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var uA=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=wm(!1,t.type),r=i.length>0?Sc([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Pe({token:n,providedIn:"environment",factory:()=>new n(je(Yt))})}return n})();function Zn(n){return _c(()=>{let e=_b(n),t=it(ae({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Ug.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(uA).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||bi.Emulated,styles:n.styles||Vn,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&qo("NgStandalone"),xb(t);let i=n.dependencies;return t.directiveDefs=qx(i,dA),t.pipeDefs=qx(i,L_),t.id=pA(t),t})}function dA(n){return kr(n)||_m(n)}function Ks(n){return _c(()=>({type:n.type,bootstrap:n.bootstrap||Vn,declarations:n.declarations||Vn,imports:n.imports||Vn,exports:n.exports||Vn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function fA(n,e){if(n==null)return Lo;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=Sd.None,c=null),t[o]=[i,a,c],e[o]=s}return t}function hA(n){if(n==null)return Lo;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function Pd(n){return _c(()=>{let e=_b(n);return xb(e),e})}function Od(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function _b(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||Lo,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Vn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:fA(n.inputs,e),outputs:hA(n.outputs),debugInfo:null}}function xb(n){n.features?.forEach(e=>e(n))}function qx(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let o=e(r);o!==null&&i.push(o)}return i}:null}function pA(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let o of i.join("|"))e=Math.imul(31,e)+o.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function mA(n,e,t,i,r,o,s,a){if(t.firstCreatePass){n.mergedAttrs=_d(n.mergedAttrs,n.attrs);let u=n.tView=Gg(2,n,r,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Vs(n,!1);let c=gA(t,e,n,i);ju()&&Yg(t,e,c,n),js(c,e);let l=ob(c,e,c,n);e[i+$t]=l,Wg(e,l),KI(l,n,e)}function hd(n,e,t,i,r,o,s,a,c,l,u){let d=t+$t,f;if(e.firstCreatePass){if(f=Id(e,d,4,s||null,a||null),l!=null){let h=ki(e.consts,l);f.localNames=[];for(let p=0;p<h.length;p+=2)f.localNames.push(h[p],-1)}}else f=e.data[d];return mA(f,n,e,t,i,r,o,c),l!=null&&Jg(n,f,u),f}var gA=vA;function vA(n,e,t,i){return Wu(!0),e[Gt].createComment("")}var nv=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();var iv=new Le("");function wc(n){return!!n&&typeof n.then=="function"}function Eb(n){return!!n&&typeof n.subscribe=="function"}var rv=new Le("");function Ld(n){return lr([{provide:rv,multi:!0,useValue:n}])}var ov=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=J(rv,{optional:!0})??[];injector=J(In);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let o=vn(this.injector,r);if(wc(o))t.push(o);else if(Eb(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Fd=new Le("");function bb(){Lp(()=>{let n="";throw new Te(600,n)})}function Mb(n){return n.isBoundToModule}var yA=10;var pr=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=J(Bi);afterRenderManager=J(VE);zonelessEnabled=J(lc);rootEffectScheduler=J(qu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Ht;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=J(jr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Mt(t=>!t))}constructor(){J(Js,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=J(Yt);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=In.NULL){return this._injector.get(An).run(()=>{Tt(mt.BootstrapComponentStart);let s=t instanceof Ad;if(!this._injector.get(ov).done){let p="";throw new Te(405,p)}let c;s?c=t:c=this._injector.get(Mc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=Mb(c)?void 0:this._injector.get(qs),u=i||c.selector,d=c.create(r,[],u,l),f=d.location.nativeElement,h=d.injector.get(iv,null);return h?.registerApplication(f),d.onDestroy(()=>{this.detachView(d.hostView),fc(this.components,d),h?.unregisterApplication(f)}),this._loadComponent(d),Tt(mt.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Tt(mt.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(qg.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Tt(mt.ChangeDetectionEnd),new Te(101,!1);let t=Ne(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ne(t),this.afterTick.next(),Tt(mt.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(jo,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<yA;){Tt(mt.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Tt(mt.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!cc(r))continue;let o=i&&!this.zonelessEnabled?0:1;tb(r,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>cc(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;fc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(Fd,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>fc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Te(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function fc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function kd(n,e,t,i){let r=ot(),o=Hs();if(Hi(r,o,e)){let s=Rn(),a=Ym();XD(a,r,n,e,t,i)}return kd}var Dg=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(e,t){this.attach(t,this.detach(e))}};function sg(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function _A(n,e,t,i){let r,o,s=0,a=n.length-1,c=void 0;if(Array.isArray(e)){Ne(i);let l=e.length-1;for(Ne(null);s<=a&&s<=l;){let u=n.at(s),d=e[s],f=sg(s,u,s,d,t);if(f!==0){f<0&&n.updateValue(s,d),s++;continue}let h=n.at(a),p=e[l],y=sg(a,h,l,p,t);if(y!==0){y<0&&n.updateValue(a,p),a--,l--;continue}let g=t(s,u),m=t(a,h),E=t(s,d);if(Object.is(E,m)){let S=t(l,p);Object.is(S,g)?(n.swap(s,a),n.updateValue(a,p),l--,a--):n.move(a,s),n.updateValue(s,d),s++;continue}if(r??=new pd,o??=Yx(n,s,a,t),Ig(n,r,s,E))n.updateValue(s,d),s++,a++;else if(o.has(E))r.set(g,n.detach(s)),a--;else{let S=n.create(s,e[s]);n.attach(s,S),s++,a++}}for(;s<=l;)Xx(n,r,t,s,e[s]),s++}else if(e!=null){Ne(i);let l=e[Symbol.iterator]();Ne(null);let u=l.next();for(;!u.done&&s<=a;){let d=n.at(s),f=u.value,h=sg(s,d,s,f,t);if(h!==0)h<0&&n.updateValue(s,f),s++,u=l.next();else{r??=new pd,o??=Yx(n,s,a,t);let p=t(s,f);if(Ig(n,r,s,p))n.updateValue(s,f),s++,a++,u=l.next();else if(!o.has(p))n.attach(s,n.create(s,f)),s++,a++,u=l.next();else{let y=t(s,d);r.set(y,n.detach(s)),a--}}}for(;!u.done;)Xx(n,r,t,n.length,u.value),u=l.next()}for(;s<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function Ig(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function Xx(n,e,t,i,r){if(Ig(n,e,i,t(i,r)))n.updateValue(i,r);else{let o=n.create(i,r);n.attach(i,o)}}function Yx(n,e,t,i){let r=new Set;for(let o=e;o<=t;o++)r.add(i(o,n.at(o)));return r}var pd=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function Nn(n,e,t,i,r,o,s,a){qo("NgControlFlow");let c=ot(),l=Rn(),u=ki(l.consts,o);return hd(c,l,n,e,t,i,r,u,256,s,a),sv}function sv(n,e,t,i,r,o,s,a){qo("NgControlFlow");let c=ot(),l=Rn(),u=ki(l.consts,o);return hd(c,l,n,e,t,i,r,u,512,s,a),sv}function Pn(n,e){qo("NgControlFlow");let t=ot(),i=Hs(),r=t[i]!==zn?t[i]:-1,o=r!==-1?md(t,$t+r):void 0,s=0;if(Hi(t,i,n)){let a=Ne(null);try{if(o!==void 0&&ab(o,s),n!==-1){let c=$t+n,l=md(t,c),u=Pg(t[Ve],c),d=lb(l,u,t),f=Td(t,u,e,{dehydratedView:d});Dd(l,f,s,pc(u,d))}}finally{Ne(a)}}else if(o!==void 0){let a=sb(o,s);a!==void 0&&(a[zt]=e)}}var Ag=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-kt}};function Cc(n,e){return e}var Rg=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function $r(n,e,t,i,r,o,s,a,c,l,u,d,f){qo("NgControlFlow");let h=ot(),p=Rn(),y=c!==void 0,g=ot(),m=a?s.bind(g[Xn][zt]):s,E=new Rg(y,m);g[$t+n]=E,hd(h,p,n+1,e,t,i,r,ki(p.consts,o),256),y&&hd(h,p,n+2,c,l,u,d,ki(p.consts,f),512)}var Ng=class extends Dg{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-kt}at(e){return this.getLView(e)[zt].$implicit}attach(e,t){let i=t[Ls];this.needsIndexUpdate||=e!==this.length,Dd(this.lContainer,t,e,pc(this.templateTNode,i)),xA(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,EA(this.lContainer,e),bA(this.lContainer,e)}create(e,t){let i=vg(this.lContainer,this.templateTNode.tView.ssrId);return Td(this.hostLView,this.templateTNode,new Ag(this.lContainer,t,e),{dehydratedView:i})}destroy(e){wd(e[Ve],e)}updateValue(e,t){this.getLView(e)[zt].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[zt].$index=e}getLView(e){return MA(this.lContainer,e)}};function qr(n){let e=Ne(null),t=Ui();try{let i=ot(),r=i[Ve],o=i[t],s=t+1,a=md(i,s);if(o.liveCollection===void 0){let l=Pg(r,s);o.liveCollection=new Ng(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(_A(c,n,o.trackByFn,e),c.updateIndexes(),o.hasEmptyBlock){let l=Hs(),u=c.length===0;if(Hi(i,l,u)){let d=t+2,f=md(i,d);if(u){let h=Pg(r,d),p=lb(f,h,i),y=Td(i,h,void 0,{dehydratedView:p});Dd(f,y,0,pc(h,p))}else r.firstUpdatePass&&yI(f),ab(f,0)}}}finally{Ne(e)}}function md(n,e){return n[e]}function xA(n,e){if(n.length<=kt)return;let t=kt+e,i=n[t],r=i?i[Br]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[ur];SD(o,r),Go.delete(i[dr]),r.detachedLeaveAnimationFns=void 0}}function EA(n,e){if(n.length<=kt)return;let t=kt+e,i=n[t],r=i?i[Br]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function bA(n,e){return gc(n,e)}function MA(n,e){return sb(n,e)}function Pg(n,e){return Ou(n,e)}function Og(n,e,t,i,r){YE(e,n,t,r?"class":"style",i)}function Wo(n,e,t,i){let r=ot(),o=r[Ve],s=n+$t,a=o.firstCreatePass?hb(s,r,2,e,qD,ix(),t,i):o.data[s];if(zr(a)){let c=r[vi].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(mb(l),()=>(Zx(n,e,r,a,i),Wo))}}return Zx(n,e,r,a,i),Wo}function Zx(n,e,t,i,r){if(qE(i,t,n,e,Sb),Pu(i)){let o=t[Ve];$E(o,t,i),TE(o,i,t)}r!=null&&Jg(t,i)}function Tc(){let n=Rn(),e=li(),t=XE(e);return n.firstCreatePass&&pb(n,t),Hm(t)&&zm(),Vm(),t.classesWithoutHost!=null&&AT(t)&&Og(n,t,ot(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&RT(t)&&Og(n,t,ot(),t.stylesWithoutHost,!1),Tc}function Yo(n,e,t,i){return Wo(n,e,t,i),Tc(),Yo}function te(n,e,t,i){let r=ot(),o=r[Ve],s=n+$t,a=o.firstCreatePass?NI(s,o,2,e,t,i):o.data[s];return qE(a,r,n,e,Sb),i!=null&&Jg(r,a),te}function le(){let n=li(),e=XE(n);return Hm(e)&&zm(),Vm(),le}function Gn(n,e,t,i){return te(n,e,t,i),le(),Gn}var Sb=(n,e,t,i,r)=>(Wu(!0),RE(e[Gt],i,_x()));function yn(){return ot()}function Jn(n,e,t){let i=ot(),r=Hs();if(Hi(i,r,e)){let o=Rn(),s=Ym();GD(s,i,n,e,i[Gt],t)}return Jn}var Dc="en-US";var SA=Dc;function wb(n){typeof n=="string"&&(SA=n.toLowerCase().replace(/_/g,"-"))}function dt(n,e,t){let i=ot(),r=Rn(),o=li();return(o.type&3||t)&&FI(o,r,i,t,i[Gt],n,e,LI(o,i,e)),dt}function Ie(n=1){return yx(n)}function Ud(n,e,t){return sA(n,e,t),Ud}function av(n){let e=ot(),t=Rn(),i=$m();Bu(i+1);let r=tv(t,i);if(n.dirty&&Z_(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let o=lA(e,i);n.reset(o,WT),n.notifyOnChanges()}return!0}return!1}function cv(){return rA(ot(),$m())}function Zo(n){let e=ax();return Pm(e,$t+n)}function Yu(n,e){return n<<17|e<<2}function $o(n){return n>>17&32767}function wA(n){return(n&2)==2}function CA(n,e){return n&131071|e<<17}function Lg(n){return n|2}function Xs(n){return(n&131068)>>2}function ag(n,e){return n&-131069|e<<2}function TA(n){return(n&1)===1}function Fg(n){return n|1}function DA(n,e,t,i,r,o){let s=o?e.classBindings:e.styleBindings,a=$o(s),c=Xs(s);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Os(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let f=$o(n[a+1]);n[i+1]=Yu(f,a),f!==0&&(n[f+1]=ag(n[f+1],i)),n[a+1]=CA(n[a+1],i)}else n[i+1]=Yu(a,0),a!==0&&(n[a+1]=ag(n[a+1],i)),a=i;else n[i+1]=Yu(c,0),a===0?a=i:n[c+1]=ag(n[c+1],i),c=i;l&&(n[i+1]=Lg(n[i+1])),Jx(n,u,i,!0),Jx(n,u,i,!1),IA(e,u,n,i,o),s=Yu(a,c),o?e.classBindings=s:e.styleBindings=s}function IA(n,e,t,i,r){let o=r?n.residualClasses:n.residualStyles;o!=null&&typeof e=="string"&&Os(o,e)>=0&&(t[i+1]=Fg(t[i+1]))}function Jx(n,e,t,i){let r=n[t+1],o=e===null,s=i?$o(r):Xs(r),a=!1;for(;s!==0&&(a===!1||o);){let c=n[s],l=n[s+1];AA(c,e)&&(a=!0,n[s+1]=i?Fg(l):Lg(l)),s=i?$o(l):Xs(l)}a&&(n[t+1]=i?Lg(r):Fg(r))}function AA(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Os(n,e)>=0:!1}var Ei={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function RA(n){return n.substring(Ei.key,Ei.keyEnd)}function NA(n){return PA(n),Cb(n,Tb(n,0,Ei.textEnd))}function Cb(n,e){let t=Ei.textEnd;return t===e?-1:(e=Ei.keyEnd=OA(n,Ei.key=e,t),Tb(n,e,t))}function PA(n){Ei.key=0,Ei.keyEnd=0,Ei.value=0,Ei.valueEnd=0,Ei.textEnd=n.length}function Tb(n,e,t){for(;e<t&&n.charCodeAt(e)<=32;)e++;return e}function OA(n,e,t){for(;e<t&&n.charCodeAt(e)>32;)e++;return e}function zi(n,e){return FA(n,e,null,!0),zi}function mr(n){kA(GA,LA,n,!0)}function LA(n,e){for(let t=NA(e);t>=0;t=Cb(e,t))Iu(n,RA(e),!0)}function FA(n,e,t,i){let r=ot(),o=Rn(),s=ku(2);if(o.firstUpdatePass&&Ib(o,n,s,i),e!==zn&&Hi(r,s,e)){let a=o.data[Ui()];Ab(o,a,r,r[Gt],n,r[s+1]=WA(e,t),i,s)}}function kA(n,e,t,i){let r=Rn(),o=ku(2);r.firstUpdatePass&&Ib(r,null,o,i);let s=ot();if(t!==zn&&Hi(s,o,t)){let a=r.data[Ui()];if(Rb(a,i)&&!Db(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=Mu(c,t||"")),Og(r,a,s,t,i)}else jA(r,a,s,s[Gt],s[o+1],s[o+1]=zA(n,e,t),i,o)}}function Db(n,e){return e>=n.expandoStartIndex}function Ib(n,e,t,i){let r=n.data;if(r[t+1]===null){let o=r[Ui()],s=Db(n,t);Rb(o,i)&&e===null&&!s&&(e=!1),e=UA(r,o,e,i),DA(r,o,e,t,s,i)}}function UA(n,e,t,i){let r=px(n),o=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=cg(null,n,e,t,i),t=yc(t,e.attrs,i),o=null);else{let s=e.directiveStylingLast;if(s===-1||n[s]!==r)if(t=cg(r,n,e,t,i),o===null){let c=BA(n,e,i);c!==void 0&&Array.isArray(c)&&(c=cg(null,n,e,c[1],i),c=yc(c,e.attrs,i),VA(n,e,i,c))}else o=HA(n,e,i)}return o!==void 0&&(i?e.residualClasses=o:e.residualStyles=o),t}function BA(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Xs(i)!==0)return n[$o(i)]}function VA(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[$o(r)]=i}function HA(n,e,t){let i,r=e.directiveEnd;for(let o=1+e.directiveStylingLast;o<r;o++){let s=n[o].hostAttrs;i=yc(i,s,t)}return yc(i,e.attrs,t)}function cg(n,e,t,i,r){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=e[a],i=yc(i,o.hostAttrs,r),o!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function yc(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let o=0;o<e.length;o++){let s=e[o];typeof s=="number"?r=s:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Iu(n,s,t?!0:e[++o]))}return n===void 0?null:n}function zA(n,e,t){if(t==null||t==="")return Vn;let i=[],r=Md(t);if(Array.isArray(r))for(let o=0;o<r.length;o++)n(i,r[o],!0);else if(r instanceof Set)for(let o of r)n(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&n(i,o,r[o]);else typeof r=="string"&&e(i,r);return i}function GA(n,e,t){let i=String(e);i!==""&&!i.includes(" ")&&Iu(n,i,t)}function jA(n,e,t,i,r,o,s,a){r===zn&&(r=Vn);let c=0,l=0,u=0<r.length?r[0]:null,d=0<o.length?o[0]:null;for(;u!==null||d!==null;){let f=c<r.length?r[c+1]:void 0,h=l<o.length?o[l+1]:void 0,p=null,y;u===d?(c+=2,l+=2,f!==h&&(p=d,y=h)):d===null||u!==null&&u<d?(c+=2,p=u):(l+=2,p=d,y=h),p!==null&&Ab(n,e,t,i,p,y,s,a),u=c<r.length?r[c]:null,d=l<o.length?o[l]:null}}function Ab(n,e,t,i,r,o,s,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=TA(l)?Kx(c,e,t,r,Xs(l),s):void 0;if(!gd(u)){gd(o)||wA(l)&&(o=Kx(c,null,t,r,a,s));let d=Nm(Ui(),t);BD(i,s,d,r,o)}}function Kx(n,e,t,i,r,o){let s=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,f=t[r+1];f===zn&&(f=d?Vn:void 0);let h=d?Au(f,i):u===i?f:void 0;if(l&&!gd(h)&&(h=Au(c,i)),gd(h)&&(a=h,s))return a;let p=n[r+1];r=s?$o(p):Xs(p)}if(e!==null){let c=o?e.residualClasses:e.residualStyles;c!=null&&(a=Au(c,i))}return a}function gd(n){return n!==void 0}function WA(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=nc(Md(n)))),n}function Rb(n,e){return(n.flags&(e?8:16))!==0}function me(n,e=""){let t=ot(),i=Rn(),r=n+$t,o=i.firstCreatePass?Id(i,r,1,e,null):i.data[r],s=$A(i,t,o,e);t[r]=s,ju()&&Yg(i,t,s,o),Vs(o,!1)}var $A=(n,e,t,i)=>(Wu(!0),nD(e[Gt],i));function Nb(n,e,t,i=""){return Hi(n,Hs(),t)?e+Ps(t)+i:zn}function qA(n,e,t,i,r,o=""){let s=lx(),a=OI(n,s,t,r);return ku(2),a?e+Ps(t)+i+Ps(r)+o:zn}function rn(n){return Gi("",n),rn}function Gi(n,e,t){let i=ot(),r=Nb(i,n,e,t);return r!==zn&&Pb(i,Ui(),r),Gi}function Bd(n,e,t,i,r){let o=ot(),s=qA(o,n,e,t,i,r);return s!==zn&&Pb(o,Ui(),s),Bd}function Pb(n,e,t){let i=Nm(e,n);iD(n[Gt],i,t)}function Xr(n,e,t=""){return Nb(ot(),n,e,t)}function XA(n,e){let t=n[e];return t===zn?void 0:t}function YA(n,e,t,i,r,o){let s=e+t;return Hi(n,s,r)?PI(n,s+1,o?i.call(o,r):i(r)):XA(n,s+1)}function lv(n,e){let t=Rn(),i,r=n+$t;t.firstCreatePass?(i=ZA(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let o=i.factory||(i.factory=Or(i.type,!0)),s,a=Tn(Rd);try{let c=id(!1),l=o();return id(c),Om(t,ot(),r,l),l}finally{Tn(a)}}function ZA(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function uv(n,e,t){let i=n+$t,r=ot(),o=Pm(r,i);return JA(r,i)?YA(r,cx(),e,o.transform,t,o):o.transform(t)}function JA(n,e){return n[Ve].data[e].pure}var vd=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},dv=(()=>{class n{compileModuleSync(t){return new fd(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=ym(t),o=LE(r.declarations).reduce((s,a)=>{let c=kr(a);return c&&s.push(new $s(c)),s},[]);return new vd(i,o)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Ob=(()=>{class n{applicationErrorHandler=J(Bi);appRef=J(pr);taskService=J(jr);ngZone=J(An);zonelessEnabled=J(lc);tracing=J(Js,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new cn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ec):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(J(tg,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Mx:Jm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ec+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Lb(){return[{provide:Ro,useExisting:Ob},{provide:An,useClass:tc},{provide:lc,useValue:!0}]}function KA(){return typeof $localize<"u"&&$localize.locale||Dc}var fv=new Le("",{factory:()=>J(fv,{optional:!0,skipSelf:!0})||KA()});function Yr(n){return A_(n)}function Qs(n,e){return $l(n,e?.equal)}var Ub=Symbol("InputSignalNode#UNSET"),l1=it(ae({},ql),{transformFn:void 0,applyValueToInputSignal(n,e){Ss(n,e)}});function Bb(n,e){let t=Object.create(l1);t.value=n,t.transformFn=e?.transform;function i(){if(Es(t),t.value===Ub){let r=null;throw new Te(-950,r)}return t.value}return i[Sn]=t,i}function Fb(n,e){return Bb(n,e)}function u1(n){return Bb(Ub,n)}var Vb=(Fb.required=u1,Fb);var hv=new Le(""),d1=new Le("");function Ic(n){return!n.moduleRef}function f1(n){let e=Ic(n)?n.r3Injector:n.moduleRef.injector,t=e.get(An);return t.run(()=>{Ic(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Bi),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Ic(n)){let o=()=>e.destroy(),s=n.platformInjector.get(hv);s.add(o),e.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>n.moduleRef.destroy(),s=n.platformInjector.get(hv);s.add(o),n.moduleRef.onDestroy(()=>{fc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),s.delete(o)})}return p1(i,t,()=>{let o=e.get(jr),s=o.add(),a=e.get(ov);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(fv,Dc);if(wb(c||Dc),!e.get(d1,!0))return Ic(n)?e.get(pr):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Ic(n)){let u=e.get(pr);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return h1?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{o.remove(s)})})})}var h1;function p1(n,e,t){try{let i=t();return wc(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Vd=null;function m1(n=[],e){return In.create({name:e,providers:[{provide:oc,useValue:"platform"},{provide:hv,useValue:new Set([()=>Vd=null])},...n]})}function g1(n=[]){if(Vd)return Vd;let e=m1(n);return Vd=e,bb(),v1(e),e}function v1(n){let e=n.get(Ed,null);vn(n,()=>{e?.forEach(t=>t())})}function Hb(){return!1}var y1=1e4;var _6=y1-1e3;var pv=(()=>{class n{static __NG_ELEMENT_ID__=_1}return n})();function _1(n){return x1(li(),ot(),(n&16)===16)}function x1(n,e,t){if(zr(n)&&!t){let i=Fi(n.index,e);return new Wr(i,i)}else if(n.type&175){let i=e[Xn];return new Wr(i,e)}return null}function zb(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;Tt(mt.BootstrapApplicationStart);try{let o=r?.injector??g1(i),s=[Lb(),wx,...t||[]],a=new vc({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return f1({r3Injector:a.injector,platformInjector:o,rootComponent:e})}catch(o){return Promise.reject(o)}finally{Tt(mt.BootstrapApplicationEnd)}}var Gb=null;function gr(){return Gb}function mv(n){Gb??=n}var Rc=class{},Hd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>J(jb),providedIn:"platform"})}return n})();var jb=(()=>{class n extends Hd{_location;_history;_doc=J(Jt);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return gr().getBaseHref(this._doc)}onPopState(t){let i=gr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=gr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function qb(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function Wb(n){let e=n.search(/#|\?|$/);return n[e-1]==="/"?n.slice(0,e-1)+n.slice(e):n}function Zr(n){return n&&n[0]!=="?"?`?${n}`:n}var zd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>J(b1),providedIn:"root"})}return n})(),E1=new Le(""),b1=(()=>{class n extends zd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??J(Jt).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return qb(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+Zr(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+Zr(o));this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+Zr(o));this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(je(Hd),je(E1,8))};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var ea=(()=>{class n{_subject=new Ht;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=w1(Wb($b(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+Zr(i))}normalize(t){return n.stripTrailingSlash(S1(this._basePath,$b(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Zr(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Zr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Zr;static joinWithSlash=qb;static stripTrailingSlash=Wb;static \u0275fac=function(i){return new(i||n)(je(zd))};static \u0275prov=Pe({token:n,factory:()=>M1(),providedIn:"root"})}return n})();function M1(){return new ea(je(zd))}function S1(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function $b(n){return n.replace(/\/index.html$/,"")}function w1(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}var Jr=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Ks({type:n});static \u0275inj=Po({})}return n})();function gv(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,o]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(o)}return null}var Nc=class{};var Xb="browser";var Pc=class{_doc;constructor(e){this._doc=e}manager},Gd=(()=>{class n extends Pc{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,o){return t.addEventListener(i,r,o),()=>this.removeEventListener(t,i,r,o)}removeEventListener(t,i,r,o){return t.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||n)(je(Jt))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),$d=new Le(""),xv=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(s=>{s.manager=this});let r=t.filter(s=>!(s instanceof Gd));this._plugins=r.slice().reverse();let o=t.find(s=>s instanceof Gd);o&&this._plugins.push(o)}addEventListener(t,i,r,o){return this._findPluginFor(i).addEventListener(t,i,r,o)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(o=>o.supports(t)),!i)throw new Te(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(je($d),je(An))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),vv="ng-app-id";function Yb(n){for(let e of n)e.remove()}function Zb(n,e){let t=e.createElement("style");return t.textContent=n,t}function T1(n,e,t,i){let r=n.head?.querySelectorAll(`style[${vv}="${e}"],link[${vv}="${e}"]`);if(r)for(let o of r)o.removeAttribute(vv),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]})}function _v(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var Ev=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,o={}){this.doc=t,this.appId=i,this.nonce=r,T1(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Zb);i?.forEach(r=>this.addUsage(r,this.external,_v))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let o=i.get(t);o?o.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(Yb(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Yb(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Zb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,_v(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(je(Jt),je(xd),je(bd,8),je(Ec))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),yv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},bv=/%COMP%/g;var Kb="%COMP%",D1=`_nghost-${Kb}`,I1=`_ngcontent-${Kb}`,A1=!0,R1=new Le("",{factory:()=>A1});function N1(n){return I1.replace(bv,n)}function P1(n){return D1.replace(bv,n)}function Qb(n,e){return e.map(t=>t.replace(bv,n))}var Mv=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Oc(t,s,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Wd?r.applyToHost(t):r instanceof Lc&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case bi.Emulated:o=new Wd(c,l,i,this.appId,u,s,a,d);break;case bi.ShadowDom:return new jd(c,t,i,s,a,this.nonce,d,l);case bi.ExperimentalIsolatedShadowDom:return new jd(c,t,i,s,a,this.nonce,d);default:o=new Lc(c,l,i,u,s,a,d);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(je(xv),je(Ev),je(xd),je(R1),je(Jt),je(An),je(bd),je(Js,8))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),Oc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(yv[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(Jb(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(Jb(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Te(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let o=yv[r];o?e.setAttributeNS(o,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=yv[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(hr.DashCase|hr.Important)?e.style.setProperty(t,i,r&hr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&hr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=gr().getGlobalEventTarget(this.doc,e),!e))throw new Te(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(e,t,o)),this.eventManager.addEventListener(e,t,o,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function Jb(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var jd=class extends Oc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,o,s,a,c){super(e,r,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=Qb(i.id,l);for(let d of l){let f=document.createElement("style");s&&f.setAttribute("nonce",s),f.textContent=d,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let d of u){let f=_v(d,r);s&&f.setAttribute("nonce",s),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Lc=class extends Oc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,o,s,a,c){super(e,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?Qb(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Go.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Wd=class extends Lc{contentAttr;hostAttr;constructor(e,t,i,r,o,s,a,c){let l=r+"-"+i.id;super(e,t,i,o,s,a,c,l),this.contentAttr=N1(l),this.hostAttr=P1(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var qd=class n extends Rc{supportsDOMEvents=!0;static makeCurrent(){mv(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=O1();return t==null?null:L1(t)}resetBaseElement(){Fc=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return gv(document.cookie,e)}},Fc=null;function O1(){return Fc=Fc||document.head.querySelector("base"),Fc?Fc.getAttribute("href"):null}function L1(n){return new URL(n,document.baseURI).pathname}var F1=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),eM=["alt","control","meta","shift"],k1={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},U1={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},tM=(()=>{class n extends Pc{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,o){let s=n.parseEventName(i),a=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>gr().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=n._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),eM.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(t,i){let r=k1[t.key]||t.key,o="";return i.indexOf("code.")>-1&&(r=t.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),eM.forEach(s=>{if(s!==r){let a=U1[s];a(t)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(t,i,r){return o=>{n.matchEventFullKeyCode(o,t)&&r.runGuarded(()=>i(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(je(Jt))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})();async function Sv(n,e,t){let i=ae({rootComponent:n},B1(e,t));return zb(i)}function B1(n,e){return{platformRef:e?.platformRef,appProviders:[...j1,...n?.providers??[]],platformProviders:G1}}function V1(){qd.makeCurrent()}function H1(){return new cr}function z1(){return Bg(document),document}var G1=[{provide:Ec,useValue:Xb},{provide:Ed,useValue:V1,multi:!0},{provide:Jt,useFactory:z1}];var j1=[{provide:oc,useValue:"root"},{provide:cr,useFactory:H1},{provide:$d,useClass:Gd,multi:!0},{provide:$d,useClass:tM,multi:!0},Mv,Ev,xv,{provide:jo,useExisting:Mv},{provide:Nc,useClass:F1},[]];var nM=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(je(Jt))};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var $e="primary",Xc=Symbol("RouteTitle"),Iv=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function ra(n){return new Iv(n)}function wv(n,e,t){for(let i=0;i<n.length;i++){let r=n[i],o=e[i];if(r[0]===":")t[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function $1(n,e,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let c={},l=n.slice(0,i.length);return wv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>n.length||t.pathMatch==="full"&&e.hasChildren()&&t.path!=="**")return null;let a={};return!wv(o,n.slice(0,o.length),a)||!wv(s,n.slice(n.length-s.length),a)?null:{consumed:n,posParams:a}}function Qd(n){return new Promise((e,t)=>{n.pipe(sr()).subscribe({next:i=>e(i),error:i=>t(i)})})}function q1(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!ji(n[t],e[t]))return!1;return!0}function ji(n,e){let t=n?Av(n):void 0,i=e?Av(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let o=0;o<t.length;o++)if(r=t[o],!uM(n[r],e[r]))return!1;return!0}function Av(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function uM(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,o)=>i[o]===r)}else return n===e}function X1(n){return n.length>0?n[n.length-1]:null}function ts(n){return du(n)?n:wc(n)?Wt(Promise.resolve(n)):st(n)}function dM(n){return du(n)?Qd(n):Promise.resolve(n)}var Y1={exact:pM,subset:mM},fM={exact:Z1,subset:J1,ignored:()=>!0},hM={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Rv={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function iM(n,e,t){return Y1[t.paths](n.root,e.root,t.matrixParams)&&fM[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function Z1(n,e){return ji(n,e)}function pM(n,e,t){if(!Ko(n.segments,e.segments)||!Zd(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!pM(n.children[i],e.children[i],t))return!1;return!0}function J1(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>uM(n[t],e[t]))}function mM(n,e,t){return gM(n,e,e.segments,t)}function gM(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!Ko(r,t)||e.hasChildren()||!Zd(r,t,i))}else if(n.segments.length===t.length){if(!Ko(n.segments,t)||!Zd(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!mM(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),o=t.slice(n.segments.length);return!Ko(n.segments,r)||!Zd(n.segments,r,i)||!n.children[$e]?!1:gM(n.children[$e],e,o,i)}}function Zd(n,e,t){return e.every((i,r)=>fM[t](n[r].parameters,i.parameters))}var Si=class{root;queryParams;fragment;_queryParamMap;constructor(e=new _t([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=ra(this.queryParams),this._queryParamMap}toString(){return eR.serialize(this)}},_t=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Jd(this)}},Jo=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=ra(this.parameters),this._parameterMap}toString(){return yM(this)}};function K1(n,e){return Ko(n,e)&&n.every((t,i)=>ji(t.parameters,e[i].parameters))}function Ko(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function Q1(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===$e&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==$e&&(t=t.concat(e(r,i)))}),t}var uf=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>new Qo,providedIn:"root"})}return n})(),Qo=class{parse(e){let t=new Pv(e);return new Si(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${kc(e.root,!0)}`,i=iR(e.queryParams),r=typeof e.fragment=="string"?`#${tR(e.fragment)}`:"";return`${t}${i}${r}`}},eR=new Qo;function Jd(n){return n.segments.map(e=>yM(e)).join("/")}function kc(n,e){if(!n.hasChildren())return Jd(n);if(e){let t=n.children[$e]?kc(n.children[$e],!1):"",i=[];return Object.entries(n.children).forEach(([r,o])=>{r!==$e&&i.push(`${r}:${kc(o,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=Q1(n,(i,r)=>r===$e?[kc(n.children[$e],!1)]:[`${r}:${kc(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[$e]!=null?`${Jd(n)}/${t[0]}`:`${Jd(n)}/(${t.join("//")})`}}function vM(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Xd(n){return vM(n).replace(/%3B/gi,";")}function tR(n){return encodeURI(n)}function Nv(n){return vM(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Kd(n){return decodeURIComponent(n)}function rM(n){return Kd(n.replace(/\+/g,"%20"))}function yM(n){return`${Nv(n.path)}${nR(n.parameters)}`}function nR(n){return Object.entries(n).map(([e,t])=>`;${Nv(e)}=${Nv(t)}`).join("")}function iR(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${Xd(t)}=${Xd(r)}`).join("&"):`${Xd(t)}=${Xd(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var rR=/^[^\/()?;#]+/;function Cv(n){let e=n.match(rR);return e?e[0]:""}var oR=/^[^\/()?;=#]+/;function sR(n){let e=n.match(oR);return e?e[0]:""}var aR=/^[^=?&#]+/;function cR(n){let e=n.match(aR);return e?e[0]:""}var lR=/^[^&#]+/;function uR(n){let e=n.match(lR);return e?e[0]:""}var Pv=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){return this.consumeOptional("/"),this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new _t([],{}):new _t([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new Te(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,e));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,e)),(t.length>0||Object.keys(i).length>0)&&(r[$e]=new _t(t,i)),r}parseSegment(){let e=Cv(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Te(4009,!1);return this.capture(e),new Jo(Kd(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=sR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=Cv(this.remaining);r&&(i=r,this.capture(i))}e[Kd(t)]=Kd(i)}parseQueryParam(e){let t=cR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let s=uR(this.remaining);s&&(i=s,this.capture(i))}let r=rM(t),o=rM(i);if(e.hasOwnProperty(r)){let s=e[r];Array.isArray(s)||(s=[s],e[r]=s),s.push(o)}else e[r]=o}parseParens(e,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Cv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new Te(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):e&&(s=$e);let a=this.parseChildren(t+1);i[s??$e]=Object.keys(a).length===1&&a[$e]?a[$e]:new _t([],a),this.consumeOptional("//")}return i}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Te(4011,!1)}};function _M(n){return n.segments.length>0?new _t([],{[$e]:n}):n}function xM(n){let e={};for(let[i,r]of Object.entries(n.children)){let o=xM(r);if(i===$e&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))e[s]=a;else(o.segments.length>0||o.hasChildren())&&(e[i]=o)}let t=new _t(n.segments,e);return dR(t)}function dR(n){if(n.numberOfChildren===1&&n.children[$e]){let e=n.children[$e];return new _t(n.segments.concat(e.segments),e.children)}return n}function oa(n){return n instanceof Si}function fR(n,e,t=null,i=null,r=new Qo){let o=EM(n);return bM(o,e,t,i,r)}function EM(n){let e;function t(o){let s={};for(let c of o.children){let l=t(c);s[c.outlet]=l}let a=new _t(o.url,s);return o===n&&(e=a),a}let i=t(n.root),r=_M(i);return e??r}function bM(n,e,t,i,r){let o=n;for(;o.parent;)o=o.parent;if(e.length===0)return Tv(o,o,o,t,i,r);let s=hR(e);if(s.toRoot())return Tv(o,o,new _t([],{}),t,i,r);let a=pR(s,o,n),c=a.processChildren?Bc(a.segmentGroup,a.index,s.commands):SM(a.segmentGroup,a.index,s.commands);return Tv(o,a.segmentGroup,c,t,i,r)}function ef(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function zc(n){return typeof n=="object"&&n!=null&&n.outlets}function oM(n,e,t){n||="\u0275";let i=new Si;return i.queryParams={[n]:e},t.parse(t.serialize(i)).queryParams[n]}function Tv(n,e,t,i,r,o){let s={};for(let[l,u]of Object.entries(i??{}))s[l]=Array.isArray(u)?u.map(d=>oM(l,d,o)):oM(l,u,o);let a;n===e?a=t:a=MM(n,e,t);let c=_M(xM(a));return new Si(c,s,r)}function MM(n,e,t){let i={};return Object.entries(n.children).forEach(([r,o])=>{o===e?i[r]=t:i[r]=MM(o,e,t)}),new _t(n.segments,i)}var tf=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&ef(i[0]))throw new Te(4003,!1);let r=i.find(zc);if(r&&r!==X1(i))throw new Te(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function hR(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new tf(!0,0,n);let e=0,t=!1,i=n.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new tf(t,e,i)}var na=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function pR(n,e,t){if(n.isAbsolute)return new na(e,!0,0);if(!t)return new na(e,!1,NaN);if(t.parent===null)return new na(t,!0,0);let i=ef(n.commands[0])?0:1,r=t.segments.length-1+i;return mR(t,r,n.numberOfDoubleDots)}function mR(n,e,t){let i=n,r=e,o=t;for(;o>r;){if(o-=r,i=i.parent,!i)throw new Te(4005,!1);r=i.segments.length}return new na(i,!1,r-o)}function gR(n){return zc(n[0])?n[0].outlets:{[$e]:n}}function SM(n,e,t){if(n??=new _t([],{}),n.segments.length===0&&n.hasChildren())return Bc(n,e,t);let i=vR(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let o=new _t(n.segments.slice(0,i.pathIndex),{});return o.children[$e]=new _t(n.segments.slice(i.pathIndex),n.children),Bc(o,0,r)}else return i.match&&r.length===0?new _t(n.segments,{}):i.match&&!n.hasChildren()?Ov(n,e,t):i.match?Bc(n,0,r):Ov(n,e,t)}function Bc(n,e,t){if(t.length===0)return new _t(n.segments,{});{let i=gR(t),r={};if(Object.keys(i).some(o=>o!==$e)&&n.children[$e]&&n.numberOfChildren===1&&n.children[$e].segments.length===0){let o=Bc(n.children[$e],e,t);return new _t(n.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=SM(n.children[o],e,s))}),Object.entries(n.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new _t(n.segments,r)}}function vR(n,e,t){let i=0,r=e,o={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return o;let s=n.segments[r],a=t[i];if(zc(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!aM(c,l,s))return o;i+=2}else{if(!aM(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Ov(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let o=t[r];if(zc(o)){let c=yR(o.outlets);return new _t(i,c)}if(r===0&&ef(t[0])){let c=n.segments[e];i.push(new Jo(c.path,sM(t[0]))),r++;continue}let s=zc(o)?o.outlets[$e]:`${o}`,a=r<t.length-1?t[r+1]:null;s&&a&&ef(a)?(i.push(new Jo(s,sM(a))),r+=2):(i.push(new Jo(s,{})),r++)}return new _t(i,{})}function yR(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=Ov(new _t([],{}),0,i))}),e}function sM(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function aM(n,e,t){return n==t.path&&ji(e,t.parameters)}var Vc="imperative",dn=(function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n})(dn||{}),ui=class{id;url;constructor(e,t){this.id=e,this.url=t}},sa=class extends ui{type=dn.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Kr=class extends ui{urlAfterRedirects;type=dn.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Ln=(function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n[n.Aborted=4]="Aborted",n})(Ln||{}),nf=(function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n})(nf||{}),Mi=class extends ui{reason;code;type=dn.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function wM(n){return n instanceof Mi&&(n.code===Ln.Redirect||n.code===Ln.SupersededByNewNavigation)}var Qr=class extends ui{reason;code;type=dn.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},aa=class extends ui{error;target;type=dn.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},rf=class extends ui{urlAfterRedirects;state;type=dn.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Lv=class extends ui{urlAfterRedirects;state;type=dn.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Fv=class extends ui{urlAfterRedirects;state;shouldActivate;type=dn.GuardsCheckEnd;constructor(e,t,i,r,o){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},kv=class extends ui{urlAfterRedirects;state;type=dn.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Uv=class extends ui{urlAfterRedirects;state;type=dn.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Bv=class{route;type=dn.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Vv=class{route;type=dn.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Hv=class{snapshot;type=dn.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},zv=class{snapshot;type=dn.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Gv=class{snapshot;type=dn.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},jv=class{snapshot;type=dn.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var ca=class{},Gc=class{},la=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function _R(n){return!(n instanceof ca)&&!(n instanceof la)&&!(n instanceof Gc)}var Wv=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new Yc(this.rootInjector)}},Yc=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new Wv(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(je(Yt))};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),of=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=$v(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=$v(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=qv(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return qv(e,this._root).map(t=>t.value)}};function $v(n,e){if(n===e.value)return e;for(let t of e.children){let i=$v(n,t);if(i)return i}return null}function qv(n,e){if(n===e.value)return[e];for(let t of e.children){let i=qv(n,t);if(i.length)return i.unshift(e),i}return[]}var Kn=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function ta(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var sf=class extends of{snapshot;constructor(e,t){super(e),this.snapshot=t,iy(this,e)}toString(){return this.snapshot.toString()}};function CM(n,e){let t=xR(n,e),i=new ln([new Jo("",{})]),r=new ln({}),o=new ln({}),s=new ln({}),a=new ln(""),c=new es(i,r,s,a,o,$e,n,t.root);return c.snapshot=t.root,new sf(new Kn(c,[]),t)}function xR(n,e){let t={},i={},r={},s=new jc([],t,r,"",i,$e,n,null,{},e);return new af("",new Kn(s,[]))}var es=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,o,s,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Mt(l=>l[Xc]))??st(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Mt(e=>ra(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Mt(e=>ra(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function ny(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:ae(ae({},e.params),n.params),data:ae(ae({},e.data),n.data),resolve:ae(ae(ae(ae({},n.data),e.data),r?.data),n._resolvedData)}:i={params:ae({},n.params),data:ae({},n.data),resolve:ae(ae({},n.data),n._resolvedData??{})},r&&DM(r)&&(i.resolve[Xc]=r.title),i}var jc=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Xc]}constructor(e,t,i,r,o,s,a,c,l,u){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=ra(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=ra(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},af=class extends of{url;constructor(e,t){super(t),this.url=e,iy(this,t)}toString(){return TM(this._root)}};function iy(n,e){e.value._routerState=n,e.children.forEach(t=>iy(n,t))}function TM(n){let e=n.children.length>0?` { ${n.children.map(TM).join(", ")} } `:"";return`${n.value}${e}`}function Dv(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,ji(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),ji(e.params,t.params)||n.paramsSubject.next(t.params),q1(e.url,t.url)||n.urlSubject.next(t.url),ji(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function Xv(n,e){let t=ji(n.params,e.params)&&K1(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||Xv(n.parent,e.parent))}function DM(n){return typeof n.title=="string"||n.title===null}var ER=new Le(""),IM=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=$e;activateEvents=new Dn;deactivateEvents=new Dn;attachEvents=new Dn;detachEvents=new Dn;routerOutletData=Vb();parentContexts=J(Yc);location=J(Xo);changeDetector=J(pv);inputBinder=J(df,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Te(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Te(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Te(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Te(4013,!1);this._activatedRoute=t;let r=this.location,s=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Yv(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=Pd({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[yd]})}return n})(),Yv=class{route;childContexts;parent;outletData;constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===es?this.route:e===Yc?this.childContexts:e===ER?this.outletData:this.parent.get(e,t)}},df=new Le("");var AM=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=Zn({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&Yo(0,"router-outlet")},dependencies:[IM],encapsulation:2})}return n})();function ry(n){let e=n.children&&n.children.map(ry),t=e?it(ae({},n),{children:e}):ae({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==$e&&(t.component=AM),t}function bR(n,e,t){let i=Wc(n,e._root,t?t._root:void 0);return new sf(i,e)}function Wc(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=MR(n,e,t);return new Kn(i,r)}else{if(n.shouldAttach(e.value)){let o=n.retrieve(e.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=e.value,s.children=e.children.map(a=>Wc(n,a)),s}}let i=SR(e.value),r=e.children.map(o=>Wc(n,o));return new Kn(i,r)}}function MR(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return Wc(n,i,r);return Wc(n,i)})}function SR(n){return new es(new ln(n.url),new ln(n.params),new ln(n.queryParams),new ln(n.fragment),new ln(n.data),n.outlet,n.component,n)}var $c=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},RM="ngNavigationCancelingError";function cf(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=oa(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=NM(!1,Ln.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function NM(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[RM]=!0,t.cancellationCode=e,t}function wR(n){return PM(n)&&oa(n.url)}function PM(n){return!!n&&n[RM]}var Zv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,o){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),Dv(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=ta(t);e.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(e,t,i){let r=e.value,o=t?t.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(e,t,s.children)}else this.deactivateChildRoutes(e,t,i);else o&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,o=ta(e);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:s,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,o=ta(e);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=ta(t);e.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new jv(o.value.snapshot))}),e.children.length&&this.forwardEvent(new zv(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,o=t?t.value:null;if(Dv(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,s.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Dv(a.route.value),this.activateChildRoutes(e,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(e,null,s.children)}else this.activateChildRoutes(e,null,i)}},lf=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},ia=class{component;route;constructor(e,t){this.component=e,this.route=t}};function CR(n,e,t){let i=n._root,r=e?e._root:null;return Uc(i,r,t,[i.value])}function TR(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function da(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!fm(n)?n:e.get(n):i}function Uc(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ta(e);return n.children.forEach(s=>{DR(s,o[s.value.outlet],t,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Hc(a,t.getContext(s),r)),r}function DR(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=n.value,s=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=IR(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new lf(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Uc(n,e,a?a.children:null,i,r):Uc(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ia(a.outlet.component,s))}else s&&Hc(e,a,r),r.canActivateChecks.push(new lf(i)),o.component?Uc(n,null,a?a.children:null,i,r):Uc(n,null,t,i,r);return r}function IR(n,e,t){if(typeof t=="function")return vn(e._environmentInjector,()=>t(n,e));switch(t){case"pathParamsChange":return!Ko(n.url,e.url);case"pathParamsOrQueryParamsChange":return!Ko(n.url,e.url)||!ji(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Xv(n,e)||!ji(n.queryParams,e.queryParams);default:return!Xv(n,e)}}function Hc(n,e,t){let i=ta(n),r=n.value;Object.entries(i).forEach(([o,s])=>{r.component?e?Hc(s,e.children.getContext(o),t):Hc(s,null,t):Hc(s,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new ia(e.outlet.component,r)):t.canDeactivateChecks.push(new ia(null,r)):t.canDeactivateChecks.push(new ia(null,r))}function Zc(n){return typeof n=="function"}function AR(n){return typeof n=="boolean"}function RR(n){return n&&Zc(n.canLoad)}function NR(n){return n&&Zc(n.canActivate)}function PR(n){return n&&Zc(n.canActivateChild)}function OR(n){return n&&Zc(n.canDeactivate)}function LR(n){return n&&Zc(n.canMatch)}function OM(n){return n instanceof To||n?.name==="EmptyError"}var Yd=Symbol("INITIAL_VALUE");function ua(){return gn(n=>Yp(n.map(e=>e.pipe(Cn(1),Jp(Yd)))).pipe(Mt(e=>{for(let t of e)if(t!==!0){if(t===Yd)return Yd;if(t===!1||FR(t))return t}return!0}),ri(e=>e!==Yd),Cn(1)))}function FR(n){return oa(n)||n instanceof $c}function LM(n){return n.aborted?st(void 0).pipe(Cn(1)):new rt(e=>{let t=()=>{e.next(),e.complete()};return n.addEventListener("abort",t),()=>n.removeEventListener("abort",t)})}function FM(n){return Ya(LM(n))}function kR(n){return wn(e=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=e;return o.length===0&&r.length===0?st(it(ae({},e),{guardsResult:!0})):UR(o,t,i).pipe(wn(s=>s&&AR(s)?BR(t,r,n):st(s)),Mt(s=>it(ae({},e),{guardsResult:s})))})}function UR(n,e,t){return Wt(n).pipe(wn(i=>jR(i.component,i.route,t,e)),sr(i=>i!==!0,!0))}function BR(n,e,t){return Wt(e).pipe(fu(i=>As(HR(i.route.parent,t),VR(i.route,t),GR(n,i.path),zR(n,i.route))),sr(i=>i!==!0,!0))}function VR(n,e){return n!==null&&e&&e(new Gv(n)),st(!0)}function HR(n,e){return n!==null&&e&&e(new Hv(n)),st(!0)}function zR(n,e){let t=e.routeConfig?e.routeConfig.canActivate:null;if(!t||t.length===0)return st(!0);let i=t.map(r=>qa(()=>{let o=e._environmentInjector,s=da(r,o),a=NR(s)?s.canActivate(e,n):vn(o,()=>s(e,n));return ts(a).pipe(sr())}));return st(i).pipe(ua())}function GR(n,e){let t=e[e.length-1],r=e.slice(0,e.length-1).reverse().map(o=>TR(o)).filter(o=>o!==null).map(o=>qa(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=da(a,c),u=PR(l)?l.canActivateChild(t,n):vn(c,()=>l(t,n));return ts(u).pipe(sr())});return st(s).pipe(ua())}));return st(r).pipe(ua())}function jR(n,e,t,i){let r=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!r||r.length===0)return st(!0);let o=r.map(s=>{let a=e._environmentInjector,c=da(s,a),l=OR(c)?c.canDeactivate(n,e,t,i):vn(a,()=>c(n,e,t,i));return ts(l).pipe(sr())});return st(o).pipe(ua())}function WR(n,e,t,i,r){let o=e.canLoad;if(o===void 0||o.length===0)return st(!0);let s=o.map(a=>{let c=da(a,n),l=RR(c)?c.canLoad(e,t):vn(n,()=>c(e,t)),u=ts(l);return r?u.pipe(FM(r)):u});return st(s).pipe(ua(),kM(i))}function kM(n){return Wp(oi(e=>{if(typeof e!="boolean")throw cf(n,e)}),Mt(e=>e===!0))}function $R(n,e,t,i,r,o){let s=e.canMatch;if(!s||s.length===0)return st(!0);let a=s.map(c=>{let l=da(c,n),u=LR(l)?l.canMatch(e,t,r):vn(n,()=>l(e,t,r));return ts(u).pipe(FM(o))});return st(a).pipe(ua(),kM(i))}var vr=class n extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,n.prototype)}},qc=class n extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,n.prototype)}};function qR(n){throw new Te(4e3,!1)}function XR(n){throw NM(!1,Ln.GuardRejected)}var Jv=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}async lineralizeSegments(e,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[$e])throw qR(`${e.redirectTo}`);r=r.children[$e]}}async applyRedirectCommands(e,t,i,r,o){let s=await YR(t,r,o);if(s instanceof Si)throw new qc(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),e,i);if(s[0]==="/")throw new qc(a);return a}applyRedirectCreateUrlTree(e,t,i,r){let o=this.createSegmentGroup(e,t.root,i,r);return new Si(o,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=t[a]}else i[r]=o}),i}createSegmentGroup(e,t,i,r){let o=this.createSegments(e,t.segments,i,r),s={};return Object.entries(t.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(e,c,i,r)}),new _t(o,s)}createSegments(e,t,i,r){return t.map(o=>o.path[0]===":"?this.findPosParam(e,o,r):this.findOrReturn(o,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Te(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}};function YR(n,e,t){if(typeof n=="string")return Promise.resolve(n);let i=n;return Qd(ts(vn(t,()=>i(e))))}function ZR(n,e){return n.providers&&!n._injector&&(n._injector=Sc(n.providers,e,`Route: ${n.path}`)),n._injector??e}function Wi(n){return n.outlet||$e}function JR(n,e){let t=n.filter(i=>Wi(i)===e);return t.push(...n.filter(i=>Wi(i)!==e)),t}var Kv={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function UM(n){return{routeConfig:n.routeConfig,url:n.url,params:n.params,queryParams:n.queryParams,fragment:n.fragment,data:n.data,outlet:n.outlet,title:n.title,paramMap:n.paramMap,queryParamMap:n.queryParamMap}}function KR(n,e,t,i,r,o,s){let a=BM(n,e,t);if(!a.matched)return st(a);let c=UM(o(a));return i=ZR(e,i),$R(i,e,t,r,c,s).pipe(Mt(l=>l===!0?a:ae({},Kv)))}function BM(n,e,t){if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?ae({},Kv):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||$1)(t,n,e);if(!r)return ae({},Kv);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?ae(ae({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function cM(n,e,t,i){return t.length>0&&tN(n,t,i)?{segmentGroup:new _t(e,eN(i,new _t(t,n.children))),slicedSegments:[]}:t.length===0&&nN(n,t,i)?{segmentGroup:new _t(n.segments,QR(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new _t(n.segments,n.children),slicedSegments:t}}function QR(n,e,t,i){let r={};for(let o of t)if(ff(n,e,o)&&!i[Wi(o)]){let s=new _t([],{});r[Wi(o)]=s}return ae(ae({},i),r)}function eN(n,e){let t={};t[$e]=e;for(let i of n)if(i.path===""&&Wi(i)!==$e){let r=new _t([],{});t[Wi(i)]=r}return t}function tN(n,e,t){return t.some(i=>ff(n,e,i)&&Wi(i)!==$e)}function nN(n,e,t){return t.some(i=>ff(n,e,i))}function ff(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function iN(n,e,t){return e.length===0&&!n.children[t]}var Qv=class{};async function rN(n,e,t,i,r,o,s="emptyOnly",a){return new ey(n,e,t,i,r,s,o,a).recognize()}var oN=31,ey=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,t,i,r,o,s,a,c){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Jv(this.urlSerializer,this.urlTree)}noMatchError(e){return new Te(4002,`'${e.segmentGroup}'`)}async recognize(){let e=cM(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=await this.match(e),r=new Kn(i,t),o=new af("",r),s=fR(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(e){let t=new jc([],Object.freeze({}),Object.freeze(ae({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),$e,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,e,$e,t),rootSnapshot:t}}catch(i){if(i instanceof qc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof vr?this.noMatchError(i):i}}async processSegmentGroup(e,t,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(e,t,i,o);let s=await this.processSegment(e,t,i,i.segments,r,!0,o);return s instanceof Kn?[s]:[]}async processChildren(e,t,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],u=JR(t,c),d=await this.processSegmentGroup(e,u,l,c,r);s.push(...d)}let a=VM(s);return sN(a),a}async processSegment(e,t,i,r,o,s,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,o,s,a)}catch(l){if(l instanceof vr||OM(l))continue;throw l}if(iN(i,r,o))return new Qv;throw new vr(i)}async processSegmentAgainstRoute(e,t,i,r,o,s,a,c){if(Wi(i)!==s&&(s===$e||!ff(r,o,i)))throw new vr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,o,s,c);throw new vr(r)}async expandSegmentAgainstRouteUsingRedirect(e,t,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:f}=BM(t,r,o);if(!c)throw new vr(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>oN&&(this.allowRedirects=!1));let h=this.createSnapshot(e,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let p=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,UM(h),e),y=await this.applyRedirects.lineralizeSegments(r,p);return this.processSegment(e,i,t,y.concat(f),s,!1,a)}createSnapshot(e,t,i,r,o){let s=new jc(i,r,Object.freeze(ae({},this.urlTree.queryParams)),this.urlTree.fragment,cN(t),Wi(t),t.component??t._loadedComponent??null,t,lN(t),e),a=ny(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(e,t,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=S=>this.createSnapshot(e,i,S.consumedSegments,S.parameters,s),c=await Qd(KR(t,i,r,e,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new vr(t);e=i._injector??e;let{routes:l}=await this.getChildConfig(e,i,r),u=i._loadedInjector??e,{parameters:d,consumedSegments:f,remainingSegments:h}=c,p=this.createSnapshot(e,i,f,d,s),{segmentGroup:y,slicedSegments:g}=cM(t,f,h,l);if(g.length===0&&y.hasChildren()){let S=await this.processChildren(u,l,y,p);return new Kn(p,S)}if(l.length===0&&g.length===0)return new Kn(p,[]);let m=Wi(i)===o,E=await this.processSegment(u,l,y,g,m?$e:o,!0,p);return new Kn(p,E instanceof Kn?[E]:[])}async getChildConfig(e,t,i){if(t.children)return{routes:t.children,injector:e};if(t.loadChildren){if(t._loadedRoutes!==void 0){let o=t._loadedNgModuleFactory;return o&&!t._loadedInjector&&(t._loadedInjector=o.create(e).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Qd(WR(e,t,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(e,t);return t._loadedRoutes=o.routes,t._loadedInjector=o.injector,t._loadedNgModuleFactory=o.factory,o}throw XR(t)}return{routes:[],injector:e}}};function sN(n){n.sort((e,t)=>e.value.outlet===$e?-1:t.value.outlet===$e?1:e.value.outlet.localeCompare(t.value.outlet))}function aN(n){let e=n.value.routeConfig;return e&&e.path===""}function VM(n){let e=[],t=new Set;for(let i of n){if(!aN(i)){e.push(i);continue}let r=e.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):e.push(i)}for(let i of t){let r=VM(i.children);e.push(new Kn(i.value,r))}return e.filter(i=>!t.has(i))}function cN(n){return n.data||{}}function lN(n){return n.resolve||{}}function uN(n,e,t,i,r,o,s){return wn(async a=>{let{state:c,tree:l}=await rN(n,e,t,i,a.extractedUrl,r,o,s);return it(ae({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function dN(n){return wn(e=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=e;if(!i.length)return st(e);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of HM(a))o.add(c);let s=0;return Wt(o).pipe(fu(a=>r.has(a)?fN(a,t,n):(a.data=ny(a,a.parent,n).resolve,st(void 0))),oi(()=>s++),hu(1),wn(a=>s===o.size?st(e):un))})}function HM(n){let e=n.children.map(t=>HM(t)).flat();return[n,...e]}function fN(n,e,t){let i=n.routeConfig,r=n._resolve;return i?.title!==void 0&&!DM(i)&&(r[Xc]=i.title),qa(()=>(n.data=ny(n,n.parent,t).resolve,hN(r,n,e).pipe(Mt(o=>(n._resolvedData=o,n.data=ae(ae({},n.data),o),null)))))}function hN(n,e,t){let i=Av(n);if(i.length===0)return st({});let r={};return Wt(i).pipe(wn(o=>pN(n[o],e,t).pipe(sr(),oi(s=>{if(s instanceof $c)throw cf(new Qo,s);r[o]=s}))),hu(1),Mt(()=>r),Xa(o=>OM(o)?un:Xp(o)))}function pN(n,e,t){let i=e._environmentInjector,r=da(n,i),o=r.resolve?r.resolve(e,t):vn(i,()=>r(e,t));return ts(o)}function lM(n){return gn(e=>{let t=n(e);return t?Wt(t).pipe(Mt(()=>e)):st(e)})}var zM=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===$e);return i}getResolvedTitleForRoute(t){return t.data[Xc]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>J(mN),providedIn:"root"})}return n})(),mN=(()=>{class n extends zM{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(je(nM))};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),hf=new Le("",{factory:()=>({})}),pf=new Le(""),GM=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=J(dv);async loadComponent(t,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await dM(vn(t,()=>i.loadComponent())),s=await WM(jM(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await gN(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();async function gN(n,e,t,i){let r=await dM(vn(t,()=>n.loadChildren())),o=await WM(jM(r)),s;o instanceof Nd||Array.isArray(o)?s=o:s=await e.compileModuleAsync(o),i&&i(n);let a,c,l=!1,u;return Array.isArray(s)?(c=s,l=!0):(a=s.create(t).injector,u=s,c=a.get(pf,[],{optional:!0,self:!0}).flat()),{routes:c.map(ry),injector:a,factory:u}}function vN(n){return n&&typeof n=="object"&&"default"in n}function jM(n){return vN(n)?n.default:n}async function WM(n){return n}var oy=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>J(yN),providedIn:"root"})}return n})(),yN=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),$M=new Le("");var _N=()=>{},qM=new Le(""),XM=(()=>{class n{currentNavigation=Hn(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Hn(null);events=new Ht;transitionAbortWithErrorSubject=new Ht;configLoader=J(GM);environmentInjector=J(Yt);destroyRef=J(fr);urlSerializer=J(uf);rootContexts=J(Yc);location=J(ea);inputBindingEnabled=J(df,{optional:!0})!==null;titleStrategy=J(zM);options=J(hf,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=J(oy);createViewTransition=J($M,{optional:!0});navigationErrorHandler=J(qM,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>st(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Bv(r)),i=r=>this.events.next(new Vv(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;Yr(()=>{this.transitions?.next(it(ae({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new ln(null),this.transitions.pipe(ri(i=>i!==null),gn(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return st(i).pipe(gn(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Ln.SupersededByNewNavigation),un;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?it(ae({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new Qr(a.id,this.urlSerializer.serialize(a.rawUrl),"",nf.IgnoredSameUrlNavigation)),a.resolve(!1),un;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return st(a).pipe(gn(d=>(this.events.next(new sa(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?un:Promise.resolve(d))),uN(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),oi(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(f=>(f.finalUrl=d.urlAfterRedirects,f)),this.events.next(new Gc)}),gn(d=>Wt(i.routesRecognizeHandler.deferredHandle??st(void 0)).pipe(Mt(()=>d))),oi(()=>{let d=new rf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:f,source:h,restoredState:p,extras:y}=a,g=new sa(d,this.urlSerializer.serialize(f),h,p);this.events.next(g);let m=CM(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=it(ae({},a),{targetSnapshot:m,urlAfterRedirects:f,extras:it(ae({},y),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(E=>(E.finalUrl=f,E)),st(i)}else return this.events.next(new Qr(a.id,this.urlSerializer.serialize(a.extractedUrl),"",nf.IgnoredByUrlHandlingStrategy)),a.resolve(!1),un}),Mt(a=>{let c=new Lv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=it(ae({},a),{guards:CR(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),kR(a=>this.events.next(a)),gn(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw cf(this.urlSerializer,a.guardsResult);let c=new Fv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return un;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",Ln.GuardRejected),un;if(a.guards.canActivateChecks.length===0)return st(a);let l=new kv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return un;let u=!1;return st(a).pipe(dN(this.paramsInheritanceStrategy),oi({next:()=>{u=!0;let d=new Uv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",Ln.NoDataFromResolver)}}))}),lM(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let f=u._environmentInjector;d.push(this.configLoader.loadComponent(f,u.routeConfig).then(h=>{u.component=h}))}for(let f of u.children)d.push(...c(f));return d},l=c(a.targetSnapshot.root);return l.length===0?st(a):Wt(Promise.all(l).then(()=>a))}),lM(()=>this.afterPreactivation()),gn(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?Wt(l).pipe(Mt(()=>i)):st(i)}),Cn(1),gn(a=>{let c=bR(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=it(ae({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new ca);let l=i.beforeActivateHandler.deferredHandle;return l?Wt(l.then(()=>a)):st(a)}),oi(a=>{new Zv(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(c=>(c.abort=_N,c)),this.lastSuccessfulNavigation.set(Yr(this.currentNavigation)),this.events.next(new Kr(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Ya(LM(o.signal).pipe(ri(()=>!r&&!i.targetRouterState),oi(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",Ln.Aborted)}))),oi({complete:()=>{r=!0}}),Ya(this.transitionAbortWithErrorSubject.pipe(oi(a=>{throw a}))),Zp(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",Ln.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Xa(a=>{if(r=!0,this.destroyed)return i.resolve(!1),un;if(PM(a))this.events.next(new Mi(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),wR(a)?this.events.next(new la(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new aa(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=vn(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof $c){let{message:u,cancellationCode:d}=cf(this.urlSerializer,l);this.events.next(new Mi(i.id,this.urlSerializer.serialize(i.extractedUrl),u,d)),this.events.next(new la(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return un}))}))}cancelNavigationTransition(t,i,r){let o=new Mi(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(o),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Yr(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function xN(n){return n!==Vc}var YM=new Le("");var EN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>J(bN),providedIn:"root"})}return n})(),ty=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}shouldDestroyInjector(e){return!0}},bN=(()=>{class n extends ty{static \u0275fac=(()=>{let t;return function(r){return(t||(t=xc(n)))(r||n)}})();static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),sy=(()=>{class n{urlSerializer=J(uf);options=J(hf,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=J(ea);urlHandlingStrategy=J(oy);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Si;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let o=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,s=r??o;return s instanceof Si?this.urlSerializer.serialize(s):s}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=CM(null,J(Yt));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:()=>J(MN),providedIn:"root"})}return n})(),MN=(()=>{class n extends sy{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof sa?this.updateStateMemento():t instanceof Qr?this.commitTransition(i):t instanceof rf?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof ca?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Mi&&!wM(t)?this.restoreHistory(i):t instanceof aa?this.restoreHistory(i,!0):t instanceof Kr&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,{extras:i,id:r}){let{replaceUrl:o,state:s}=i;if(this.location.isCurrentPathEqualTo(t)||o){let a=this.browserPageId,c=ae(ae({},s),this.generateNgRouterState(r,a));this.location.replaceState(t,"",c)}else{let a=ae(ae({},s),this.generateNgRouterState(r,this.browserPageId+1));this.location.go(t,"",a)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===t.finalUrl&&o===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i){return this.canceledNavigationResolution==="computed"?{navigationId:t,\u0275routerPageId:i}:{navigationId:t}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=xc(n)))(r||n)}})();static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ZM(n,e){n.events.pipe(ri(t=>t instanceof Kr||t instanceof Mi||t instanceof aa||t instanceof Qr),Mt(t=>t instanceof Kr||t instanceof Qr?0:(t instanceof Mi?t.code===Ln.Redirect||t.code===Ln.SupersededByNewNavigation:!1)?2:1),ri(t=>t!==2),Cn(1)).subscribe(()=>{e()})}var ay=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=J(nv);stateManager=J(sy);options=J(hf,{optional:!0})||{};pendingTasks=J(jr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=J(XM);urlSerializer=J(uf);location=J(ea);urlHandlingStrategy=J(oy);injector=J(Yt);_events=new Ht;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=J(EN);injectorCleanup=J(YM,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=J(pf,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!J(df,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new cn;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Yr(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Mi&&i.code!==Ln.Redirect&&i.code!==Ln.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Kr)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof la){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=ae({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||xN(r.source)},s);this.scheduleNavigation(a,Vc,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}_R(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Vc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,o)=>{this.navigateToSyncWithBrowser(t,r,i,o)})}navigateToSyncWithBrowser(t,i,r,o){let s=r?.navigationId?r:null;if(r){let c=ae({},r);delete c.navigationId,delete c.\u0275routerPageId,Object.keys(c).length!==0&&(o.state=c)}let a=this.parseUrl(t);this.scheduleNavigation(a,i,s,o).catch(c=>{this.disposed||this.injector.get(Bi)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Yr(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(ry),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=ae(ae({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let f=r?r.snapshot:this.routerState.snapshot.root;d=EM(f)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return bM(d,t,u,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=oa(t)?t:this.parseUrl(t),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Vc,null,i)}navigate(t,i={skipLocationChange:!1}){return SN(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(No(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=ae({},hM):i===!1?r=ae({},Rv):r=ae(ae({},Rv),i),oa(t))return iM(this.currentUrlTree,t,r);let o=this.parseUrl(t);return iM(this.currentUrlTree,o,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(t,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((d,f)=>{a=d,c=f});let u=this.pendingTasks.add();return ZM(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function SN(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Te(4008,!1)}var wN=new Le("");function cy(n,...e){return lr([{provide:pf,multi:!0,useValue:n},[],{provide:es,useFactory:CN},{provide:Fd,multi:!0,useFactory:TN},e.map(t=>t.\u0275providers)])}function CN(){return J(ay).routerState.root}function TN(){let n=J(In);return e=>{let t=n.get(pr);if(e!==t.components[0])return;let i=n.get(ay),r=n.get(DN);n.get(IN)===1&&i.initialNavigation(),n.get(AN,null,{optional:!0})?.setUpPreloading(),n.get(wN,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var DN=new Le("",{factory:()=>new Ht}),IN=new Le("",{factory:()=>1});var AN=new Le("");var ly="Service workers are disabled or not supported by this browser",fa=class{serviceWorker;worker;registration;events;constructor(e,t){if(this.serviceWorker=e,!e)this.worker=this.events=this.registration=new rt(i=>i.error(new Te(5601,!1)));else{let i=null,r=new Ht;this.worker=new rt(l=>(i!==null&&l.next(i),r.subscribe(u=>l.next(u))));let o=()=>{let{controller:l}=e;l!==null&&(i=l,r.next(i))};e.addEventListener("controllerchange",o),o(),this.registration=this.worker.pipe(gn(()=>e.getRegistration().then(l=>{if(!l)throw new Te(5601,!1);return l})));let s=new Ht;this.events=s.asObservable();let a=l=>{let{data:u}=l;u?.type&&s.next(u)};e.addEventListener("message",a),t?.get(pr,null,{optional:!0})?.onDestroy(()=>{e.removeEventListener("controllerchange",o),e.removeEventListener("message",a)})}}postMessage(e,t){return new Promise(i=>{this.worker.pipe(Cn(1)).subscribe(r=>{r.postMessage(ae({action:e},t)),i()})})}postMessageWithOperation(e,t,i){let r=this.waitForOperationCompleted(i),o=this.postMessage(e,t);return Promise.all([o,r]).then(([,s])=>s)}generateNonce(){return Math.round(Math.random()*1e7)}eventsOfType(e){let t;return typeof e=="string"?t=i=>i.type===e:t=i=>e.includes(i.type),this.events.pipe(ri(t))}nextEventOfType(e){return this.eventsOfType(e).pipe(Cn(1))}waitForOperationCompleted(e){return new Promise((t,i)=>{this.eventsOfType("OPERATION_COMPLETED").pipe(ri(r=>r.nonce===e),Cn(1),Mt(r=>{if(r.result!==void 0)return r.result;throw new Error(r.error)})).subscribe({next:t,error:i})})}get isEnabled(){return!!this.serviceWorker}},NN=(()=>{class n{sw;messages;notificationClicks;notificationCloses;pushSubscriptionChanges;subscription;get isEnabled(){return this.sw.isEnabled}pushManager=null;subscriptionChanges=new Ht;constructor(t){if(this.sw=t,!t.isEnabled){this.messages=or,this.notificationClicks=or,this.notificationCloses=or,this.pushSubscriptionChanges=or,this.subscription=or;return}this.messages=this.sw.eventsOfType("PUSH").pipe(Mt(r=>r.data)),this.notificationClicks=this.sw.eventsOfType("NOTIFICATION_CLICK").pipe(Mt(r=>r.data)),this.notificationCloses=this.sw.eventsOfType("NOTIFICATION_CLOSE").pipe(Mt(r=>r.data)),this.pushSubscriptionChanges=this.sw.eventsOfType("PUSH_SUBSCRIPTION_CHANGE").pipe(Mt(r=>r.data)),this.pushManager=this.sw.registration.pipe(Mt(r=>r.pushManager));let i=this.pushManager.pipe(gn(r=>r.getSubscription()));this.subscription=new rt(r=>{let o=i.subscribe(r),s=this.subscriptionChanges.subscribe(r);return()=>{o.unsubscribe(),s.unsubscribe()}})}requestSubscription(t){if(!this.sw.isEnabled||this.pushManager===null)return Promise.reject(new Error(ly));let i={userVisibleOnly:!0},r=this.decodeBase64(t.serverPublicKey.replace(/_/g,"/").replace(/-/g,"+")),o=new Uint8Array(new ArrayBuffer(r.length));for(let s=0;s<r.length;s++)o[s]=r.charCodeAt(s);return i.applicationServerKey=o,new Promise((s,a)=>{this.pushManager.pipe(gn(c=>c.subscribe(i)),Cn(1)).subscribe({next:c=>{this.subscriptionChanges.next(c),s(c)},error:a})})}unsubscribe(){if(!this.sw.isEnabled)return Promise.reject(new Error(ly));let t=i=>{if(i===null)throw new Te(5602,!1);return i.unsubscribe().then(r=>{if(!r)throw new Te(5603,!1);this.subscriptionChanges.next(null)})};return new Promise((i,r)=>{this.subscription.pipe(Cn(1),gn(t)).subscribe({next:i,error:r})})}decodeBase64(t){return atob(t)}static \u0275fac=function(i){return new(i||n)(je(fa))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),PN=(()=>{class n{sw;versionUpdates;unrecoverable;get isEnabled(){return this.sw.isEnabled}ongoingCheckForUpdate=null;constructor(t){if(this.sw=t,!t.isEnabled){this.versionUpdates=or,this.unrecoverable=or;return}this.versionUpdates=this.sw.eventsOfType(["VERSION_DETECTED","VERSION_INSTALLATION_FAILED","VERSION_READY","NO_NEW_VERSION_DETECTED"]),this.unrecoverable=this.sw.eventsOfType("UNRECOVERABLE_STATE")}checkForUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Error(ly));if(this.ongoingCheckForUpdate)return this.ongoingCheckForUpdate;let t=this.sw.generateNonce();return this.ongoingCheckForUpdate=this.sw.postMessageWithOperation("CHECK_FOR_UPDATES",{nonce:t},t).finally(()=>{this.ongoingCheckForUpdate=null}),this.ongoingCheckForUpdate}activateUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Te(5601,!1));let t=this.sw.generateNonce();return this.sw.postMessageWithOperation("ACTIVATE_UPDATE",{nonce:t},t)}static \u0275fac=function(i){return new(i||n)(je(fa))};static \u0275prov=Pe({token:n,factory:n.\u0275fac})}return n})(),KM=new Le("");function ON(){let n=J(Jc);if(!("serviceWorker"in navigator&&n.enabled!==!1))return;let e=J(KM),t=J(An),i=J(pr);t.runOutsideAngular(()=>{let r=navigator.serviceWorker,o=()=>r.controller?.postMessage({action:"INITIALIZE"});r.addEventListener("controllerchange",o),i.onDestroy(()=>{r.removeEventListener("controllerchange",o)})}),t.runOutsideAngular(()=>{let r,{registrationStrategy:o}=n;if(typeof o=="function")r=new Promise(s=>o().subscribe(()=>s()));else{let[s,...a]=(o||"registerWhenStable:30000").split(":");switch(s){case"registerImmediately":r=Promise.resolve();break;case"registerWithDelay":r=JM(+a[0]||0);break;case"registerWhenStable":r=Promise.race([i.whenStable(),JM(+a[0])]);break;default:throw new Te(5600,!1)}}r.then(()=>{i.destroyed||navigator.serviceWorker.register(e,{scope:n.scope,updateViaCache:n.updateViaCache,type:n.type}).catch(s=>console.error(No(5604,!1)))})})}function JM(n){return new Promise(e=>setTimeout(e,n))}function LN(){let n=J(Jc),e=J(In),t=!0;return new fa(t&&n.enabled!==!1?navigator.serviceWorker:void 0,e)}var Jc=class{enabled;updateViaCache;type;scope;registrationStrategy};function QM(n,e={}){return lr([NN,PN,{provide:KM,useValue:n},{provide:Jc,useValue:e},{provide:fa,useFactory:LN},Ld(ON)])}var eS=[];var tS={providers:[eg(),cy(eS),QM("ngsw-worker.js",{enabled:!Hb(),registrationStrategy:"registerWhenStable:30000"})]};var mf=[{id:"1",text:"\u0420\u0430\u0437\u043C\u0435\u0442\u043A\u0430 \u0438 \u0432\u044B\u0440\u0430\u0432\u043D\u0438\u0432\u0430\u043D\u0438\u0435 \u043E\u0441\u043D\u043E\u0432\u0430\u043D\u0438\u044F",done:!0},{id:"2",text:"\u0423\u0441\u0442\u0430\u043D\u043E\u0432\u043A\u0430 \u0430\u043D\u043A\u0435\u0440\u043D\u044B\u0445 \u0431\u043E\u043B\u0442\u043E\u0432",done:!0},{id:"3",text:"\u041F\u0440\u043E\u0432\u0435\u0440\u043A\u0430 \u0433\u043E\u0440\u0438\u0437\u043E\u043D\u0442\u0430\u043B\u044C\u043D\u043E\u0433\u043E \u0443\u0440\u043E\u0432\u043D\u044F",done:!1}],gf=[{id:"1",text:"\u0423\u0441\u0442\u0430\u043D\u043E\u0432\u043A\u0430 \u043D\u0430 \u0444\u0443\u043D\u0434\u0430\u043C\u0435\u043D\u0442",done:!0},{id:"2",text:"\u0412\u0435\u0440\u0442\u0438\u043A\u0430\u043B\u044C\u043D\u043E\u0435 \u0432\u044B\u0440\u0430\u0432\u043D\u0438\u0432\u0430\u043D\u0438\u0435",done:!0},{id:"3",text:"\u0417\u0430\u0442\u044F\u0436\u043A\u0430 \u0431\u043E\u043B\u0442\u043E\u0432\u044B\u0445 \u0441\u043E\u0435\u0434\u0438\u043D\u0435\u043D\u0438\u0439",done:!1},{id:"4",text:"\u0421\u0432\u0430\u0440\u043A\u0430 \u0441\u0442\u044B\u043A\u043E\u0432",done:!1}],nS=[{id:"1",text:"\u041F\u043E\u0434\u044A\u0451\u043C \u0438 \u043F\u043E\u0437\u0438\u0446\u0438\u043E\u043D\u0438\u0440\u043E\u0432\u0430\u043D\u0438\u0435",done:!0},{id:"2",text:"\u041A\u0440\u0435\u043F\u043B\u0435\u043D\u0438\u0435 \u043A \u0441\u0442\u043E\u0439\u043A\u0430\u043C",done:!1},{id:"3",text:"\u041A\u043E\u043D\u0442\u0440\u043E\u043B\u044C \u0433\u0435\u043E\u043C\u0435\u0442\u0440\u0438\u0438 \u043F\u0440\u043E\u043B\u0451\u0442\u0430",done:!1}],iS=[{id:"1",text:"\u041F\u043E\u0434\u0433\u043E\u0442\u043E\u0432\u043A\u0430 \u043E\u043F\u043E\u0440\u043D\u043E\u0433\u043E \u043A\u043E\u043D\u0442\u0443\u0440\u0430",done:!1},{id:"2",text:"\u041C\u043E\u043D\u0442\u0430\u0436 \u043F\u0430\u043D\u0435\u043B\u0438",done:!1},{id:"3",text:"\u0413\u0435\u0440\u043C\u0435\u0442\u0438\u0437\u0430\u0446\u0438\u044F \u0441\u0442\u044B\u043A\u043E\u0432",done:!1},{id:"4",text:"\u041F\u0440\u043E\u0432\u0435\u0440\u043A\u0430 \u043F\u043B\u043E\u0441\u043A\u043E\u0441\u0442\u043D\u043E\u0441\u0442\u0438",done:!1}],rS=[{id:"1",text:"\u0423\u0441\u0442\u0430\u043D\u043E\u0432\u043A\u0430 \u0442\u043E\u0440\u0446\u0435\u0432\u043E\u0439 \u043F\u0430\u043D\u0435\u043B\u0438",done:!1},{id:"2",text:"\u0421\u0442\u044B\u043A\u043E\u0432\u043A\u0430 \u0441\u043E \u0441\u0442\u0435\u043D\u0430\u043C\u0438",done:!1},{id:"3",text:"\u0413\u0435\u0440\u043C\u0435\u0442\u0438\u0437\u0430\u0446\u0438\u044F",done:!1}],FN=[{id:"1",text:"\u041C\u043E\u043D\u0442\u0430\u0436 \u043A\u0440\u043E\u0432\u0435\u043B\u044C\u043D\u043E\u0439 \u043F\u0430\u043D\u0435\u043B\u0438",done:!1},{id:"2",text:"\u041A\u0440\u0435\u043F\u043B\u0435\u043D\u0438\u0435 \u043A \u0431\u0430\u043B\u043A\u0430\u043C",done:!1},{id:"3",text:"\u041F\u0440\u043E\u0432\u0435\u0440\u043A\u0430 \u0432\u043E\u0434\u043E\u043D\u0435\u043F\u0440\u043E\u043D\u0438\u0446\u0430\u0435\u043C\u043E\u0441\u0442\u0438",done:!1}],kN=[{id:"fundament1",name:"\u0424\u0443\u043D\u0434\u0430\u043C\u0435\u043D\u0442 1",stlFile:"fundament.stl",status:"installed",quality:"approved",checklist:mf,position:{x:0,y:0,z:0},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"fundament2",name:"\u0424\u0443\u043D\u0434\u0430\u043C\u0435\u043D\u0442 2",stlFile:"fundament.stl",status:"installed",quality:"approved",checklist:mf,position:{x:2.7,y:0,z:0},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"fundament3",name:"\u0424\u0443\u043D\u0434\u0430\u043C\u0435\u043D\u0442 3",stlFile:"fundament.stl",status:"installed",quality:"approved",checklist:mf,position:{x:2.7,y:0,z:5.8},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"fundament4",name:"\u0424\u0443\u043D\u0434\u0430\u043C\u0435\u043D\u0442 4",stlFile:"fundament.stl",status:"installed",quality:"approved",checklist:mf,position:{x:0,y:0,z:5.8},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"stoyka1",name:"\u0421\u0442\u043E\u0439\u043A\u0430 1",stlFile:"stoyka.stl",status:"installed",quality:"approved",checklist:gf,position:{x:0,y:1.7,z:0},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"stoyka2",name:"\u0421\u0442\u043E\u0439\u043A\u0430 2",stlFile:"stoyka.stl",status:"installed",quality:"rework",checklist:gf,position:{x:2.7,y:1.7,z:0},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"stoyka3",name:"\u0421\u0442\u043E\u0439\u043A\u0430 3",stlFile:"stoyka.stl",status:"installed",quality:"approved",checklist:gf,position:{x:2.7,y:1.7,z:5.8},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"stoyka4",name:"\u0421\u0442\u043E\u0439\u043A\u0430 4",stlFile:"stoyka.stl",status:"installed",quality:"approved",checklist:gf,position:{x:0,y:1.7,z:5.8},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"balka1",name:"\u0411\u0430\u043B\u043A\u0430 1",stlFile:"balka.stl",status:"installed",quality:"approved",checklist:nS,position:{x:1.35,y:3.37,z:0},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"balka2",name:"\u0411\u0430\u043B\u043A\u0430 2",stlFile:"balka.stl",status:"installed",quality:"approved",checklist:nS,position:{x:1.35,y:3.37,z:5.8},rotation:{rx:-90,ry:0,rz:0},photos:[]},{id:"stena_dver",name:"\u0421\u0442\u0435\u043D\u0430 (\u043E\u043A\u043D\u043E+\u0434\u0432\u0435\u0440\u044C)",stlFile:"stena_okno_dver.stl",status:"in_progress",quality:null,checklist:iS,position:{x:2.9,y:2,z:2.9},rotation:{rx:0,ry:90,rz:90},photos:[]},{id:"stena_okno",name:"\u0421\u0442\u0435\u043D\u0430 (\u043E\u043A\u043D\u043E+\u043E\u043A\u043D\u043E)",stlFile:"stena_okno_okno.stl",status:"in_progress",quality:null,checklist:iS,position:{x:-.05,y:2,z:2.9},rotation:{rx:0,ry:-90,rz:90},photos:[]},{id:"torets_gluhoy",name:"\u0422\u043E\u0440\u0435\u0446 \u0433\u043B\u0443\u0445\u043E\u0439",stlFile:"torets_gluhoy.stl",status:"not_installed",quality:null,checklist:rS,position:{x:1.35,y:2,z:6},rotation:{rx:0,ry:0,rz:90},photos:[]},{id:"torets_okno",name:"\u0422\u043E\u0440\u0435\u0446 \u0441 \u043E\u043A\u043D\u043E\u043C",stlFile:"torets_okno.stl",status:"not_installed",quality:null,checklist:rS,position:{x:1.35,y:2,z:-.1},rotation:{rx:0,ry:180,rz:90},photos:[]},{id:"krovlya",name:"\u041A\u0440\u043E\u0432\u043B\u044F",stlFile:"krovlya.stl",status:"not_installed",quality:null,checklist:FN,position:{x:1.35,y:3.55,z:2.9},rotation:{rx:-90,ry:0,rz:0},photos:[]}],$i=class n{nodes=Hn(kN);selected=Hn(null);selectedNode=Qs(()=>{let e=this.nodes(),t=this.selected();for(let i of e)if(i.id===t)return i;return null});stats=Qs(()=>{let e=this.nodes(),t=0,i=0,r=0;for(let o of e)o.status==="installed"&&(t=t+1),o.status==="in_progress"&&(i=i+1),o.status==="not_installed"&&(r=r+1);return{total:e.length,installed:t,inProgress:i,notInstalled:r}});select(e){this.selected.set(e)}patch(e,t){this.nodes.update(i=>i.map(r=>r.id===e?ae(ae({},r),t):r))}addPhoto(e,t){let i=this.selectedNode();if(i===null)return;let r=[...i.photos,t];this.patch(e,{photos:r})}addChecklistItem(e,t){let i=t.trim();if(i==="")return;let r=this.nodes(),o=null;for(let c of r)if(c.id===e){o=c;break}if(o===null)return;let s={id:crypto.randomUUID(),text:i,done:!1},a=[...o.checklist,s];this.patch(e,{checklist:a})}toggleChecklistItem(e,t){let i=this.nodes(),r=null;for(let s of i)if(s.id===e){r=s;break}if(r===null)return;let o=r.checklist.map(s=>s.id===t?it(ae({},s),{done:!s.done}):s);this.patch(e,{checklist:o})}editChecklistItem(e,t,i){let r=i.trim();if(r==="")return;let o=this.nodes(),s=null;for(let c of o)if(c.id===e){s=c;break}if(s===null)return;let a=s.checklist.map(c=>c.id===t?it(ae({},c),{text:r}):c);this.patch(e,{checklist:a})}deleteChecklistItem(e,t){let i=this.nodes(),r=null;for(let s of i)if(s.id===e){r=s;break}if(r===null)return;let o=r.checklist.filter(s=>s.id!==t);this.patch(e,{checklist:o})}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Pe({token:n,factory:n.\u0275fac,providedIn:"root"})};var vf=class n{state=J($i);get s(){return this.state.stats()}get progressPercent(){return this.s.total===0?0:Math.round(this.s.installed/this.s.total*100)}get circleLength(){return(2*Math.PI*16).toFixed(2)}get circleOffset(){return(2*Math.PI*16*(1-this.progressPercent/100)).toFixed(2)}exportCsv(){let e=["ID","\u041D\u0430\u0437\u0432\u0430\u043D\u0438\u0435","\u0424\u0430\u0439\u043B","\u0421\u0442\u0430\u0442\u0443\u0441","X","Y","Z","RX","RY","RZ","\u041E\u0446\u0435\u043D\u043A\u0430"].join(";"),t=this.state.nodes().map(r=>[r.id,r.name,r.stlFile,r.status,r.position.x,r.position.y,r.position.z,r.rotation.rx,r.rotation.ry,r.rotation.rz,r.quality??""].join(";")),i="\uFEFF"+[e,...t].join(`
`);this.downloadFile(i,"cuby.csv","text/csv;charset=utf-8")}exportJson(){let e=JSON.stringify(this.state.nodes(),null,2);this.downloadFile(e,"cuby.json","application/json")}downloadFile(e,t,i){let r=new Blob([e],{type:i}),o=URL.createObjectURL(r),s=document.createElement("a");s.href=o,s.download=t,s.click(),URL.revokeObjectURL(o)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Zn({type:n,selectors:[["app-header"]],decls:33,vars:6,consts:[[1,"brand"],[1,"logo"],[1,"sub"],[1,"stats"],[1,"stat"],[1,"inst"],[1,"prog"],[1,"pend"],[1,"ring"],["viewBox","0 0 40 40"],["cx","20","cy","20","r","16","fill","none","stroke","#1e293b","stroke-width","4"],["cx","20","cy","20","r","16","fill","none","stroke","#22c55e","stroke-width","4","stroke-linecap","round","transform","rotate(-90 20 20)"],[1,"actions"],[3,"click"]],template:function(t,i){t&1&&(te(0,"header")(1,"div",0)(2,"span",1),me(3,"\u2B21 CUBY"),le(),te(4,"span",2),me(5,"Assembly Monitor"),le()(),te(6,"div",3)(7,"div",4)(8,"span",5),me(9),le(),te(10,"em"),me(11,"\u0423\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D\u043E"),le()(),te(12,"div",4)(13,"span",6),me(14),le(),te(15,"em"),me(16,"\u0412 \u0440\u0430\u0431\u043E\u0442\u0435"),le()(),te(17,"div",4)(18,"span",7),me(19),le(),te(20,"em"),me(21,"\u041D\u0435 \u0443\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D\u043E"),le()(),te(22,"div",8),zu(),te(23,"svg",9),Gn(24,"circle",10)(25,"circle",11),le(),Gu(),te(26,"span"),me(27),le()()(),te(28,"div",12)(29,"button",13),dt("click",function(){return i.exportCsv()}),me(30,"\u2B07 CSV"),le(),te(31,"button",13),dt("click",function(){return i.exportJson()}),me(32,"\u2B07 JSON"),le()()()),t&2&&(Oe(9),rn(i.s.installed),Oe(5),rn(i.s.inProgress),Oe(5),rn(i.s.notInstalled),Oe(6),kd("stroke-dasharray",i.circleLength)("stroke-dashoffset",i.circleOffset),Oe(2),Gi("",i.progressPercent,"%"))},dependencies:[Jr],styles:['@charset "UTF-8";header[_ngcontent-%COMP%]{display:flex;align-items:center;gap:20px;padding:10px 18px;background:#080d16;border-bottom:1px solid #1e293b;flex-wrap:wrap;flex-shrink:0}.brand[_ngcontent-%COMP%]{display:flex;flex-direction:column}.logo[_ngcontent-%COMP%]{font-size:18px;font-weight:700;color:#38bdf8;letter-spacing:2px;font-family:monospace}.sub[_ngcontent-%COMP%]{font-size:10px;color:#475569;letter-spacing:1px;text-transform:uppercase}.stats[_ngcontent-%COMP%]{display:flex;align-items:center;gap:14px;flex:1;flex-wrap:wrap}.stat[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:1px}.stat[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:20px;font-weight:700;line-height:1}.stat[_ngcontent-%COMP%]   em[_ngcontent-%COMP%]{font-size:10px;color:#475569;font-style:normal;text-transform:uppercase}.inst[_ngcontent-%COMP%]{color:#22c55e}.prog[_ngcontent-%COMP%]{color:#f59e0b}.pend[_ngcontent-%COMP%]{color:#64748b}.ring[_ngcontent-%COMP%]{position:relative;width:40px;height:40px}.ring[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{width:40px;height:40px}.ring[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#22c55e}.actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{background:transparent;border:1px solid #1e293b;border-radius:7px;color:#64748b;padding:5px 11px;font-size:12px;cursor:pointer;font-family:inherit;transition:all .15s;margin-left:6px}.actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]:hover{border-color:#38bdf8;color:#38bdf8}'],changeDetection:0})};var UN=(n,e)=>e.id;function BN(n,e){if(n&1&&(te(0,"span"),me(1),le()),n&2){let t=Ie().$implicit,i=Ie();mr(Xr("grade ",t.quality)),Oe(),rn(i.short(t.quality))}}function VN(n,e){if(n&1){let t=yn();te(0,"div",4),dt("click",function(){let r=ht(t).$implicit,o=Ie();return pt(o.state.select(r.id))}),te(1,"span"),me(2),le(),te(3,"div",5)(4,"div",6),me(5),le(),te(6,"div",7),me(7),le()(),Nn(8,BN,2,4,"span",8),le()}if(n&2){let t=e.$implicit,i=Ie();zi("sel",i.state.selected()===t.id),Oe(),mr(Xr("ico ",t.status)),Oe(),rn(i.icon(t.status)),Oe(3),rn(t.name),Oe(2),rn(t.stlFile),Oe(),Pn(t.quality!==null?8:-1)}}var HN={installed:"\u2713",in_progress:"\u27F3",not_installed:"\u25CB"},zN={approved:"OK",rework:"!",defect:"\u2717"},yf=class n{state=J($i);icon(e){return HN[e]}short(e){return zN[e]}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Zn({type:n,selectors:[["app-sidebar"]],decls:8,vars:1,consts:[[1,"sidebar"],[1,"hdr"],[1,"list"],[1,"item",3,"sel"],[1,"item",3,"click"],[1,"info"],[1,"name"],[1,"file"],[3,"class"]],template:function(t,i){t&1&&(te(0,"div",0)(1,"div",1),me(2," \u042D\u043B\u0435\u043C\u0435\u043D\u0442\u044B "),te(3,"span"),me(4),le()(),te(5,"div",2),$r(6,VN,9,9,"div",3,UN),le()()),t&2&&(Oe(4),rn(i.state.nodes().length),Oe(2),qr(i.state.nodes()))},dependencies:[Jr],styles:['@charset "UTF-8";.sidebar[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:100%;background:#080d16}.hdr[_ngcontent-%COMP%]{padding:12px 14px;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#475569;border-bottom:1px solid #1e293b;display:flex;justify-content:space-between;align-items:center}.hdr[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{background:#1e293b;border-radius:10px;padding:1px 8px;font-size:11px;color:#64748b}.list[_ngcontent-%COMP%]{overflow-y:auto;flex:1}.item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:9px;padding:9px 14px;cursor:pointer;border-bottom:1px solid #0f172a;transition:background .15s}.item[_ngcontent-%COMP%]:hover{background:#0f172a}.item.sel[_ngcontent-%COMP%]{background:#38bdf80f;border-left:2px solid #38bdf8;padding-left:12px}.ico[_ngcontent-%COMP%]{font-size:13px;width:18px;text-align:center;font-weight:700}.ico.installed[_ngcontent-%COMP%]{color:#22c55e}.ico.in_progress[_ngcontent-%COMP%]{color:#f59e0b}.ico.not_installed[_ngcontent-%COMP%]{color:#475569}.info[_ngcontent-%COMP%]{flex:1;min-width:0}.name[_ngcontent-%COMP%]{font-size:12px;color:#cbd5e1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.file[_ngcontent-%COMP%]{font-size:10px;color:#334155;font-family:monospace}.grade[_ngcontent-%COMP%]{font-size:11px;font-weight:700;padding:2px 7px;border-radius:10px}.grade.approved[_ngcontent-%COMP%]{background:#22c55e1a;color:#22c55e}.grade.rework[_ngcontent-%COMP%]{background:#f59e0b1a;color:#f59e0b}.grade.defect[_ngcontent-%COMP%]{background:#ef44441a;color:#ef4444}'],changeDetection:0})};var RS=0,Xy=1,NS=2;var wl=1,PS=2,ka=3,Cr=0,kn=1,Ki=2,Qi=0,cs=1,Yy=2,Zy=3,Jy=4,OS=5;var so=100,LS=101,FS=102,kS=103,US=104,BS=200,VS=201,HS=202,zS=203,$f=204,qf=205,GS=206,jS=207,WS=208,$S=209,qS=210,XS=211,YS=212,ZS=213,JS=214,Xf=0,Yf=1,Zf=2,ls=3,Jf=4,Kf=5,Qf=6,eh=7,Ky=0,KS=1,QS=2,Ai=0,Qy=1,e0=2,t0=3,n0=4,i0=5,r0=6,o0=7;var Ly=300,po=301,hs=302,Th=303,Dh=304,Cl=306,th=1e3,Yi=1001,nh=1002,sn=1003,ew=1004;var Tl=1005;var hn=1006,Ih=1007;var mo=1008;var $n=1009,s0=1010,a0=1011,Ua=1012,Ah=1013,Ri=1014,Ni=1015,er=1016,Rh=1017,Nh=1018,Ba=1020,c0=35902,l0=35899,u0=1021,d0=1022,fi=1023,Zi=1026,go=1027,f0=1028,Ph=1029,ps=1030,Oh=1031;var Lh=1033,Dl=33776,Il=33777,Al=33778,Rl=33779,Fh=35840,kh=35841,Uh=35842,Bh=35843,Vh=36196,Hh=37492,zh=37496,Gh=37488,jh=37489,Wh=37490,$h=37491,qh=37808,Xh=37809,Yh=37810,Zh=37811,Jh=37812,Kh=37813,Qh=37814,ep=37815,tp=37816,np=37817,ip=37818,rp=37819,op=37820,sp=37821,ap=36492,cp=36494,lp=36495,up=36283,dp=36284,fp=36285,hp=36286;var rl=2300,ih=2301,jf=2302,Fy=2303,ky=2400,Uy=2401,By=2402;var tw=3200;var h0=0,nw=1,Ir="",Fn="srgb",us="srgb-linear",ol="linear",xt="srgb";var ss=7680;var Vy=519,iw=512,rw=513,ow=514,pp=515,sw=516,aw=517,mp=518,cw=519,Hy=35044;var p0="300 es",Di=2e3,Ca=2001;function GN(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function jN(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function sl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function lw(){let n=sl("canvas");return n.style.display="block",n}var oS={},Ta=null;function m0(...n){let e="THREE."+n.shift();Ta?Ta("log",e,...n):console.log(e,...n)}function uw(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Fe(...n){n=uw(n);let e="THREE."+n.shift();if(Ta)Ta("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Ue(...n){n=uw(n);let e="THREE."+n.shift();if(Ta)Ta("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function al(...n){let e=n.join(" ");e in oS||(oS[e]=!0,Fe(...n))}function dw(n,e,t){return new Promise(function(i,r){function o(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(o,t);break;default:i()}}setTimeout(o,t)})}var fw={[Xf]:Yf,[Zf]:Qf,[Jf]:eh,[ls]:Kf,[Yf]:Xf,[Qf]:Zf,[eh]:Jf,[Kf]:ls},Tr=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let o=r.indexOf(t);o!==-1&&r.splice(o,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let o=0,s=r.length;o<s;o++)r[o].call(this,e);e.target=null}}},_n=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var Wf=Math.PI/180,rh=180/Math.PI;function Nl(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(_n[n&255]+_n[n>>8&255]+_n[n>>16&255]+_n[n>>24&255]+"-"+_n[e&255]+_n[e>>8&255]+"-"+_n[e>>16&15|64]+_n[e>>24&255]+"-"+_n[t&63|128]+_n[t>>8&255]+"-"+_n[t>>16&255]+_n[t>>24&255]+_n[i&255]+_n[i>>8&255]+_n[i>>16&255]+_n[i>>24&255]).toLowerCase()}function at(n,e,t){return Math.max(e,Math.min(t,n))}function WN(n,e){return(n%e+e)%e}function uy(n,e,t){return(1-t)*n+t*e}function Kc(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function jn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var lt=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=at(this.x,e.x,t.x),this.y=at(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=at(this.x,e,t),this.y=at(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(at(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(at(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),o=this.x-e.x,s=this.y-e.y;return this.x=o*i-s*r+e.x,this.y=o*r+s*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Ji=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,o,s,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=o[s+0],h=o[s+1],p=o[s+2],y=o[s+3];if(d!==y||c!==f||l!==h||u!==p){let g=c*f+l*h+u*p+d*y;g<0&&(f=-f,h=-h,p=-p,y=-y,g=-g);let m=1-a;if(g<.9995){let E=Math.acos(g),S=Math.sin(E);m=Math.sin(m*E)/S,a=Math.sin(a*E)/S,c=c*m+f*a,l=l*m+h*a,u=u*m+p*a,d=d*m+y*a}else{c=c*m+f*a,l=l*m+h*a,u=u*m+p*a,d=d*m+y*a;let E=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=E,l*=E,u*=E,d*=E}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,o,s){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=o[s],f=o[s+1],h=o[s+2],p=o[s+3];return e[t]=a*p+u*d+c*h-l*f,e[t+1]=c*p+u*f+l*d-a*h,e[t+2]=l*p+u*h+a*f-c*d,e[t+3]=u*p-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,o=e._z,s=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(o/2),f=c(i/2),h=c(r/2),p=c(o/2);switch(s){case"XYZ":this._x=f*u*d+l*h*p,this._y=l*h*d-f*u*p,this._z=l*u*p+f*h*d,this._w=l*u*d-f*h*p;break;case"YXZ":this._x=f*u*d+l*h*p,this._y=l*h*d-f*u*p,this._z=l*u*p-f*h*d,this._w=l*u*d+f*h*p;break;case"ZXY":this._x=f*u*d-l*h*p,this._y=l*h*d+f*u*p,this._z=l*u*p+f*h*d,this._w=l*u*d-f*h*p;break;case"ZYX":this._x=f*u*d-l*h*p,this._y=l*h*d+f*u*p,this._z=l*u*p-f*h*d,this._w=l*u*d+f*h*p;break;case"YZX":this._x=f*u*d+l*h*p,this._y=l*h*d+f*u*p,this._z=l*u*p-f*h*d,this._w=l*u*d-f*h*p;break;case"XZY":this._x=f*u*d-l*h*p,this._y=l*h*d-f*u*p,this._z=l*u*p+f*h*d,this._w=l*u*d+f*h*p;break;default:Fe("Quaternion: .setFromEuler() encountered an unknown order: "+s)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],o=t[8],s=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(o-l)*h,this._z=(s-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+s)/h,this._z=(o+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(o-l)/h,this._x=(r+s)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(s-r)/h,this._x=(o+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(at(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,o=e._z,s=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+s*a+r*l-o*c,this._y=r*u+s*c+o*a-i*l,this._z=o*u+s*l+i*c-r*a,this._w=s*u-i*a-r*c-o*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,o=e._z,s=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,o=-o,s=-s,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+o*t,this._w=this._w*c+s*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+o*t,this._w=this._w*c+s*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),o=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),o*Math.sin(t),o*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},L=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(sS.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(sS.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,o=e.elements;return this.x=o[0]*t+o[3]*i+o[6]*r,this.y=o[1]*t+o[4]*i+o[7]*r,this.z=o[2]*t+o[5]*i+o[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,o=e.elements,s=1/(o[3]*t+o[7]*i+o[11]*r+o[15]);return this.x=(o[0]*t+o[4]*i+o[8]*r+o[12])*s,this.y=(o[1]*t+o[5]*i+o[9]*r+o[13])*s,this.z=(o[2]*t+o[6]*i+o[10]*r+o[14])*s,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,o=e.x,s=e.y,a=e.z,c=e.w,l=2*(s*r-a*i),u=2*(a*t-o*r),d=2*(o*i-s*t);return this.x=t+c*l+s*d-a*u,this.y=i+c*u+a*l-o*d,this.z=r+c*d+o*u-s*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r,this.y=o[1]*t+o[5]*i+o[9]*r,this.z=o[2]*t+o[6]*i+o[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=at(this.x,e.x,t.x),this.y=at(this.y,e.y,t.y),this.z=at(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=at(this.x,e,t),this.y=at(this.y,e,t),this.z=at(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(at(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,o=e.z,s=t.x,a=t.y,c=t.z;return this.x=r*c-o*a,this.y=o*s-i*c,this.z=i*a-r*s,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return dy.copy(this).projectOnVector(e),this.sub(dy)}reflect(e){return this.sub(dy.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(at(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},dy=new L,sS=new Ji,Ye=class n{constructor(e,t,i,r,o,s,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,o,s,a,c,l)}set(e,t,i,r,o,s,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=o,u[5]=c,u[6]=i,u[7]=s,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,o=this.elements,s=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],p=i[8],y=r[0],g=r[3],m=r[6],E=r[1],S=r[4],w=r[7],D=r[2],C=r[5],I=r[8];return o[0]=s*y+a*E+c*D,o[3]=s*g+a*S+c*C,o[6]=s*m+a*w+c*I,o[1]=l*y+u*E+d*D,o[4]=l*g+u*S+d*C,o[7]=l*m+u*w+d*I,o[2]=f*y+h*E+p*D,o[5]=f*g+h*S+p*C,o[8]=f*m+h*w+p*I,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],o=e[3],s=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*s*u-t*a*l-i*o*u+i*a*c+r*o*l-r*s*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],o=e[3],s=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*s-a*l,f=a*c-u*o,h=l*o-s*c,p=t*d+i*f+r*h;if(p===0)return this.set(0,0,0,0,0,0,0,0,0);let y=1/p;return e[0]=d*y,e[1]=(r*l-u*i)*y,e[2]=(a*i-r*s)*y,e[3]=f*y,e[4]=(u*t-r*c)*y,e[5]=(r*o-a*t)*y,e[6]=h*y,e[7]=(i*c-l*t)*y,e[8]=(s*t-i*o)*y,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,o,s,a){let c=Math.cos(o),l=Math.sin(o);return this.set(i*c,i*l,-i*(c*s+l*a)+s+e,-r*l,r*c,-r*(-l*s+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(fy.makeScale(e,t)),this}rotate(e){return this.premultiply(fy.makeRotation(-e)),this}translate(e,t){return this.premultiply(fy.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},fy=new Ye,aS=new Ye().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),cS=new Ye().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function $N(){let n={enabled:!0,workingColorSpace:us,spaces:{},convert:function(r,o,s){return this.enabled===!1||o===s||!o||!s||(this.spaces[o].transfer===xt&&(r.r=wr(r.r),r.g=wr(r.g),r.b=wr(r.b)),this.spaces[o].primaries!==this.spaces[s].primaries&&(r.applyMatrix3(this.spaces[o].toXYZ),r.applyMatrix3(this.spaces[s].fromXYZ)),this.spaces[s].transfer===xt&&(r.r=wa(r.r),r.g=wa(r.g),r.b=wa(r.b))),r},workingToColorSpace:function(r,o){return this.convert(r,this.workingColorSpace,o)},colorSpaceToWorking:function(r,o){return this.convert(r,o,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===Ir?ol:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,o=this.workingColorSpace){return r.fromArray(this.spaces[o].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,o,s){return r.copy(this.spaces[o].toXYZ).multiply(this.spaces[s].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,o){return al("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,o)},toWorkingColorSpace:function(r,o){return al("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,o)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[us]:{primaries:e,whitePoint:i,transfer:ol,toXYZ:aS,fromXYZ:cS,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Fn},outputColorSpaceConfig:{drawingBufferColorSpace:Fn}},[Fn]:{primaries:e,whitePoint:i,transfer:xt,toXYZ:aS,fromXYZ:cS,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Fn}}}),n}var ct=$N();function wr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function wa(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var ha,oh=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{ha===void 0&&(ha=sl("canvas")),ha.width=e.width,ha.height=e.height;let r=ha.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=ha}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=sl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),o=r.data;for(let s=0;s<o.length;s++)o[s]=wr(o[s]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(wr(t[i]/255)*255):t[i]=wr(t[i]);return{data:t,width:e.width,height:e.height}}else return Fe("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},qN=0,Da=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:qN++}),this.uuid=Nl(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayHeight,t.displayWidth,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let o;if(Array.isArray(r)){o=[];for(let s=0,a=r.length;s<a;s++)r[s].isDataTexture?o.push(hy(r[s].image)):o.push(hy(r[s]))}else o=hy(r);i.url=o}return t||(e.images[this.uuid]=i),i}};function hy(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?oh.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Fe("Texture: Unable to serialize Texture."),{})}var XN=0,py=new L,Ar=(()=>{class n extends Tr{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Yi,o=Yi,s=hn,a=mo,c=fi,l=$n,u=n.DEFAULT_ANISOTROPY,d=Ir){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:XN++}),this.uuid=Nl(),this.name="",this.source=new Da(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=o,this.magFilter=s,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new lt(0,0),this.repeat=new lt(1,1),this.center=new lt(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Ye,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0}get width(){return this.source.getSize(py).x}get height(){return this.source.getSize(py).y}get depth(){return this.source.getSize(py).z}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Fe(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let o=this[i];if(o===void 0){Fe(`Texture.setValues(): property '${i}' does not exist.`);continue}o&&r&&o.isVector2&&r.isVector2||o&&r&&o.isVector3&&r.isVector3||o&&r&&o.isMatrix3&&r.isMatrix3?o.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Ly)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case th:t.x=t.x-Math.floor(t.x);break;case Yi:t.x=t.x<0?0:1;break;case nh:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case th:t.y=t.y-Math.floor(t.y);break;case Yi:t.y=t.y<0?0:1;break;case nh:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Ly,n.DEFAULT_ANISOTROPY=1,n})(),Lt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,o=this.w,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r+s[12]*o,this.y=s[1]*t+s[5]*i+s[9]*r+s[13]*o,this.z=s[2]*t+s[6]*i+s[10]*r+s[14]*o,this.w=s[3]*t+s[7]*i+s[11]*r+s[15]*o,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,o,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],p=c[9],y=c[2],g=c[6],m=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-y)<.01&&Math.abs(p-g)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+y)<.1&&Math.abs(p+g)<.1&&Math.abs(l+h+m-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let S=(l+1)/2,w=(h+1)/2,D=(m+1)/2,C=(u+f)/4,I=(d+y)/4,_=(p+g)/4;return S>w&&S>D?S<.01?(i=0,r=.707106781,o=.707106781):(i=Math.sqrt(S),r=C/i,o=I/i):w>D?w<.01?(i=.707106781,r=0,o=.707106781):(r=Math.sqrt(w),i=C/r,o=_/r):D<.01?(i=.707106781,r=.707106781,o=0):(o=Math.sqrt(D),i=I/o,r=_/o),this.set(i,r,o,t),this}let E=Math.sqrt((g-p)*(g-p)+(d-y)*(d-y)+(f-u)*(f-u));return Math.abs(E)<.001&&(E=1),this.x=(g-p)/E,this.y=(d-y)/E,this.z=(f-u)/E,this.w=Math.acos((l+h+m-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=at(this.x,e.x,t.x),this.y=at(this.y,e.y,t.y),this.z=at(this.z,e.z,t.z),this.w=at(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=at(this.x,e,t),this.y=at(this.y,e,t),this.z=at(this.z,e,t),this.w=at(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(at(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},sh=class extends Tr{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:hn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Lt(0,0,e,t),this.scissorTest=!1,this.viewport=new Lt(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},o=new Ar(r),s=i.count;for(let a=0;a<s;a++)this.textures[a]=o.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:hn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,o=this.textures.length;r<o;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new Da(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},ti=class extends sh{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},cl=class extends Ar{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=sn,this.minFilter=sn,this.wrapR=Yi,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var ah=class extends Ar{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=sn,this.minFilter=sn,this.wrapR=Yi,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ot=class n{constructor(e,t,i,r,o,s,a,c,l,u,d,f,h,p,y,g){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,o,s,a,c,l,u,d,f,h,p,y,g)}set(e,t,i,r,o,s,a,c,l,u,d,f,h,p,y,g){let m=this.elements;return m[0]=e,m[4]=t,m[8]=i,m[12]=r,m[1]=o,m[5]=s,m[9]=a,m[13]=c,m[2]=l,m[6]=u,m[10]=d,m[14]=f,m[3]=h,m[7]=p,m[11]=y,m[15]=g,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/pa.setFromMatrixColumn(e,0).length(),o=1/pa.setFromMatrixColumn(e,1).length(),s=1/pa.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*o,t[5]=i[5]*o,t[6]=i[6]*o,t[7]=0,t[8]=i[8]*s,t[9]=i[9]*s,t[10]=i[10]*s,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,o=e.z,s=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(o),d=Math.sin(o);if(e.order==="XYZ"){let f=s*u,h=s*d,p=a*u,y=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+p*l,t[5]=f-y*l,t[9]=-a*c,t[2]=y-f*l,t[6]=p+h*l,t[10]=s*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,p=l*u,y=l*d;t[0]=f+y*a,t[4]=p*a-h,t[8]=s*l,t[1]=s*d,t[5]=s*u,t[9]=-a,t[2]=h*a-p,t[6]=y+f*a,t[10]=s*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,p=l*u,y=l*d;t[0]=f-y*a,t[4]=-s*d,t[8]=p+h*a,t[1]=h+p*a,t[5]=s*u,t[9]=y-f*a,t[2]=-s*l,t[6]=a,t[10]=s*c}else if(e.order==="ZYX"){let f=s*u,h=s*d,p=a*u,y=a*d;t[0]=c*u,t[4]=p*l-h,t[8]=f*l+y,t[1]=c*d,t[5]=y*l+f,t[9]=h*l-p,t[2]=-l,t[6]=a*c,t[10]=s*c}else if(e.order==="YZX"){let f=s*c,h=s*l,p=a*c,y=a*l;t[0]=c*u,t[4]=y-f*d,t[8]=p*d+h,t[1]=d,t[5]=s*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+p,t[10]=f-y*d}else if(e.order==="XZY"){let f=s*c,h=s*l,p=a*c,y=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+y,t[5]=s*u,t[9]=h*d-p,t[2]=p*d-h,t[6]=a*u,t[10]=y*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(YN,e,ZN)}lookAt(e,t,i){let r=this.elements;return Qn.subVectors(e,t),Qn.lengthSq()===0&&(Qn.z=1),Qn.normalize(),eo.crossVectors(i,Qn),eo.lengthSq()===0&&(Math.abs(i.z)===1?Qn.x+=1e-4:Qn.z+=1e-4,Qn.normalize(),eo.crossVectors(i,Qn)),eo.normalize(),_f.crossVectors(Qn,eo),r[0]=eo.x,r[4]=_f.x,r[8]=Qn.x,r[1]=eo.y,r[5]=_f.y,r[9]=Qn.y,r[2]=eo.z,r[6]=_f.z,r[10]=Qn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,o=this.elements,s=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],p=i[2],y=i[6],g=i[10],m=i[14],E=i[3],S=i[7],w=i[11],D=i[15],C=r[0],I=r[4],_=r[8],b=r[12],W=r[1],T=r[5],F=r[9],H=r[13],G=r[2],B=r[6],U=r[10],O=r[14],Z=r[3],K=r[7],he=r[11],_e=r[15];return o[0]=s*C+a*W+c*G+l*Z,o[4]=s*I+a*T+c*B+l*K,o[8]=s*_+a*F+c*U+l*he,o[12]=s*b+a*H+c*O+l*_e,o[1]=u*C+d*W+f*G+h*Z,o[5]=u*I+d*T+f*B+h*K,o[9]=u*_+d*F+f*U+h*he,o[13]=u*b+d*H+f*O+h*_e,o[2]=p*C+y*W+g*G+m*Z,o[6]=p*I+y*T+g*B+m*K,o[10]=p*_+y*F+g*U+m*he,o[14]=p*b+y*H+g*O+m*_e,o[3]=E*C+S*W+w*G+D*Z,o[7]=E*I+S*T+w*B+D*K,o[11]=E*_+S*F+w*U+D*he,o[15]=E*b+S*H+w*O+D*_e,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],o=e[12],s=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],p=e[3],y=e[7],g=e[11],m=e[15],E=c*h-l*f,S=a*h-l*d,w=a*f-c*d,D=s*h-l*u,C=s*f-c*u,I=s*d-a*u;return t*(y*E-g*S+m*w)-i*(p*E-g*D+m*C)+r*(p*S-y*D+m*I)-o*(p*w-y*C+g*I)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],o=e[3],s=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],p=e[12],y=e[13],g=e[14],m=e[15],E=t*a-i*s,S=t*c-r*s,w=t*l-o*s,D=i*c-r*a,C=i*l-o*a,I=r*l-o*c,_=u*y-d*p,b=u*g-f*p,W=u*m-h*p,T=d*g-f*y,F=d*m-h*y,H=f*m-h*g,G=E*H-S*F+w*T+D*W-C*b+I*_;if(G===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let B=1/G;return e[0]=(a*H-c*F+l*T)*B,e[1]=(r*F-i*H-o*T)*B,e[2]=(y*I-g*C+m*D)*B,e[3]=(f*C-d*I-h*D)*B,e[4]=(c*W-s*H-l*b)*B,e[5]=(t*H-r*W+o*b)*B,e[6]=(g*w-p*I-m*S)*B,e[7]=(u*I-f*w+h*S)*B,e[8]=(s*F-a*W+l*_)*B,e[9]=(i*W-t*F-o*_)*B,e[10]=(p*C-y*w+m*E)*B,e[11]=(d*w-u*C-h*E)*B,e[12]=(a*b-s*T-c*_)*B,e[13]=(t*T-i*b+r*_)*B,e[14]=(y*S-p*D-g*E)*B,e[15]=(u*D-d*S+f*E)*B,this}scale(e){let t=this.elements,i=e.x,r=e.y,o=e.z;return t[0]*=i,t[4]*=r,t[8]*=o,t[1]*=i,t[5]*=r,t[9]*=o,t[2]*=i,t[6]*=r,t[10]*=o,t[3]*=i,t[7]*=r,t[11]*=o,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),o=1-i,s=e.x,a=e.y,c=e.z,l=o*s,u=o*a;return this.set(l*s+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*s,0,l*c-r*a,u*c+r*s,o*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,o,s){return this.set(1,i,o,0,e,1,s,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,o=t._x,s=t._y,a=t._z,c=t._w,l=o+o,u=s+s,d=a+a,f=o*l,h=o*u,p=o*d,y=s*u,g=s*d,m=a*d,E=c*l,S=c*u,w=c*d,D=i.x,C=i.y,I=i.z;return r[0]=(1-(y+m))*D,r[1]=(h+w)*D,r[2]=(p-S)*D,r[3]=0,r[4]=(h-w)*C,r[5]=(1-(f+m))*C,r[6]=(g+E)*C,r[7]=0,r[8]=(p+S)*I,r[9]=(g-E)*I,r[10]=(1-(f+y))*I,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let o=this.determinant();if(o===0)return i.set(1,1,1),t.identity(),this;let s=pa.set(r[0],r[1],r[2]).length(),a=pa.set(r[4],r[5],r[6]).length(),c=pa.set(r[8],r[9],r[10]).length();o<0&&(s=-s),wi.copy(this);let l=1/s,u=1/a,d=1/c;return wi.elements[0]*=l,wi.elements[1]*=l,wi.elements[2]*=l,wi.elements[4]*=u,wi.elements[5]*=u,wi.elements[6]*=u,wi.elements[8]*=d,wi.elements[9]*=d,wi.elements[10]*=d,t.setFromRotationMatrix(wi),i.x=s,i.y=a,i.z=c,this}makePerspective(e,t,i,r,o,s,a=Di,c=!1){let l=this.elements,u=2*o/(t-e),d=2*o/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),p,y;if(c)p=o/(s-o),y=s*o/(s-o);else if(a===Di)p=-(s+o)/(s-o),y=-2*s*o/(s-o);else if(a===Ca)p=-s/(s-o),y=-s*o/(s-o);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=p,l[14]=y,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,o,s,a=Di,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),p,y;if(c)p=1/(s-o),y=s/(s-o);else if(a===Di)p=-2/(s-o),y=-(s+o)/(s-o);else if(a===Ca)p=-1/(s-o),y=-o/(s-o);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=p,l[14]=y,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},pa=new L,wi=new Ot,YN=new L(0,0,0),ZN=new L(1,1,1),eo=new L,_f=new L,Qn=new L,lS=new Ot,uS=new Ji,ao=(()=>{class n{constructor(t=0,i=0,r=0,o=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=o}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,o=this._order){return this._x=t,this._y=i,this._z=r,this._order=o,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let o=t.elements,s=o[0],a=o[4],c=o[8],l=o[1],u=o[5],d=o[9],f=o[2],h=o[6],p=o[10];switch(i){case"XYZ":this._y=Math.asin(at(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,p),this._z=Math.atan2(-a,s)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-at(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,p),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,s),this._z=0);break;case"ZXY":this._x=Math.asin(at(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,p),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,s));break;case"ZYX":this._y=Math.asin(-at(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,p),this._z=Math.atan2(l,s)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(at(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,s)):(this._x=0,this._y=Math.atan2(c,p));break;case"XZY":this._z=Math.asin(-at(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,s)):(this._x=Math.atan2(-d,p),this._y=0);break;default:Fe("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return lS.makeRotationFromQuaternion(t),this.setFromRotationMatrix(lS,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return uS.setFromEuler(this),this.setFromQuaternion(uS,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Ia=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},JN=0,dS=new L,ma=new Ji,yr=new Ot,xf=new L,Qc=new L,KN=new L,QN=new Ji,fS=new L(1,0,0),hS=new L(0,1,0),pS=new L(0,0,1),mS={type:"added"},eP={type:"removed"},ga={type:"childadded",child:null},my={type:"childremoved",child:null},Ii=(()=>{class n extends Tr{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:JN++}),this.uuid=Nl(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new L,i=new ao,r=new Ji,o=new L(1,1,1);function s(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(s),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:o},modelViewMatrix:{value:new Ot},normalMatrix:{value:new Ye}}),this.matrix=new Ot,this.matrixWorld=new Ot,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Ia,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return ma.setFromAxisAngle(t,i),this.quaternion.multiply(ma),this}rotateOnWorldAxis(t,i){return ma.setFromAxisAngle(t,i),this.quaternion.premultiply(ma),this}rotateX(t){return this.rotateOnAxis(fS,t)}rotateY(t){return this.rotateOnAxis(hS,t)}rotateZ(t){return this.rotateOnAxis(pS,t)}translateOnAxis(t,i){return dS.copy(t).applyQuaternion(this.quaternion),this.position.add(dS.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(fS,t)}translateY(t){return this.translateOnAxis(hS,t)}translateZ(t){return this.translateOnAxis(pS,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(yr.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?xf.copy(t):xf.set(t,i,r);let o=this.parent;this.updateWorldMatrix(!0,!1),Qc.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?yr.lookAt(Qc,xf,this.up):yr.lookAt(xf,Qc,this.up),this.quaternion.setFromRotationMatrix(yr),o&&(yr.extractRotation(o.matrixWorld),ma.setFromRotationMatrix(yr),this.quaternion.premultiply(ma.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Ue("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(mS),ga.child=t,this.dispatchEvent(ga),ga.child=null):Ue("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(eP),my.child=t,this.dispatchEvent(my),my.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),yr.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),yr.multiply(t.parent.matrixWorld)),t.applyMatrix4(yr),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(mS),ga.child=t,this.dispatchEvent(ga),ga.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,o=this.children.length;r<o;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let o=this.children;for(let s=0,a=o.length;s<a;s++)o[s].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Qc,t,KN),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Qc,QN,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,o=i.length;r<o;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,o=i.length;r<o;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,o=t.z,s=this.matrix.elements;s[12]+=i-s[0]*i-s[4]*r-s[8]*o,s[13]+=r-s[1]*i-s[5]*r-s[9]*o,s[14]+=o-s[2]*i-s[6]*r-s[10]*o}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,o=i.length;r<o;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let o=this.children;for(let s=0,a=o.length;s<a;s++)o[s].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let o={};o.uuid=this.uuid,o.type=this.type,this.name!==""&&(o.name=this.name),this.castShadow===!0&&(o.castShadow=!0),this.receiveShadow===!0&&(o.receiveShadow=!0),this.visible===!1&&(o.visible=!1),this.frustumCulled===!1&&(o.frustumCulled=!1),this.renderOrder!==0&&(o.renderOrder=this.renderOrder),this.static!==!1&&(o.static=this.static),Object.keys(this.userData).length>0&&(o.userData=this.userData),o.layers=this.layers.mask,o.matrix=this.matrix.toArray(),o.up=this.up.toArray(),this.pivot!==null&&(o.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(o.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(o.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(o.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(o.type="InstancedMesh",o.count=this.count,o.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(o.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(o.type="BatchedMesh",o.perObjectFrustumCulled=this.perObjectFrustumCulled,o.sortObjects=this.sortObjects,o.drawRanges=this._drawRanges,o.reservedRanges=this._reservedRanges,o.geometryInfo=this._geometryInfo.map(c=>it(ae({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),o.instanceInfo=this._instanceInfo.map(c=>ae({},c)),o.availableInstanceIds=this._availableInstanceIds.slice(),o.availableGeometryIds=this._availableGeometryIds.slice(),o.nextIndexStart=this._nextIndexStart,o.nextVertexStart=this._nextVertexStart,o.geometryCount=this._geometryCount,o.maxInstanceCount=this._maxInstanceCount,o.maxVertexCount=this._maxVertexCount,o.maxIndexCount=this._maxIndexCount,o.geometryInitialized=this._geometryInitialized,o.matricesTexture=this._matricesTexture.toJSON(t),o.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(o.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(o.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(o.boundingBox=this.boundingBox.toJSON()));function s(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?o.background=this.background.toJSON():this.background.isTexture&&(o.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(o.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){o.geometry=s(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];s(t.shapes,f)}else s(t.shapes,l)}}if(this.isSkinnedMesh&&(o.bindMode=this.bindMode,o.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(s(t.skeletons,this.skeleton),o.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(s(t.materials,this.material[l]));o.material=c}else o.material=s(t.materials,this.material);if(this.children.length>0){o.children=[];for(let c=0;c<this.children.length;c++)o.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){o.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];o.animations.push(s(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),p=a(t.animations),y=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),p.length>0&&(r.animations=p),y.length>0&&(r.nodes=y)}return r.object=o,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),t.pivot!==null&&(this.pivot=t.pivot.clone()),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let o=t.children[r];this.add(o.clone())}return this}}return n.DEFAULT_UP=new L(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),as=class extends Ii{constructor(){super(),this.isGroup=!0,this.type="Group"}},tP={type:"move"},Aa=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new as,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new as,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new L,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new L),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new as,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new L,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new L),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,o=null,s=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){s=!0;for(let y of e.hand.values()){let g=t.getJointPose(y,i),m=this._getHandJoint(l,y);g!==null&&(m.matrix.fromArray(g.transform.matrix),m.matrix.decompose(m.position,m.rotation,m.scale),m.matrixWorldNeedsUpdate=!0,m.jointRadius=g.radius),m.visible=g!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,p=.005;l.inputState.pinching&&f>h+p?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-p&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(o=t.getPose(e.gripSpace,i),o!==null&&(c.matrix.fromArray(o.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,o.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(o.linearVelocity)):c.hasLinearVelocity=!1,o.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(o.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&o!==null&&(r=o),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(tP)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=o!==null),l!==null&&(l.visible=s!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new as;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},hw={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},to={h:0,s:0,l:0},Ef={h:0,s:0,l:0};function gy(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var nt=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Fn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,ct.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=ct.workingColorSpace){return this.r=e,this.g=t,this.b=i,ct.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=ct.workingColorSpace){if(e=WN(e,1),t=at(t,0,1),i=at(i,0,1),t===0)this.r=this.g=this.b=i;else{let o=i<=.5?i*(1+t):i+t-i*t,s=2*i-o;this.r=gy(s,o,e+1/3),this.g=gy(s,o,e),this.b=gy(s,o,e-1/3)}return ct.colorSpaceToWorking(this,r),this}setStyle(e,t=Fn){function i(o){o!==void 0&&parseFloat(o)<1&&Fe("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let o,s=r[1],a=r[2];switch(s){case"rgb":case"rgba":if(o=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(o[4]),this.setRGB(Math.min(255,parseInt(o[1],10))/255,Math.min(255,parseInt(o[2],10))/255,Math.min(255,parseInt(o[3],10))/255,t);if(o=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(o[4]),this.setRGB(Math.min(100,parseInt(o[1],10))/100,Math.min(100,parseInt(o[2],10))/100,Math.min(100,parseInt(o[3],10))/100,t);break;case"hsl":case"hsla":if(o=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(o[4]),this.setHSL(parseFloat(o[1])/360,parseFloat(o[2])/100,parseFloat(o[3])/100,t);break;default:Fe("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let o=r[1],s=o.length;if(s===3)return this.setRGB(parseInt(o.charAt(0),16)/15,parseInt(o.charAt(1),16)/15,parseInt(o.charAt(2),16)/15,t);if(s===6)return this.setHex(parseInt(o,16),t);Fe("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Fn){let i=hw[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Fe("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=wr(e.r),this.g=wr(e.g),this.b=wr(e.b),this}copyLinearToSRGB(e){return this.r=wa(e.r),this.g=wa(e.g),this.b=wa(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Fn){return ct.workingToColorSpace(xn.copy(this),e),Math.round(at(xn.r*255,0,255))*65536+Math.round(at(xn.g*255,0,255))*256+Math.round(at(xn.b*255,0,255))}getHexString(e=Fn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=ct.workingColorSpace){ct.workingToColorSpace(xn.copy(this),t);let i=xn.r,r=xn.g,o=xn.b,s=Math.max(i,r,o),a=Math.min(i,r,o),c,l,u=(a+s)/2;if(a===s)c=0,l=0;else{let d=s-a;switch(l=u<=.5?d/(s+a):d/(2-s-a),s){case i:c=(r-o)/d+(r<o?6:0);break;case r:c=(o-i)/d+2;break;case o:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=ct.workingColorSpace){return ct.workingToColorSpace(xn.copy(this),t),e.r=xn.r,e.g=xn.g,e.b=xn.b,e}getStyle(e=Fn){ct.workingToColorSpace(xn.copy(this),e);let t=xn.r,i=xn.g,r=xn.b;return e!==Fn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(to),this.setHSL(to.h+e,to.s+t,to.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(to),e.getHSL(Ef);let i=uy(to.h,Ef.h,t),r=uy(to.s,Ef.s,t),o=uy(to.l,Ef.l,t);return this.setHSL(i,r,o),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,o=e.elements;return this.r=o[0]*t+o[3]*i+o[6]*r,this.g=o[1]*t+o[4]*i+o[7]*r,this.b=o[2]*t+o[5]*i+o[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},xn=new nt;nt.NAMES=hw;var ll=class extends Ii{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new ao,this.environmentIntensity=1,this.environmentRotation=new ao,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},Ci=new L,_r=new L,vy=new L,xr=new L,va=new L,ya=new L,gS=new L,yy=new L,_y=new L,xy=new L,Ey=new Lt,by=new Lt,My=new Lt,Sr=class n{constructor(e=new L,t=new L,i=new L){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Ci.subVectors(e,t),r.cross(Ci);let o=r.lengthSq();return o>0?r.multiplyScalar(1/Math.sqrt(o)):r.set(0,0,0)}static getBarycoord(e,t,i,r,o){Ci.subVectors(r,t),_r.subVectors(i,t),vy.subVectors(e,t);let s=Ci.dot(Ci),a=Ci.dot(_r),c=Ci.dot(vy),l=_r.dot(_r),u=_r.dot(vy),d=s*l-a*a;if(d===0)return o.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,p=(s*u-a*c)*f;return o.set(1-h-p,p,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,xr)===null?!1:xr.x>=0&&xr.y>=0&&xr.x+xr.y<=1}static getInterpolation(e,t,i,r,o,s,a,c){return this.getBarycoord(e,t,i,r,xr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(o,xr.x),c.addScaledVector(s,xr.y),c.addScaledVector(a,xr.z),c)}static getInterpolatedAttribute(e,t,i,r,o,s){return Ey.setScalar(0),by.setScalar(0),My.setScalar(0),Ey.fromBufferAttribute(e,t),by.fromBufferAttribute(e,i),My.fromBufferAttribute(e,r),s.setScalar(0),s.addScaledVector(Ey,o.x),s.addScaledVector(by,o.y),s.addScaledVector(My,o.z),s}static isFrontFacing(e,t,i,r){return Ci.subVectors(i,t),_r.subVectors(e,t),Ci.cross(_r).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Ci.subVectors(this.c,this.b),_r.subVectors(this.a,this.b),Ci.cross(_r).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,o){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,o)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,o=this.c,s,a;va.subVectors(r,i),ya.subVectors(o,i),yy.subVectors(e,i);let c=va.dot(yy),l=ya.dot(yy);if(c<=0&&l<=0)return t.copy(i);_y.subVectors(e,r);let u=va.dot(_y),d=ya.dot(_y);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return s=c/(c-u),t.copy(i).addScaledVector(va,s);xy.subVectors(e,o);let h=va.dot(xy),p=ya.dot(xy);if(p>=0&&h<=p)return t.copy(o);let y=h*l-c*p;if(y<=0&&l>=0&&p<=0)return a=l/(l-p),t.copy(i).addScaledVector(ya,a);let g=u*p-h*d;if(g<=0&&d-u>=0&&h-p>=0)return gS.subVectors(o,r),a=(d-u)/(d-u+(h-p)),t.copy(r).addScaledVector(gS,a);let m=1/(g+y+f);return s=y*m,a=f*m,t.copy(i).addScaledVector(va,s).addScaledVector(ya,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},co=class{constructor(e=new L(1/0,1/0,1/0),t=new L(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Ti.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Ti.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Ti.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let o=i.getAttribute("position");if(t===!0&&o!==void 0&&e.isInstancedMesh!==!0)for(let s=0,a=o.count;s<a;s++)e.isMesh===!0?e.getVertexPosition(s,Ti):Ti.fromBufferAttribute(o,s),Ti.applyMatrix4(e.matrixWorld),this.expandByPoint(Ti);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),bf.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),bf.copy(i.boundingBox)),bf.applyMatrix4(e.matrixWorld),this.union(bf)}let r=e.children;for(let o=0,s=r.length;o<s;o++)this.expandByObject(r[o],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Ti),Ti.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(el),Mf.subVectors(this.max,el),_a.subVectors(e.a,el),xa.subVectors(e.b,el),Ea.subVectors(e.c,el),no.subVectors(xa,_a),io.subVectors(Ea,xa),ns.subVectors(_a,Ea);let t=[0,-no.z,no.y,0,-io.z,io.y,0,-ns.z,ns.y,no.z,0,-no.x,io.z,0,-io.x,ns.z,0,-ns.x,-no.y,no.x,0,-io.y,io.x,0,-ns.y,ns.x,0];return!Sy(t,_a,xa,Ea,Mf)||(t=[1,0,0,0,1,0,0,0,1],!Sy(t,_a,xa,Ea,Mf))?!1:(Sf.crossVectors(no,io),t=[Sf.x,Sf.y,Sf.z],Sy(t,_a,xa,Ea,Mf))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Ti).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Ti).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Er[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Er[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Er[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Er[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Er[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Er[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Er[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Er[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Er),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},Er=[new L,new L,new L,new L,new L,new L,new L,new L],Ti=new L,bf=new co,_a=new L,xa=new L,Ea=new L,no=new L,io=new L,ns=new L,el=new L,Mf=new L,Sf=new L,is=new L;function Sy(n,e,t,i,r){for(let o=0,s=n.length-3;o<=s;o+=3){is.fromArray(n,o);let a=r.x*Math.abs(is.x)+r.y*Math.abs(is.y)+r.z*Math.abs(is.z),c=e.dot(is),l=t.dot(is),u=i.dot(is);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var qt=new L,wf=new lt,nP=0,fn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:nP++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=Hy,this.updateRanges=[],this.gpuType=Ni,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,o=this.itemSize;r<o;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)wf.fromBufferAttribute(this,t),wf.applyMatrix3(e),this.setXY(t,wf.x,wf.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)qt.fromBufferAttribute(this,t),qt.applyMatrix3(e),this.setXYZ(t,qt.x,qt.y,qt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)qt.fromBufferAttribute(this,t),qt.applyMatrix4(e),this.setXYZ(t,qt.x,qt.y,qt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)qt.fromBufferAttribute(this,t),qt.applyNormalMatrix(e),this.setXYZ(t,qt.x,qt.y,qt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)qt.fromBufferAttribute(this,t),qt.transformDirection(e),this.setXYZ(t,qt.x,qt.y,qt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Kc(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=jn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Kc(t,this.array)),t}setX(e,t){return this.normalized&&(t=jn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Kc(t,this.array)),t}setY(e,t){return this.normalized&&(t=jn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Kc(t,this.array)),t}setZ(e,t){return this.normalized&&(t=jn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Kc(t,this.array)),t}setW(e,t){return this.normalized&&(t=jn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=jn(t,this.array),i=jn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=jn(t,this.array),i=jn(i,this.array),r=jn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,o){return e*=this.itemSize,this.normalized&&(t=jn(t,this.array),i=jn(i,this.array),r=jn(r,this.array),o=jn(o,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=o,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==Hy&&(e.usage=this.usage),e}};var ul=class extends fn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var dl=class extends fn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Xt=class extends fn{constructor(e,t,i){super(new Float32Array(e),t,i)}},iP=new co,tl=new L,wy=new L,ds=class{constructor(e=new L,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):iP.setFromPoints(e).getCenter(i);let r=0;for(let o=0,s=e.length;o<s;o++)r=Math.max(r,i.distanceToSquared(e[o]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;tl.subVectors(e,this.center);let t=tl.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(tl,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(wy.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(tl.copy(e.center).add(wy)),this.expandByPoint(tl.copy(e.center).sub(wy))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},rP=0,di=new Ot,Cy=new Ii,ba=new L,ei=new co,nl=new co,on=new L,bn=class n extends Tr{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:rP++}),this.uuid=Nl(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(GN(e)?dl:ul)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let o=new Ye().getNormalMatrix(e);i.applyNormalMatrix(o),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return di.makeRotationFromQuaternion(e),this.applyMatrix4(di),this}rotateX(e){return di.makeRotationX(e),this.applyMatrix4(di),this}rotateY(e){return di.makeRotationY(e),this.applyMatrix4(di),this}rotateZ(e){return di.makeRotationZ(e),this.applyMatrix4(di),this}translate(e,t,i){return di.makeTranslation(e,t,i),this.applyMatrix4(di),this}scale(e,t,i){return di.makeScale(e,t,i),this.applyMatrix4(di),this}lookAt(e){return Cy.lookAt(e),Cy.updateMatrix(),this.applyMatrix4(Cy.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(ba).negate(),this.translate(ba.x,ba.y,ba.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,o=e.length;r<o;r++){let s=e[r];i.push(s.x,s.y,s.z||0)}this.setAttribute("position",new Xt(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let o=e[r];t.setXYZ(r,o.x,o.y,o.z||0)}e.length>t.count&&Fe("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new co);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ue("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new L(-1/0,-1/0,-1/0),new L(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let o=t[i];ei.setFromBufferAttribute(o),this.morphTargetsRelative?(on.addVectors(this.boundingBox.min,ei.min),this.boundingBox.expandByPoint(on),on.addVectors(this.boundingBox.max,ei.max),this.boundingBox.expandByPoint(on)):(this.boundingBox.expandByPoint(ei.min),this.boundingBox.expandByPoint(ei.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Ue('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new ds);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ue("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new L,1/0);return}if(e){let i=this.boundingSphere.center;if(ei.setFromBufferAttribute(e),t)for(let o=0,s=t.length;o<s;o++){let a=t[o];nl.setFromBufferAttribute(a),this.morphTargetsRelative?(on.addVectors(ei.min,nl.min),ei.expandByPoint(on),on.addVectors(ei.max,nl.max),ei.expandByPoint(on)):(ei.expandByPoint(nl.min),ei.expandByPoint(nl.max))}ei.getCenter(i);let r=0;for(let o=0,s=e.count;o<s;o++)on.fromBufferAttribute(e,o),r=Math.max(r,i.distanceToSquared(on));if(t)for(let o=0,s=t.length;o<s;o++){let a=t[o],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)on.fromBufferAttribute(a,l),c&&(ba.fromBufferAttribute(e,l),on.add(ba)),r=Math.max(r,i.distanceToSquared(on))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Ue('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Ue("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,o=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new fn(new Float32Array(4*i.count),4));let s=this.getAttribute("tangent"),a=[],c=[];for(let _=0;_<i.count;_++)a[_]=new L,c[_]=new L;let l=new L,u=new L,d=new L,f=new lt,h=new lt,p=new lt,y=new L,g=new L;function m(_,b,W){l.fromBufferAttribute(i,_),u.fromBufferAttribute(i,b),d.fromBufferAttribute(i,W),f.fromBufferAttribute(o,_),h.fromBufferAttribute(o,b),p.fromBufferAttribute(o,W),u.sub(l),d.sub(l),h.sub(f),p.sub(f);let T=1/(h.x*p.y-p.x*h.y);isFinite(T)&&(y.copy(u).multiplyScalar(p.y).addScaledVector(d,-h.y).multiplyScalar(T),g.copy(d).multiplyScalar(h.x).addScaledVector(u,-p.x).multiplyScalar(T),a[_].add(y),a[b].add(y),a[W].add(y),c[_].add(g),c[b].add(g),c[W].add(g))}let E=this.groups;E.length===0&&(E=[{start:0,count:e.count}]);for(let _=0,b=E.length;_<b;++_){let W=E[_],T=W.start,F=W.count;for(let H=T,G=T+F;H<G;H+=3)m(e.getX(H+0),e.getX(H+1),e.getX(H+2))}let S=new L,w=new L,D=new L,C=new L;function I(_){D.fromBufferAttribute(r,_),C.copy(D);let b=a[_];S.copy(b),S.sub(D.multiplyScalar(D.dot(b))).normalize(),w.crossVectors(C,b);let T=w.dot(c[_])<0?-1:1;s.setXYZW(_,S.x,S.y,S.z,T)}for(let _=0,b=E.length;_<b;++_){let W=E[_],T=W.start,F=W.count;for(let H=T,G=T+F;H<G;H+=3)I(e.getX(H+0)),I(e.getX(H+1)),I(e.getX(H+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new fn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new L,o=new L,s=new L,a=new L,c=new L,l=new L,u=new L,d=new L;if(e)for(let f=0,h=e.count;f<h;f+=3){let p=e.getX(f+0),y=e.getX(f+1),g=e.getX(f+2);r.fromBufferAttribute(t,p),o.fromBufferAttribute(t,y),s.fromBufferAttribute(t,g),u.subVectors(s,o),d.subVectors(r,o),u.cross(d),a.fromBufferAttribute(i,p),c.fromBufferAttribute(i,y),l.fromBufferAttribute(i,g),a.add(u),c.add(u),l.add(u),i.setXYZ(p,a.x,a.y,a.z),i.setXYZ(y,c.x,c.y,c.z),i.setXYZ(g,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),o.fromBufferAttribute(t,f+1),s.fromBufferAttribute(t,f+2),u.subVectors(s,o),d.subVectors(r,o),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)on.fromBufferAttribute(e,t),on.normalize(),e.setXYZ(t,on.x,on.y,on.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,p=0;for(let y=0,g=c.length;y<g;y++){a.isInterleavedBufferAttribute?h=c[y]*a.data.stride+a.offset:h=c[y]*u;for(let m=0;m<u;m++)f[p++]=l[h++]}return new fn(f,u,d)}if(this.index===null)return Fe("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let o=this.morphAttributes;for(let a in o){let c=[],l=o[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let s=this.groups;for(let a=0,c=s.length;a<c;a++){let l=s[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},o=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,o=!0)}o&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let s=this.groups;s.length>0&&(e.data.groups=JSON.parse(JSON.stringify(s)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let o=e.morphAttributes;for(let l in o){let u=[],d=o[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let s=e.groups;for(let l=0,u=s.length;l<u;l++){let d=s[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var oP=0,Dr=class extends Tr{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:oP++}),this.uuid=Nl(),this.name="",this.type="Material",this.blending=cs,this.side=Cr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=$f,this.blendDst=qf,this.blendEquation=so,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new nt(0,0,0),this.blendAlpha=0,this.depthFunc=ls,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Vy,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=ss,this.stencilZFail=ss,this.stencilZPass=ss,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Fe(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Fe(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==cs&&(i.blending=this.blending),this.side!==Cr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==$f&&(i.blendSrc=this.blendSrc),this.blendDst!==qf&&(i.blendDst=this.blendDst),this.blendEquation!==so&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==ls&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Vy&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==ss&&(i.stencilFail=this.stencilFail),this.stencilZFail!==ss&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==ss&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(o){let s=[];for(let a in o){let c=o[a];delete c.metadata,s.push(c)}return s}if(t){let o=r(e.textures),s=r(e.images);o.length>0&&(i.textures=o),s.length>0&&(i.images=s)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let o=0;o!==r;++o)i[o]=t[o].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var br=new L,Ty=new L,Cf=new L,ro=new L,Dy=new L,Tf=new L,Iy=new L,Ra=class{constructor(e=new L,t=new L(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,br)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=br.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(br.copy(this.origin).addScaledVector(this.direction,t),br.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Ty.copy(e).add(t).multiplyScalar(.5),Cf.copy(t).sub(e).normalize(),ro.copy(this.origin).sub(Ty);let o=e.distanceTo(t)*.5,s=-this.direction.dot(Cf),a=ro.dot(this.direction),c=-ro.dot(Cf),l=ro.lengthSq(),u=Math.abs(1-s*s),d,f,h,p;if(u>0)if(d=s*c-a,f=s*a-c,p=o*u,d>=0)if(f>=-p)if(f<=p){let y=1/u;d*=y,f*=y,h=d*(d+s*f+2*a)+f*(s*d+f+2*c)+l}else f=o,d=Math.max(0,-(s*f+a)),h=-d*d+f*(f+2*c)+l;else f=-o,d=Math.max(0,-(s*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-p?(d=Math.max(0,-(-s*o+a)),f=d>0?-o:Math.min(Math.max(-o,-c),o),h=-d*d+f*(f+2*c)+l):f<=p?(d=0,f=Math.min(Math.max(-o,-c),o),h=f*(f+2*c)+l):(d=Math.max(0,-(s*o+a)),f=d>0?o:Math.min(Math.max(-o,-c),o),h=-d*d+f*(f+2*c)+l);else f=s>0?-o:o,d=Math.max(0,-(s*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Ty).addScaledVector(Cf,f),h}intersectSphere(e,t){br.subVectors(e.center,this.origin);let i=br.dot(this.direction),r=br.dot(br)-i*i,o=e.radius*e.radius;if(r>o)return null;let s=Math.sqrt(o-r),a=i-s,c=i+s;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,o,s,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(o=(e.min.y-f.y)*u,s=(e.max.y-f.y)*u):(o=(e.max.y-f.y)*u,s=(e.min.y-f.y)*u),i>s||o>r||((o>i||isNaN(i))&&(i=o),(s<r||isNaN(r))&&(r=s),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,br)!==null}intersectTriangle(e,t,i,r,o){Dy.subVectors(t,e),Tf.subVectors(i,e),Iy.crossVectors(Dy,Tf);let s=this.direction.dot(Iy),a;if(s>0){if(r)return null;a=1}else if(s<0)a=-1,s=-s;else return null;ro.subVectors(this.origin,e);let c=a*this.direction.dot(Tf.crossVectors(ro,Tf));if(c<0)return null;let l=a*this.direction.dot(Dy.cross(ro));if(l<0||c+l>s)return null;let u=-a*ro.dot(Iy);return u<0?null:this.at(u/s,o)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},fl=class extends Dr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new nt(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new ao,this.combine=Ky,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},vS=new Ot,rs=new Ra,Df=new ds,yS=new L,If=new L,Af=new L,Rf=new L,Ay=new L,Nf=new L,_S=new L,Pf=new L,Wn=class extends Ii{constructor(e=new bn,t=new fl){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let o=0,s=r.length;o<s;o++){let a=r[o].name||String(o);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=o}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,o=i.morphAttributes.position,s=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(o&&a){Nf.set(0,0,0);for(let c=0,l=o.length;c<l;c++){let u=a[c],d=o[c];u!==0&&(Ay.fromBufferAttribute(d,e),s?Nf.addScaledVector(Ay,u):Nf.addScaledVector(Ay.sub(t),u))}t.add(Nf)}return t}raycast(e,t){let i=this.geometry,r=this.material,o=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Df.copy(i.boundingSphere),Df.applyMatrix4(o),rs.copy(e.ray).recast(e.near),!(Df.containsPoint(rs.origin)===!1&&(rs.intersectSphere(Df,yS)===null||rs.origin.distanceToSquared(yS)>(e.far-e.near)**2))&&(vS.copy(o).invert(),rs.copy(e.ray).applyMatrix4(vS),!(i.boundingBox!==null&&rs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,rs)))}_computeIntersections(e,t,i){let r,o=this.geometry,s=this.material,a=o.index,c=o.attributes.position,l=o.attributes.uv,u=o.attributes.uv1,d=o.attributes.normal,f=o.groups,h=o.drawRange;if(a!==null)if(Array.isArray(s))for(let p=0,y=f.length;p<y;p++){let g=f[p],m=s[g.materialIndex],E=Math.max(g.start,h.start),S=Math.min(a.count,Math.min(g.start+g.count,h.start+h.count));for(let w=E,D=S;w<D;w+=3){let C=a.getX(w),I=a.getX(w+1),_=a.getX(w+2);r=Of(this,m,e,i,l,u,d,C,I,_),r&&(r.faceIndex=Math.floor(w/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let p=Math.max(0,h.start),y=Math.min(a.count,h.start+h.count);for(let g=p,m=y;g<m;g+=3){let E=a.getX(g),S=a.getX(g+1),w=a.getX(g+2);r=Of(this,s,e,i,l,u,d,E,S,w),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(s))for(let p=0,y=f.length;p<y;p++){let g=f[p],m=s[g.materialIndex],E=Math.max(g.start,h.start),S=Math.min(c.count,Math.min(g.start+g.count,h.start+h.count));for(let w=E,D=S;w<D;w+=3){let C=w,I=w+1,_=w+2;r=Of(this,m,e,i,l,u,d,C,I,_),r&&(r.faceIndex=Math.floor(w/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let p=Math.max(0,h.start),y=Math.min(c.count,h.start+h.count);for(let g=p,m=y;g<m;g+=3){let E=g,S=g+1,w=g+2;r=Of(this,s,e,i,l,u,d,E,S,w),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}}};function sP(n,e,t,i,r,o,s,a){let c;if(e.side===kn?c=i.intersectTriangle(s,o,r,!0,a):c=i.intersectTriangle(r,o,s,e.side===Cr,a),c===null)return null;Pf.copy(a),Pf.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(Pf);return l<t.near||l>t.far?null:{distance:l,point:Pf.clone(),object:n}}function Of(n,e,t,i,r,o,s,a,c,l){n.getVertexPosition(a,If),n.getVertexPosition(c,Af),n.getVertexPosition(l,Rf);let u=sP(n,e,t,i,If,Af,Rf,_S);if(u){let d=new L;Sr.getBarycoord(_S,If,Af,Rf,d),r&&(u.uv=Sr.getInterpolatedAttribute(r,a,c,l,d,new lt)),o&&(u.uv1=Sr.getInterpolatedAttribute(o,a,c,l,d,new lt)),s&&(u.normal=Sr.getInterpolatedAttribute(s,a,c,l,d,new L),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new L,materialIndex:0};Sr.getNormal(If,Af,Rf,f.normal),u.face=f,u.barycoord=d}return u}var ch=class extends Ar{constructor(e=null,t=1,i=1,r,o,s,a,c,l=sn,u=sn,d,f){super(null,s,a,c,l,u,r,o,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ry=new L,aP=new L,cP=new Ye,Xi=class{constructor(e=new L(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Ry.subVectors(i,t).cross(aP.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(Ry),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/r;return o<0||o>1?null:t.copy(e.start).addScaledVector(i,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||cP.getNormalMatrix(e),r=this.coplanarPoint(Ry).applyMatrix4(e),o=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(o),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},os=new ds,lP=new lt(.5,.5),Lf=new L,Na=class{constructor(e=new Xi,t=new Xi,i=new Xi,r=new Xi,o=new Xi,s=new Xi){this.planes=[e,t,i,r,o,s]}set(e,t,i,r,o,s){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(o),a[5].copy(s),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Di,i=!1){let r=this.planes,o=e.elements,s=o[0],a=o[1],c=o[2],l=o[3],u=o[4],d=o[5],f=o[6],h=o[7],p=o[8],y=o[9],g=o[10],m=o[11],E=o[12],S=o[13],w=o[14],D=o[15];if(r[0].setComponents(l-s,h-u,m-p,D-E).normalize(),r[1].setComponents(l+s,h+u,m+p,D+E).normalize(),r[2].setComponents(l+a,h+d,m+y,D+S).normalize(),r[3].setComponents(l-a,h-d,m-y,D-S).normalize(),i)r[4].setComponents(c,f,g,w).normalize(),r[5].setComponents(l-c,h-f,m-g,D-w).normalize();else if(r[4].setComponents(l-c,h-f,m-g,D-w).normalize(),t===Di)r[5].setComponents(l+c,h+f,m+g,D+w).normalize();else if(t===Ca)r[5].setComponents(c,f,g,w).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),os.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),os.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(os)}intersectsSprite(e){os.center.set(0,0,0);let t=lP.distanceTo(e.center);return os.radius=.7071067811865476+t,os.applyMatrix4(e.matrixWorld),this.intersectsSphere(os)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let o=0;o<6;o++)if(t[o].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Lf.x=r.normal.x>0?e.max.x:e.min.x,Lf.y=r.normal.y>0?e.max.y:e.min.y,Lf.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Lf)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var fs=class extends Dr{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new nt(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},lh=new L,uh=new L,xS=new Ot,il=new Ra,Ff=new ds,Ny=new L,ES=new L,dh=class extends Ii{constructor(e=new bn,t=new fs){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,o=t.count;r<o;r++)lh.fromBufferAttribute(t,r-1),uh.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=lh.distanceTo(uh);e.setAttribute("lineDistance",new Xt(i,1))}else Fe("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,o=e.params.Line.threshold,s=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Ff.copy(i.boundingSphere),Ff.applyMatrix4(r),Ff.radius+=o,e.ray.intersectsSphere(Ff)===!1)return;xS.copy(r).invert(),il.copy(e.ray).applyMatrix4(xS);let a=o/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,s.start),p=Math.min(u.count,s.start+s.count);for(let y=h,g=p-1;y<g;y+=l){let m=u.getX(y),E=u.getX(y+1),S=kf(this,e,il,c,m,E,y);S&&t.push(S)}if(this.isLineLoop){let y=u.getX(p-1),g=u.getX(h),m=kf(this,e,il,c,y,g,p-1);m&&t.push(m)}}else{let h=Math.max(0,s.start),p=Math.min(f.count,s.start+s.count);for(let y=h,g=p-1;y<g;y+=l){let m=kf(this,e,il,c,y,y+1,y);m&&t.push(m)}if(this.isLineLoop){let y=kf(this,e,il,c,p-1,h,p-1);y&&t.push(y)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let o=0,s=r.length;o<s;o++){let a=r[o].name||String(o);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=o}}}}};function kf(n,e,t,i,r,o,s){let a=n.geometry.attributes.position;if(lh.fromBufferAttribute(a,r),uh.fromBufferAttribute(a,o),t.distanceSqToSegment(lh,uh,Ny,ES)>i)return;Ny.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(Ny);if(!(l<e.near||l>e.far))return{distance:l,point:ES.clone().applyMatrix4(n.matrixWorld),index:s,face:null,faceIndex:null,barycoord:null,object:n}}var bS=new L,MS=new L,Pa=class extends dh{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,o=t.count;r<o;r+=2)bS.fromBufferAttribute(t,r),MS.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+bS.distanceTo(MS);e.setAttribute("lineDistance",new Xt(i,1))}else Fe("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var hl=class extends Ar{constructor(e=[],t=po,i,r,o,s,a,c,l,u){super(e,t,i,r,o,s,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var lo=class extends Ar{constructor(e,t,i=Ri,r,o,s,a=sn,c=sn,l,u=Zi,d=1){if(u!==Zi&&u!==go)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,o,s,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new Da(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},fh=class extends lo{constructor(e,t=Ri,i=po,r,o,s=sn,a=sn,c,l=Zi){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,o,s,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},pl=class extends Ar{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Oa=class n extends bn{constructor(e=1,t=1,i=1,r=1,o=1,s=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:o,depthSegments:s};let a=this;r=Math.floor(r),o=Math.floor(o),s=Math.floor(s);let c=[],l=[],u=[],d=[],f=0,h=0;p("z","y","x",-1,-1,i,t,e,s,o,0),p("z","y","x",1,-1,i,t,-e,s,o,1),p("x","z","y",1,1,e,i,t,r,s,2),p("x","z","y",1,-1,e,i,-t,r,s,3),p("x","y","z",1,-1,e,t,i,r,o,4),p("x","y","z",-1,-1,e,t,-i,r,o,5),this.setIndex(c),this.setAttribute("position",new Xt(l,3)),this.setAttribute("normal",new Xt(u,3)),this.setAttribute("uv",new Xt(d,2));function p(y,g,m,E,S,w,D,C,I,_,b){let W=w/I,T=D/_,F=w/2,H=D/2,G=C/2,B=I+1,U=_+1,O=0,Z=0,K=new L;for(let he=0;he<U;he++){let _e=he*T-H;for(let ge=0;ge<B;ge++){let Je=ge*W-F;K[y]=Je*E,K[g]=_e*S,K[m]=G,l.push(K.x,K.y,K.z),K[y]=0,K[g]=0,K[m]=C>0?1:-1,u.push(K.x,K.y,K.z),d.push(ge/I),d.push(1-he/_),O+=1}}for(let he=0;he<_;he++)for(let _e=0;_e<I;_e++){let ge=f+_e+B*he,Je=f+_e+B*(he+1),Pt=f+(_e+1)+B*(he+1),Nt=f+(_e+1)+B*he;c.push(ge,Je,Nt),c.push(Je,Pt,Nt),Z+=6}a.addGroup(h,Z,b),h+=Z,f+=O}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Uf=new L,Bf=new L,Py=new L,Vf=new Sr,ml=class extends bn{constructor(e=null,t=1){if(super(),this.type="EdgesGeometry",this.parameters={geometry:e,thresholdAngle:t},e!==null){let r=Math.pow(10,4),o=Math.cos(Wf*t),s=e.getIndex(),a=e.getAttribute("position"),c=s?s.count:a.count,l=[0,0,0],u=["a","b","c"],d=new Array(3),f={},h=[];for(let p=0;p<c;p+=3){s?(l[0]=s.getX(p),l[1]=s.getX(p+1),l[2]=s.getX(p+2)):(l[0]=p,l[1]=p+1,l[2]=p+2);let{a:y,b:g,c:m}=Vf;if(y.fromBufferAttribute(a,l[0]),g.fromBufferAttribute(a,l[1]),m.fromBufferAttribute(a,l[2]),Vf.getNormal(Py),d[0]=`${Math.round(y.x*r)},${Math.round(y.y*r)},${Math.round(y.z*r)}`,d[1]=`${Math.round(g.x*r)},${Math.round(g.y*r)},${Math.round(g.z*r)}`,d[2]=`${Math.round(m.x*r)},${Math.round(m.y*r)},${Math.round(m.z*r)}`,!(d[0]===d[1]||d[1]===d[2]||d[2]===d[0]))for(let E=0;E<3;E++){let S=(E+1)%3,w=d[E],D=d[S],C=Vf[u[E]],I=Vf[u[S]],_=`${w}_${D}`,b=`${D}_${w}`;b in f&&f[b]?(Py.dot(f[b].normal)<=o&&(h.push(C.x,C.y,C.z),h.push(I.x,I.y,I.z)),f[b]=null):_ in f||(f[_]={index0:l[E],index1:l[S],normal:Py.clone()})}}for(let p in f)if(f[p]){let{index0:y,index1:g}=f[p];Uf.fromBufferAttribute(a,y),Bf.fromBufferAttribute(a,g),h.push(Uf.x,Uf.y,Uf.z),h.push(Bf.x,Bf.y,Bf.z)}this.setAttribute("position",new Xt(h,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};var gl=class n extends bn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let o=e/2,s=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],p=[],y=[],g=[];for(let m=0;m<u;m++){let E=m*f-s;for(let S=0;S<l;S++){let w=S*d-o;p.push(w,-E,0),y.push(0,0,1),g.push(S/a),g.push(1-m/c)}}for(let m=0;m<c;m++)for(let E=0;E<a;E++){let S=E+l*m,w=E+l*(m+1),D=E+1+l*(m+1),C=E+1+l*m;h.push(S,w,C),h.push(w,D,C)}this.setIndex(h),this.setAttribute("position",new Xt(p,3)),this.setAttribute("normal",new Xt(y,3)),this.setAttribute("uv",new Xt(g,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function ms(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(Fe("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function Mn(n){let e={};for(let t=0;t<n.length;t++){let i=ms(n[t]);for(let r in i)e[r]=i[r]}return e}function uP(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function g0(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:ct.workingColorSpace}var pw={clone:ms,merge:Mn},dP=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,fP=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,ni=class extends Dr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=dP,this.fragmentShader=fP,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=ms(e.uniforms),this.uniformsGroups=uP(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let s=this.uniforms[r].value;s&&s.isTexture?t.uniforms[r]={type:"t",value:s.toJSON(e).uuid}:s&&s.isColor?t.uniforms[r]={type:"c",value:s.getHex()}:s&&s.isVector2?t.uniforms[r]={type:"v2",value:s.toArray()}:s&&s.isVector3?t.uniforms[r]={type:"v3",value:s.toArray()}:s&&s.isVector4?t.uniforms[r]={type:"v4",value:s.toArray()}:s&&s.isMatrix3?t.uniforms[r]={type:"m3",value:s.toArray()}:s&&s.isMatrix4?t.uniforms[r]={type:"m4",value:s.toArray()}:t.uniforms[r]={value:s}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},hh=class extends ni{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},vl=class extends Dr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new nt(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new nt(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=h0,this.normalScale=new lt(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new ao,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};var ph=class extends Dr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=tw,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},mh=class extends Dr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function Hf(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var uo=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],o=t[i-1];n:{e:{let s;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<o)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(o=r,r=t[++i],e<r)break e}s=t.length;break t}if(!(e>=o)){let a=t[1];e<a&&(i=2,o=a);for(let c=i-2;;){if(o===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=o,o=t[--i-1],e>=o)break e}s=i,i=0;break t}break n}for(;i<s;){let a=i+s>>>1;e<t[a]?s=a:i=a+1}if(r=t[i],o=t[i-1],o===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,o,r)}return this.interpolate_(i,o,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,o=e*r;for(let s=0;s!==r;++s)t[s]=i[o+s];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},gh=class extends uo{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:ky,endingEnd:ky}}intervalChanged_(e,t,i){let r=this.parameterPositions,o=e-2,s=e+1,a=r[o],c=r[s];if(a===void 0)switch(this.getSettings_().endingStart){case Uy:o=e,a=2*t-i;break;case By:o=r.length-2,a=t+r[o]-r[o+1];break;default:o=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Uy:s=e,c=2*i-t;break;case By:s=1,c=i+r[1]-r[0];break;default:s=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=o*u,this._offsetNext=s*u}interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,p=(i-t)/(r-t),y=p*p,g=y*p,m=-f*g+2*f*y-f*p,E=(1+f)*g+(-1.5-2*f)*y+(-.5+f)*p+1,S=(-1-h)*g+(1.5+h)*y+.5*p,w=h*g-h*y;for(let D=0;D!==a;++D)o[D]=m*s[u+D]+E*s[l+D]+S*s[c+D]+w*s[d+D];return o}},vh=class extends uo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)o[f]=s[l+f]*d+s[c+f]*u;return o}},yh=class extends uo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},_h=class extends uo{interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,d=u.inTangents,f=u.outTangents;if(!d||!f){let y=(i-t)/(r-t),g=1-y;for(let m=0;m!==a;++m)o[m]=s[l+m]*g+s[c+m]*y;return o}let h=a*2,p=e-1;for(let y=0;y!==a;++y){let g=s[l+y],m=s[c+y],E=p*h+y*2,S=f[E],w=f[E+1],D=e*h+y*2,C=d[D],I=d[D+1],_=(i-t)/(r-t),b,W,T,F,H;for(let G=0;G<8;G++){b=_*_,W=b*_,T=1-_,F=T*T,H=F*T;let U=H*t+3*F*_*S+3*T*b*C+W*r-i;if(Math.abs(U)<1e-10)break;let O=3*F*(S-t)+6*T*_*(C-S)+3*b*(r-C);if(Math.abs(O)<1e-10)break;_=_-U/O,_=Math.max(0,Math.min(1,_))}o[y]=H*g+3*F*_*w+3*T*b*I+W*m}return o}},ii=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Hf(t,this.TimeBufferType),this.values=Hf(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Hf(e.times,Array),values:Hf(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new yh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new vh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new gh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new _h(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case rl:t=this.InterpolantFactoryMethodDiscrete;break;case ih:t=this.InterpolantFactoryMethodLinear;break;case jf:t=this.InterpolantFactoryMethodSmooth;break;case Fy:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Fe("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return rl;case this.InterpolantFactoryMethodLinear:return ih;case this.InterpolantFactoryMethodSmooth:return jf;case this.InterpolantFactoryMethodBezier:return Fy}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,o=0,s=r-1;for(;o!==r&&i[o]<e;)++o;for(;s!==-1&&i[s]>t;)--s;if(++s,o!==0||s!==r){o>=s&&(s=Math.max(s,1),o=s-1);let a=this.getValueSize();this.times=i.slice(o,s),this.values=this.values.slice(o*a,s*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Ue("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,o=i.length;o===0&&(Ue("KeyframeTrack: Track is empty.",this),e=!1);let s=null;for(let a=0;a!==o;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Ue("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(s!==null&&s>c){Ue("KeyframeTrack: Out of order keys.",this,a,c,s),e=!1;break}s=c}if(r!==void 0&&jN(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Ue("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===jf,o=e.length-1,s=1;for(let a=1;a<o;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let p=0;p!==i;++p){let y=t[d+p];if(y!==t[f+p]||y!==t[h+p]){c=!0;break}}}if(c){if(a!==s){e[s]=e[a];let d=a*i,f=s*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++s}}if(o>0){e[s]=e[o];for(let a=o*i,c=s*i,l=0;l!==i;++l)t[c+l]=t[a+l];++s}return s!==e.length?(this.times=e.slice(0,s),this.values=t.slice(0,s*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};ii.prototype.ValueTypeName="";ii.prototype.TimeBufferType=Float32Array;ii.prototype.ValueBufferType=Float32Array;ii.prototype.DefaultInterpolation=ih;var fo=class extends ii{constructor(e,t,i){super(e,t,i)}};fo.prototype.ValueTypeName="bool";fo.prototype.ValueBufferType=Array;fo.prototype.DefaultInterpolation=rl;fo.prototype.InterpolantFactoryMethodLinear=void 0;fo.prototype.InterpolantFactoryMethodSmooth=void 0;var xh=class extends ii{constructor(e,t,i,r){super(e,t,i,r)}};xh.prototype.ValueTypeName="color";var Eh=class extends ii{constructor(e,t,i,r){super(e,t,i,r)}};Eh.prototype.ValueTypeName="number";var bh=class extends uo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Ji.slerpFlat(o,0,s,l-a,s,l,c);return o}},yl=class extends ii{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new bh(this.times,this.values,this.getValueSize(),e)}};yl.prototype.ValueTypeName="quaternion";yl.prototype.InterpolantFactoryMethodSmooth=void 0;var ho=class extends ii{constructor(e,t,i){super(e,t,i)}};ho.prototype.ValueTypeName="string";ho.prototype.ValueBufferType=Array;ho.prototype.DefaultInterpolation=rl;ho.prototype.InterpolantFactoryMethodLinear=void 0;ho.prototype.InterpolantFactoryMethodSmooth=void 0;var Mh=class extends ii{constructor(e,t,i,r){super(e,t,i,r)}};Mh.prototype.ValueTypeName="vector";var zy={enabled:!1,files:{},add:function(n,e){this.enabled!==!1&&(SS(n)||(this.files[n]=e))},get:function(n){if(this.enabled!==!1&&!SS(n))return this.files[n]},remove:function(n){delete this.files[n]},clear:function(){this.files={}}};function SS(n){try{let e=n.slice(n.indexOf(":")+1);return new URL(e).protocol==="blob:"}catch{return!1}}var Sh=class{constructor(e,t,i){let r=this,o=!1,s=0,a=0,c,l=[];this.onStart=void 0,this.onLoad=e,this.onProgress=t,this.onError=i,this._abortController=null,this.itemStart=function(u){a++,o===!1&&r.onStart!==void 0&&r.onStart(u,s,a),o=!0},this.itemEnd=function(u){s++,r.onProgress!==void 0&&r.onProgress(u,s,a),s===a&&(o=!1,r.onLoad!==void 0&&r.onLoad())},this.itemError=function(u){r.onError!==void 0&&r.onError(u)},this.resolveURL=function(u){return c?c(u):u},this.setURLModifier=function(u){return c=u,this},this.addHandler=function(u,d){return l.push(u,d),this},this.removeHandler=function(u){let d=l.indexOf(u);return d!==-1&&l.splice(d,2),this},this.getHandler=function(u){for(let d=0,f=l.length;d<f;d+=2){let h=l[d],p=l[d+1];if(h.global&&(h.lastIndex=0),h.test(u))return p}return null},this.abort=function(){return this.abortController.abort(),this._abortController=null,this}}get abortController(){return this._abortController||(this._abortController=new AbortController),this._abortController}},mw=new Sh,gp=(()=>{class n{constructor(t){this.manager=t!==void 0?t:mw,this.crossOrigin="anonymous",this.withCredentials=!1,this.path="",this.resourcePath="",this.requestHeader={},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}load(){}loadAsync(t,i){let r=this;return new Promise(function(o,s){r.load(t,o,i,s)})}parse(){}setCrossOrigin(t){return this.crossOrigin=t,this}setWithCredentials(t){return this.withCredentials=t,this}setPath(t){return this.path=t,this}setResourcePath(t){return this.resourcePath=t,this}setRequestHeader(t){return this.requestHeader=t,this}abort(){return this}}return n.DEFAULT_MATERIAL_NAME="__DEFAULT",n})(),Mr={},Gy=class extends Error{constructor(e,t){super(e),this.response=t}},_l=class extends gp{constructor(e){super(e),this.mimeType="",this.responseType="",this._abortController=new AbortController}load(e,t,i,r){e===void 0&&(e=""),this.path!==void 0&&(e=this.path+e),e=this.manager.resolveURL(e);let o=zy.get(`file:${e}`);if(o!==void 0)return this.manager.itemStart(e),setTimeout(()=>{t&&t(o),this.manager.itemEnd(e)},0),o;if(Mr[e]!==void 0){Mr[e].push({onLoad:t,onProgress:i,onError:r});return}Mr[e]=[],Mr[e].push({onLoad:t,onProgress:i,onError:r});let s=new Request(e,{headers:new Headers(this.requestHeader),credentials:this.withCredentials?"include":"same-origin",signal:typeof AbortSignal.any=="function"?AbortSignal.any([this._abortController.signal,this.manager.abortController.signal]):this._abortController.signal}),a=this.mimeType,c=this.responseType;fetch(s).then(l=>{if(l.status===200||l.status===0){if(l.status===0&&Fe("FileLoader: HTTP Status 0 received."),typeof ReadableStream>"u"||l.body===void 0||l.body.getReader===void 0)return l;let u=Mr[e],d=l.body.getReader(),f=l.headers.get("X-File-Size")||l.headers.get("Content-Length"),h=f?parseInt(f):0,p=h!==0,y=0,g=new ReadableStream({start(m){E();function E(){d.read().then(({done:S,value:w})=>{if(S)m.close();else{y+=w.byteLength;let D=new ProgressEvent("progress",{lengthComputable:p,loaded:y,total:h});for(let C=0,I=u.length;C<I;C++){let _=u[C];_.onProgress&&_.onProgress(D)}m.enqueue(w),E()}},S=>{m.error(S)})}}});return new Response(g)}else throw new Gy(`fetch for "${l.url}" responded with ${l.status}: ${l.statusText}`,l)}).then(l=>{switch(c){case"arraybuffer":return l.arrayBuffer();case"blob":return l.blob();case"document":return l.text().then(u=>new DOMParser().parseFromString(u,a));case"json":return l.json();default:if(a==="")return l.text();{let d=/charset="?([^;"\s]*)"?/i.exec(a),f=d&&d[1]?d[1].toLowerCase():void 0,h=new TextDecoder(f);return l.arrayBuffer().then(p=>h.decode(p))}}}).then(l=>{zy.add(`file:${e}`,l);let u=Mr[e];delete Mr[e];for(let d=0,f=u.length;d<f;d++){let h=u[d];h.onLoad&&h.onLoad(l)}}).catch(l=>{let u=Mr[e];if(u===void 0)throw this.manager.itemError(e),l;delete Mr[e];for(let d=0,f=u.length;d<f;d++){let h=u[d];h.onError&&h.onError(l)}this.manager.itemError(e)}).finally(()=>{this.manager.itemEnd(e)}),this.manager.itemStart(e)}setResponseType(e){return this.responseType=e,this}setMimeType(e){return this.mimeType=e,this}abort(){return this._abortController.abort(),this._abortController=new AbortController,this}};var xl=class extends Ii{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new nt(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}};var Oy=new Ot,wS=new L,CS=new L,jy=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.biasNode=null,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new lt(512,512),this.mapType=$n,this.map=null,this.mapPass=null,this.matrix=new Ot,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Na,this._frameExtents=new lt(1,1),this._viewportCount=1,this._viewports=[new Lt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;wS.setFromMatrixPosition(e.matrixWorld),t.position.copy(wS),CS.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(CS),t.updateMatrixWorld(),Oy.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Oy,t.coordinateSystem,t.reversedDepth),t.coordinateSystem===Ca||t.reversedDepth?i.set(.5,0,0,.5,0,.5,0,.5,0,0,1,0,0,0,0,1):i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Oy)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.autoUpdate=e.autoUpdate,this.needsUpdate=e.needsUpdate,this.normalBias=e.normalBias,this.blurSamples=e.blurSamples,this.mapSize.copy(e.mapSize),this.biasNode=e.biasNode,this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}},zf=new L,Gf=new Ji,qi=new L,El=class extends Ii{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Ot,this.projectionMatrix=new Ot,this.projectionMatrixInverse=new Ot,this.coordinateSystem=Di,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(zf,Gf,qi),qi.x===1&&qi.y===1&&qi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(zf,Gf,qi.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(zf,Gf,qi),qi.x===1&&qi.y===1&&qi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(zf,Gf,qi.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},oo=new L,TS=new lt,DS=new lt,En=class extends El{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=rh*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Wf*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return rh*2*Math.atan(Math.tan(Wf*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){oo.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(oo.x,oo.y).multiplyScalar(-e/oo.z),oo.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(oo.x,oo.y).multiplyScalar(-e/oo.z)}getViewSize(e,t){return this.getViewBounds(e,TS,DS),t.subVectors(DS,TS)}setViewOffset(e,t,i,r,o,s){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=o,this.view.height=s,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Wf*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,o=-.5*r,s=this.view;if(this.view!==null&&this.view.enabled){let c=s.fullWidth,l=s.fullHeight;o+=s.offsetX*r/c,t-=s.offsetY*i/l,r*=s.width/c,i*=s.height/l}let a=this.filmOffset;a!==0&&(o+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(o,o+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var La=class extends El{constructor(e=-1,t=1,i=1,r=-1,o=.1,s=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=o,this.far=s,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,o,s){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=o,this.view.height=s,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,o=i-e,s=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;o+=l*this.view.offsetX,s=o+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(o,s,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Wy=class extends jy{constructor(){super(new La(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},Fa=class extends xl{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(Ii.DEFAULT_UP),this.updateMatrix(),this.target=new Ii,this.shadow=new Wy}dispose(){super.dispose(),this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.shadow=this.shadow.toJSON(),t.object.target=this.target.uuid,t}},bl=class extends xl{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var Ma=-90,Sa=1,wh=class extends Ii{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new En(Ma,Sa,e,t);r.layers=this.layers,this.add(r);let o=new En(Ma,Sa,e,t);o.layers=this.layers,this.add(o);let s=new En(Ma,Sa,e,t);s.layers=this.layers,this.add(s);let a=new En(Ma,Sa,e,t);a.layers=this.layers,this.add(a);let c=new En(Ma,Sa,e,t);c.layers=this.layers,this.add(c);let l=new En(Ma,Sa,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,o,s,a,c]=t;for(let l of t)this.remove(l);if(e===Di)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),o.up.set(0,0,-1),o.lookAt(0,1,0),s.up.set(0,0,1),s.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Ca)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),o.up.set(0,0,1),o.lookAt(0,1,0),s.up.set(0,0,-1),s.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[o,s,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),p=e.xr.enabled;e.xr.enabled=!1;let y=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let g=!1;e.isWebGLRenderer===!0?g=e.state.buffers.depth.getReversed():g=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,1,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,2,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=y,e.setRenderTarget(i,5,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=p,i.texture.needsPMREMUpdate=!0}},Ch=class extends En{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var v0="\\[\\]\\.:\\/",hP=new RegExp("["+v0+"]","g"),y0="[^"+v0+"]",pP="[^"+v0.replace("\\.","")+"]",mP=/((?:WC+[\/:])*)/.source.replace("WC",y0),gP=/(WCOD+)?/.source.replace("WCOD",pP),vP=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",y0),yP=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",y0),_P=new RegExp("^"+mP+gP+vP+yP+"$"),xP=["material","materials","bones","map"],$y=class{constructor(e,t,i){let r=i||Ut.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,o=i.length;r!==o;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Ut=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(hP,"")}static parseTrackName(t){let i=_P.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},o=r.nodeName&&r.nodeName.lastIndexOf(".");if(o!==void 0&&o!==-1){let s=r.nodeName.substring(o+1);xP.indexOf(s)!==-1&&(r.nodeName=r.nodeName.substring(0,o),r.objectName=s)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(s){for(let a=0;a<s.length;a++){let c=s[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},o=r(t.children);if(o)return o}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)t[i++]=r[o]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)r[o]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)r[o]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)r[o]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,o=i.propertyName,s=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Fe("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Ue("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Ue("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Ue("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Ue("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Ue("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Ue("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Ue("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[o];if(a===void 0){let u=i.nodeName;Ue("PropertyBinding: Trying to update property for track: "+u+"."+o+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(s!==void 0){if(o==="morphTargetInfluences"){if(!t.geometry){Ue("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Ue("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[s]!==void 0&&(s=t.morphTargetDictionary[s])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=s}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=o;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=$y,n})();Ut.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Ut.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Ut.prototype.GetterByBindingType=[Ut.prototype._getValue_direct,Ut.prototype._getValue_array,Ut.prototype._getValue_arrayElement,Ut.prototype._getValue_toArray];Ut.prototype.SetterByBindingTypeAndVersioning=[[Ut.prototype._setValue_direct,Ut.prototype._setValue_direct_setNeedsUpdate,Ut.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Ut.prototype._setValue_array,Ut.prototype._setValue_array_setNeedsUpdate,Ut.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Ut.prototype._setValue_arrayElement,Ut.prototype._setValue_arrayElement_setNeedsUpdate,Ut.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Ut.prototype._setValue_fromArray,Ut.prototype._setValue_fromArray_setNeedsUpdate,Ut.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var uX=new Float32Array(1);var IS=new Ot,Ml=class{constructor(e,t,i=0,r=1/0){this.ray=new Ra(e,t),this.near=i,this.far=r,this.camera=null,this.layers=new Ia,this.params={Mesh:{},Line:{threshold:1},LOD:{},Points:{threshold:1},Sprite:{}}}set(e,t){this.ray.set(e,t)}setFromCamera(e,t){t.isPerspectiveCamera?(this.ray.origin.setFromMatrixPosition(t.matrixWorld),this.ray.direction.set(e.x,e.y,.5).unproject(t).sub(this.ray.origin).normalize(),this.camera=t):t.isOrthographicCamera?(this.ray.origin.set(e.x,e.y,(t.near+t.far)/(t.near-t.far)).unproject(t),this.ray.direction.set(0,0,-1).transformDirection(t.matrixWorld),this.camera=t):Ue("Raycaster: Unsupported camera type: "+t.type)}setFromXRController(e){return IS.identity().extractRotation(e.matrixWorld),this.ray.origin.setFromMatrixPosition(e.matrixWorld),this.ray.direction.set(0,0,-1).applyMatrix4(IS),this}intersectObject(e,t=!0,i=[]){return qy(e,this,i,t),i.sort(AS),i}intersectObjects(e,t=!0,i=[]){for(let r=0,o=e.length;r<o;r++)qy(e[r],this,i,t);return i.sort(AS),i}};function AS(n,e){return n.distance-e.distance}function qy(n,e,t,i){let r=!0;if(n.layers.test(e.layers)&&n.raycast(e,t)===!1&&(r=!1),r===!0&&i===!0){let o=n.children;for(let s=0,a=o.length;s<a;s++)qy(o[s],e,t,!0)}}var Sl=class extends Pa{constructor(e=10,t=10,i=4473924,r=8947848){i=new nt(i),r=new nt(r);let o=t/2,s=e/t,a=e/2,c=[],l=[];for(let f=0,h=0,p=-a;f<=t;f++,p+=s){c.push(-a,0,p,a,0,p),c.push(p,0,-a,p,0,a);let y=f===o?i:r;y.toArray(l,h),h+=3,y.toArray(l,h),h+=3,y.toArray(l,h),h+=3,y.toArray(l,h),h+=3}let u=new bn;u.setAttribute("position",new Xt(c,3)),u.setAttribute("color",new Xt(l,3));let d=new fs({vertexColors:!0,toneMapped:!1});super(u,d),this.type="GridHelper"}dispose(){this.geometry.dispose(),this.material.dispose()}};function _0(n,e,t,i){let r=EP(i);switch(t){case u0:return n*e;case f0:return n*e/r.components*r.byteLength;case Ph:return n*e/r.components*r.byteLength;case ps:return n*e*2/r.components*r.byteLength;case Oh:return n*e*2/r.components*r.byteLength;case d0:return n*e*3/r.components*r.byteLength;case fi:return n*e*4/r.components*r.byteLength;case Lh:return n*e*4/r.components*r.byteLength;case Dl:case Il:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Al:case Rl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case kh:case Bh:return Math.max(n,16)*Math.max(e,8)/4;case Fh:case Uh:return Math.max(n,8)*Math.max(e,8)/2;case Vh:case Hh:case Gh:case jh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case zh:case Wh:case $h:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case qh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Xh:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Yh:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Zh:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Jh:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Kh:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Qh:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case ep:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case tp:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case np:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case ip:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case rp:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case op:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case sp:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case ap:case cp:case lp:return Math.ceil(n/4)*Math.ceil(e/4)*16;case up:case dp:return Math.ceil(n/4)*Math.ceil(e/4)*8;case fp:case hp:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function EP(n){switch(n){case $n:case s0:return{byteLength:1,components:1};case Ua:case a0:case er:return{byteLength:2,components:1};case Rh:case Nh:return{byteLength:2,components:4};case Ri:case Ah:case Ni:return{byteLength:4,components:1};case c0:case l0:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"183"}}));typeof window<"u"&&(window.__THREE__?Fe("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="183");function Uw(){let n=null,e=!1,t=null,i=null;function r(o,s){t(o,s),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(o){t=o},setContext:function(o){n=o}}}function MP(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,p)=>h.start-p.start);let f=0;for(let h=1;h<d.length;h++){let p=d[f],y=d[h];y.start<=p.start+p.count+1?p.count=Math.max(p.count,y.start+y.count-p.start):(++f,d[f]=y)}d.length=f+1;for(let h=0,p=d.length;h<p;h++){let y=d[h];n.bufferSubData(l,y.start*u.BYTES_PER_ELEMENT,u,y.start,y.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function o(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function s(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:o,update:s}}var SP=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,wP=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,CP=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,TP=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,DP=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,IP=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,AP=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,RP=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,NP=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,PP=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,OP=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,LP=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,FP=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,kP=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,UP=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,BP=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,VP=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,HP=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,zP=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,GP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,jP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,WP=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,$P=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,qP=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,XP=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,YP=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,ZP=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,JP=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,KP=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,QP=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,eO="gl_FragColor = linearToOutputTexel( gl_FragColor );",tO=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,nO=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,iO=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,rO=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,oO=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,sO=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,aO=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,cO=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,lO=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,uO=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,dO=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,fO=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,hO=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,pO=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,mO=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,gO=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,vO=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,yO=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,_O=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,xO=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,EO=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,bO=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return v;
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,MO=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,SO=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,wO=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,CO=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,TO=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,DO=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,IO=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,AO=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,RO=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,NO=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,PO=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,OO=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,LO=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,FO=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,kO=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,UO=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,BO=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,VO=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,HO=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,zO=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,GO=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,jO=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,WO=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,$O=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,qO=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,XO=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,YO=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,ZO=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,JO=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,KO=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,QO=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,eL=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,tL=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,nL=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,iL=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,rL=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,oL=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,sL=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,aL=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,cL=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,lL=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,uL=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,dL=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,fL=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,hL=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,pL=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,mL=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,gL=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,vL=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,yL=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,_L=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,xL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,EL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,bL=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,ML=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,SL=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,wL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,CL=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,TL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,DL=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,IL=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,AL=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,RL=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,NL=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,PL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,OL=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,LL=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,FL=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,kL=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,UL=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,BL=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,VL=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,HL=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,zL=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,GL=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,jL=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,WL=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,$L=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,qL=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,XL=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,YL=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,ZL=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,JL=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,KL=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,QL=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,eF=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,tF=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,nF=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,Ke={alphahash_fragment:SP,alphahash_pars_fragment:wP,alphamap_fragment:CP,alphamap_pars_fragment:TP,alphatest_fragment:DP,alphatest_pars_fragment:IP,aomap_fragment:AP,aomap_pars_fragment:RP,batching_pars_vertex:NP,batching_vertex:PP,begin_vertex:OP,beginnormal_vertex:LP,bsdfs:FP,iridescence_fragment:kP,bumpmap_pars_fragment:UP,clipping_planes_fragment:BP,clipping_planes_pars_fragment:VP,clipping_planes_pars_vertex:HP,clipping_planes_vertex:zP,color_fragment:GP,color_pars_fragment:jP,color_pars_vertex:WP,color_vertex:$P,common:qP,cube_uv_reflection_fragment:XP,defaultnormal_vertex:YP,displacementmap_pars_vertex:ZP,displacementmap_vertex:JP,emissivemap_fragment:KP,emissivemap_pars_fragment:QP,colorspace_fragment:eO,colorspace_pars_fragment:tO,envmap_fragment:nO,envmap_common_pars_fragment:iO,envmap_pars_fragment:rO,envmap_pars_vertex:oO,envmap_physical_pars_fragment:gO,envmap_vertex:sO,fog_vertex:aO,fog_pars_vertex:cO,fog_fragment:lO,fog_pars_fragment:uO,gradientmap_pars_fragment:dO,lightmap_pars_fragment:fO,lights_lambert_fragment:hO,lights_lambert_pars_fragment:pO,lights_pars_begin:mO,lights_toon_fragment:vO,lights_toon_pars_fragment:yO,lights_phong_fragment:_O,lights_phong_pars_fragment:xO,lights_physical_fragment:EO,lights_physical_pars_fragment:bO,lights_fragment_begin:MO,lights_fragment_maps:SO,lights_fragment_end:wO,logdepthbuf_fragment:CO,logdepthbuf_pars_fragment:TO,logdepthbuf_pars_vertex:DO,logdepthbuf_vertex:IO,map_fragment:AO,map_pars_fragment:RO,map_particle_fragment:NO,map_particle_pars_fragment:PO,metalnessmap_fragment:OO,metalnessmap_pars_fragment:LO,morphinstance_vertex:FO,morphcolor_vertex:kO,morphnormal_vertex:UO,morphtarget_pars_vertex:BO,morphtarget_vertex:VO,normal_fragment_begin:HO,normal_fragment_maps:zO,normal_pars_fragment:GO,normal_pars_vertex:jO,normal_vertex:WO,normalmap_pars_fragment:$O,clearcoat_normal_fragment_begin:qO,clearcoat_normal_fragment_maps:XO,clearcoat_pars_fragment:YO,iridescence_pars_fragment:ZO,opaque_fragment:JO,packing:KO,premultiplied_alpha_fragment:QO,project_vertex:eL,dithering_fragment:tL,dithering_pars_fragment:nL,roughnessmap_fragment:iL,roughnessmap_pars_fragment:rL,shadowmap_pars_fragment:oL,shadowmap_pars_vertex:sL,shadowmap_vertex:aL,shadowmask_pars_fragment:cL,skinbase_vertex:lL,skinning_pars_vertex:uL,skinning_vertex:dL,skinnormal_vertex:fL,specularmap_fragment:hL,specularmap_pars_fragment:pL,tonemapping_fragment:mL,tonemapping_pars_fragment:gL,transmission_fragment:vL,transmission_pars_fragment:yL,uv_pars_fragment:_L,uv_pars_vertex:xL,uv_vertex:EL,worldpos_vertex:bL,background_vert:ML,background_frag:SL,backgroundCube_vert:wL,backgroundCube_frag:CL,cube_vert:TL,cube_frag:DL,depth_vert:IL,depth_frag:AL,distance_vert:RL,distance_frag:NL,equirect_vert:PL,equirect_frag:OL,linedashed_vert:LL,linedashed_frag:FL,meshbasic_vert:kL,meshbasic_frag:UL,meshlambert_vert:BL,meshlambert_frag:VL,meshmatcap_vert:HL,meshmatcap_frag:zL,meshnormal_vert:GL,meshnormal_frag:jL,meshphong_vert:WL,meshphong_frag:$L,meshphysical_vert:qL,meshphysical_frag:XL,meshtoon_vert:YL,meshtoon_frag:ZL,points_vert:JL,points_frag:KL,shadow_vert:QL,shadow_frag:eF,sprite_vert:tF,sprite_frag:nF},ue={common:{diffuse:{value:new nt(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Ye},alphaMap:{value:null},alphaMapTransform:{value:new Ye},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Ye}},envmap:{envMap:{value:null},envMapRotation:{value:new Ye},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Ye}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Ye}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Ye},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Ye},normalScale:{value:new lt(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Ye},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Ye}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Ye}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Ye}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new nt(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new nt(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Ye},alphaTest:{value:0},uvTransform:{value:new Ye}},sprite:{diffuse:{value:new nt(16777215)},opacity:{value:1},center:{value:new lt(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Ye},alphaMap:{value:null},alphaMapTransform:{value:new Ye},alphaTest:{value:0}}},nr={basic:{uniforms:Mn([ue.common,ue.specularmap,ue.envmap,ue.aomap,ue.lightmap,ue.fog]),vertexShader:Ke.meshbasic_vert,fragmentShader:Ke.meshbasic_frag},lambert:{uniforms:Mn([ue.common,ue.specularmap,ue.envmap,ue.aomap,ue.lightmap,ue.emissivemap,ue.bumpmap,ue.normalmap,ue.displacementmap,ue.fog,ue.lights,{emissive:{value:new nt(0)},envMapIntensity:{value:1}}]),vertexShader:Ke.meshlambert_vert,fragmentShader:Ke.meshlambert_frag},phong:{uniforms:Mn([ue.common,ue.specularmap,ue.envmap,ue.aomap,ue.lightmap,ue.emissivemap,ue.bumpmap,ue.normalmap,ue.displacementmap,ue.fog,ue.lights,{emissive:{value:new nt(0)},specular:{value:new nt(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:Ke.meshphong_vert,fragmentShader:Ke.meshphong_frag},standard:{uniforms:Mn([ue.common,ue.envmap,ue.aomap,ue.lightmap,ue.emissivemap,ue.bumpmap,ue.normalmap,ue.displacementmap,ue.roughnessmap,ue.metalnessmap,ue.fog,ue.lights,{emissive:{value:new nt(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Ke.meshphysical_vert,fragmentShader:Ke.meshphysical_frag},toon:{uniforms:Mn([ue.common,ue.aomap,ue.lightmap,ue.emissivemap,ue.bumpmap,ue.normalmap,ue.displacementmap,ue.gradientmap,ue.fog,ue.lights,{emissive:{value:new nt(0)}}]),vertexShader:Ke.meshtoon_vert,fragmentShader:Ke.meshtoon_frag},matcap:{uniforms:Mn([ue.common,ue.bumpmap,ue.normalmap,ue.displacementmap,ue.fog,{matcap:{value:null}}]),vertexShader:Ke.meshmatcap_vert,fragmentShader:Ke.meshmatcap_frag},points:{uniforms:Mn([ue.points,ue.fog]),vertexShader:Ke.points_vert,fragmentShader:Ke.points_frag},dashed:{uniforms:Mn([ue.common,ue.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Ke.linedashed_vert,fragmentShader:Ke.linedashed_frag},depth:{uniforms:Mn([ue.common,ue.displacementmap]),vertexShader:Ke.depth_vert,fragmentShader:Ke.depth_frag},normal:{uniforms:Mn([ue.common,ue.bumpmap,ue.normalmap,ue.displacementmap,{opacity:{value:1}}]),vertexShader:Ke.meshnormal_vert,fragmentShader:Ke.meshnormal_frag},sprite:{uniforms:Mn([ue.sprite,ue.fog]),vertexShader:Ke.sprite_vert,fragmentShader:Ke.sprite_frag},background:{uniforms:{uvTransform:{value:new Ye},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Ke.background_vert,fragmentShader:Ke.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Ye}},vertexShader:Ke.backgroundCube_vert,fragmentShader:Ke.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Ke.cube_vert,fragmentShader:Ke.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Ke.equirect_vert,fragmentShader:Ke.equirect_frag},distance:{uniforms:Mn([ue.common,ue.displacementmap,{referencePosition:{value:new L},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Ke.distance_vert,fragmentShader:Ke.distance_frag},shadow:{uniforms:Mn([ue.lights,ue.fog,{color:{value:new nt(0)},opacity:{value:1}}]),vertexShader:Ke.shadow_vert,fragmentShader:Ke.shadow_frag}};nr.physical={uniforms:Mn([nr.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Ye},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Ye},clearcoatNormalScale:{value:new lt(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Ye},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Ye},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Ye},sheen:{value:0},sheenColor:{value:new nt(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Ye},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Ye},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Ye},transmissionSamplerSize:{value:new lt},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Ye},attenuationDistance:{value:0},attenuationColor:{value:new nt(0)},specularColor:{value:new nt(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Ye},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Ye},anisotropyVector:{value:new lt},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Ye}}]),vertexShader:Ke.meshphysical_vert,fragmentShader:Ke.meshphysical_frag};var vp={r:0,b:0,g:0},gs=new ao,iF=new Ot;function rF(n,e,t,i,r,o){let s=new nt(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(E){let S=E.isScene===!0?E.background:null;if(S&&S.isTexture){let w=E.backgroundBlurriness>0;S=e.get(S,w)}return S}function p(E){let S=!1,w=h(E);w===null?g(s,a):w&&w.isColor&&(g(w,1),S=!0);let D=n.xr.getEnvironmentBlendMode();D==="additive"?t.buffers.color.setClear(0,0,0,1,o):D==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,o),(n.autoClear||S)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function y(E,S){let w=h(S);w&&(w.isCubeTexture||w.mapping===Cl)?(l===void 0&&(l=new Wn(new Oa(1,1,1),new ni({name:"BackgroundCubeMaterial",uniforms:ms(nr.backgroundCube.uniforms),vertexShader:nr.backgroundCube.vertexShader,fragmentShader:nr.backgroundCube.fragmentShader,side:kn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(D,C,I){this.matrixWorld.copyPosition(I.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),gs.copy(S.backgroundRotation),gs.x*=-1,gs.y*=-1,gs.z*=-1,w.isCubeTexture&&w.isRenderTargetTexture===!1&&(gs.y*=-1,gs.z*=-1),l.material.uniforms.envMap.value=w,l.material.uniforms.flipEnvMap.value=w.isCubeTexture&&w.isRenderTargetTexture===!1?-1:1,l.material.uniforms.backgroundBlurriness.value=S.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=S.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(iF.makeRotationFromEuler(gs)),l.material.toneMapped=ct.getTransfer(w.colorSpace)!==xt,(u!==w||d!==w.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=w,d=w.version,f=n.toneMapping),l.layers.enableAll(),E.unshift(l,l.geometry,l.material,0,0,null)):w&&w.isTexture&&(c===void 0&&(c=new Wn(new gl(2,2),new ni({name:"BackgroundMaterial",uniforms:ms(nr.background.uniforms),vertexShader:nr.background.vertexShader,fragmentShader:nr.background.fragmentShader,side:Cr,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=w,c.material.uniforms.backgroundIntensity.value=S.backgroundIntensity,c.material.toneMapped=ct.getTransfer(w.colorSpace)!==xt,w.matrixAutoUpdate===!0&&w.updateMatrix(),c.material.uniforms.uvTransform.value.copy(w.matrix),(u!==w||d!==w.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=w,d=w.version,f=n.toneMapping),c.layers.enableAll(),E.unshift(c,c.geometry,c.material,0,0,null))}function g(E,S){E.getRGB(vp,g0(n)),t.buffers.color.setClear(vp.r,vp.g,vp.b,S,o)}function m(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return s},setClearColor:function(E,S=1){s.set(E),a=S,g(s,a)},getClearAlpha:function(){return a},setClearAlpha:function(E){a=E,g(s,a)},render:p,addToRenderList:y,dispose:m}}function oF(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),o=r,s=!1;function a(T,F,H,G,B){let U=!1,O=d(T,G,H,F);o!==O&&(o=O,l(o.object)),U=h(T,G,H,B),U&&p(T,G,H,B),B!==null&&e.update(B,n.ELEMENT_ARRAY_BUFFER),(U||s)&&(s=!1,w(T,F,H,G),B!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(B).buffer))}function c(){return n.createVertexArray()}function l(T){return n.bindVertexArray(T)}function u(T){return n.deleteVertexArray(T)}function d(T,F,H,G){let B=G.wireframe===!0,U=i[F.id];U===void 0&&(U={},i[F.id]=U);let O=T.isInstancedMesh===!0?T.id:0,Z=U[O];Z===void 0&&(Z={},U[O]=Z);let K=Z[H.id];K===void 0&&(K={},Z[H.id]=K);let he=K[B];return he===void 0&&(he=f(c()),K[B]=he),he}function f(T){let F=[],H=[],G=[];for(let B=0;B<t;B++)F[B]=0,H[B]=0,G[B]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:F,enabledAttributes:H,attributeDivisors:G,object:T,attributes:{},index:null}}function h(T,F,H,G){let B=o.attributes,U=F.attributes,O=0,Z=H.getAttributes();for(let K in Z)if(Z[K].location>=0){let _e=B[K],ge=U[K];if(ge===void 0&&(K==="instanceMatrix"&&T.instanceMatrix&&(ge=T.instanceMatrix),K==="instanceColor"&&T.instanceColor&&(ge=T.instanceColor)),_e===void 0||_e.attribute!==ge||ge&&_e.data!==ge.data)return!0;O++}return o.attributesNum!==O||o.index!==G}function p(T,F,H,G){let B={},U=F.attributes,O=0,Z=H.getAttributes();for(let K in Z)if(Z[K].location>=0){let _e=U[K];_e===void 0&&(K==="instanceMatrix"&&T.instanceMatrix&&(_e=T.instanceMatrix),K==="instanceColor"&&T.instanceColor&&(_e=T.instanceColor));let ge={};ge.attribute=_e,_e&&_e.data&&(ge.data=_e.data),B[K]=ge,O++}o.attributes=B,o.attributesNum=O,o.index=G}function y(){let T=o.newAttributes;for(let F=0,H=T.length;F<H;F++)T[F]=0}function g(T){m(T,0)}function m(T,F){let H=o.newAttributes,G=o.enabledAttributes,B=o.attributeDivisors;H[T]=1,G[T]===0&&(n.enableVertexAttribArray(T),G[T]=1),B[T]!==F&&(n.vertexAttribDivisor(T,F),B[T]=F)}function E(){let T=o.newAttributes,F=o.enabledAttributes;for(let H=0,G=F.length;H<G;H++)F[H]!==T[H]&&(n.disableVertexAttribArray(H),F[H]=0)}function S(T,F,H,G,B,U,O){O===!0?n.vertexAttribIPointer(T,F,H,B,U):n.vertexAttribPointer(T,F,H,G,B,U)}function w(T,F,H,G){y();let B=G.attributes,U=H.getAttributes(),O=F.defaultAttributeValues;for(let Z in U){let K=U[Z];if(K.location>=0){let he=B[Z];if(he===void 0&&(Z==="instanceMatrix"&&T.instanceMatrix&&(he=T.instanceMatrix),Z==="instanceColor"&&T.instanceColor&&(he=T.instanceColor)),he!==void 0){let _e=he.normalized,ge=he.itemSize,Je=e.get(he);if(Je===void 0)continue;let Pt=Je.buffer,Nt=Je.type,X=Je.bytesPerElement,re=Nt===n.INT||Nt===n.UNSIGNED_INT||he.gpuType===Ah;if(he.isInterleavedBufferAttribute){let ce=he.data,Ze=ce.stride,ke=he.offset;if(ce.isInstancedInterleavedBuffer){for(let He=0;He<K.locationSize;He++)m(K.location+He,ce.meshPerAttribute);T.isInstancedMesh!==!0&&G._maxInstanceCount===void 0&&(G._maxInstanceCount=ce.meshPerAttribute*ce.count)}else for(let He=0;He<K.locationSize;He++)g(K.location+He);n.bindBuffer(n.ARRAY_BUFFER,Pt);for(let He=0;He<K.locationSize;He++)S(K.location+He,ge/K.locationSize,Nt,_e,Ze*X,(ke+ge/K.locationSize*He)*X,re)}else{if(he.isInstancedBufferAttribute){for(let ce=0;ce<K.locationSize;ce++)m(K.location+ce,he.meshPerAttribute);T.isInstancedMesh!==!0&&G._maxInstanceCount===void 0&&(G._maxInstanceCount=he.meshPerAttribute*he.count)}else for(let ce=0;ce<K.locationSize;ce++)g(K.location+ce);n.bindBuffer(n.ARRAY_BUFFER,Pt);for(let ce=0;ce<K.locationSize;ce++)S(K.location+ce,ge/K.locationSize,Nt,_e,ge*X,ge/K.locationSize*ce*X,re)}}else if(O!==void 0){let _e=O[Z];if(_e!==void 0)switch(_e.length){case 2:n.vertexAttrib2fv(K.location,_e);break;case 3:n.vertexAttrib3fv(K.location,_e);break;case 4:n.vertexAttrib4fv(K.location,_e);break;default:n.vertexAttrib1fv(K.location,_e)}}}}E()}function D(){b();for(let T in i){let F=i[T];for(let H in F){let G=F[H];for(let B in G){let U=G[B];for(let O in U)u(U[O].object),delete U[O];delete G[B]}}delete i[T]}}function C(T){if(i[T.id]===void 0)return;let F=i[T.id];for(let H in F){let G=F[H];for(let B in G){let U=G[B];for(let O in U)u(U[O].object),delete U[O];delete G[B]}}delete i[T.id]}function I(T){for(let F in i){let H=i[F];for(let G in H){let B=H[G];if(B[T.id]===void 0)continue;let U=B[T.id];for(let O in U)u(U[O].object),delete U[O];delete B[T.id]}}}function _(T){for(let F in i){let H=i[F],G=T.isInstancedMesh===!0?T.id:0,B=H[G];if(B!==void 0){for(let U in B){let O=B[U];for(let Z in O)u(O[Z].object),delete O[Z];delete B[U]}delete H[G],Object.keys(H).length===0&&delete i[F]}}}function b(){W(),s=!0,o!==r&&(o=r,l(o.object))}function W(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:b,resetDefaultState:W,dispose:D,releaseStatesOfGeometry:C,releaseStatesOfObject:_,releaseStatesOfProgram:I,initAttributes:y,enableAttribute:g,disableUnusedAttributes:E}}function sF(n,e,t){let i;function r(l){i=l}function o(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function s(l,u,d){d!==0&&(n.drawArraysInstanced(i,l,u,d),t.update(u,i,d))}function a(l,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,d);let h=0;for(let p=0;p<d;p++)h+=u[p];t.update(h,i,1)}function c(l,u,d,f){if(d===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let p=0;p<l.length;p++)s(l[p],u[p],f[p]);else{h.multiDrawArraysInstancedWEBGL(i,l,0,u,0,f,0,d);let p=0;for(let y=0;y<d;y++)p+=u[y]*f[y];t.update(p,i,1)}}this.setMode=r,this.render=o,this.renderInstances=s,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function aF(n,e,t,i){let r;function o(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let I=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(I.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function s(I){return!(I!==fi&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(I){let _=I===er&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(I!==$n&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&I!==Ni&&!_)}function c(I){if(I==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";I="mediump"}return I==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Fe("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control"),h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),p=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),y=n.getParameter(n.MAX_TEXTURE_SIZE),g=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),m=n.getParameter(n.MAX_VERTEX_ATTRIBS),E=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),S=n.getParameter(n.MAX_VARYING_VECTORS),w=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),D=n.getParameter(n.MAX_SAMPLES),C=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:o,getMaxPrecision:c,textureFormatReadable:s,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:f,maxTextures:h,maxVertexTextures:p,maxTextureSize:y,maxCubemapSize:g,maxAttributes:m,maxVertexUniforms:E,maxVaryings:S,maxFragmentUniforms:w,maxSamples:D,samples:C}}function cF(n){let e=this,t=null,i=0,r=!1,o=!1,s=new Xi,a=new Ye,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){o=!0,u(null)},this.endShadows=function(){o=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let p=d.clippingPlanes,y=d.clipIntersection,g=d.clipShadows,m=n.get(d);if(!r||p===null||p.length===0||o&&!g)o?u(null):l();else{let E=o?0:i,S=E*4,w=m.clippingState||null;c.value=w,w=u(p,f,S,h);for(let D=0;D!==S;++D)w[D]=t[D];m.clippingState=w,this.numIntersection=y?this.numPlanes:0,this.numPlanes+=E}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,p){let y=d!==null?d.length:0,g=null;if(y!==0){if(g=c.value,p!==!0||g===null){let m=h+y*4,E=f.matrixWorldInverse;a.getNormalMatrix(E),(g===null||g.length<m)&&(g=new Float32Array(m));for(let S=0,w=h;S!==y;++S,w+=4)s.copy(d[S]).applyMatrix4(E,a),s.normal.toArray(g,w),g[w+3]=s.constant}c.value=g,c.needsUpdate=!0}return e.numPlanes=y,e.numIntersection=0,g}}var vo=4,gw=[.125,.215,.35,.446,.526,.582],ys=20,lF=256,Pl=new La,vw=new nt,x0=null,E0=0,b0=0,M0=!1,uF=new L,_p=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,o={}){let{size:s=256,position:a=uF}=o;x0=this._renderer.getRenderTarget(),E0=this._renderer.getActiveCubeFace(),b0=this._renderer.getActiveMipmapLevel(),M0=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(s);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=xw(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=_w(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(x0,E0,b0),this._renderer.xr.enabled=M0,e.scissorTest=!1,Va(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===po||e.mapping===hs?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),x0=this._renderer.getRenderTarget(),E0=this._renderer.getActiveCubeFace(),b0=this._renderer.getActiveMipmapLevel(),M0=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:hn,minFilter:hn,generateMipmaps:!1,type:er,format:fi,colorSpace:us,depthBuffer:!1},r=yw(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=yw(e,t,i);let{_lodMax:o}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=dF(o)),this._blurMaterial=hF(o,e,t),this._ggxMaterial=fF(o,e,t)}return r}_compileMaterial(e){let t=new Wn(new bn,e);this._renderer.compile(t,Pl)}_sceneToCubeUV(e,t,i,r,o){let c=new En(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,f=d.autoClear,h=d.toneMapping;d.getClearColor(vw),d.toneMapping=Ai,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Wn(new Oa,new fl({name:"PMREM.Background",side:kn,depthWrite:!1,depthTest:!1})));let y=this._backgroundBox,g=y.material,m=!1,E=e.background;E?E.isColor&&(g.color.copy(E),e.background=null,m=!0):(g.color.copy(vw),m=!0);for(let S=0;S<6;S++){let w=S%3;w===0?(c.up.set(0,l[S],0),c.position.set(o.x,o.y,o.z),c.lookAt(o.x+u[S],o.y,o.z)):w===1?(c.up.set(0,0,l[S]),c.position.set(o.x,o.y,o.z),c.lookAt(o.x,o.y+u[S],o.z)):(c.up.set(0,l[S],0),c.position.set(o.x,o.y,o.z),c.lookAt(o.x,o.y,o.z+u[S]));let D=this._cubeSize;Va(r,w*D,S>2?D:0,D,D),d.setRenderTarget(r),m&&d.render(y,c),d.render(e,c)}d.toneMapping=h,d.autoClear=f,e.background=E}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===po||e.mapping===hs;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=xw()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=_w());let o=r?this._cubemapMaterial:this._equirectMaterial,s=this._lodMeshes[0];s.material=o;let a=o.uniforms;a.envMap.value=e;let c=this._cubeSize;Va(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(s,Pl)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let o=1;o<r;o++)this._applyGGXFilter(e,o-1,o);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,o=this._pingPongRenderTarget,s=this._ggxMaterial,a=this._lodMeshes[i];a.material=s;let c=s.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),f=0+l*1.25,h=d*f,{_lodMax:p}=this,y=this._sizeLods[i],g=3*y*(i>p-vo?i-p+vo:0),m=4*(this._cubeSize-y);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=p-t,Va(o,g,m,3*y,2*y),r.setRenderTarget(o),r.render(a,Pl),c.envMap.value=o.texture,c.roughness.value=0,c.mipInt.value=p-i,Va(e,g,m,3*y,2*y),r.setRenderTarget(e),r.render(a,Pl)}_blur(e,t,i,r,o){let s=this._pingPongRenderTarget;this._halfBlur(e,s,t,i,r,"latitudinal",o),this._halfBlur(s,e,i,i,r,"longitudinal",o)}_halfBlur(e,t,i,r,o,s,a){let c=this._renderer,l=this._blurMaterial;s!=="latitudinal"&&s!=="longitudinal"&&Ue("blur direction must be either latitudinal or longitudinal!");let u=3,d=this._lodMeshes[r];d.material=l;let f=l.uniforms,h=this._sizeLods[i]-1,p=isFinite(o)?Math.PI/(2*h):2*Math.PI/(2*ys-1),y=o/p,g=isFinite(o)?1+Math.floor(u*y):ys;g>ys&&Fe(`sigmaRadians, ${o}, is too large and will clip, as it requested ${g} samples when the maximum is set to ${ys}`);let m=[],E=0;for(let I=0;I<ys;++I){let _=I/y,b=Math.exp(-_*_/2);m.push(b),I===0?E+=b:I<g&&(E+=2*b)}for(let I=0;I<m.length;I++)m[I]=m[I]/E;f.envMap.value=e.texture,f.samples.value=g,f.weights.value=m,f.latitudinal.value=s==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:S}=this;f.dTheta.value=p,f.mipInt.value=S-i;let w=this._sizeLods[r],D=3*w*(r>S-vo?r-S+vo:0),C=4*(this._cubeSize-w);Va(t,D,C,3*w,2*w),c.setRenderTarget(t),c.render(d,Pl)}};function dF(n){let e=[],t=[],i=[],r=n,o=n-vo+1+gw.length;for(let s=0;s<o;s++){let a=Math.pow(2,r);e.push(a);let c=1/a;s>n-vo?c=gw[s-n+vo-1]:s===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,d=1+l,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,p=6,y=3,g=2,m=1,E=new Float32Array(y*p*h),S=new Float32Array(g*p*h),w=new Float32Array(m*p*h);for(let C=0;C<h;C++){let I=C%3*2/3-1,_=C>2?0:-1,b=[I,_,0,I+2/3,_,0,I+2/3,_+1,0,I,_,0,I+2/3,_+1,0,I,_+1,0];E.set(b,y*p*C),S.set(f,g*p*C);let W=[C,C,C,C,C,C];w.set(W,m*p*C)}let D=new bn;D.setAttribute("position",new fn(E,y)),D.setAttribute("uv",new fn(S,g)),D.setAttribute("faceIndex",new fn(w,m)),i.push(new Wn(D,null)),r>vo&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function yw(n,e,t){let i=new ti(n,e,t);return i.texture.mapping=Cl,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Va(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function fF(n,e,t){return new ni({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:lF,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:bp(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:Qi,depthTest:!1,depthWrite:!1})}function hF(n,e,t){let i=new Float32Array(ys),r=new L(0,1,0);return new ni({name:"SphericalGaussianBlur",defines:{n:ys,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:bp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Qi,depthTest:!1,depthWrite:!1})}function _w(){return new ni({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:bp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Qi,depthTest:!1,depthWrite:!1})}function xw(){return new ni({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:bp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Qi,depthTest:!1,depthWrite:!1})}function bp(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var xp=class extends ti{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new hl(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Oa(5,5,5),o=new ni({name:"CubemapFromEquirect",uniforms:ms(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:kn,blending:Qi});o.uniforms.tEquirect.value=t;let s=new Wn(r,o),a=t.minFilter;return t.minFilter===mo&&(t.minFilter=hn),new wh(1,10,this).update(e,s),t.minFilter=a,s.geometry.dispose(),s.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let o=e.getRenderTarget();for(let s=0;s<6;s++)e.setRenderTarget(this,s),e.clear(t,i,r);e.setRenderTarget(o)}};function pF(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?s(f):o(f)}function o(f){if(f&&f.isTexture){let h=f.mapping;if(h===Th||h===Dh)if(e.has(f)){let p=e.get(f).texture;return a(p,f.mapping)}else{let p=f.image;if(p&&p.height>0){let y=new xp(p.height);return y.fromEquirectangularTexture(n,f),e.set(f,y),f.addEventListener("dispose",l),a(y.texture,f.mapping)}else return null}}return f}function s(f){if(f&&f.isTexture){let h=f.mapping,p=h===Th||h===Dh,y=h===po||h===hs;if(p||y){let g=t.get(f),m=g!==void 0?g.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==m)return i===null&&(i=new _p(n)),g=p?i.fromEquirectangular(f,g):i.fromCubemap(f,g),g.texture.pmremVersion=f.pmremVersion,t.set(f,g),g.texture;if(g!==void 0)return g.texture;{let E=f.image;return p&&E&&E.height>0||y&&E&&c(E)?(i===null&&(i=new _p(n)),g=p?i.fromEquirectangular(f):i.fromCubemap(f),g.texture.pmremVersion=f.pmremVersion,t.set(f,g),f.addEventListener("dispose",u),g.texture):null}}}return f}function a(f,h){return h===Th?f.mapping=po:h===Dh&&(f.mapping=hs),f}function c(f){let h=0,p=6;for(let y=0;y<p;y++)f[y]!==void 0&&h++;return h===p}function l(f){let h=f.target;h.removeEventListener("dispose",l);let p=e.get(h);p!==void 0&&(e.delete(h),p.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let p=t.get(h);p!==void 0&&(t.delete(h),p.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function mF(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&al("WebGLRenderer: "+i+" extension not supported."),r}}}function gF(n,e,t,i){let r={},o=new WeakMap;function s(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let p in f.attributes)e.remove(f.attributes[p]);f.removeEventListener("dispose",s),delete r[f.id];let h=o.get(f);h&&(e.remove(h),o.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",s),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,p=d.attributes.position,y=0;if(p===void 0)return;if(h!==null){let E=h.array;y=h.version;for(let S=0,w=E.length;S<w;S+=3){let D=E[S+0],C=E[S+1],I=E[S+2];f.push(D,C,C,I,I,D)}}else{let E=p.array;y=p.version;for(let S=0,w=E.length/3-1;S<w;S+=3){let D=S+0,C=S+1,I=S+2;f.push(D,C,C,I,I,D)}}let g=new(p.count>=65535?dl:ul)(f,1);g.version=y;let m=o.get(d);m&&e.remove(m),o.set(d,g)}function u(d){let f=o.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return o.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function vF(n,e,t){let i;function r(f){i=f}let o,s;function a(f){o=f.type,s=f.bytesPerElement}function c(f,h){n.drawElements(i,h,o,f*s),t.update(h,i,1)}function l(f,h,p){p!==0&&(n.drawElementsInstanced(i,h,o,f*s,p),t.update(h,i,p))}function u(f,h,p){if(p===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,o,f,0,p);let g=0;for(let m=0;m<p;m++)g+=h[m];t.update(g,i,1)}function d(f,h,p,y){if(p===0)return;let g=e.get("WEBGL_multi_draw");if(g===null)for(let m=0;m<f.length;m++)l(f[m]/s,h[m],y[m]);else{g.multiDrawElementsInstancedWEBGL(i,h,0,o,f,0,y,0,p);let m=0;for(let E=0;E<p;E++)m+=h[E]*y[E];t.update(m,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function yF(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(o,s,a){switch(t.calls++,s){case n.TRIANGLES:t.triangles+=a*(o/3);break;case n.LINES:t.lines+=a*(o/2);break;case n.LINE_STRIP:t.lines+=a*(o-1);break;case n.LINE_LOOP:t.lines+=a*o;break;case n.POINTS:t.points+=a*o;break;default:Ue("WebGLInfo: Unknown draw mode:",s);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function _F(n,e,t){let i=new WeakMap,r=new Lt;function o(s,a,c){let l=s.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let W=function(){_.dispose(),i.delete(a),a.removeEventListener("dispose",W)};var h=W;f!==void 0&&f.texture.dispose();let p=a.morphAttributes.position!==void 0,y=a.morphAttributes.normal!==void 0,g=a.morphAttributes.color!==void 0,m=a.morphAttributes.position||[],E=a.morphAttributes.normal||[],S=a.morphAttributes.color||[],w=0;p===!0&&(w=1),y===!0&&(w=2),g===!0&&(w=3);let D=a.attributes.position.count*w,C=1;D>e.maxTextureSize&&(C=Math.ceil(D/e.maxTextureSize),D=e.maxTextureSize);let I=new Float32Array(D*C*4*d),_=new cl(I,D,C,d);_.type=Ni,_.needsUpdate=!0;let b=w*4;for(let T=0;T<d;T++){let F=m[T],H=E[T],G=S[T],B=D*C*4*T;for(let U=0;U<F.count;U++){let O=U*b;p===!0&&(r.fromBufferAttribute(F,U),I[B+O+0]=r.x,I[B+O+1]=r.y,I[B+O+2]=r.z,I[B+O+3]=0),y===!0&&(r.fromBufferAttribute(H,U),I[B+O+4]=r.x,I[B+O+5]=r.y,I[B+O+6]=r.z,I[B+O+7]=0),g===!0&&(r.fromBufferAttribute(G,U),I[B+O+8]=r.x,I[B+O+9]=r.y,I[B+O+10]=r.z,I[B+O+11]=G.itemSize===4?r.w:1)}}f={count:d,texture:_,size:new lt(D,C)},i.set(a,f),a.addEventListener("dispose",W)}if(s.isInstancedMesh===!0&&s.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",s.morphTexture,t);else{let p=0;for(let g=0;g<l.length;g++)p+=l[g];let y=a.morphTargetsRelative?1:1-p;c.getUniforms().setValue(n,"morphTargetBaseInfluence",y),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:o}}function xF(n,e,t,i,r){let o=new WeakMap;function s(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(o.get(f)!==u&&(e.update(f),o.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),o.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),o.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;o.get(h)!==u&&(h.update(),o.set(h,u))}return f}function a(){o=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:s,dispose:a}}var EF={[Qy]:"LINEAR_TONE_MAPPING",[e0]:"REINHARD_TONE_MAPPING",[t0]:"CINEON_TONE_MAPPING",[n0]:"ACES_FILMIC_TONE_MAPPING",[r0]:"AGX_TONE_MAPPING",[o0]:"NEUTRAL_TONE_MAPPING",[i0]:"CUSTOM_TONE_MAPPING"};function bF(n,e,t,i,r){let o=new ti(e,t,{type:n,depthBuffer:i,stencilBuffer:r}),s=new ti(e,t,{type:er,depthBuffer:!1,stencilBuffer:!1}),a=new bn;a.setAttribute("position",new Xt([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new Xt([0,2,0,0,2,0],2));let c=new hh({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new Wn(a,c),u=new La(-1,1,1,-1,0,1),d=null,f=null,h=!1,p,y=null,g=[],m=!1;this.setSize=function(E,S){o.setSize(E,S),s.setSize(E,S);for(let w=0;w<g.length;w++){let D=g[w];D.setSize&&D.setSize(E,S)}},this.setEffects=function(E){g=E,m=g.length>0&&g[0].isRenderPass===!0;let S=o.width,w=o.height;for(let D=0;D<g.length;D++){let C=g[D];C.setSize&&C.setSize(S,w)}},this.begin=function(E,S){if(h||E.toneMapping===Ai&&g.length===0)return!1;if(y=S,S!==null){let w=S.width,D=S.height;(o.width!==w||o.height!==D)&&this.setSize(w,D)}return m===!1&&E.setRenderTarget(o),p=E.toneMapping,E.toneMapping=Ai,!0},this.hasRenderPass=function(){return m},this.end=function(E,S){E.toneMapping=p,h=!0;let w=o,D=s;for(let C=0;C<g.length;C++){let I=g[C];if(I.enabled!==!1&&(I.render(E,D,w,S),I.needsSwap!==!1)){let _=w;w=D,D=_}}if(d!==E.outputColorSpace||f!==E.toneMapping){d=E.outputColorSpace,f=E.toneMapping,c.defines={},ct.getTransfer(d)===xt&&(c.defines.SRGB_TRANSFER="");let C=EF[f];C&&(c.defines[C]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=w.texture,E.setRenderTarget(y),E.render(l,u),y=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){o.dispose(),s.dispose(),a.dispose(),c.dispose()}}var Bw=new Ar,C0=new lo(1,1),Vw=new cl,Hw=new ah,zw=new hl,Ew=[],bw=[],Mw=new Float32Array(16),Sw=new Float32Array(9),ww=new Float32Array(4);function za(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,o=Ew[r];if(o===void 0&&(o=new Float32Array(r),Ew[r]=o),e!==0){i.toArray(o,0);for(let s=1,a=0;s!==e;++s)a+=t,n[s].toArray(o,a)}return o}function Kt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Qt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Mp(n,e){let t=bw[e];t===void 0&&(t=new Int32Array(e),bw[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function MF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function SF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2fv(this.addr,e),Qt(t,e)}}function wF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Kt(t,e))return;n.uniform3fv(this.addr,e),Qt(t,e)}}function CF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4fv(this.addr,e),Qt(t,e)}}function TF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Qt(t,e)}else{if(Kt(t,i))return;ww.set(i),n.uniformMatrix2fv(this.addr,!1,ww),Qt(t,i)}}function DF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Qt(t,e)}else{if(Kt(t,i))return;Sw.set(i),n.uniformMatrix3fv(this.addr,!1,Sw),Qt(t,i)}}function IF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Qt(t,e)}else{if(Kt(t,i))return;Mw.set(i),n.uniformMatrix4fv(this.addr,!1,Mw),Qt(t,i)}}function AF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function RF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2iv(this.addr,e),Qt(t,e)}}function NF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Kt(t,e))return;n.uniform3iv(this.addr,e),Qt(t,e)}}function PF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4iv(this.addr,e),Qt(t,e)}}function OF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function LF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2uiv(this.addr,e),Qt(t,e)}}function FF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Kt(t,e))return;n.uniform3uiv(this.addr,e),Qt(t,e)}}function kF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4uiv(this.addr,e),Qt(t,e)}}function UF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let o;this.type===n.SAMPLER_2D_SHADOW?(C0.compareFunction=t.isReversedDepthBuffer()?mp:pp,o=C0):o=Bw,t.setTexture2D(e||o,r)}function BF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||Hw,r)}function VF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||zw,r)}function HF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||Vw,r)}function zF(n){switch(n){case 5126:return MF;case 35664:return SF;case 35665:return wF;case 35666:return CF;case 35674:return TF;case 35675:return DF;case 35676:return IF;case 5124:case 35670:return AF;case 35667:case 35671:return RF;case 35668:case 35672:return NF;case 35669:case 35673:return PF;case 5125:return OF;case 36294:return LF;case 36295:return FF;case 36296:return kF;case 35678:case 36198:case 36298:case 36306:case 35682:return UF;case 35679:case 36299:case 36307:return BF;case 35680:case 36300:case 36308:case 36293:return VF;case 36289:case 36303:case 36311:case 36292:return HF}}function GF(n,e){n.uniform1fv(this.addr,e)}function jF(n,e){let t=za(e,this.size,2);n.uniform2fv(this.addr,t)}function WF(n,e){let t=za(e,this.size,3);n.uniform3fv(this.addr,t)}function $F(n,e){let t=za(e,this.size,4);n.uniform4fv(this.addr,t)}function qF(n,e){let t=za(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function XF(n,e){let t=za(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function YF(n,e){let t=za(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function ZF(n,e){n.uniform1iv(this.addr,e)}function JF(n,e){n.uniform2iv(this.addr,e)}function KF(n,e){n.uniform3iv(this.addr,e)}function QF(n,e){n.uniform4iv(this.addr,e)}function e2(n,e){n.uniform1uiv(this.addr,e)}function t2(n,e){n.uniform2uiv(this.addr,e)}function n2(n,e){n.uniform3uiv(this.addr,e)}function i2(n,e){n.uniform4uiv(this.addr,e)}function r2(n,e,t){let i=this.cache,r=e.length,o=Mp(t,r);Kt(i,o)||(n.uniform1iv(this.addr,o),Qt(i,o));let s;this.type===n.SAMPLER_2D_SHADOW?s=C0:s=Bw;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||s,o[a])}function o2(n,e,t){let i=this.cache,r=e.length,o=Mp(t,r);Kt(i,o)||(n.uniform1iv(this.addr,o),Qt(i,o));for(let s=0;s!==r;++s)t.setTexture3D(e[s]||Hw,o[s])}function s2(n,e,t){let i=this.cache,r=e.length,o=Mp(t,r);Kt(i,o)||(n.uniform1iv(this.addr,o),Qt(i,o));for(let s=0;s!==r;++s)t.setTextureCube(e[s]||zw,o[s])}function a2(n,e,t){let i=this.cache,r=e.length,o=Mp(t,r);Kt(i,o)||(n.uniform1iv(this.addr,o),Qt(i,o));for(let s=0;s!==r;++s)t.setTexture2DArray(e[s]||Vw,o[s])}function c2(n){switch(n){case 5126:return GF;case 35664:return jF;case 35665:return WF;case 35666:return $F;case 35674:return qF;case 35675:return XF;case 35676:return YF;case 5124:case 35670:return ZF;case 35667:case 35671:return JF;case 35668:case 35672:return KF;case 35669:case 35673:return QF;case 5125:return e2;case 36294:return t2;case 36295:return n2;case 36296:return i2;case 35678:case 36198:case 36298:case 36306:case 35682:return r2;case 35679:case 36299:case 36307:return o2;case 35680:case 36300:case 36308:case 36293:return s2;case 36289:case 36303:case 36311:case 36292:return a2}}var T0=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=zF(t.type)}},D0=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=c2(t.type)}},I0=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let o=0,s=r.length;o!==s;++o){let a=r[o];a.setValue(e,t[a.id],i)}}},S0=/(\w+)(\])?(\[|\.)?/g;function Cw(n,e){n.seq.push(e),n.map[e.id]=e}function l2(n,e,t){let i=n.name,r=i.length;for(S0.lastIndex=0;;){let o=S0.exec(i),s=S0.lastIndex,a=o[1],c=o[2]==="]",l=o[3];if(c&&(a=a|0),l===void 0||l==="["&&s+2===r){Cw(t,l===void 0?new T0(a,n,e):new D0(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new I0(a),Cw(t,d)),t=d}}}var Ha=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let s=0;s<i;++s){let a=e.getActiveUniform(t,s),c=e.getUniformLocation(t,a.name);l2(a,c,this)}let r=[],o=[];for(let s of this.seq)s.type===e.SAMPLER_2D_SHADOW||s.type===e.SAMPLER_CUBE_SHADOW||s.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(s):o.push(s);r.length>0&&(this.seq=r.concat(o))}setValue(e,t,i,r){let o=this.map[t];o!==void 0&&o.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let o=0,s=t.length;o!==s;++o){let a=t[o],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,o=e.length;r!==o;++r){let s=e[r];s.id in t&&i.push(s)}return i}};function Tw(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var u2=37297,d2=0;function f2(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),o=Math.min(e+6,t.length);for(let s=r;s<o;s++){let a=s+1;i.push(`${a===e?">":" "} ${a}: ${t[s]}`)}return i.join(`
`)}var Dw=new Ye;function h2(n){ct._getMatrix(Dw,ct.workingColorSpace,n);let e=`mat3( ${Dw.elements.map(t=>t.toFixed(4))} )`;switch(ct.getTransfer(n)){case ol:return[e,"LinearTransferOETF"];case xt:return[e,"sRGBTransferOETF"];default:return Fe("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function Iw(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),o=(n.getShaderInfoLog(e)||"").trim();if(i&&o==="")return"";let s=/ERROR: 0:(\d+)/.exec(o);if(s){let a=parseInt(s[1]);return t.toUpperCase()+`

`+o+`

`+f2(n.getShaderSource(e),a)}else return o}function p2(n,e){let t=h2(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var m2={[Qy]:"Linear",[e0]:"Reinhard",[t0]:"Cineon",[n0]:"ACESFilmic",[r0]:"AgX",[o0]:"Neutral",[i0]:"Custom"};function g2(n,e){let t=m2[e];return t===void 0?(Fe("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var yp=new L;function v2(){ct.getLuminanceCoefficients(yp);let n=yp.x.toFixed(4),e=yp.y.toFixed(4),t=yp.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function y2(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Ll).join(`
`)}function _2(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function x2(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let o=n.getActiveAttrib(e,r),s=o.name,a=1;o.type===n.FLOAT_MAT2&&(a=2),o.type===n.FLOAT_MAT3&&(a=3),o.type===n.FLOAT_MAT4&&(a=4),t[s]={type:o.type,location:n.getAttribLocation(e,s),locationSize:a}}return t}function Ll(n){return n!==""}function Aw(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function Rw(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var E2=/^[ \t]*#include +<([\w\d./]+)>/gm;function A0(n){return n.replace(E2,M2)}var b2=new Map;function M2(n,e){let t=Ke[e];if(t===void 0){let i=b2.get(e);if(i!==void 0)t=Ke[i],Fe('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return A0(t)}var S2=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function Nw(n){return n.replace(S2,w2)}function w2(n,e,t,i){let r="";for(let o=parseInt(e);o<parseInt(t);o++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+o+" ]").replace(/UNROLLED_LOOP_INDEX/g,o);return r}function Pw(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var C2={[wl]:"SHADOWMAP_TYPE_PCF",[ka]:"SHADOWMAP_TYPE_VSM"};function T2(n){return C2[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var D2={[po]:"ENVMAP_TYPE_CUBE",[hs]:"ENVMAP_TYPE_CUBE",[Cl]:"ENVMAP_TYPE_CUBE_UV"};function I2(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":D2[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var A2={[hs]:"ENVMAP_MODE_REFRACTION"};function R2(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":A2[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var N2={[Ky]:"ENVMAP_BLENDING_MULTIPLY",[KS]:"ENVMAP_BLENDING_MIX",[QS]:"ENVMAP_BLENDING_ADD"};function P2(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":N2[n.combine]||"ENVMAP_BLENDING_NONE"}function O2(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function L2(n,e,t,i){let r=n.getContext(),o=t.defines,s=t.vertexShader,a=t.fragmentShader,c=T2(t),l=I2(t),u=R2(t),d=P2(t),f=O2(t),h=y2(t),p=_2(o),y=r.createProgram(),g,m,E=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(g=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,p].filter(Ll).join(`
`),g.length>0&&(g+=`
`),m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,p].filter(Ll).join(`
`),m.length>0&&(m+=`
`)):(g=[Pw(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,p,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Ll).join(`
`),m=[Pw(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,p,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Ai?"#define TONE_MAPPING":"",t.toneMapping!==Ai?Ke.tonemapping_pars_fragment:"",t.toneMapping!==Ai?g2("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Ke.colorspace_pars_fragment,p2("linearToOutputTexel",t.outputColorSpace),v2(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Ll).join(`
`)),s=A0(s),s=Aw(s,t),s=Rw(s,t),a=A0(a),a=Aw(a,t),a=Rw(a,t),s=Nw(s),a=Nw(a),t.isRawShaderMaterial!==!0&&(E=`#version 300 es
`,g=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+g,m=["#define varying in",t.glslVersion===p0?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===p0?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+m);let S=E+g+s,w=E+m+a,D=Tw(r,r.VERTEX_SHADER,S),C=Tw(r,r.FRAGMENT_SHADER,w);r.attachShader(y,D),r.attachShader(y,C),t.index0AttributeName!==void 0?r.bindAttribLocation(y,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(y,0,"position"),r.linkProgram(y);function I(T){if(n.debug.checkShaderErrors){let F=r.getProgramInfoLog(y)||"",H=r.getShaderInfoLog(D)||"",G=r.getShaderInfoLog(C)||"",B=F.trim(),U=H.trim(),O=G.trim(),Z=!0,K=!0;if(r.getProgramParameter(y,r.LINK_STATUS)===!1)if(Z=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,y,D,C);else{let he=Iw(r,D,"vertex"),_e=Iw(r,C,"fragment");Ue("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(y,r.VALIDATE_STATUS)+`

Material Name: `+T.name+`
Material Type: `+T.type+`

Program Info Log: `+B+`
`+he+`
`+_e)}else B!==""?Fe("WebGLProgram: Program Info Log:",B):(U===""||O==="")&&(K=!1);K&&(T.diagnostics={runnable:Z,programLog:B,vertexShader:{log:U,prefix:g},fragmentShader:{log:O,prefix:m}})}r.deleteShader(D),r.deleteShader(C),_=new Ha(r,y),b=x2(r,y)}let _;this.getUniforms=function(){return _===void 0&&I(this),_};let b;this.getAttributes=function(){return b===void 0&&I(this),b};let W=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return W===!1&&(W=r.getProgramParameter(y,u2)),W},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(y),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=d2++,this.cacheKey=e,this.usedTimes=1,this.program=y,this.vertexShader=D,this.fragmentShader=C,this}var F2=0,R0=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),o=this._getShaderStage(i),s=this._getShaderCacheForMaterial(e);return s.has(r)===!1&&(s.add(r),r.usedTimes++),s.has(o)===!1&&(s.add(o),o.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new N0(e),t.set(e,i)),i}},N0=class{constructor(e){this.id=F2++,this.code=e,this.usedTimes=0}};function k2(n,e,t,i,r,o){let s=new Ia,a=new R0,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function p(_){return c.add(_),_===0?"uv":`uv${_}`}function y(_,b,W,T,F){let H=T.fog,G=F.geometry,B=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?T.environment:null,U=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap,O=e.get(_.envMap||B,U),Z=O&&O.mapping===Cl?O.image.height:null,K=h[_.type];_.precision!==null&&(f=i.getMaxPrecision(_.precision),f!==_.precision&&Fe("WebGLProgram.getParameters:",_.precision,"not supported, using",f,"instead."));let he=G.morphAttributes.position||G.morphAttributes.normal||G.morphAttributes.color,_e=he!==void 0?he.length:0,ge=0;G.morphAttributes.position!==void 0&&(ge=1),G.morphAttributes.normal!==void 0&&(ge=2),G.morphAttributes.color!==void 0&&(ge=3);let Je,Pt,Nt,X;if(K){let bt=nr[K];Je=bt.vertexShader,Pt=bt.fragmentShader}else Je=_.vertexShader,Pt=_.fragmentShader,a.update(_),Nt=a.getVertexShaderID(_),X=a.getFragmentShaderID(_);let re=n.getRenderTarget(),ce=n.state.buffers.depth.getReversed(),Ze=F.isInstancedMesh===!0,ke=F.isBatchedMesh===!0,He=!!_.map,en=!!_.matcap,ut=!!O,Et=!!_.aoMap,Dt=!!_.lightMap,Qe=!!_.bumpMap,Bt=!!_.normalMap,A=!!_.displacementMap,jt=!!_.emissiveMap,gt=!!_.metalnessMap,At=!!_.roughnessMap,we=_.anisotropy>0,M=_.clearcoat>0,v=_.dispersion>0,N=_.iridescence>0,q=_.sheen>0,Y=_.transmission>0,$=we&&!!_.anisotropyMap,xe=M&&!!_.clearcoatMap,oe=M&&!!_.clearcoatNormalMap,Re=M&&!!_.clearcoatRoughnessMap,Be=N&&!!_.iridescenceMap,Q=N&&!!_.iridescenceThicknessMap,ne=q&&!!_.sheenColorMap,Ee=q&&!!_.sheenRoughnessMap,Me=!!_.specularMap,pe=!!_.specularColorMap,et=!!_.specularIntensityMap,R=Y&&!!_.transmissionMap,se=Y&&!!_.thicknessMap,ie=!!_.gradientMap,ye=!!_.alphaMap,ee=_.alphaTest>0,j=!!_.alphaHash,be=!!_.extensions,ze=Ai;_.toneMapped&&(re===null||re.isXRRenderTarget===!0)&&(ze=n.toneMapping);let Rt={shaderID:K,shaderType:_.type,shaderName:_.name,vertexShader:Je,fragmentShader:Pt,defines:_.defines,customVertexShaderID:Nt,customFragmentShaderID:X,isRawShaderMaterial:_.isRawShaderMaterial===!0,glslVersion:_.glslVersion,precision:f,batching:ke,batchingColor:ke&&F._colorsTexture!==null,instancing:Ze,instancingColor:Ze&&F.instanceColor!==null,instancingMorph:Ze&&F.morphTexture!==null,outputColorSpace:re===null?n.outputColorSpace:re.isXRRenderTarget===!0?re.texture.colorSpace:us,alphaToCoverage:!!_.alphaToCoverage,map:He,matcap:en,envMap:ut,envMapMode:ut&&O.mapping,envMapCubeUVHeight:Z,aoMap:Et,lightMap:Dt,bumpMap:Qe,normalMap:Bt,displacementMap:A,emissiveMap:jt,normalMapObjectSpace:Bt&&_.normalMapType===nw,normalMapTangentSpace:Bt&&_.normalMapType===h0,metalnessMap:gt,roughnessMap:At,anisotropy:we,anisotropyMap:$,clearcoat:M,clearcoatMap:xe,clearcoatNormalMap:oe,clearcoatRoughnessMap:Re,dispersion:v,iridescence:N,iridescenceMap:Be,iridescenceThicknessMap:Q,sheen:q,sheenColorMap:ne,sheenRoughnessMap:Ee,specularMap:Me,specularColorMap:pe,specularIntensityMap:et,transmission:Y,transmissionMap:R,thicknessMap:se,gradientMap:ie,opaque:_.transparent===!1&&_.blending===cs&&_.alphaToCoverage===!1,alphaMap:ye,alphaTest:ee,alphaHash:j,combine:_.combine,mapUv:He&&p(_.map.channel),aoMapUv:Et&&p(_.aoMap.channel),lightMapUv:Dt&&p(_.lightMap.channel),bumpMapUv:Qe&&p(_.bumpMap.channel),normalMapUv:Bt&&p(_.normalMap.channel),displacementMapUv:A&&p(_.displacementMap.channel),emissiveMapUv:jt&&p(_.emissiveMap.channel),metalnessMapUv:gt&&p(_.metalnessMap.channel),roughnessMapUv:At&&p(_.roughnessMap.channel),anisotropyMapUv:$&&p(_.anisotropyMap.channel),clearcoatMapUv:xe&&p(_.clearcoatMap.channel),clearcoatNormalMapUv:oe&&p(_.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:Re&&p(_.clearcoatRoughnessMap.channel),iridescenceMapUv:Be&&p(_.iridescenceMap.channel),iridescenceThicknessMapUv:Q&&p(_.iridescenceThicknessMap.channel),sheenColorMapUv:ne&&p(_.sheenColorMap.channel),sheenRoughnessMapUv:Ee&&p(_.sheenRoughnessMap.channel),specularMapUv:Me&&p(_.specularMap.channel),specularColorMapUv:pe&&p(_.specularColorMap.channel),specularIntensityMapUv:et&&p(_.specularIntensityMap.channel),transmissionMapUv:R&&p(_.transmissionMap.channel),thicknessMapUv:se&&p(_.thicknessMap.channel),alphaMapUv:ye&&p(_.alphaMap.channel),vertexTangents:!!G.attributes.tangent&&(Bt||we),vertexColors:_.vertexColors,vertexAlphas:_.vertexColors===!0&&!!G.attributes.color&&G.attributes.color.itemSize===4,pointsUvs:F.isPoints===!0&&!!G.attributes.uv&&(He||ye),fog:!!H,useFog:_.fog===!0,fogExp2:!!H&&H.isFogExp2,flatShading:_.wireframe===!1&&(_.flatShading===!0||G.attributes.normal===void 0&&Bt===!1&&(_.isMeshLambertMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isMeshPhysicalMaterial)),sizeAttenuation:_.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:ce,skinning:F.isSkinnedMesh===!0,morphTargets:G.morphAttributes.position!==void 0,morphNormals:G.morphAttributes.normal!==void 0,morphColors:G.morphAttributes.color!==void 0,morphTargetsCount:_e,morphTextureStride:ge,numDirLights:b.directional.length,numPointLights:b.point.length,numSpotLights:b.spot.length,numSpotLightMaps:b.spotLightMap.length,numRectAreaLights:b.rectArea.length,numHemiLights:b.hemi.length,numDirLightShadows:b.directionalShadowMap.length,numPointLightShadows:b.pointShadowMap.length,numSpotLightShadows:b.spotShadowMap.length,numSpotLightShadowsWithMaps:b.numSpotLightShadowsWithMaps,numLightProbes:b.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:_.dithering,shadowMapEnabled:n.shadowMap.enabled&&W.length>0,shadowMapType:n.shadowMap.type,toneMapping:ze,decodeVideoTexture:He&&_.map.isVideoTexture===!0&&ct.getTransfer(_.map.colorSpace)===xt,decodeVideoTextureEmissive:jt&&_.emissiveMap.isVideoTexture===!0&&ct.getTransfer(_.emissiveMap.colorSpace)===xt,premultipliedAlpha:_.premultipliedAlpha,doubleSided:_.side===Ki,flipSided:_.side===kn,useDepthPacking:_.depthPacking>=0,depthPacking:_.depthPacking||0,index0AttributeName:_.index0AttributeName,extensionClipCullDistance:be&&_.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(be&&_.extensions.multiDraw===!0||ke)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:_.customProgramCacheKey()};return Rt.vertexUv1s=c.has(1),Rt.vertexUv2s=c.has(2),Rt.vertexUv3s=c.has(3),c.clear(),Rt}function g(_){let b=[];if(_.shaderID?b.push(_.shaderID):(b.push(_.customVertexShaderID),b.push(_.customFragmentShaderID)),_.defines!==void 0)for(let W in _.defines)b.push(W),b.push(_.defines[W]);return _.isRawShaderMaterial===!1&&(m(b,_),E(b,_),b.push(n.outputColorSpace)),b.push(_.customProgramCacheKey),b.join()}function m(_,b){_.push(b.precision),_.push(b.outputColorSpace),_.push(b.envMapMode),_.push(b.envMapCubeUVHeight),_.push(b.mapUv),_.push(b.alphaMapUv),_.push(b.lightMapUv),_.push(b.aoMapUv),_.push(b.bumpMapUv),_.push(b.normalMapUv),_.push(b.displacementMapUv),_.push(b.emissiveMapUv),_.push(b.metalnessMapUv),_.push(b.roughnessMapUv),_.push(b.anisotropyMapUv),_.push(b.clearcoatMapUv),_.push(b.clearcoatNormalMapUv),_.push(b.clearcoatRoughnessMapUv),_.push(b.iridescenceMapUv),_.push(b.iridescenceThicknessMapUv),_.push(b.sheenColorMapUv),_.push(b.sheenRoughnessMapUv),_.push(b.specularMapUv),_.push(b.specularColorMapUv),_.push(b.specularIntensityMapUv),_.push(b.transmissionMapUv),_.push(b.thicknessMapUv),_.push(b.combine),_.push(b.fogExp2),_.push(b.sizeAttenuation),_.push(b.morphTargetsCount),_.push(b.morphAttributeCount),_.push(b.numDirLights),_.push(b.numPointLights),_.push(b.numSpotLights),_.push(b.numSpotLightMaps),_.push(b.numHemiLights),_.push(b.numRectAreaLights),_.push(b.numDirLightShadows),_.push(b.numPointLightShadows),_.push(b.numSpotLightShadows),_.push(b.numSpotLightShadowsWithMaps),_.push(b.numLightProbes),_.push(b.shadowMapType),_.push(b.toneMapping),_.push(b.numClippingPlanes),_.push(b.numClipIntersection),_.push(b.depthPacking)}function E(_,b){s.disableAll(),b.instancing&&s.enable(0),b.instancingColor&&s.enable(1),b.instancingMorph&&s.enable(2),b.matcap&&s.enable(3),b.envMap&&s.enable(4),b.normalMapObjectSpace&&s.enable(5),b.normalMapTangentSpace&&s.enable(6),b.clearcoat&&s.enable(7),b.iridescence&&s.enable(8),b.alphaTest&&s.enable(9),b.vertexColors&&s.enable(10),b.vertexAlphas&&s.enable(11),b.vertexUv1s&&s.enable(12),b.vertexUv2s&&s.enable(13),b.vertexUv3s&&s.enable(14),b.vertexTangents&&s.enable(15),b.anisotropy&&s.enable(16),b.alphaHash&&s.enable(17),b.batching&&s.enable(18),b.dispersion&&s.enable(19),b.batchingColor&&s.enable(20),b.gradientMap&&s.enable(21),_.push(s.mask),s.disableAll(),b.fog&&s.enable(0),b.useFog&&s.enable(1),b.flatShading&&s.enable(2),b.logarithmicDepthBuffer&&s.enable(3),b.reversedDepthBuffer&&s.enable(4),b.skinning&&s.enable(5),b.morphTargets&&s.enable(6),b.morphNormals&&s.enable(7),b.morphColors&&s.enable(8),b.premultipliedAlpha&&s.enable(9),b.shadowMapEnabled&&s.enable(10),b.doubleSided&&s.enable(11),b.flipSided&&s.enable(12),b.useDepthPacking&&s.enable(13),b.dithering&&s.enable(14),b.transmission&&s.enable(15),b.sheen&&s.enable(16),b.opaque&&s.enable(17),b.pointsUvs&&s.enable(18),b.decodeVideoTexture&&s.enable(19),b.decodeVideoTextureEmissive&&s.enable(20),b.alphaToCoverage&&s.enable(21),_.push(s.mask)}function S(_){let b=h[_.type],W;if(b){let T=nr[b];W=pw.clone(T.uniforms)}else W=_.uniforms;return W}function w(_,b){let W=u.get(b);return W!==void 0?++W.usedTimes:(W=new L2(n,b,_,r),l.push(W),u.set(b,W)),W}function D(_){if(--_.usedTimes===0){let b=l.indexOf(_);l[b]=l[l.length-1],l.pop(),u.delete(_.cacheKey),_.destroy()}}function C(_){a.remove(_)}function I(){a.dispose()}return{getParameters:y,getProgramCacheKey:g,getUniforms:S,acquireProgram:w,releaseProgram:D,releaseShaderCache:C,programs:l,dispose:I}}function U2(){let n=new WeakMap;function e(s){return n.has(s)}function t(s){let a=n.get(s);return a===void 0&&(a={},n.set(s,a)),a}function i(s){n.delete(s)}function r(s,a,c){n.get(s)[a]=c}function o(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:o}}function B2(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function Ow(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function Lw(){let n=[],e=0,t=[],i=[],r=[];function o(){e=0,t.length=0,i.length=0,r.length=0}function s(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,p,y,g,m){let E=n[e];return E===void 0?(E={id:f.id,object:f,geometry:h,material:p,materialVariant:s(f),groupOrder:y,renderOrder:f.renderOrder,z:g,group:m},n[e]=E):(E.id=f.id,E.object=f,E.geometry=h,E.material=p,E.materialVariant=s(f),E.groupOrder=y,E.renderOrder=f.renderOrder,E.z=g,E.group=m),e++,E}function c(f,h,p,y,g,m){let E=a(f,h,p,y,g,m);p.transmission>0?i.push(E):p.transparent===!0?r.push(E):t.push(E)}function l(f,h,p,y,g,m){let E=a(f,h,p,y,g,m);p.transmission>0?i.unshift(E):p.transparent===!0?r.unshift(E):t.unshift(E)}function u(f,h){t.length>1&&t.sort(f||B2),i.length>1&&i.sort(h||Ow),r.length>1&&r.sort(h||Ow)}function d(){for(let f=e,h=n.length;f<h;f++){let p=n[f];if(p.id===null)break;p.id=null,p.object=null,p.geometry=null,p.material=null,p.group=null}}return{opaque:t,transmissive:i,transparent:r,init:o,push:c,unshift:l,finish:d,sort:u}}function V2(){let n=new WeakMap;function e(i,r){let o=n.get(i),s;return o===void 0?(s=new Lw,n.set(i,[s])):r>=o.length?(s=new Lw,o.push(s)):s=o[r],s}function t(){n=new WeakMap}return{get:e,dispose:t}}function H2(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new L,color:new nt};break;case"SpotLight":t={position:new L,direction:new L,color:new nt,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new L,color:new nt,distance:0,decay:0};break;case"HemisphereLight":t={direction:new L,skyColor:new nt,groundColor:new nt};break;case"RectAreaLight":t={color:new nt,position:new L,halfWidth:new L,halfHeight:new L};break}return n[e.id]=t,t}}}function z2(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new lt};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new lt};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new lt,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var G2=0;function j2(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function W2(n){let e=new H2,t=z2(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new L);let r=new L,o=new Ot,s=new Ot;function a(l){let u=0,d=0,f=0;for(let b=0;b<9;b++)i.probe[b].set(0,0,0);let h=0,p=0,y=0,g=0,m=0,E=0,S=0,w=0,D=0,C=0,I=0;l.sort(j2);for(let b=0,W=l.length;b<W;b++){let T=l[b],F=T.color,H=T.intensity,G=T.distance,B=null;if(T.shadow&&T.shadow.map&&(T.shadow.map.texture.format===ps?B=T.shadow.map.texture:B=T.shadow.map.depthTexture||T.shadow.map.texture),T.isAmbientLight)u+=F.r*H,d+=F.g*H,f+=F.b*H;else if(T.isLightProbe){for(let U=0;U<9;U++)i.probe[U].addScaledVector(T.sh.coefficients[U],H);I++}else if(T.isDirectionalLight){let U=e.get(T);if(U.color.copy(T.color).multiplyScalar(T.intensity),T.castShadow){let O=T.shadow,Z=t.get(T);Z.shadowIntensity=O.intensity,Z.shadowBias=O.bias,Z.shadowNormalBias=O.normalBias,Z.shadowRadius=O.radius,Z.shadowMapSize=O.mapSize,i.directionalShadow[h]=Z,i.directionalShadowMap[h]=B,i.directionalShadowMatrix[h]=T.shadow.matrix,E++}i.directional[h]=U,h++}else if(T.isSpotLight){let U=e.get(T);U.position.setFromMatrixPosition(T.matrixWorld),U.color.copy(F).multiplyScalar(H),U.distance=G,U.coneCos=Math.cos(T.angle),U.penumbraCos=Math.cos(T.angle*(1-T.penumbra)),U.decay=T.decay,i.spot[y]=U;let O=T.shadow;if(T.map&&(i.spotLightMap[D]=T.map,D++,O.updateMatrices(T),T.castShadow&&C++),i.spotLightMatrix[y]=O.matrix,T.castShadow){let Z=t.get(T);Z.shadowIntensity=O.intensity,Z.shadowBias=O.bias,Z.shadowNormalBias=O.normalBias,Z.shadowRadius=O.radius,Z.shadowMapSize=O.mapSize,i.spotShadow[y]=Z,i.spotShadowMap[y]=B,w++}y++}else if(T.isRectAreaLight){let U=e.get(T);U.color.copy(F).multiplyScalar(H),U.halfWidth.set(T.width*.5,0,0),U.halfHeight.set(0,T.height*.5,0),i.rectArea[g]=U,g++}else if(T.isPointLight){let U=e.get(T);if(U.color.copy(T.color).multiplyScalar(T.intensity),U.distance=T.distance,U.decay=T.decay,T.castShadow){let O=T.shadow,Z=t.get(T);Z.shadowIntensity=O.intensity,Z.shadowBias=O.bias,Z.shadowNormalBias=O.normalBias,Z.shadowRadius=O.radius,Z.shadowMapSize=O.mapSize,Z.shadowCameraNear=O.camera.near,Z.shadowCameraFar=O.camera.far,i.pointShadow[p]=Z,i.pointShadowMap[p]=B,i.pointShadowMatrix[p]=T.shadow.matrix,S++}i.point[p]=U,p++}else if(T.isHemisphereLight){let U=e.get(T);U.skyColor.copy(T.color).multiplyScalar(H),U.groundColor.copy(T.groundColor).multiplyScalar(H),i.hemi[m]=U,m++}}g>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ue.LTC_FLOAT_1,i.rectAreaLTC2=ue.LTC_FLOAT_2):(i.rectAreaLTC1=ue.LTC_HALF_1,i.rectAreaLTC2=ue.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let _=i.hash;(_.directionalLength!==h||_.pointLength!==p||_.spotLength!==y||_.rectAreaLength!==g||_.hemiLength!==m||_.numDirectionalShadows!==E||_.numPointShadows!==S||_.numSpotShadows!==w||_.numSpotMaps!==D||_.numLightProbes!==I)&&(i.directional.length=h,i.spot.length=y,i.rectArea.length=g,i.point.length=p,i.hemi.length=m,i.directionalShadow.length=E,i.directionalShadowMap.length=E,i.pointShadow.length=S,i.pointShadowMap.length=S,i.spotShadow.length=w,i.spotShadowMap.length=w,i.directionalShadowMatrix.length=E,i.pointShadowMatrix.length=S,i.spotLightMatrix.length=w+D-C,i.spotLightMap.length=D,i.numSpotLightShadowsWithMaps=C,i.numLightProbes=I,_.directionalLength=h,_.pointLength=p,_.spotLength=y,_.rectAreaLength=g,_.hemiLength=m,_.numDirectionalShadows=E,_.numPointShadows=S,_.numSpotShadows=w,_.numSpotMaps=D,_.numLightProbes=I,i.version=G2++)}function c(l,u){let d=0,f=0,h=0,p=0,y=0,g=u.matrixWorldInverse;for(let m=0,E=l.length;m<E;m++){let S=l[m];if(S.isDirectionalLight){let w=i.directional[d];w.direction.setFromMatrixPosition(S.matrixWorld),r.setFromMatrixPosition(S.target.matrixWorld),w.direction.sub(r),w.direction.transformDirection(g),d++}else if(S.isSpotLight){let w=i.spot[h];w.position.setFromMatrixPosition(S.matrixWorld),w.position.applyMatrix4(g),w.direction.setFromMatrixPosition(S.matrixWorld),r.setFromMatrixPosition(S.target.matrixWorld),w.direction.sub(r),w.direction.transformDirection(g),h++}else if(S.isRectAreaLight){let w=i.rectArea[p];w.position.setFromMatrixPosition(S.matrixWorld),w.position.applyMatrix4(g),s.identity(),o.copy(S.matrixWorld),o.premultiply(g),s.extractRotation(o),w.halfWidth.set(S.width*.5,0,0),w.halfHeight.set(0,S.height*.5,0),w.halfWidth.applyMatrix4(s),w.halfHeight.applyMatrix4(s),p++}else if(S.isPointLight){let w=i.point[f];w.position.setFromMatrixPosition(S.matrixWorld),w.position.applyMatrix4(g),f++}else if(S.isHemisphereLight){let w=i.hemi[y];w.direction.setFromMatrixPosition(S.matrixWorld),w.direction.transformDirection(g),y++}}}return{setup:a,setupView:c,state:i}}function Fw(n){let e=new W2(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function o(u){t.push(u)}function s(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:o,pushShadow:s}}function $2(n){let e=new WeakMap;function t(r,o=0){let s=e.get(r),a;return s===void 0?(a=new Fw(n),e.set(r,[a])):o>=s.length?(a=new Fw(n),s.push(a)):a=s[o],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var q2=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,X2=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,Y2=[new L(1,0,0),new L(-1,0,0),new L(0,1,0),new L(0,-1,0),new L(0,0,1),new L(0,0,-1)],Z2=[new L(0,-1,0),new L(0,-1,0),new L(0,0,1),new L(0,0,-1),new L(0,-1,0),new L(0,-1,0)],kw=new Ot,Ol=new L,w0=new L;function J2(n,e,t){let i=new Na,r=new lt,o=new lt,s=new Lt,a=new ph,c=new mh,l={},u=t.maxTextureSize,d={[Cr]:kn,[kn]:Cr,[Ki]:Ki},f=new ni({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new lt},radius:{value:4}},vertexShader:q2,fragmentShader:X2}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let p=new bn;p.setAttribute("position",new fn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let y=new Wn(p,f),g=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=wl;let m=this.type;this.render=function(C,I,_){if(g.enabled===!1||g.autoUpdate===!1&&g.needsUpdate===!1||C.length===0)return;this.type===PS&&(Fe("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=wl);let b=n.getRenderTarget(),W=n.getActiveCubeFace(),T=n.getActiveMipmapLevel(),F=n.state;F.setBlending(Qi),F.buffers.depth.getReversed()===!0?F.buffers.color.setClear(0,0,0,0):F.buffers.color.setClear(1,1,1,1),F.buffers.depth.setTest(!0),F.setScissorTest(!1);let H=m!==this.type;H&&I.traverse(function(G){G.material&&(Array.isArray(G.material)?G.material.forEach(B=>B.needsUpdate=!0):G.material.needsUpdate=!0)});for(let G=0,B=C.length;G<B;G++){let U=C[G],O=U.shadow;if(O===void 0){Fe("WebGLShadowMap:",U,"has no shadow.");continue}if(O.autoUpdate===!1&&O.needsUpdate===!1)continue;r.copy(O.mapSize);let Z=O.getFrameExtents();r.multiply(Z),o.copy(O.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(o.x=Math.floor(u/Z.x),r.x=o.x*Z.x,O.mapSize.x=o.x),r.y>u&&(o.y=Math.floor(u/Z.y),r.y=o.y*Z.y,O.mapSize.y=o.y));let K=n.state.buffers.depth.getReversed();if(O.camera._reversedDepth=K,O.map===null||H===!0){if(O.map!==null&&(O.map.depthTexture!==null&&(O.map.depthTexture.dispose(),O.map.depthTexture=null),O.map.dispose()),this.type===ka){if(U.isPointLight){Fe("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}O.map=new ti(r.x,r.y,{format:ps,type:er,minFilter:hn,magFilter:hn,generateMipmaps:!1}),O.map.texture.name=U.name+".shadowMap",O.map.depthTexture=new lo(r.x,r.y,Ni),O.map.depthTexture.name=U.name+".shadowMapDepth",O.map.depthTexture.format=Zi,O.map.depthTexture.compareFunction=null,O.map.depthTexture.minFilter=sn,O.map.depthTexture.magFilter=sn}else U.isPointLight?(O.map=new xp(r.x),O.map.depthTexture=new fh(r.x,Ri)):(O.map=new ti(r.x,r.y),O.map.depthTexture=new lo(r.x,r.y,Ri)),O.map.depthTexture.name=U.name+".shadowMap",O.map.depthTexture.format=Zi,this.type===wl?(O.map.depthTexture.compareFunction=K?mp:pp,O.map.depthTexture.minFilter=hn,O.map.depthTexture.magFilter=hn):(O.map.depthTexture.compareFunction=null,O.map.depthTexture.minFilter=sn,O.map.depthTexture.magFilter=sn);O.camera.updateProjectionMatrix()}let he=O.map.isWebGLCubeRenderTarget?6:1;for(let _e=0;_e<he;_e++){if(O.map.isWebGLCubeRenderTarget)n.setRenderTarget(O.map,_e),n.clear();else{_e===0&&(n.setRenderTarget(O.map),n.clear());let ge=O.getViewport(_e);s.set(o.x*ge.x,o.y*ge.y,o.x*ge.z,o.y*ge.w),F.viewport(s)}if(U.isPointLight){let ge=O.camera,Je=O.matrix,Pt=U.distance||ge.far;Pt!==ge.far&&(ge.far=Pt,ge.updateProjectionMatrix()),Ol.setFromMatrixPosition(U.matrixWorld),ge.position.copy(Ol),w0.copy(ge.position),w0.add(Y2[_e]),ge.up.copy(Z2[_e]),ge.lookAt(w0),ge.updateMatrixWorld(),Je.makeTranslation(-Ol.x,-Ol.y,-Ol.z),kw.multiplyMatrices(ge.projectionMatrix,ge.matrixWorldInverse),O._frustum.setFromProjectionMatrix(kw,ge.coordinateSystem,ge.reversedDepth)}else O.updateMatrices(U);i=O.getFrustum(),w(I,_,O.camera,U,this.type)}O.isPointLightShadow!==!0&&this.type===ka&&E(O,_),O.needsUpdate=!1}m=this.type,g.needsUpdate=!1,n.setRenderTarget(b,W,T)};function E(C,I){let _=e.update(y);f.defines.VSM_SAMPLES!==C.blurSamples&&(f.defines.VSM_SAMPLES=C.blurSamples,h.defines.VSM_SAMPLES=C.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),C.mapPass===null&&(C.mapPass=new ti(r.x,r.y,{format:ps,type:er})),f.uniforms.shadow_pass.value=C.map.depthTexture,f.uniforms.resolution.value=C.mapSize,f.uniforms.radius.value=C.radius,n.setRenderTarget(C.mapPass),n.clear(),n.renderBufferDirect(I,null,_,f,y,null),h.uniforms.shadow_pass.value=C.mapPass.texture,h.uniforms.resolution.value=C.mapSize,h.uniforms.radius.value=C.radius,n.setRenderTarget(C.map),n.clear(),n.renderBufferDirect(I,null,_,h,y,null)}function S(C,I,_,b){let W=null,T=_.isPointLight===!0?C.customDistanceMaterial:C.customDepthMaterial;if(T!==void 0)W=T;else if(W=_.isPointLight===!0?c:a,n.localClippingEnabled&&I.clipShadows===!0&&Array.isArray(I.clippingPlanes)&&I.clippingPlanes.length!==0||I.displacementMap&&I.displacementScale!==0||I.alphaMap&&I.alphaTest>0||I.map&&I.alphaTest>0||I.alphaToCoverage===!0){let F=W.uuid,H=I.uuid,G=l[F];G===void 0&&(G={},l[F]=G);let B=G[H];B===void 0&&(B=W.clone(),G[H]=B,I.addEventListener("dispose",D)),W=B}if(W.visible=I.visible,W.wireframe=I.wireframe,b===ka?W.side=I.shadowSide!==null?I.shadowSide:I.side:W.side=I.shadowSide!==null?I.shadowSide:d[I.side],W.alphaMap=I.alphaMap,W.alphaTest=I.alphaToCoverage===!0?.5:I.alphaTest,W.map=I.map,W.clipShadows=I.clipShadows,W.clippingPlanes=I.clippingPlanes,W.clipIntersection=I.clipIntersection,W.displacementMap=I.displacementMap,W.displacementScale=I.displacementScale,W.displacementBias=I.displacementBias,W.wireframeLinewidth=I.wireframeLinewidth,W.linewidth=I.linewidth,_.isPointLight===!0&&W.isMeshDistanceMaterial===!0){let F=n.properties.get(W);F.light=_}return W}function w(C,I,_,b,W){if(C.visible===!1)return;if(C.layers.test(I.layers)&&(C.isMesh||C.isLine||C.isPoints)&&(C.castShadow||C.receiveShadow&&W===ka)&&(!C.frustumCulled||i.intersectsObject(C))){C.modelViewMatrix.multiplyMatrices(_.matrixWorldInverse,C.matrixWorld);let H=e.update(C),G=C.material;if(Array.isArray(G)){let B=H.groups;for(let U=0,O=B.length;U<O;U++){let Z=B[U],K=G[Z.materialIndex];if(K&&K.visible){let he=S(C,K,b,W);C.onBeforeShadow(n,C,I,_,H,he,Z),n.renderBufferDirect(_,null,H,he,C,Z),C.onAfterShadow(n,C,I,_,H,he,Z)}}}else if(G.visible){let B=S(C,G,b,W);C.onBeforeShadow(n,C,I,_,H,B,null),n.renderBufferDirect(_,null,H,B,C,null),C.onAfterShadow(n,C,I,_,H,B,null)}}let F=C.children;for(let H=0,G=F.length;H<G;H++)w(F[H],I,_,b,W)}function D(C){C.target.removeEventListener("dispose",D);for(let _ in l){let b=l[_],W=C.target.uuid;W in b&&(b[W].dispose(),delete b[W])}}}function K2(n,e){function t(){let R=!1,se=new Lt,ie=null,ye=new Lt(0,0,0,0);return{setMask:function(ee){ie!==ee&&!R&&(n.colorMask(ee,ee,ee,ee),ie=ee)},setLocked:function(ee){R=ee},setClear:function(ee,j,be,ze,Rt){Rt===!0&&(ee*=ze,j*=ze,be*=ze),se.set(ee,j,be,ze),ye.equals(se)===!1&&(n.clearColor(ee,j,be,ze),ye.copy(se))},reset:function(){R=!1,ie=null,ye.set(-1,0,0,0)}}}function i(){let R=!1,se=!1,ie=null,ye=null,ee=null;return{setReversed:function(j){if(se!==j){let be=e.get("EXT_clip_control");j?be.clipControlEXT(be.LOWER_LEFT_EXT,be.ZERO_TO_ONE_EXT):be.clipControlEXT(be.LOWER_LEFT_EXT,be.NEGATIVE_ONE_TO_ONE_EXT),se=j;let ze=ee;ee=null,this.setClear(ze)}},getReversed:function(){return se},setTest:function(j){j?re(n.DEPTH_TEST):ce(n.DEPTH_TEST)},setMask:function(j){ie!==j&&!R&&(n.depthMask(j),ie=j)},setFunc:function(j){if(se&&(j=fw[j]),ye!==j){switch(j){case Xf:n.depthFunc(n.NEVER);break;case Yf:n.depthFunc(n.ALWAYS);break;case Zf:n.depthFunc(n.LESS);break;case ls:n.depthFunc(n.LEQUAL);break;case Jf:n.depthFunc(n.EQUAL);break;case Kf:n.depthFunc(n.GEQUAL);break;case Qf:n.depthFunc(n.GREATER);break;case eh:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}ye=j}},setLocked:function(j){R=j},setClear:function(j){ee!==j&&(ee=j,se&&(j=1-j),n.clearDepth(j))},reset:function(){R=!1,ie=null,ye=null,ee=null,se=!1}}}function r(){let R=!1,se=null,ie=null,ye=null,ee=null,j=null,be=null,ze=null,Rt=null;return{setTest:function(bt){R||(bt?re(n.STENCIL_TEST):ce(n.STENCIL_TEST))},setMask:function(bt){se!==bt&&!R&&(n.stencilMask(bt),se=bt)},setFunc:function(bt,ir,rr){(ie!==bt||ye!==ir||ee!==rr)&&(n.stencilFunc(bt,ir,rr),ie=bt,ye=ir,ee=rr)},setOp:function(bt,ir,rr){(j!==bt||be!==ir||ze!==rr)&&(n.stencilOp(bt,ir,rr),j=bt,be=ir,ze=rr)},setLocked:function(bt){R=bt},setClear:function(bt){Rt!==bt&&(n.clearStencil(bt),Rt=bt)},reset:function(){R=!1,se=null,ie=null,ye=null,ee=null,j=null,be=null,ze=null,Rt=null}}}let o=new t,s=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f=new WeakMap,h=[],p=null,y=!1,g=null,m=null,E=null,S=null,w=null,D=null,C=null,I=new nt(0,0,0),_=0,b=!1,W=null,T=null,F=null,H=null,G=null,B=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),U=!1,O=0,Z=n.getParameter(n.VERSION);Z.indexOf("WebGL")!==-1?(O=parseFloat(/^WebGL (\d)/.exec(Z)[1]),U=O>=1):Z.indexOf("OpenGL ES")!==-1&&(O=parseFloat(/^OpenGL ES (\d)/.exec(Z)[1]),U=O>=2);let K=null,he={},_e=n.getParameter(n.SCISSOR_BOX),ge=n.getParameter(n.VIEWPORT),Je=new Lt().fromArray(_e),Pt=new Lt().fromArray(ge);function Nt(R,se,ie,ye){let ee=new Uint8Array(4),j=n.createTexture();n.bindTexture(R,j),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let be=0;be<ie;be++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(se,0,n.RGBA,1,1,ye,0,n.RGBA,n.UNSIGNED_BYTE,ee):n.texImage2D(se+be,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,ee);return j}let X={};X[n.TEXTURE_2D]=Nt(n.TEXTURE_2D,n.TEXTURE_2D,1),X[n.TEXTURE_CUBE_MAP]=Nt(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),X[n.TEXTURE_2D_ARRAY]=Nt(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),X[n.TEXTURE_3D]=Nt(n.TEXTURE_3D,n.TEXTURE_3D,1,1),o.setClear(0,0,0,1),s.setClear(1),a.setClear(0),re(n.DEPTH_TEST),s.setFunc(ls),Qe(!1),Bt(Xy),re(n.CULL_FACE),Et(Qi);function re(R){u[R]!==!0&&(n.enable(R),u[R]=!0)}function ce(R){u[R]!==!1&&(n.disable(R),u[R]=!1)}function Ze(R,se){return d[R]!==se?(n.bindFramebuffer(R,se),d[R]=se,R===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=se),R===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=se),!0):!1}function ke(R,se){let ie=h,ye=!1;if(R){ie=f.get(se),ie===void 0&&(ie=[],f.set(se,ie));let ee=R.textures;if(ie.length!==ee.length||ie[0]!==n.COLOR_ATTACHMENT0){for(let j=0,be=ee.length;j<be;j++)ie[j]=n.COLOR_ATTACHMENT0+j;ie.length=ee.length,ye=!0}}else ie[0]!==n.BACK&&(ie[0]=n.BACK,ye=!0);ye&&n.drawBuffers(ie)}function He(R){return p!==R?(n.useProgram(R),p=R,!0):!1}let en={[so]:n.FUNC_ADD,[LS]:n.FUNC_SUBTRACT,[FS]:n.FUNC_REVERSE_SUBTRACT};en[kS]=n.MIN,en[US]=n.MAX;let ut={[BS]:n.ZERO,[VS]:n.ONE,[HS]:n.SRC_COLOR,[$f]:n.SRC_ALPHA,[qS]:n.SRC_ALPHA_SATURATE,[WS]:n.DST_COLOR,[GS]:n.DST_ALPHA,[zS]:n.ONE_MINUS_SRC_COLOR,[qf]:n.ONE_MINUS_SRC_ALPHA,[$S]:n.ONE_MINUS_DST_COLOR,[jS]:n.ONE_MINUS_DST_ALPHA,[XS]:n.CONSTANT_COLOR,[YS]:n.ONE_MINUS_CONSTANT_COLOR,[ZS]:n.CONSTANT_ALPHA,[JS]:n.ONE_MINUS_CONSTANT_ALPHA};function Et(R,se,ie,ye,ee,j,be,ze,Rt,bt){if(R===Qi){y===!0&&(ce(n.BLEND),y=!1);return}if(y===!1&&(re(n.BLEND),y=!0),R!==OS){if(R!==g||bt!==b){if((m!==so||w!==so)&&(n.blendEquation(n.FUNC_ADD),m=so,w=so),bt)switch(R){case cs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Yy:n.blendFunc(n.ONE,n.ONE);break;case Zy:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Jy:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Ue("WebGLState: Invalid blending: ",R);break}else switch(R){case cs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Yy:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case Zy:Ue("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Jy:Ue("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Ue("WebGLState: Invalid blending: ",R);break}E=null,S=null,D=null,C=null,I.set(0,0,0),_=0,g=R,b=bt}return}ee=ee||se,j=j||ie,be=be||ye,(se!==m||ee!==w)&&(n.blendEquationSeparate(en[se],en[ee]),m=se,w=ee),(ie!==E||ye!==S||j!==D||be!==C)&&(n.blendFuncSeparate(ut[ie],ut[ye],ut[j],ut[be]),E=ie,S=ye,D=j,C=be),(ze.equals(I)===!1||Rt!==_)&&(n.blendColor(ze.r,ze.g,ze.b,Rt),I.copy(ze),_=Rt),g=R,b=!1}function Dt(R,se){R.side===Ki?ce(n.CULL_FACE):re(n.CULL_FACE);let ie=R.side===kn;se&&(ie=!ie),Qe(ie),R.blending===cs&&R.transparent===!1?Et(Qi):Et(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),s.setFunc(R.depthFunc),s.setTest(R.depthTest),s.setMask(R.depthWrite),o.setMask(R.colorWrite);let ye=R.stencilWrite;a.setTest(ye),ye&&(a.setMask(R.stencilWriteMask),a.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),a.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),jt(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?re(n.SAMPLE_ALPHA_TO_COVERAGE):ce(n.SAMPLE_ALPHA_TO_COVERAGE)}function Qe(R){W!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),W=R)}function Bt(R){R!==RS?(re(n.CULL_FACE),R!==T&&(R===Xy?n.cullFace(n.BACK):R===NS?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):ce(n.CULL_FACE),T=R}function A(R){R!==F&&(U&&n.lineWidth(R),F=R)}function jt(R,se,ie){R?(re(n.POLYGON_OFFSET_FILL),(H!==se||G!==ie)&&(H=se,G=ie,s.getReversed()&&(se=-se),n.polygonOffset(se,ie))):ce(n.POLYGON_OFFSET_FILL)}function gt(R){R?re(n.SCISSOR_TEST):ce(n.SCISSOR_TEST)}function At(R){R===void 0&&(R=n.TEXTURE0+B-1),K!==R&&(n.activeTexture(R),K=R)}function we(R,se,ie){ie===void 0&&(K===null?ie=n.TEXTURE0+B-1:ie=K);let ye=he[ie];ye===void 0&&(ye={type:void 0,texture:void 0},he[ie]=ye),(ye.type!==R||ye.texture!==se)&&(K!==ie&&(n.activeTexture(ie),K=ie),n.bindTexture(R,se||X[R]),ye.type=R,ye.texture=se)}function M(){let R=he[K];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function v(){try{n.compressedTexImage2D(...arguments)}catch(R){Ue("WebGLState:",R)}}function N(){try{n.compressedTexImage3D(...arguments)}catch(R){Ue("WebGLState:",R)}}function q(){try{n.texSubImage2D(...arguments)}catch(R){Ue("WebGLState:",R)}}function Y(){try{n.texSubImage3D(...arguments)}catch(R){Ue("WebGLState:",R)}}function $(){try{n.compressedTexSubImage2D(...arguments)}catch(R){Ue("WebGLState:",R)}}function xe(){try{n.compressedTexSubImage3D(...arguments)}catch(R){Ue("WebGLState:",R)}}function oe(){try{n.texStorage2D(...arguments)}catch(R){Ue("WebGLState:",R)}}function Re(){try{n.texStorage3D(...arguments)}catch(R){Ue("WebGLState:",R)}}function Be(){try{n.texImage2D(...arguments)}catch(R){Ue("WebGLState:",R)}}function Q(){try{n.texImage3D(...arguments)}catch(R){Ue("WebGLState:",R)}}function ne(R){Je.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),Je.copy(R))}function Ee(R){Pt.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),Pt.copy(R))}function Me(R,se){let ie=l.get(se);ie===void 0&&(ie=new WeakMap,l.set(se,ie));let ye=ie.get(R);ye===void 0&&(ye=n.getUniformBlockIndex(se,R.name),ie.set(R,ye))}function pe(R,se){let ye=l.get(se).get(R);c.get(se)!==ye&&(n.uniformBlockBinding(se,ye,R.__bindingPointIndex),c.set(se,ye))}function et(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),s.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},K=null,he={},d={},f=new WeakMap,h=[],p=null,y=!1,g=null,m=null,E=null,S=null,w=null,D=null,C=null,I=new nt(0,0,0),_=0,b=!1,W=null,T=null,F=null,H=null,G=null,Je.set(0,0,n.canvas.width,n.canvas.height),Pt.set(0,0,n.canvas.width,n.canvas.height),o.reset(),s.reset(),a.reset()}return{buffers:{color:o,depth:s,stencil:a},enable:re,disable:ce,bindFramebuffer:Ze,drawBuffers:ke,useProgram:He,setBlending:Et,setMaterial:Dt,setFlipSided:Qe,setCullFace:Bt,setLineWidth:A,setPolygonOffset:jt,setScissorTest:gt,activeTexture:At,bindTexture:we,unbindTexture:M,compressedTexImage2D:v,compressedTexImage3D:N,texImage2D:Be,texImage3D:Q,updateUBOMapping:Me,uniformBlockBinding:pe,texStorage2D:oe,texStorage3D:Re,texSubImage2D:q,texSubImage3D:Y,compressedTexSubImage2D:$,compressedTexSubImage3D:xe,scissor:ne,viewport:Ee,reset:et}}function Q2(n,e,t,i,r,o,s){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new lt,u=new WeakMap,d,f=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function p(M,v){return h?new OffscreenCanvas(M,v):sl("canvas")}function y(M,v,N){let q=1,Y=we(M);if((Y.width>N||Y.height>N)&&(q=N/Math.max(Y.width,Y.height)),q<1)if(typeof HTMLImageElement<"u"&&M instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&M instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&M instanceof ImageBitmap||typeof VideoFrame<"u"&&M instanceof VideoFrame){let $=Math.floor(q*Y.width),xe=Math.floor(q*Y.height);d===void 0&&(d=p($,xe));let oe=v?p($,xe):d;return oe.width=$,oe.height=xe,oe.getContext("2d").drawImage(M,0,0,$,xe),Fe("WebGLRenderer: Texture has been resized from ("+Y.width+"x"+Y.height+") to ("+$+"x"+xe+")."),oe}else return"data"in M&&Fe("WebGLRenderer: Image in DataTexture is too big ("+Y.width+"x"+Y.height+")."),M;return M}function g(M){return M.generateMipmaps}function m(M){n.generateMipmap(M)}function E(M){return M.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:M.isWebGL3DRenderTarget?n.TEXTURE_3D:M.isWebGLArrayRenderTarget||M.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function S(M,v,N,q,Y=!1){if(M!==null){if(n[M]!==void 0)return n[M];Fe("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+M+"'")}let $=v;if(v===n.RED&&(N===n.FLOAT&&($=n.R32F),N===n.HALF_FLOAT&&($=n.R16F),N===n.UNSIGNED_BYTE&&($=n.R8)),v===n.RED_INTEGER&&(N===n.UNSIGNED_BYTE&&($=n.R8UI),N===n.UNSIGNED_SHORT&&($=n.R16UI),N===n.UNSIGNED_INT&&($=n.R32UI),N===n.BYTE&&($=n.R8I),N===n.SHORT&&($=n.R16I),N===n.INT&&($=n.R32I)),v===n.RG&&(N===n.FLOAT&&($=n.RG32F),N===n.HALF_FLOAT&&($=n.RG16F),N===n.UNSIGNED_BYTE&&($=n.RG8)),v===n.RG_INTEGER&&(N===n.UNSIGNED_BYTE&&($=n.RG8UI),N===n.UNSIGNED_SHORT&&($=n.RG16UI),N===n.UNSIGNED_INT&&($=n.RG32UI),N===n.BYTE&&($=n.RG8I),N===n.SHORT&&($=n.RG16I),N===n.INT&&($=n.RG32I)),v===n.RGB_INTEGER&&(N===n.UNSIGNED_BYTE&&($=n.RGB8UI),N===n.UNSIGNED_SHORT&&($=n.RGB16UI),N===n.UNSIGNED_INT&&($=n.RGB32UI),N===n.BYTE&&($=n.RGB8I),N===n.SHORT&&($=n.RGB16I),N===n.INT&&($=n.RGB32I)),v===n.RGBA_INTEGER&&(N===n.UNSIGNED_BYTE&&($=n.RGBA8UI),N===n.UNSIGNED_SHORT&&($=n.RGBA16UI),N===n.UNSIGNED_INT&&($=n.RGBA32UI),N===n.BYTE&&($=n.RGBA8I),N===n.SHORT&&($=n.RGBA16I),N===n.INT&&($=n.RGBA32I)),v===n.RGB&&(N===n.UNSIGNED_INT_5_9_9_9_REV&&($=n.RGB9_E5),N===n.UNSIGNED_INT_10F_11F_11F_REV&&($=n.R11F_G11F_B10F)),v===n.RGBA){let xe=Y?ol:ct.getTransfer(q);N===n.FLOAT&&($=n.RGBA32F),N===n.HALF_FLOAT&&($=n.RGBA16F),N===n.UNSIGNED_BYTE&&($=xe===xt?n.SRGB8_ALPHA8:n.RGBA8),N===n.UNSIGNED_SHORT_4_4_4_4&&($=n.RGBA4),N===n.UNSIGNED_SHORT_5_5_5_1&&($=n.RGB5_A1)}return($===n.R16F||$===n.R32F||$===n.RG16F||$===n.RG32F||$===n.RGBA16F||$===n.RGBA32F)&&e.get("EXT_color_buffer_float"),$}function w(M,v){let N;return M?v===null||v===Ri||v===Ba?N=n.DEPTH24_STENCIL8:v===Ni?N=n.DEPTH32F_STENCIL8:v===Ua&&(N=n.DEPTH24_STENCIL8,Fe("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===Ri||v===Ba?N=n.DEPTH_COMPONENT24:v===Ni?N=n.DEPTH_COMPONENT32F:v===Ua&&(N=n.DEPTH_COMPONENT16),N}function D(M,v){return g(M)===!0||M.isFramebufferTexture&&M.minFilter!==sn&&M.minFilter!==hn?Math.log2(Math.max(v.width,v.height))+1:M.mipmaps!==void 0&&M.mipmaps.length>0?M.mipmaps.length:M.isCompressedTexture&&Array.isArray(M.image)?v.mipmaps.length:1}function C(M){let v=M.target;v.removeEventListener("dispose",C),_(v),v.isVideoTexture&&u.delete(v)}function I(M){let v=M.target;v.removeEventListener("dispose",I),W(v)}function _(M){let v=i.get(M);if(v.__webglInit===void 0)return;let N=M.source,q=f.get(N);if(q){let Y=q[v.__cacheKey];Y.usedTimes--,Y.usedTimes===0&&b(M),Object.keys(q).length===0&&f.delete(N)}i.remove(M)}function b(M){let v=i.get(M);n.deleteTexture(v.__webglTexture);let N=M.source,q=f.get(N);delete q[v.__cacheKey],s.memory.textures--}function W(M){let v=i.get(M);if(M.depthTexture&&(M.depthTexture.dispose(),i.remove(M.depthTexture)),M.isWebGLCubeRenderTarget)for(let q=0;q<6;q++){if(Array.isArray(v.__webglFramebuffer[q]))for(let Y=0;Y<v.__webglFramebuffer[q].length;Y++)n.deleteFramebuffer(v.__webglFramebuffer[q][Y]);else n.deleteFramebuffer(v.__webglFramebuffer[q]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[q])}else{if(Array.isArray(v.__webglFramebuffer))for(let q=0;q<v.__webglFramebuffer.length;q++)n.deleteFramebuffer(v.__webglFramebuffer[q]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let q=0;q<v.__webglColorRenderbuffer.length;q++)v.__webglColorRenderbuffer[q]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[q]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let N=M.textures;for(let q=0,Y=N.length;q<Y;q++){let $=i.get(N[q]);$.__webglTexture&&(n.deleteTexture($.__webglTexture),s.memory.textures--),i.remove(N[q])}i.remove(M)}let T=0;function F(){T=0}function H(){let M=T;return M>=r.maxTextures&&Fe("WebGLTextures: Trying to use "+M+" texture units while this GPU supports only "+r.maxTextures),T+=1,M}function G(M){let v=[];return v.push(M.wrapS),v.push(M.wrapT),v.push(M.wrapR||0),v.push(M.magFilter),v.push(M.minFilter),v.push(M.anisotropy),v.push(M.internalFormat),v.push(M.format),v.push(M.type),v.push(M.generateMipmaps),v.push(M.premultiplyAlpha),v.push(M.flipY),v.push(M.unpackAlignment),v.push(M.colorSpace),v.join()}function B(M,v){let N=i.get(M);if(M.isVideoTexture&&gt(M),M.isRenderTargetTexture===!1&&M.isExternalTexture!==!0&&M.version>0&&N.__version!==M.version){let q=M.image;if(q===null)Fe("WebGLRenderer: Texture marked for update but no image data found.");else if(q.complete===!1)Fe("WebGLRenderer: Texture marked for update but image is incomplete");else{X(N,M,v);return}}else M.isExternalTexture&&(N.__webglTexture=M.sourceTexture?M.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,N.__webglTexture,n.TEXTURE0+v)}function U(M,v){let N=i.get(M);if(M.isRenderTargetTexture===!1&&M.version>0&&N.__version!==M.version){X(N,M,v);return}else M.isExternalTexture&&(N.__webglTexture=M.sourceTexture?M.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,N.__webglTexture,n.TEXTURE0+v)}function O(M,v){let N=i.get(M);if(M.isRenderTargetTexture===!1&&M.version>0&&N.__version!==M.version){X(N,M,v);return}t.bindTexture(n.TEXTURE_3D,N.__webglTexture,n.TEXTURE0+v)}function Z(M,v){let N=i.get(M);if(M.isCubeDepthTexture!==!0&&M.version>0&&N.__version!==M.version){re(N,M,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,N.__webglTexture,n.TEXTURE0+v)}let K={[th]:n.REPEAT,[Yi]:n.CLAMP_TO_EDGE,[nh]:n.MIRRORED_REPEAT},he={[sn]:n.NEAREST,[ew]:n.NEAREST_MIPMAP_NEAREST,[Tl]:n.NEAREST_MIPMAP_LINEAR,[hn]:n.LINEAR,[Ih]:n.LINEAR_MIPMAP_NEAREST,[mo]:n.LINEAR_MIPMAP_LINEAR},_e={[iw]:n.NEVER,[cw]:n.ALWAYS,[rw]:n.LESS,[pp]:n.LEQUAL,[ow]:n.EQUAL,[mp]:n.GEQUAL,[sw]:n.GREATER,[aw]:n.NOTEQUAL};function ge(M,v){if(v.type===Ni&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===hn||v.magFilter===Ih||v.magFilter===Tl||v.magFilter===mo||v.minFilter===hn||v.minFilter===Ih||v.minFilter===Tl||v.minFilter===mo)&&Fe("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(M,n.TEXTURE_WRAP_S,K[v.wrapS]),n.texParameteri(M,n.TEXTURE_WRAP_T,K[v.wrapT]),(M===n.TEXTURE_3D||M===n.TEXTURE_2D_ARRAY)&&n.texParameteri(M,n.TEXTURE_WRAP_R,K[v.wrapR]),n.texParameteri(M,n.TEXTURE_MAG_FILTER,he[v.magFilter]),n.texParameteri(M,n.TEXTURE_MIN_FILTER,he[v.minFilter]),v.compareFunction&&(n.texParameteri(M,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(M,n.TEXTURE_COMPARE_FUNC,_e[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===sn||v.minFilter!==Tl&&v.minFilter!==mo||v.type===Ni&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let N=e.get("EXT_texture_filter_anisotropic");n.texParameterf(M,N.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function Je(M,v){let N=!1;M.__webglInit===void 0&&(M.__webglInit=!0,v.addEventListener("dispose",C));let q=v.source,Y=f.get(q);Y===void 0&&(Y={},f.set(q,Y));let $=G(v);if($!==M.__cacheKey){Y[$]===void 0&&(Y[$]={texture:n.createTexture(),usedTimes:0},s.memory.textures++,N=!0),Y[$].usedTimes++;let xe=Y[M.__cacheKey];xe!==void 0&&(Y[M.__cacheKey].usedTimes--,xe.usedTimes===0&&b(v)),M.__cacheKey=$,M.__webglTexture=Y[$].texture}return N}function Pt(M,v,N){return Math.floor(Math.floor(M/N)/v)}function Nt(M,v,N,q){let $=M.updateRanges;if($.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,N,q,v.data);else{$.sort((Q,ne)=>Q.start-ne.start);let xe=0;for(let Q=1;Q<$.length;Q++){let ne=$[xe],Ee=$[Q],Me=ne.start+ne.count,pe=Pt(Ee.start,v.width,4),et=Pt(ne.start,v.width,4);Ee.start<=Me+1&&pe===et&&Pt(Ee.start+Ee.count-1,v.width,4)===pe?ne.count=Math.max(ne.count,Ee.start+Ee.count-ne.start):(++xe,$[xe]=Ee)}$.length=xe+1;let oe=n.getParameter(n.UNPACK_ROW_LENGTH),Re=n.getParameter(n.UNPACK_SKIP_PIXELS),Be=n.getParameter(n.UNPACK_SKIP_ROWS);n.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let Q=0,ne=$.length;Q<ne;Q++){let Ee=$[Q],Me=Math.floor(Ee.start/4),pe=Math.ceil(Ee.count/4),et=Me%v.width,R=Math.floor(Me/v.width),se=pe,ie=1;n.pixelStorei(n.UNPACK_SKIP_PIXELS,et),n.pixelStorei(n.UNPACK_SKIP_ROWS,R),t.texSubImage2D(n.TEXTURE_2D,0,et,R,se,ie,N,q,v.data)}M.clearUpdateRanges(),n.pixelStorei(n.UNPACK_ROW_LENGTH,oe),n.pixelStorei(n.UNPACK_SKIP_PIXELS,Re),n.pixelStorei(n.UNPACK_SKIP_ROWS,Be)}}function X(M,v,N){let q=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&(q=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&(q=n.TEXTURE_3D);let Y=Je(M,v),$=v.source;t.bindTexture(q,M.__webglTexture,n.TEXTURE0+N);let xe=i.get($);if($.version!==xe.__version||Y===!0){t.activeTexture(n.TEXTURE0+N);let oe=ct.getPrimaries(ct.workingColorSpace),Re=v.colorSpace===Ir?null:ct.getPrimaries(v.colorSpace),Be=v.colorSpace===Ir||oe===Re?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Be);let Q=y(v.image,!1,r.maxTextureSize);Q=At(v,Q);let ne=o.convert(v.format,v.colorSpace),Ee=o.convert(v.type),Me=S(v.internalFormat,ne,Ee,v.colorSpace,v.isVideoTexture);ge(q,v);let pe,et=v.mipmaps,R=v.isVideoTexture!==!0,se=xe.__version===void 0||Y===!0,ie=$.dataReady,ye=D(v,Q);if(v.isDepthTexture)Me=w(v.format===go,v.type),se&&(R?t.texStorage2D(n.TEXTURE_2D,1,Me,Q.width,Q.height):t.texImage2D(n.TEXTURE_2D,0,Me,Q.width,Q.height,0,ne,Ee,null));else if(v.isDataTexture)if(et.length>0){R&&se&&t.texStorage2D(n.TEXTURE_2D,ye,Me,et[0].width,et[0].height);for(let ee=0,j=et.length;ee<j;ee++)pe=et[ee],R?ie&&t.texSubImage2D(n.TEXTURE_2D,ee,0,0,pe.width,pe.height,ne,Ee,pe.data):t.texImage2D(n.TEXTURE_2D,ee,Me,pe.width,pe.height,0,ne,Ee,pe.data);v.generateMipmaps=!1}else R?(se&&t.texStorage2D(n.TEXTURE_2D,ye,Me,Q.width,Q.height),ie&&Nt(v,Q,ne,Ee)):t.texImage2D(n.TEXTURE_2D,0,Me,Q.width,Q.height,0,ne,Ee,Q.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){R&&se&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ye,Me,et[0].width,et[0].height,Q.depth);for(let ee=0,j=et.length;ee<j;ee++)if(pe=et[ee],v.format!==fi)if(ne!==null)if(R){if(ie)if(v.layerUpdates.size>0){let be=_0(pe.width,pe.height,v.format,v.type);for(let ze of v.layerUpdates){let Rt=pe.data.subarray(ze*be/pe.data.BYTES_PER_ELEMENT,(ze+1)*be/pe.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,ee,0,0,ze,pe.width,pe.height,1,ne,Rt)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,ee,0,0,0,pe.width,pe.height,Q.depth,ne,pe.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,ee,Me,pe.width,pe.height,Q.depth,0,pe.data,0,0);else Fe("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else R?ie&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,ee,0,0,0,pe.width,pe.height,Q.depth,ne,Ee,pe.data):t.texImage3D(n.TEXTURE_2D_ARRAY,ee,Me,pe.width,pe.height,Q.depth,0,ne,Ee,pe.data)}else{R&&se&&t.texStorage2D(n.TEXTURE_2D,ye,Me,et[0].width,et[0].height);for(let ee=0,j=et.length;ee<j;ee++)pe=et[ee],v.format!==fi?ne!==null?R?ie&&t.compressedTexSubImage2D(n.TEXTURE_2D,ee,0,0,pe.width,pe.height,ne,pe.data):t.compressedTexImage2D(n.TEXTURE_2D,ee,Me,pe.width,pe.height,0,pe.data):Fe("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):R?ie&&t.texSubImage2D(n.TEXTURE_2D,ee,0,0,pe.width,pe.height,ne,Ee,pe.data):t.texImage2D(n.TEXTURE_2D,ee,Me,pe.width,pe.height,0,ne,Ee,pe.data)}else if(v.isDataArrayTexture)if(R){if(se&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ye,Me,Q.width,Q.height,Q.depth),ie)if(v.layerUpdates.size>0){let ee=_0(Q.width,Q.height,v.format,v.type);for(let j of v.layerUpdates){let be=Q.data.subarray(j*ee/Q.data.BYTES_PER_ELEMENT,(j+1)*ee/Q.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,j,Q.width,Q.height,1,ne,Ee,be)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,Q.width,Q.height,Q.depth,ne,Ee,Q.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Me,Q.width,Q.height,Q.depth,0,ne,Ee,Q.data);else if(v.isData3DTexture)R?(se&&t.texStorage3D(n.TEXTURE_3D,ye,Me,Q.width,Q.height,Q.depth),ie&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,Q.width,Q.height,Q.depth,ne,Ee,Q.data)):t.texImage3D(n.TEXTURE_3D,0,Me,Q.width,Q.height,Q.depth,0,ne,Ee,Q.data);else if(v.isFramebufferTexture){if(se)if(R)t.texStorage2D(n.TEXTURE_2D,ye,Me,Q.width,Q.height);else{let ee=Q.width,j=Q.height;for(let be=0;be<ye;be++)t.texImage2D(n.TEXTURE_2D,be,Me,ee,j,0,ne,Ee,null),ee>>=1,j>>=1}}else if(et.length>0){if(R&&se){let ee=we(et[0]);t.texStorage2D(n.TEXTURE_2D,ye,Me,ee.width,ee.height)}for(let ee=0,j=et.length;ee<j;ee++)pe=et[ee],R?ie&&t.texSubImage2D(n.TEXTURE_2D,ee,0,0,ne,Ee,pe):t.texImage2D(n.TEXTURE_2D,ee,Me,ne,Ee,pe);v.generateMipmaps=!1}else if(R){if(se){let ee=we(Q);t.texStorage2D(n.TEXTURE_2D,ye,Me,ee.width,ee.height)}ie&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ne,Ee,Q)}else t.texImage2D(n.TEXTURE_2D,0,Me,ne,Ee,Q);g(v)&&m(q),xe.__version=$.version,v.onUpdate&&v.onUpdate(v)}M.__version=v.version}function re(M,v,N){if(v.image.length!==6)return;let q=Je(M,v),Y=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,M.__webglTexture,n.TEXTURE0+N);let $=i.get(Y);if(Y.version!==$.__version||q===!0){t.activeTexture(n.TEXTURE0+N);let xe=ct.getPrimaries(ct.workingColorSpace),oe=v.colorSpace===Ir?null:ct.getPrimaries(v.colorSpace),Re=v.colorSpace===Ir||xe===oe?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Re);let Be=v.isCompressedTexture||v.image[0].isCompressedTexture,Q=v.image[0]&&v.image[0].isDataTexture,ne=[];for(let j=0;j<6;j++)!Be&&!Q?ne[j]=y(v.image[j],!0,r.maxCubemapSize):ne[j]=Q?v.image[j].image:v.image[j],ne[j]=At(v,ne[j]);let Ee=ne[0],Me=o.convert(v.format,v.colorSpace),pe=o.convert(v.type),et=S(v.internalFormat,Me,pe,v.colorSpace),R=v.isVideoTexture!==!0,se=$.__version===void 0||q===!0,ie=Y.dataReady,ye=D(v,Ee);ge(n.TEXTURE_CUBE_MAP,v);let ee;if(Be){R&&se&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ye,et,Ee.width,Ee.height);for(let j=0;j<6;j++){ee=ne[j].mipmaps;for(let be=0;be<ee.length;be++){let ze=ee[be];v.format!==fi?Me!==null?R?ie&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be,0,0,ze.width,ze.height,Me,ze.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be,et,ze.width,ze.height,0,ze.data):Fe("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):R?ie&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be,0,0,ze.width,ze.height,Me,pe,ze.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be,et,ze.width,ze.height,0,Me,pe,ze.data)}}}else{if(ee=v.mipmaps,R&&se){ee.length>0&&ye++;let j=we(ne[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ye,et,j.width,j.height)}for(let j=0;j<6;j++)if(Q){R?ie&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,0,0,ne[j].width,ne[j].height,Me,pe,ne[j].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,et,ne[j].width,ne[j].height,0,Me,pe,ne[j].data);for(let be=0;be<ee.length;be++){let Rt=ee[be].image[j].image;R?ie&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be+1,0,0,Rt.width,Rt.height,Me,pe,Rt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be+1,et,Rt.width,Rt.height,0,Me,pe,Rt.data)}}else{R?ie&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,0,0,Me,pe,ne[j]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,et,Me,pe,ne[j]);for(let be=0;be<ee.length;be++){let ze=ee[be];R?ie&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be+1,0,0,Me,pe,ze.image[j]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,be+1,et,Me,pe,ze.image[j])}}}g(v)&&m(n.TEXTURE_CUBE_MAP),$.__version=Y.version,v.onUpdate&&v.onUpdate(v)}M.__version=v.version}function ce(M,v,N,q,Y,$){let xe=o.convert(N.format,N.colorSpace),oe=o.convert(N.type),Re=S(N.internalFormat,xe,oe,N.colorSpace),Be=i.get(v),Q=i.get(N);if(Q.__renderTarget=v,!Be.__hasExternalTextures){let ne=Math.max(1,v.width>>$),Ee=Math.max(1,v.height>>$);Y===n.TEXTURE_3D||Y===n.TEXTURE_2D_ARRAY?t.texImage3D(Y,$,Re,ne,Ee,v.depth,0,xe,oe,null):t.texImage2D(Y,$,Re,ne,Ee,0,xe,oe,null)}t.bindFramebuffer(n.FRAMEBUFFER,M),jt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,Y,Q.__webglTexture,0,A(v)):(Y===n.TEXTURE_2D||Y>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Y<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,q,Y,Q.__webglTexture,$),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ze(M,v,N){if(n.bindRenderbuffer(n.RENDERBUFFER,M),v.depthBuffer){let q=v.depthTexture,Y=q&&q.isDepthTexture?q.type:null,$=w(v.stencilBuffer,Y),xe=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;jt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,A(v),$,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,A(v),$,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,$,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,xe,n.RENDERBUFFER,M)}else{let q=v.textures;for(let Y=0;Y<q.length;Y++){let $=q[Y],xe=o.convert($.format,$.colorSpace),oe=o.convert($.type),Re=S($.internalFormat,xe,oe,$.colorSpace);jt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,A(v),Re,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,A(v),Re,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,Re,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function ke(M,v,N){let q=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,M),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(v.depthTexture);if(Y.__renderTarget=v,(!Y.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),q){if(Y.__webglInit===void 0&&(Y.__webglInit=!0,v.depthTexture.addEventListener("dispose",C)),Y.__webglTexture===void 0){Y.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),ge(n.TEXTURE_CUBE_MAP,v.depthTexture);let Be=o.convert(v.depthTexture.format),Q=o.convert(v.depthTexture.type),ne;v.depthTexture.format===Zi?ne=n.DEPTH_COMPONENT24:v.depthTexture.format===go&&(ne=n.DEPTH24_STENCIL8);for(let Ee=0;Ee<6;Ee++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Ee,0,ne,v.width,v.height,0,Be,Q,null)}}else B(v.depthTexture,0);let $=Y.__webglTexture,xe=A(v),oe=q?n.TEXTURE_CUBE_MAP_POSITIVE_X+N:n.TEXTURE_2D,Re=v.depthTexture.format===go?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Zi)jt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Re,oe,$,0,xe):n.framebufferTexture2D(n.FRAMEBUFFER,Re,oe,$,0);else if(v.depthTexture.format===go)jt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Re,oe,$,0,xe):n.framebufferTexture2D(n.FRAMEBUFFER,Re,oe,$,0);else throw new Error("Unknown depthTexture format")}function He(M){let v=i.get(M),N=M.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==M.depthTexture){let q=M.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),q){let Y=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,q.removeEventListener("dispose",Y)};q.addEventListener("dispose",Y),v.__depthDisposeCallback=Y}v.__boundDepthTexture=q}if(M.depthTexture&&!v.__autoAllocateDepthBuffer)if(N)for(let q=0;q<6;q++)ke(v.__webglFramebuffer[q],M,q);else{let q=M.texture.mipmaps;q&&q.length>0?ke(v.__webglFramebuffer[0],M,0):ke(v.__webglFramebuffer,M,0)}else if(N){v.__webglDepthbuffer=[];for(let q=0;q<6;q++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[q]),v.__webglDepthbuffer[q]===void 0)v.__webglDepthbuffer[q]=n.createRenderbuffer(),Ze(v.__webglDepthbuffer[q],M,!1);else{let Y=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,$=v.__webglDepthbuffer[q];n.bindRenderbuffer(n.RENDERBUFFER,$),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,$)}}else{let q=M.texture.mipmaps;if(q&&q.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Ze(v.__webglDepthbuffer,M,!1);else{let Y=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,$=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,$),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,$)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function en(M,v,N){let q=i.get(M);v!==void 0&&ce(q.__webglFramebuffer,M,M.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),N!==void 0&&He(M)}function ut(M){let v=M.texture,N=i.get(M),q=i.get(v);M.addEventListener("dispose",I);let Y=M.textures,$=M.isWebGLCubeRenderTarget===!0,xe=Y.length>1;if(xe||(q.__webglTexture===void 0&&(q.__webglTexture=n.createTexture()),q.__version=v.version,s.memory.textures++),$){N.__webglFramebuffer=[];for(let oe=0;oe<6;oe++)if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer[oe]=[];for(let Re=0;Re<v.mipmaps.length;Re++)N.__webglFramebuffer[oe][Re]=n.createFramebuffer()}else N.__webglFramebuffer[oe]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer=[];for(let oe=0;oe<v.mipmaps.length;oe++)N.__webglFramebuffer[oe]=n.createFramebuffer()}else N.__webglFramebuffer=n.createFramebuffer();if(xe)for(let oe=0,Re=Y.length;oe<Re;oe++){let Be=i.get(Y[oe]);Be.__webglTexture===void 0&&(Be.__webglTexture=n.createTexture(),s.memory.textures++)}if(M.samples>0&&jt(M)===!1){N.__webglMultisampledFramebuffer=n.createFramebuffer(),N.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,N.__webglMultisampledFramebuffer);for(let oe=0;oe<Y.length;oe++){let Re=Y[oe];N.__webglColorRenderbuffer[oe]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,N.__webglColorRenderbuffer[oe]);let Be=o.convert(Re.format,Re.colorSpace),Q=o.convert(Re.type),ne=S(Re.internalFormat,Be,Q,Re.colorSpace,M.isXRRenderTarget===!0),Ee=A(M);n.renderbufferStorageMultisample(n.RENDERBUFFER,Ee,ne,M.width,M.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+oe,n.RENDERBUFFER,N.__webglColorRenderbuffer[oe])}n.bindRenderbuffer(n.RENDERBUFFER,null),M.depthBuffer&&(N.__webglDepthRenderbuffer=n.createRenderbuffer(),Ze(N.__webglDepthRenderbuffer,M,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if($){t.bindTexture(n.TEXTURE_CUBE_MAP,q.__webglTexture),ge(n.TEXTURE_CUBE_MAP,v);for(let oe=0;oe<6;oe++)if(v.mipmaps&&v.mipmaps.length>0)for(let Re=0;Re<v.mipmaps.length;Re++)ce(N.__webglFramebuffer[oe][Re],M,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+oe,Re);else ce(N.__webglFramebuffer[oe],M,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+oe,0);g(v)&&m(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(xe){for(let oe=0,Re=Y.length;oe<Re;oe++){let Be=Y[oe],Q=i.get(Be),ne=n.TEXTURE_2D;(M.isWebGL3DRenderTarget||M.isWebGLArrayRenderTarget)&&(ne=M.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ne,Q.__webglTexture),ge(ne,Be),ce(N.__webglFramebuffer,M,Be,n.COLOR_ATTACHMENT0+oe,ne,0),g(Be)&&m(ne)}t.unbindTexture()}else{let oe=n.TEXTURE_2D;if((M.isWebGL3DRenderTarget||M.isWebGLArrayRenderTarget)&&(oe=M.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(oe,q.__webglTexture),ge(oe,v),v.mipmaps&&v.mipmaps.length>0)for(let Re=0;Re<v.mipmaps.length;Re++)ce(N.__webglFramebuffer[Re],M,v,n.COLOR_ATTACHMENT0,oe,Re);else ce(N.__webglFramebuffer,M,v,n.COLOR_ATTACHMENT0,oe,0);g(v)&&m(oe),t.unbindTexture()}M.depthBuffer&&He(M)}function Et(M){let v=M.textures;for(let N=0,q=v.length;N<q;N++){let Y=v[N];if(g(Y)){let $=E(M),xe=i.get(Y).__webglTexture;t.bindTexture($,xe),m($),t.unbindTexture()}}}let Dt=[],Qe=[];function Bt(M){if(M.samples>0){if(jt(M)===!1){let v=M.textures,N=M.width,q=M.height,Y=n.COLOR_BUFFER_BIT,$=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,xe=i.get(M),oe=v.length>1;if(oe)for(let Be=0;Be<v.length;Be++)t.bindFramebuffer(n.FRAMEBUFFER,xe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Be,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,xe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Be,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,xe.__webglMultisampledFramebuffer);let Re=M.texture.mipmaps;Re&&Re.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,xe.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,xe.__webglFramebuffer);for(let Be=0;Be<v.length;Be++){if(M.resolveDepthBuffer&&(M.depthBuffer&&(Y|=n.DEPTH_BUFFER_BIT),M.stencilBuffer&&M.resolveStencilBuffer&&(Y|=n.STENCIL_BUFFER_BIT)),oe){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,xe.__webglColorRenderbuffer[Be]);let Q=i.get(v[Be]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,Q,0)}n.blitFramebuffer(0,0,N,q,0,0,N,q,Y,n.NEAREST),c===!0&&(Dt.length=0,Qe.length=0,Dt.push(n.COLOR_ATTACHMENT0+Be),M.depthBuffer&&M.resolveDepthBuffer===!1&&(Dt.push($),Qe.push($),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,Qe)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Dt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),oe)for(let Be=0;Be<v.length;Be++){t.bindFramebuffer(n.FRAMEBUFFER,xe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Be,n.RENDERBUFFER,xe.__webglColorRenderbuffer[Be]);let Q=i.get(v[Be]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,xe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Be,n.TEXTURE_2D,Q,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,xe.__webglMultisampledFramebuffer)}else if(M.depthBuffer&&M.resolveDepthBuffer===!1&&c){let v=M.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function A(M){return Math.min(r.maxSamples,M.samples)}function jt(M){let v=i.get(M);return M.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function gt(M){let v=s.render.frame;u.get(M)!==v&&(u.set(M,v),M.update())}function At(M,v){let N=M.colorSpace,q=M.format,Y=M.type;return M.isCompressedTexture===!0||M.isVideoTexture===!0||N!==us&&N!==Ir&&(ct.getTransfer(N)===xt?(q!==fi||Y!==$n)&&Fe("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Ue("WebGLTextures: Unsupported texture color space:",N)),v}function we(M){return typeof HTMLImageElement<"u"&&M instanceof HTMLImageElement?(l.width=M.naturalWidth||M.width,l.height=M.naturalHeight||M.height):typeof VideoFrame<"u"&&M instanceof VideoFrame?(l.width=M.displayWidth,l.height=M.displayHeight):(l.width=M.width,l.height=M.height),l}this.allocateTextureUnit=H,this.resetTextureUnits=F,this.setTexture2D=B,this.setTexture2DArray=U,this.setTexture3D=O,this.setTextureCube=Z,this.rebindTextures=en,this.setupRenderTarget=ut,this.updateRenderTargetMipmap=Et,this.updateMultisampleRenderTarget=Bt,this.setupDepthRenderbuffer=He,this.setupFrameBufferTexture=ce,this.useMultisampledRTT=jt,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function ek(n,e){function t(i,r=Ir){let o,s=ct.getTransfer(r);if(i===$n)return n.UNSIGNED_BYTE;if(i===Rh)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Nh)return n.UNSIGNED_SHORT_5_5_5_1;if(i===c0)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===l0)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===s0)return n.BYTE;if(i===a0)return n.SHORT;if(i===Ua)return n.UNSIGNED_SHORT;if(i===Ah)return n.INT;if(i===Ri)return n.UNSIGNED_INT;if(i===Ni)return n.FLOAT;if(i===er)return n.HALF_FLOAT;if(i===u0)return n.ALPHA;if(i===d0)return n.RGB;if(i===fi)return n.RGBA;if(i===Zi)return n.DEPTH_COMPONENT;if(i===go)return n.DEPTH_STENCIL;if(i===f0)return n.RED;if(i===Ph)return n.RED_INTEGER;if(i===ps)return n.RG;if(i===Oh)return n.RG_INTEGER;if(i===Lh)return n.RGBA_INTEGER;if(i===Dl||i===Il||i===Al||i===Rl)if(s===xt)if(o=e.get("WEBGL_compressed_texture_s3tc_srgb"),o!==null){if(i===Dl)return o.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Il)return o.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Al)return o.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Rl)return o.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(o=e.get("WEBGL_compressed_texture_s3tc"),o!==null){if(i===Dl)return o.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Il)return o.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Al)return o.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Rl)return o.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Fh||i===kh||i===Uh||i===Bh)if(o=e.get("WEBGL_compressed_texture_pvrtc"),o!==null){if(i===Fh)return o.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===kh)return o.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Uh)return o.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Bh)return o.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Vh||i===Hh||i===zh||i===Gh||i===jh||i===Wh||i===$h)if(o=e.get("WEBGL_compressed_texture_etc"),o!==null){if(i===Vh||i===Hh)return s===xt?o.COMPRESSED_SRGB8_ETC2:o.COMPRESSED_RGB8_ETC2;if(i===zh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:o.COMPRESSED_RGBA8_ETC2_EAC;if(i===Gh)return o.COMPRESSED_R11_EAC;if(i===jh)return o.COMPRESSED_SIGNED_R11_EAC;if(i===Wh)return o.COMPRESSED_RG11_EAC;if(i===$h)return o.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===qh||i===Xh||i===Yh||i===Zh||i===Jh||i===Kh||i===Qh||i===ep||i===tp||i===np||i===ip||i===rp||i===op||i===sp)if(o=e.get("WEBGL_compressed_texture_astc"),o!==null){if(i===qh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:o.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Xh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:o.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Yh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:o.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Zh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:o.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Jh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:o.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Kh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:o.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Qh)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:o.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===ep)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:o.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===tp)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:o.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===np)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:o.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===ip)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:o.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===rp)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:o.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===op)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:o.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===sp)return s===xt?o.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:o.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===ap||i===cp||i===lp)if(o=e.get("EXT_texture_compression_bptc"),o!==null){if(i===ap)return s===xt?o.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:o.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===cp)return o.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===lp)return o.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===up||i===dp||i===fp||i===hp)if(o=e.get("EXT_texture_compression_rgtc"),o!==null){if(i===up)return o.COMPRESSED_RED_RGTC1_EXT;if(i===dp)return o.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===fp)return o.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===hp)return o.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Ba?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var tk=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,nk=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,P0=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new pl(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new ni({vertexShader:tk,fragmentShader:nk,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Wn(new gl(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},O0=class extends Tr{constructor(e,t){super();let i=this,r=null,o=1,s=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,p=null,y=typeof XRWebGLBinding<"u",g=new P0,m={},E=t.getContextAttributes(),S=null,w=null,D=[],C=[],I=new lt,_=null,b=new En;b.viewport=new Lt;let W=new En;W.viewport=new Lt;let T=[b,W],F=new Ch,H=null,G=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let re=D[X];return re===void 0&&(re=new Aa,D[X]=re),re.getTargetRaySpace()},this.getControllerGrip=function(X){let re=D[X];return re===void 0&&(re=new Aa,D[X]=re),re.getGripSpace()},this.getHand=function(X){let re=D[X];return re===void 0&&(re=new Aa,D[X]=re),re.getHandSpace()};function B(X){let re=C.indexOf(X.inputSource);if(re===-1)return;let ce=D[re];ce!==void 0&&(ce.update(X.inputSource,X.frame,l||s),ce.dispatchEvent({type:X.type,data:X.inputSource}))}function U(){r.removeEventListener("select",B),r.removeEventListener("selectstart",B),r.removeEventListener("selectend",B),r.removeEventListener("squeeze",B),r.removeEventListener("squeezestart",B),r.removeEventListener("squeezeend",B),r.removeEventListener("end",U),r.removeEventListener("inputsourceschange",O);for(let X=0;X<D.length;X++){let re=C[X];re!==null&&(C[X]=null,D[X].disconnect(re))}H=null,G=null,g.reset();for(let X in m)delete m[X];e.setRenderTarget(S),h=null,f=null,d=null,r=null,w=null,Nt.stop(),i.isPresenting=!1,e.setPixelRatio(_),e.setSize(I.width,I.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){o=X,i.isPresenting===!0&&Fe("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&Fe("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||s},this.setReferenceSpace=function(X){l=X},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&y&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return p},this.getSession=function(){return r},this.setSession=async function(X){if(r=X,r!==null){if(S=e.getRenderTarget(),r.addEventListener("select",B),r.addEventListener("selectstart",B),r.addEventListener("selectend",B),r.addEventListener("squeeze",B),r.addEventListener("squeezestart",B),r.addEventListener("squeezeend",B),r.addEventListener("end",U),r.addEventListener("inputsourceschange",O),E.xrCompatible!==!0&&await t.makeXRCompatible(),_=e.getPixelRatio(),e.getSize(I),y&&"createProjectionLayer"in XRWebGLBinding.prototype){let ce=null,Ze=null,ke=null;E.depth&&(ke=E.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ce=E.stencil?go:Zi,Ze=E.stencil?Ba:Ri);let He={colorFormat:t.RGBA8,depthFormat:ke,scaleFactor:o};d=this.getBinding(),f=d.createProjectionLayer(He),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),w=new ti(f.textureWidth,f.textureHeight,{format:fi,type:$n,depthTexture:new lo(f.textureWidth,f.textureHeight,Ze,void 0,void 0,void 0,void 0,void 0,void 0,ce),stencilBuffer:E.stencil,colorSpace:e.outputColorSpace,samples:E.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1})}else{let ce={antialias:E.antialias,alpha:!0,depth:E.depth,stencil:E.stencil,framebufferScaleFactor:o};h=new XRWebGLLayer(r,t,ce),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),w=new ti(h.framebufferWidth,h.framebufferHeight,{format:fi,type:$n,colorSpace:e.outputColorSpace,stencilBuffer:E.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}w.isXRRenderTarget=!0,this.setFoveation(c),l=null,s=await r.requestReferenceSpace(a),Nt.setContext(r),Nt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return g.getDepthTexture()};function O(X){for(let re=0;re<X.removed.length;re++){let ce=X.removed[re],Ze=C.indexOf(ce);Ze>=0&&(C[Ze]=null,D[Ze].disconnect(ce))}for(let re=0;re<X.added.length;re++){let ce=X.added[re],Ze=C.indexOf(ce);if(Ze===-1){for(let He=0;He<D.length;He++)if(He>=C.length){C.push(ce),Ze=He;break}else if(C[He]===null){C[He]=ce,Ze=He;break}if(Ze===-1)break}let ke=D[Ze];ke&&ke.connect(ce)}}let Z=new L,K=new L;function he(X,re,ce){Z.setFromMatrixPosition(re.matrixWorld),K.setFromMatrixPosition(ce.matrixWorld);let Ze=Z.distanceTo(K),ke=re.projectionMatrix.elements,He=ce.projectionMatrix.elements,en=ke[14]/(ke[10]-1),ut=ke[14]/(ke[10]+1),Et=(ke[9]+1)/ke[5],Dt=(ke[9]-1)/ke[5],Qe=(ke[8]-1)/ke[0],Bt=(He[8]+1)/He[0],A=en*Qe,jt=en*Bt,gt=Ze/(-Qe+Bt),At=gt*-Qe;if(re.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(At),X.translateZ(gt),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),ke[10]===-1)X.projectionMatrix.copy(re.projectionMatrix),X.projectionMatrixInverse.copy(re.projectionMatrixInverse);else{let we=en+gt,M=ut+gt,v=A-At,N=jt+(Ze-At),q=Et*ut/M*we,Y=Dt*ut/M*we;X.projectionMatrix.makePerspective(v,N,q,Y,we,M),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function _e(X,re){re===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(re.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let re=X.near,ce=X.far;g.texture!==null&&(g.depthNear>0&&(re=g.depthNear),g.depthFar>0&&(ce=g.depthFar)),F.near=W.near=b.near=re,F.far=W.far=b.far=ce,(H!==F.near||G!==F.far)&&(r.updateRenderState({depthNear:F.near,depthFar:F.far}),H=F.near,G=F.far),F.layers.mask=X.layers.mask|6,b.layers.mask=F.layers.mask&-5,W.layers.mask=F.layers.mask&-3;let Ze=X.parent,ke=F.cameras;_e(F,Ze);for(let He=0;He<ke.length;He++)_e(ke[He],Ze);ke.length===2?he(F,b,W):F.projectionMatrix.copy(b.projectionMatrix),ge(X,F,Ze)};function ge(X,re,ce){ce===null?X.matrix.copy(re.matrixWorld):(X.matrix.copy(ce.matrixWorld),X.matrix.invert(),X.matrix.multiply(re.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(re.projectionMatrix),X.projectionMatrixInverse.copy(re.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=rh*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return F},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(X){c=X,f!==null&&(f.fixedFoveation=X),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=X)},this.hasDepthSensing=function(){return g.texture!==null},this.getDepthSensingMesh=function(){return g.getMesh(F)},this.getCameraTexture=function(X){return m[X]};let Je=null;function Pt(X,re){if(u=re.getViewerPose(l||s),p=re,u!==null){let ce=u.views;h!==null&&(e.setRenderTargetFramebuffer(w,h.framebuffer),e.setRenderTarget(w));let Ze=!1;ce.length!==F.cameras.length&&(F.cameras.length=0,Ze=!0);for(let ut=0;ut<ce.length;ut++){let Et=ce[ut],Dt=null;if(h!==null)Dt=h.getViewport(Et);else{let Bt=d.getViewSubImage(f,Et);Dt=Bt.viewport,ut===0&&(e.setRenderTargetTextures(w,Bt.colorTexture,Bt.depthStencilTexture),e.setRenderTarget(w))}let Qe=T[ut];Qe===void 0&&(Qe=new En,Qe.layers.enable(ut),Qe.viewport=new Lt,T[ut]=Qe),Qe.matrix.fromArray(Et.transform.matrix),Qe.matrix.decompose(Qe.position,Qe.quaternion,Qe.scale),Qe.projectionMatrix.fromArray(Et.projectionMatrix),Qe.projectionMatrixInverse.copy(Qe.projectionMatrix).invert(),Qe.viewport.set(Dt.x,Dt.y,Dt.width,Dt.height),ut===0&&(F.matrix.copy(Qe.matrix),F.matrix.decompose(F.position,F.quaternion,F.scale)),Ze===!0&&F.cameras.push(Qe)}let ke=r.enabledFeatures;if(ke&&ke.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&y){d=i.getBinding();let ut=d.getDepthInformation(ce[0]);ut&&ut.isValid&&ut.texture&&g.init(ut,r.renderState)}if(ke&&ke.includes("camera-access")&&y){e.state.unbindTexture(),d=i.getBinding();for(let ut=0;ut<ce.length;ut++){let Et=ce[ut].camera;if(Et){let Dt=m[Et];Dt||(Dt=new pl,m[Et]=Dt);let Qe=d.getCameraImage(Et);Dt.sourceTexture=Qe}}}}for(let ce=0;ce<D.length;ce++){let Ze=C[ce],ke=D[ce];Ze!==null&&ke!==void 0&&ke.update(Ze,re,l||s)}Je&&Je(X,re),re.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:re}),p=null}let Nt=new Uw;Nt.setAnimationLoop(Pt),this.setAnimationLoop=function(X){Je=X},this.dispose=function(){}}},vs=new ao,ik=new Ot;function rk(n,e){function t(g,m){g.matrixAutoUpdate===!0&&g.updateMatrix(),m.value.copy(g.matrix)}function i(g,m){m.color.getRGB(g.fogColor.value,g0(n)),m.isFog?(g.fogNear.value=m.near,g.fogFar.value=m.far):m.isFogExp2&&(g.fogDensity.value=m.density)}function r(g,m,E,S,w){m.isMeshBasicMaterial?o(g,m):m.isMeshLambertMaterial?(o(g,m),m.envMap&&(g.envMapIntensity.value=m.envMapIntensity)):m.isMeshToonMaterial?(o(g,m),d(g,m)):m.isMeshPhongMaterial?(o(g,m),u(g,m),m.envMap&&(g.envMapIntensity.value=m.envMapIntensity)):m.isMeshStandardMaterial?(o(g,m),f(g,m),m.isMeshPhysicalMaterial&&h(g,m,w)):m.isMeshMatcapMaterial?(o(g,m),p(g,m)):m.isMeshDepthMaterial?o(g,m):m.isMeshDistanceMaterial?(o(g,m),y(g,m)):m.isMeshNormalMaterial?o(g,m):m.isLineBasicMaterial?(s(g,m),m.isLineDashedMaterial&&a(g,m)):m.isPointsMaterial?c(g,m,E,S):m.isSpriteMaterial?l(g,m):m.isShadowMaterial?(g.color.value.copy(m.color),g.opacity.value=m.opacity):m.isShaderMaterial&&(m.uniformsNeedUpdate=!1)}function o(g,m){g.opacity.value=m.opacity,m.color&&g.diffuse.value.copy(m.color),m.emissive&&g.emissive.value.copy(m.emissive).multiplyScalar(m.emissiveIntensity),m.map&&(g.map.value=m.map,t(m.map,g.mapTransform)),m.alphaMap&&(g.alphaMap.value=m.alphaMap,t(m.alphaMap,g.alphaMapTransform)),m.bumpMap&&(g.bumpMap.value=m.bumpMap,t(m.bumpMap,g.bumpMapTransform),g.bumpScale.value=m.bumpScale,m.side===kn&&(g.bumpScale.value*=-1)),m.normalMap&&(g.normalMap.value=m.normalMap,t(m.normalMap,g.normalMapTransform),g.normalScale.value.copy(m.normalScale),m.side===kn&&g.normalScale.value.negate()),m.displacementMap&&(g.displacementMap.value=m.displacementMap,t(m.displacementMap,g.displacementMapTransform),g.displacementScale.value=m.displacementScale,g.displacementBias.value=m.displacementBias),m.emissiveMap&&(g.emissiveMap.value=m.emissiveMap,t(m.emissiveMap,g.emissiveMapTransform)),m.specularMap&&(g.specularMap.value=m.specularMap,t(m.specularMap,g.specularMapTransform)),m.alphaTest>0&&(g.alphaTest.value=m.alphaTest);let E=e.get(m),S=E.envMap,w=E.envMapRotation;S&&(g.envMap.value=S,vs.copy(w),vs.x*=-1,vs.y*=-1,vs.z*=-1,S.isCubeTexture&&S.isRenderTargetTexture===!1&&(vs.y*=-1,vs.z*=-1),g.envMapRotation.value.setFromMatrix4(ik.makeRotationFromEuler(vs)),g.flipEnvMap.value=S.isCubeTexture&&S.isRenderTargetTexture===!1?-1:1,g.reflectivity.value=m.reflectivity,g.ior.value=m.ior,g.refractionRatio.value=m.refractionRatio),m.lightMap&&(g.lightMap.value=m.lightMap,g.lightMapIntensity.value=m.lightMapIntensity,t(m.lightMap,g.lightMapTransform)),m.aoMap&&(g.aoMap.value=m.aoMap,g.aoMapIntensity.value=m.aoMapIntensity,t(m.aoMap,g.aoMapTransform))}function s(g,m){g.diffuse.value.copy(m.color),g.opacity.value=m.opacity,m.map&&(g.map.value=m.map,t(m.map,g.mapTransform))}function a(g,m){g.dashSize.value=m.dashSize,g.totalSize.value=m.dashSize+m.gapSize,g.scale.value=m.scale}function c(g,m,E,S){g.diffuse.value.copy(m.color),g.opacity.value=m.opacity,g.size.value=m.size*E,g.scale.value=S*.5,m.map&&(g.map.value=m.map,t(m.map,g.uvTransform)),m.alphaMap&&(g.alphaMap.value=m.alphaMap,t(m.alphaMap,g.alphaMapTransform)),m.alphaTest>0&&(g.alphaTest.value=m.alphaTest)}function l(g,m){g.diffuse.value.copy(m.color),g.opacity.value=m.opacity,g.rotation.value=m.rotation,m.map&&(g.map.value=m.map,t(m.map,g.mapTransform)),m.alphaMap&&(g.alphaMap.value=m.alphaMap,t(m.alphaMap,g.alphaMapTransform)),m.alphaTest>0&&(g.alphaTest.value=m.alphaTest)}function u(g,m){g.specular.value.copy(m.specular),g.shininess.value=Math.max(m.shininess,1e-4)}function d(g,m){m.gradientMap&&(g.gradientMap.value=m.gradientMap)}function f(g,m){g.metalness.value=m.metalness,m.metalnessMap&&(g.metalnessMap.value=m.metalnessMap,t(m.metalnessMap,g.metalnessMapTransform)),g.roughness.value=m.roughness,m.roughnessMap&&(g.roughnessMap.value=m.roughnessMap,t(m.roughnessMap,g.roughnessMapTransform)),m.envMap&&(g.envMapIntensity.value=m.envMapIntensity)}function h(g,m,E){g.ior.value=m.ior,m.sheen>0&&(g.sheenColor.value.copy(m.sheenColor).multiplyScalar(m.sheen),g.sheenRoughness.value=m.sheenRoughness,m.sheenColorMap&&(g.sheenColorMap.value=m.sheenColorMap,t(m.sheenColorMap,g.sheenColorMapTransform)),m.sheenRoughnessMap&&(g.sheenRoughnessMap.value=m.sheenRoughnessMap,t(m.sheenRoughnessMap,g.sheenRoughnessMapTransform))),m.clearcoat>0&&(g.clearcoat.value=m.clearcoat,g.clearcoatRoughness.value=m.clearcoatRoughness,m.clearcoatMap&&(g.clearcoatMap.value=m.clearcoatMap,t(m.clearcoatMap,g.clearcoatMapTransform)),m.clearcoatRoughnessMap&&(g.clearcoatRoughnessMap.value=m.clearcoatRoughnessMap,t(m.clearcoatRoughnessMap,g.clearcoatRoughnessMapTransform)),m.clearcoatNormalMap&&(g.clearcoatNormalMap.value=m.clearcoatNormalMap,t(m.clearcoatNormalMap,g.clearcoatNormalMapTransform),g.clearcoatNormalScale.value.copy(m.clearcoatNormalScale),m.side===kn&&g.clearcoatNormalScale.value.negate())),m.dispersion>0&&(g.dispersion.value=m.dispersion),m.iridescence>0&&(g.iridescence.value=m.iridescence,g.iridescenceIOR.value=m.iridescenceIOR,g.iridescenceThicknessMinimum.value=m.iridescenceThicknessRange[0],g.iridescenceThicknessMaximum.value=m.iridescenceThicknessRange[1],m.iridescenceMap&&(g.iridescenceMap.value=m.iridescenceMap,t(m.iridescenceMap,g.iridescenceMapTransform)),m.iridescenceThicknessMap&&(g.iridescenceThicknessMap.value=m.iridescenceThicknessMap,t(m.iridescenceThicknessMap,g.iridescenceThicknessMapTransform))),m.transmission>0&&(g.transmission.value=m.transmission,g.transmissionSamplerMap.value=E.texture,g.transmissionSamplerSize.value.set(E.width,E.height),m.transmissionMap&&(g.transmissionMap.value=m.transmissionMap,t(m.transmissionMap,g.transmissionMapTransform)),g.thickness.value=m.thickness,m.thicknessMap&&(g.thicknessMap.value=m.thicknessMap,t(m.thicknessMap,g.thicknessMapTransform)),g.attenuationDistance.value=m.attenuationDistance,g.attenuationColor.value.copy(m.attenuationColor)),m.anisotropy>0&&(g.anisotropyVector.value.set(m.anisotropy*Math.cos(m.anisotropyRotation),m.anisotropy*Math.sin(m.anisotropyRotation)),m.anisotropyMap&&(g.anisotropyMap.value=m.anisotropyMap,t(m.anisotropyMap,g.anisotropyMapTransform))),g.specularIntensity.value=m.specularIntensity,g.specularColor.value.copy(m.specularColor),m.specularColorMap&&(g.specularColorMap.value=m.specularColorMap,t(m.specularColorMap,g.specularColorMapTransform)),m.specularIntensityMap&&(g.specularIntensityMap.value=m.specularIntensityMap,t(m.specularIntensityMap,g.specularIntensityMapTransform))}function p(g,m){m.matcap&&(g.matcap.value=m.matcap)}function y(g,m){let E=e.get(m).light;g.referencePosition.value.setFromMatrixPosition(E.matrixWorld),g.nearDistance.value=E.shadow.camera.near,g.farDistance.value=E.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function ok(n,e,t,i){let r={},o={},s=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(E,S){let w=S.program;i.uniformBlockBinding(E,w)}function l(E,S){let w=r[E.id];w===void 0&&(p(E),w=u(E),r[E.id]=w,E.addEventListener("dispose",g));let D=S.program;i.updateUBOMapping(E,D);let C=e.render.frame;o[E.id]!==C&&(f(E),o[E.id]=C)}function u(E){let S=d();E.__bindingPointIndex=S;let w=n.createBuffer(),D=E.__size,C=E.usage;return n.bindBuffer(n.UNIFORM_BUFFER,w),n.bufferData(n.UNIFORM_BUFFER,D,C),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,S,w),w}function d(){for(let E=0;E<a;E++)if(s.indexOf(E)===-1)return s.push(E),E;return Ue("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(E){let S=r[E.id],w=E.uniforms,D=E.__cache;n.bindBuffer(n.UNIFORM_BUFFER,S);for(let C=0,I=w.length;C<I;C++){let _=Array.isArray(w[C])?w[C]:[w[C]];for(let b=0,W=_.length;b<W;b++){let T=_[b];if(h(T,C,b,D)===!0){let F=T.__offset,H=Array.isArray(T.value)?T.value:[T.value],G=0;for(let B=0;B<H.length;B++){let U=H[B],O=y(U);typeof U=="number"||typeof U=="boolean"?(T.__data[0]=U,n.bufferSubData(n.UNIFORM_BUFFER,F+G,T.__data)):U.isMatrix3?(T.__data[0]=U.elements[0],T.__data[1]=U.elements[1],T.__data[2]=U.elements[2],T.__data[3]=0,T.__data[4]=U.elements[3],T.__data[5]=U.elements[4],T.__data[6]=U.elements[5],T.__data[7]=0,T.__data[8]=U.elements[6],T.__data[9]=U.elements[7],T.__data[10]=U.elements[8],T.__data[11]=0):(U.toArray(T.__data,G),G+=O.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,F,T.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(E,S,w,D){let C=E.value,I=S+"_"+w;if(D[I]===void 0)return typeof C=="number"||typeof C=="boolean"?D[I]=C:D[I]=C.clone(),!0;{let _=D[I];if(typeof C=="number"||typeof C=="boolean"){if(_!==C)return D[I]=C,!0}else if(_.equals(C)===!1)return _.copy(C),!0}return!1}function p(E){let S=E.uniforms,w=0,D=16;for(let I=0,_=S.length;I<_;I++){let b=Array.isArray(S[I])?S[I]:[S[I]];for(let W=0,T=b.length;W<T;W++){let F=b[W],H=Array.isArray(F.value)?F.value:[F.value];for(let G=0,B=H.length;G<B;G++){let U=H[G],O=y(U),Z=w%D,K=Z%O.boundary,he=Z+K;w+=K,he!==0&&D-he<O.storage&&(w+=D-he),F.__data=new Float32Array(O.storage/Float32Array.BYTES_PER_ELEMENT),F.__offset=w,w+=O.storage}}}let C=w%D;return C>0&&(w+=D-C),E.__size=w,E.__cache={},this}function y(E){let S={boundary:0,storage:0};return typeof E=="number"||typeof E=="boolean"?(S.boundary=4,S.storage=4):E.isVector2?(S.boundary=8,S.storage=8):E.isVector3||E.isColor?(S.boundary=16,S.storage=12):E.isVector4?(S.boundary=16,S.storage=16):E.isMatrix3?(S.boundary=48,S.storage=48):E.isMatrix4?(S.boundary=64,S.storage=64):E.isTexture?Fe("WebGLRenderer: Texture samplers can not be part of an uniforms group."):Fe("WebGLRenderer: Unsupported uniform value type.",E),S}function g(E){let S=E.target;S.removeEventListener("dispose",g);let w=s.indexOf(S.__bindingPointIndex);s.splice(w,1),n.deleteBuffer(r[S.id]),delete r[S.id],delete o[S.id]}function m(){for(let E in r)n.deleteBuffer(r[E]);s=[],r={},o={}}return{bind:c,update:l,dispose:m}}var sk=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),tr=null;function ak(){return tr===null&&(tr=new ch(sk,16,16,ps,er),tr.name="DFG_LUT",tr.minFilter=hn,tr.magFilter=hn,tr.wrapS=Yi,tr.wrapT=Yi,tr.generateMipmaps=!1,tr.needsUpdate=!0),tr}var Ep=class{constructor(e={}){let{canvas:t=lw(),context:i=null,depth:r=!0,stencil:o=!1,alpha:s=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=$n}=e;this.isWebGLRenderer=!0;let p;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");p=i.getContextAttributes().alpha}else p=s;let y=h,g=new Set([Lh,Oh,Ph]),m=new Set([$n,Ri,Ua,Ba,Rh,Nh]),E=new Uint32Array(4),S=new Int32Array(4),w=null,D=null,C=[],I=[],_=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=Ai,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let b=this,W=!1;this._outputColorSpace=Fn;let T=0,F=0,H=null,G=-1,B=null,U=new Lt,O=new Lt,Z=null,K=new nt(0),he=0,_e=t.width,ge=t.height,Je=1,Pt=null,Nt=null,X=new Lt(0,0,_e,ge),re=new Lt(0,0,_e,ge),ce=!1,Ze=new Na,ke=!1,He=!1,en=new Ot,ut=new L,Et=new Lt,Dt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Qe=!1;function Bt(){return H===null?Je:1}let A=i;function jt(x,P){return t.getContext(x,P)}try{let x={alpha:!0,depth:r,stencil:o,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"183"}`),t.addEventListener("webglcontextlost",be,!1),t.addEventListener("webglcontextrestored",ze,!1),t.addEventListener("webglcontextcreationerror",Rt,!1),A===null){let P="webgl2";if(A=jt(P,x),A===null)throw jt(P)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw Ue("WebGLRenderer: "+x.message),x}let gt,At,we,M,v,N,q,Y,$,xe,oe,Re,Be,Q,ne,Ee,Me,pe,et,R,se,ie,ye;function ee(){gt=new mF(A),gt.init(),se=new ek(A,gt),At=new aF(A,gt,e,se),we=new K2(A,gt),At.reversedDepthBuffer&&f&&we.buffers.depth.setReversed(!0),M=new yF(A),v=new U2,N=new Q2(A,gt,we,v,At,se,M),q=new pF(b),Y=new MP(A),ie=new oF(A,Y),$=new gF(A,Y,M,ie),xe=new xF(A,$,Y,ie,M),pe=new _F(A,At,N),ne=new cF(v),oe=new k2(b,q,gt,At,ie,ne),Re=new rk(b,v),Be=new V2,Q=new $2(gt),Me=new rF(b,q,we,xe,p,c),Ee=new J2(b,xe,At),ye=new ok(A,M,At,we),et=new sF(A,gt,M),R=new vF(A,gt,M),M.programs=oe.programs,b.capabilities=At,b.extensions=gt,b.properties=v,b.renderLists=Be,b.shadowMap=Ee,b.state=we,b.info=M}ee(),y!==$n&&(_=new bF(y,t.width,t.height,r,o));let j=new O0(b,A);this.xr=j,this.getContext=function(){return A},this.getContextAttributes=function(){return A.getContextAttributes()},this.forceContextLoss=function(){let x=gt.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=gt.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return Je},this.setPixelRatio=function(x){x!==void 0&&(Je=x,this.setSize(_e,ge,!1))},this.getSize=function(x){return x.set(_e,ge)},this.setSize=function(x,P,z=!0){if(j.isPresenting){Fe("WebGLRenderer: Can't change size while VR device is presenting.");return}_e=x,ge=P,t.width=Math.floor(x*Je),t.height=Math.floor(P*Je),z===!0&&(t.style.width=x+"px",t.style.height=P+"px"),_!==null&&_.setSize(t.width,t.height),this.setViewport(0,0,x,P)},this.getDrawingBufferSize=function(x){return x.set(_e*Je,ge*Je).floor()},this.setDrawingBufferSize=function(x,P,z){_e=x,ge=P,Je=z,t.width=Math.floor(x*z),t.height=Math.floor(P*z),this.setViewport(0,0,x,P)},this.setEffects=function(x){if(y===$n){console.error("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let P=0;P<x.length;P++)if(x[P].isOutputPass===!0){console.warn("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}_.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(U)},this.getViewport=function(x){return x.copy(X)},this.setViewport=function(x,P,z,V){x.isVector4?X.set(x.x,x.y,x.z,x.w):X.set(x,P,z,V),we.viewport(U.copy(X).multiplyScalar(Je).round())},this.getScissor=function(x){return x.copy(re)},this.setScissor=function(x,P,z,V){x.isVector4?re.set(x.x,x.y,x.z,x.w):re.set(x,P,z,V),we.scissor(O.copy(re).multiplyScalar(Je).round())},this.getScissorTest=function(){return ce},this.setScissorTest=function(x){we.setScissorTest(ce=x)},this.setOpaqueSort=function(x){Pt=x},this.setTransparentSort=function(x){Nt=x},this.getClearColor=function(x){return x.copy(Me.getClearColor())},this.setClearColor=function(){Me.setClearColor(...arguments)},this.getClearAlpha=function(){return Me.getClearAlpha()},this.setClearAlpha=function(){Me.setClearAlpha(...arguments)},this.clear=function(x=!0,P=!0,z=!0){let V=0;if(x){let k=!1;if(H!==null){let de=H.texture.format;k=g.has(de)}if(k){let de=H.texture.type,ve=m.has(de),fe=Me.getClearColor(),Se=Me.getClearAlpha(),De=fe.r,qe=fe.g,tt=fe.b;ve?(E[0]=De,E[1]=qe,E[2]=tt,E[3]=Se,A.clearBufferuiv(A.COLOR,0,E)):(S[0]=De,S[1]=qe,S[2]=tt,S[3]=Se,A.clearBufferiv(A.COLOR,0,S))}else V|=A.COLOR_BUFFER_BIT}P&&(V|=A.DEPTH_BUFFER_BIT),z&&(V|=A.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),V!==0&&A.clear(V)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",be,!1),t.removeEventListener("webglcontextrestored",ze,!1),t.removeEventListener("webglcontextcreationerror",Rt,!1),Me.dispose(),Be.dispose(),Q.dispose(),v.dispose(),q.dispose(),xe.dispose(),ie.dispose(),ye.dispose(),oe.dispose(),j.dispose(),j.removeEventListener("sessionstart",F0),j.removeEventListener("sessionend",k0),yo.stop()};function be(x){x.preventDefault(),m0("WebGLRenderer: Context Lost."),W=!0}function ze(){m0("WebGLRenderer: Context Restored."),W=!1;let x=M.autoReset,P=Ee.enabled,z=Ee.autoUpdate,V=Ee.needsUpdate,k=Ee.type;ee(),M.autoReset=x,Ee.enabled=P,Ee.autoUpdate=z,Ee.needsUpdate=V,Ee.type=k}function Rt(x){Ue("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function bt(x){let P=x.target;P.removeEventListener("dispose",bt),ir(P)}function ir(x){rr(x),v.remove(x)}function rr(x){let P=v.get(x).programs;P!==void 0&&(P.forEach(function(z){oe.releaseProgram(z)}),x.isShaderMaterial&&oe.releaseShaderCache(x))}this.renderBufferDirect=function(x,P,z,V,k,de){P===null&&(P=Dt);let ve=k.isMesh&&k.matrixWorld.determinant()<0,fe=Yw(x,P,z,V,k);we.setMaterial(V,ve);let Se=z.index,De=1;if(V.wireframe===!0){if(Se=$.getWireframeAttribute(z),Se===void 0)return;De=2}let qe=z.drawRange,tt=z.attributes.position,Ae=qe.start*De,St=(qe.start+qe.count)*De;de!==null&&(Ae=Math.max(Ae,de.start*De),St=Math.min(St,(de.start+de.count)*De)),Se!==null?(Ae=Math.max(Ae,0),St=Math.min(St,Se.count)):tt!=null&&(Ae=Math.max(Ae,0),St=Math.min(St,tt.count));let Vt=St-Ae;if(Vt<0||Vt===1/0)return;ie.setup(k,V,fe,z,Se);let Ft,wt=et;if(Se!==null&&(Ft=Y.get(Se),wt=R,wt.setIndex(Ft)),k.isMesh)V.wireframe===!0?(we.setLineWidth(V.wireframeLinewidth*Bt()),wt.setMode(A.LINES)):wt.setMode(A.TRIANGLES);else if(k.isLine){let pn=V.linewidth;pn===void 0&&(pn=1),we.setLineWidth(pn*Bt()),k.isLineSegments?wt.setMode(A.LINES):k.isLineLoop?wt.setMode(A.LINE_LOOP):wt.setMode(A.LINE_STRIP)}else k.isPoints?wt.setMode(A.POINTS):k.isSprite&&wt.setMode(A.TRIANGLES);if(k.isBatchedMesh)if(k._multiDrawInstances!==null)al("WebGLRenderer: renderMultiDrawInstances has been deprecated and will be removed in r184. Append to renderMultiDraw arguments and use indirection."),wt.renderMultiDrawInstances(k._multiDrawStarts,k._multiDrawCounts,k._multiDrawCount,k._multiDrawInstances);else if(gt.get("WEBGL_multi_draw"))wt.renderMultiDraw(k._multiDrawStarts,k._multiDrawCounts,k._multiDrawCount);else{let pn=k._multiDrawStarts,Ce=k._multiDrawCounts,qn=k._multiDrawCount,ft=Se?Y.get(Se).bytesPerElement:1,hi=v.get(V).currentProgram.getUniforms();for(let Pi=0;Pi<qn;Pi++)hi.setValue(A,"_gl_DrawID",Pi),wt.render(pn[Pi]/ft,Ce[Pi])}else if(k.isInstancedMesh)wt.renderInstances(Ae,Vt,k.count);else if(z.isInstancedBufferGeometry){let pn=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,Ce=Math.min(z.instanceCount,pn);wt.renderInstances(Ae,Vt,Ce)}else wt.render(Ae,Vt)};function L0(x,P,z){x.transparent===!0&&x.side===Ki&&x.forceSinglePass===!1?(x.side=kn,x.needsUpdate=!0,kl(x,P,z),x.side=Cr,x.needsUpdate=!0,kl(x,P,z),x.side=Ki):kl(x,P,z)}this.compile=function(x,P,z=null){z===null&&(z=x),D=Q.get(z),D.init(P),I.push(D),z.traverseVisible(function(k){k.isLight&&k.layers.test(P.layers)&&(D.pushLight(k),k.castShadow&&D.pushShadow(k))}),x!==z&&x.traverseVisible(function(k){k.isLight&&k.layers.test(P.layers)&&(D.pushLight(k),k.castShadow&&D.pushShadow(k))}),D.setupLights();let V=new Set;return x.traverse(function(k){if(!(k.isMesh||k.isPoints||k.isLine||k.isSprite))return;let de=k.material;if(de)if(Array.isArray(de))for(let ve=0;ve<de.length;ve++){let fe=de[ve];L0(fe,z,k),V.add(fe)}else L0(de,z,k),V.add(de)}),D=I.pop(),V},this.compileAsync=function(x,P,z=null){let V=this.compile(x,P,z);return new Promise(k=>{function de(){if(V.forEach(function(ve){v.get(ve).currentProgram.isReady()&&V.delete(ve)}),V.size===0){k(x);return}setTimeout(de,10)}gt.get("KHR_parallel_shader_compile")!==null?de():setTimeout(de,10)})};let Ip=null;function Xw(x){Ip&&Ip(x)}function F0(){yo.stop()}function k0(){yo.start()}let yo=new Uw;yo.setAnimationLoop(Xw),typeof self<"u"&&yo.setContext(self),this.setAnimationLoop=function(x){Ip=x,j.setAnimationLoop(x),x===null?yo.stop():yo.start()},j.addEventListener("sessionstart",F0),j.addEventListener("sessionend",k0),this.render=function(x,P){if(P!==void 0&&P.isCamera!==!0){Ue("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(W===!0)return;let z=j.enabled===!0&&j.isPresenting===!0,V=_!==null&&(H===null||z)&&_.begin(b,H);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),P.parent===null&&P.matrixWorldAutoUpdate===!0&&P.updateMatrixWorld(),j.enabled===!0&&j.isPresenting===!0&&(_===null||_.isCompositing()===!1)&&(j.cameraAutoUpdate===!0&&j.updateCamera(P),P=j.getCamera()),x.isScene===!0&&x.onBeforeRender(b,x,P,H),D=Q.get(x,I.length),D.init(P),I.push(D),en.multiplyMatrices(P.projectionMatrix,P.matrixWorldInverse),Ze.setFromProjectionMatrix(en,Di,P.reversedDepth),He=this.localClippingEnabled,ke=ne.init(this.clippingPlanes,He),w=Be.get(x,C.length),w.init(),C.push(w),j.enabled===!0&&j.isPresenting===!0){let ve=b.xr.getDepthSensingMesh();ve!==null&&Ap(ve,P,-1/0,b.sortObjects)}Ap(x,P,0,b.sortObjects),w.finish(),b.sortObjects===!0&&w.sort(Pt,Nt),Qe=j.enabled===!1||j.isPresenting===!1||j.hasDepthSensing()===!1,Qe&&Me.addToRenderList(w,x),this.info.render.frame++,ke===!0&&ne.beginShadows();let k=D.state.shadowsArray;if(Ee.render(k,x,P),ke===!0&&ne.endShadows(),this.info.autoReset===!0&&this.info.reset(),(V&&_.hasRenderPass())===!1){let ve=w.opaque,fe=w.transmissive;if(D.setupLights(),P.isArrayCamera){let Se=P.cameras;if(fe.length>0)for(let De=0,qe=Se.length;De<qe;De++){let tt=Se[De];B0(ve,fe,x,tt)}Qe&&Me.render(x);for(let De=0,qe=Se.length;De<qe;De++){let tt=Se[De];U0(w,x,tt,tt.viewport)}}else fe.length>0&&B0(ve,fe,x,P),Qe&&Me.render(x),U0(w,x,P)}H!==null&&F===0&&(N.updateMultisampleRenderTarget(H),N.updateRenderTargetMipmap(H)),V&&_.end(b),x.isScene===!0&&x.onAfterRender(b,x,P),ie.resetDefaultState(),G=-1,B=null,I.pop(),I.length>0?(D=I[I.length-1],ke===!0&&ne.setGlobalState(b.clippingPlanes,D.state.camera)):D=null,C.pop(),C.length>0?w=C[C.length-1]:w=null};function Ap(x,P,z,V){if(x.visible===!1)return;if(x.layers.test(P.layers)){if(x.isGroup)z=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(P);else if(x.isLight)D.pushLight(x),x.castShadow&&D.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||Ze.intersectsSprite(x)){V&&Et.setFromMatrixPosition(x.matrixWorld).applyMatrix4(en);let ve=xe.update(x),fe=x.material;fe.visible&&w.push(x,ve,fe,z,Et.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||Ze.intersectsObject(x))){let ve=xe.update(x),fe=x.material;if(V&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),Et.copy(x.boundingSphere.center)):(ve.boundingSphere===null&&ve.computeBoundingSphere(),Et.copy(ve.boundingSphere.center)),Et.applyMatrix4(x.matrixWorld).applyMatrix4(en)),Array.isArray(fe)){let Se=ve.groups;for(let De=0,qe=Se.length;De<qe;De++){let tt=Se[De],Ae=fe[tt.materialIndex];Ae&&Ae.visible&&w.push(x,ve,Ae,z,Et.z,tt)}}else fe.visible&&w.push(x,ve,fe,z,Et.z,null)}}let de=x.children;for(let ve=0,fe=de.length;ve<fe;ve++)Ap(de[ve],P,z,V)}function U0(x,P,z,V){let{opaque:k,transmissive:de,transparent:ve}=x;D.setupLightsView(z),ke===!0&&ne.setGlobalState(b.clippingPlanes,z),V&&we.viewport(U.copy(V)),k.length>0&&Fl(k,P,z),de.length>0&&Fl(de,P,z),ve.length>0&&Fl(ve,P,z),we.buffers.depth.setTest(!0),we.buffers.depth.setMask(!0),we.buffers.color.setMask(!0),we.setPolygonOffset(!1)}function B0(x,P,z,V){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(D.state.transmissionRenderTarget[V.id]===void 0){let Ae=gt.has("EXT_color_buffer_half_float")||gt.has("EXT_color_buffer_float");D.state.transmissionRenderTarget[V.id]=new ti(1,1,{generateMipmaps:!0,type:Ae?er:$n,minFilter:mo,samples:Math.max(4,At.samples),stencilBuffer:o,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:ct.workingColorSpace})}let de=D.state.transmissionRenderTarget[V.id],ve=V.viewport||U;de.setSize(ve.z*b.transmissionResolutionScale,ve.w*b.transmissionResolutionScale);let fe=b.getRenderTarget(),Se=b.getActiveCubeFace(),De=b.getActiveMipmapLevel();b.setRenderTarget(de),b.getClearColor(K),he=b.getClearAlpha(),he<1&&b.setClearColor(16777215,.5),b.clear(),Qe&&Me.render(z);let qe=b.toneMapping;b.toneMapping=Ai;let tt=V.viewport;if(V.viewport!==void 0&&(V.viewport=void 0),D.setupLightsView(V),ke===!0&&ne.setGlobalState(b.clippingPlanes,V),Fl(x,z,V),N.updateMultisampleRenderTarget(de),N.updateRenderTargetMipmap(de),gt.has("WEBGL_multisampled_render_to_texture")===!1){let Ae=!1;for(let St=0,Vt=P.length;St<Vt;St++){let Ft=P[St],{object:wt,geometry:pn,material:Ce,group:qn}=Ft;if(Ce.side===Ki&&wt.layers.test(V.layers)){let ft=Ce.side;Ce.side=kn,Ce.needsUpdate=!0,V0(wt,z,V,pn,Ce,qn),Ce.side=ft,Ce.needsUpdate=!0,Ae=!0}}Ae===!0&&(N.updateMultisampleRenderTarget(de),N.updateRenderTargetMipmap(de))}b.setRenderTarget(fe,Se,De),b.setClearColor(K,he),tt!==void 0&&(V.viewport=tt),b.toneMapping=qe}function Fl(x,P,z){let V=P.isScene===!0?P.overrideMaterial:null;for(let k=0,de=x.length;k<de;k++){let ve=x[k],{object:fe,geometry:Se,group:De}=ve,qe=ve.material;qe.allowOverride===!0&&V!==null&&(qe=V),fe.layers.test(z.layers)&&V0(fe,P,z,Se,qe,De)}}function V0(x,P,z,V,k,de){x.onBeforeRender(b,P,z,V,k,de),x.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),k.onBeforeRender(b,P,z,V,x,de),k.transparent===!0&&k.side===Ki&&k.forceSinglePass===!1?(k.side=kn,k.needsUpdate=!0,b.renderBufferDirect(z,P,V,k,x,de),k.side=Cr,k.needsUpdate=!0,b.renderBufferDirect(z,P,V,k,x,de),k.side=Ki):b.renderBufferDirect(z,P,V,k,x,de),x.onAfterRender(b,P,z,V,k,de)}function kl(x,P,z){P.isScene!==!0&&(P=Dt);let V=v.get(x),k=D.state.lights,de=D.state.shadowsArray,ve=k.state.version,fe=oe.getParameters(x,k.state,de,P,z),Se=oe.getProgramCacheKey(fe),De=V.programs;V.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?P.environment:null,V.fog=P.fog;let qe=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;V.envMap=q.get(x.envMap||V.environment,qe),V.envMapRotation=V.environment!==null&&x.envMap===null?P.environmentRotation:x.envMapRotation,De===void 0&&(x.addEventListener("dispose",bt),De=new Map,V.programs=De);let tt=De.get(Se);if(tt!==void 0){if(V.currentProgram===tt&&V.lightsStateVersion===ve)return z0(x,fe),tt}else fe.uniforms=oe.getUniforms(x),x.onBeforeCompile(fe,b),tt=oe.acquireProgram(fe,Se),De.set(Se,tt),V.uniforms=fe.uniforms;let Ae=V.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(Ae.clippingPlanes=ne.uniform),z0(x,fe),V.needsLights=Jw(x),V.lightsStateVersion=ve,V.needsLights&&(Ae.ambientLightColor.value=k.state.ambient,Ae.lightProbe.value=k.state.probe,Ae.directionalLights.value=k.state.directional,Ae.directionalLightShadows.value=k.state.directionalShadow,Ae.spotLights.value=k.state.spot,Ae.spotLightShadows.value=k.state.spotShadow,Ae.rectAreaLights.value=k.state.rectArea,Ae.ltc_1.value=k.state.rectAreaLTC1,Ae.ltc_2.value=k.state.rectAreaLTC2,Ae.pointLights.value=k.state.point,Ae.pointLightShadows.value=k.state.pointShadow,Ae.hemisphereLights.value=k.state.hemi,Ae.directionalShadowMatrix.value=k.state.directionalShadowMatrix,Ae.spotLightMatrix.value=k.state.spotLightMatrix,Ae.spotLightMap.value=k.state.spotLightMap,Ae.pointShadowMatrix.value=k.state.pointShadowMatrix),V.currentProgram=tt,V.uniformsList=null,tt}function H0(x){if(x.uniformsList===null){let P=x.currentProgram.getUniforms();x.uniformsList=Ha.seqWithValue(P.seq,x.uniforms)}return x.uniformsList}function z0(x,P){let z=v.get(x);z.outputColorSpace=P.outputColorSpace,z.batching=P.batching,z.batchingColor=P.batchingColor,z.instancing=P.instancing,z.instancingColor=P.instancingColor,z.instancingMorph=P.instancingMorph,z.skinning=P.skinning,z.morphTargets=P.morphTargets,z.morphNormals=P.morphNormals,z.morphColors=P.morphColors,z.morphTargetsCount=P.morphTargetsCount,z.numClippingPlanes=P.numClippingPlanes,z.numIntersection=P.numClipIntersection,z.vertexAlphas=P.vertexAlphas,z.vertexTangents=P.vertexTangents,z.toneMapping=P.toneMapping}function Yw(x,P,z,V,k){P.isScene!==!0&&(P=Dt),N.resetTextureUnits();let de=P.fog,ve=V.isMeshStandardMaterial||V.isMeshLambertMaterial||V.isMeshPhongMaterial?P.environment:null,fe=H===null?b.outputColorSpace:H.isXRRenderTarget===!0?H.texture.colorSpace:us,Se=V.isMeshStandardMaterial||V.isMeshLambertMaterial&&!V.envMap||V.isMeshPhongMaterial&&!V.envMap,De=q.get(V.envMap||ve,Se),qe=V.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,tt=!!z.attributes.tangent&&(!!V.normalMap||V.anisotropy>0),Ae=!!z.morphAttributes.position,St=!!z.morphAttributes.normal,Vt=!!z.morphAttributes.color,Ft=Ai;V.toneMapped&&(H===null||H.isXRRenderTarget===!0)&&(Ft=b.toneMapping);let wt=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,pn=wt!==void 0?wt.length:0,Ce=v.get(V),qn=D.state.lights;if(ke===!0&&(He===!0||x!==B)){let tn=x===B&&V.id===G;ne.setState(V,x,tn)}let ft=!1;V.version===Ce.__version?(Ce.needsLights&&Ce.lightsStateVersion!==qn.state.version||Ce.outputColorSpace!==fe||k.isBatchedMesh&&Ce.batching===!1||!k.isBatchedMesh&&Ce.batching===!0||k.isBatchedMesh&&Ce.batchingColor===!0&&k.colorTexture===null||k.isBatchedMesh&&Ce.batchingColor===!1&&k.colorTexture!==null||k.isInstancedMesh&&Ce.instancing===!1||!k.isInstancedMesh&&Ce.instancing===!0||k.isSkinnedMesh&&Ce.skinning===!1||!k.isSkinnedMesh&&Ce.skinning===!0||k.isInstancedMesh&&Ce.instancingColor===!0&&k.instanceColor===null||k.isInstancedMesh&&Ce.instancingColor===!1&&k.instanceColor!==null||k.isInstancedMesh&&Ce.instancingMorph===!0&&k.morphTexture===null||k.isInstancedMesh&&Ce.instancingMorph===!1&&k.morphTexture!==null||Ce.envMap!==De||V.fog===!0&&Ce.fog!==de||Ce.numClippingPlanes!==void 0&&(Ce.numClippingPlanes!==ne.numPlanes||Ce.numIntersection!==ne.numIntersection)||Ce.vertexAlphas!==qe||Ce.vertexTangents!==tt||Ce.morphTargets!==Ae||Ce.morphNormals!==St||Ce.morphColors!==Vt||Ce.toneMapping!==Ft||Ce.morphTargetsCount!==pn)&&(ft=!0):(ft=!0,Ce.__version=V.version);let hi=Ce.currentProgram;ft===!0&&(hi=kl(V,P,k));let Pi=!1,_o=!1,_s=!1,It=hi.getUniforms(),an=Ce.uniforms;if(we.useProgram(hi.program)&&(Pi=!0,_o=!0,_s=!0),V.id!==G&&(G=V.id,_o=!0),Pi||B!==x){we.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),It.setValue(A,"projectionMatrix",x.projectionMatrix),It.setValue(A,"viewMatrix",x.matrixWorldInverse);let Nr=It.map.cameraPosition;Nr!==void 0&&Nr.setValue(A,ut.setFromMatrixPosition(x.matrixWorld)),At.logarithmicDepthBuffer&&It.setValue(A,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(V.isMeshPhongMaterial||V.isMeshToonMaterial||V.isMeshLambertMaterial||V.isMeshBasicMaterial||V.isMeshStandardMaterial||V.isShaderMaterial)&&It.setValue(A,"isOrthographic",x.isOrthographicCamera===!0),B!==x&&(B=x,_o=!0,_s=!0)}if(Ce.needsLights&&(qn.state.directionalShadowMap.length>0&&It.setValue(A,"directionalShadowMap",qn.state.directionalShadowMap,N),qn.state.spotShadowMap.length>0&&It.setValue(A,"spotShadowMap",qn.state.spotShadowMap,N),qn.state.pointShadowMap.length>0&&It.setValue(A,"pointShadowMap",qn.state.pointShadowMap,N)),k.isSkinnedMesh){It.setOptional(A,k,"bindMatrix"),It.setOptional(A,k,"bindMatrixInverse");let tn=k.skeleton;tn&&(tn.boneTexture===null&&tn.computeBoneTexture(),It.setValue(A,"boneTexture",tn.boneTexture,N))}k.isBatchedMesh&&(It.setOptional(A,k,"batchingTexture"),It.setValue(A,"batchingTexture",k._matricesTexture,N),It.setOptional(A,k,"batchingIdTexture"),It.setValue(A,"batchingIdTexture",k._indirectTexture,N),It.setOptional(A,k,"batchingColorTexture"),k._colorsTexture!==null&&It.setValue(A,"batchingColorTexture",k._colorsTexture,N));let Rr=z.morphAttributes;if((Rr.position!==void 0||Rr.normal!==void 0||Rr.color!==void 0)&&pe.update(k,z,hi),(_o||Ce.receiveShadow!==k.receiveShadow)&&(Ce.receiveShadow=k.receiveShadow,It.setValue(A,"receiveShadow",k.receiveShadow)),(V.isMeshStandardMaterial||V.isMeshLambertMaterial||V.isMeshPhongMaterial)&&V.envMap===null&&P.environment!==null&&(an.envMapIntensity.value=P.environmentIntensity),an.dfgLUT!==void 0&&(an.dfgLUT.value=ak()),_o&&(It.setValue(A,"toneMappingExposure",b.toneMappingExposure),Ce.needsLights&&Zw(an,_s),de&&V.fog===!0&&Re.refreshFogUniforms(an,de),Re.refreshMaterialUniforms(an,V,Je,ge,D.state.transmissionRenderTarget[x.id]),Ha.upload(A,H0(Ce),an,N)),V.isShaderMaterial&&V.uniformsNeedUpdate===!0&&(Ha.upload(A,H0(Ce),an,N),V.uniformsNeedUpdate=!1),V.isSpriteMaterial&&It.setValue(A,"center",k.center),It.setValue(A,"modelViewMatrix",k.modelViewMatrix),It.setValue(A,"normalMatrix",k.normalMatrix),It.setValue(A,"modelMatrix",k.matrixWorld),V.isShaderMaterial||V.isRawShaderMaterial){let tn=V.uniformsGroups;for(let Nr=0,xs=tn.length;Nr<xs;Nr++){let G0=tn[Nr];ye.update(G0,hi),ye.bind(G0,hi)}}return hi}function Zw(x,P){x.ambientLightColor.needsUpdate=P,x.lightProbe.needsUpdate=P,x.directionalLights.needsUpdate=P,x.directionalLightShadows.needsUpdate=P,x.pointLights.needsUpdate=P,x.pointLightShadows.needsUpdate=P,x.spotLights.needsUpdate=P,x.spotLightShadows.needsUpdate=P,x.rectAreaLights.needsUpdate=P,x.hemisphereLights.needsUpdate=P}function Jw(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return T},this.getActiveMipmapLevel=function(){return F},this.getRenderTarget=function(){return H},this.setRenderTargetTextures=function(x,P,z){let V=v.get(x);V.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,V.__autoAllocateDepthBuffer===!1&&(V.__useRenderToTexture=!1),v.get(x.texture).__webglTexture=P,v.get(x.depthTexture).__webglTexture=V.__autoAllocateDepthBuffer?void 0:z,V.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,P){let z=v.get(x);z.__webglFramebuffer=P,z.__useDefaultFramebuffer=P===void 0};let Kw=A.createFramebuffer();this.setRenderTarget=function(x,P=0,z=0){H=x,T=P,F=z;let V=null,k=!1,de=!1;if(x){let fe=v.get(x);if(fe.__useDefaultFramebuffer!==void 0){we.bindFramebuffer(A.FRAMEBUFFER,fe.__webglFramebuffer),U.copy(x.viewport),O.copy(x.scissor),Z=x.scissorTest,we.viewport(U),we.scissor(O),we.setScissorTest(Z),G=-1;return}else if(fe.__webglFramebuffer===void 0)N.setupRenderTarget(x);else if(fe.__hasExternalTextures)N.rebindTextures(x,v.get(x.texture).__webglTexture,v.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let qe=x.depthTexture;if(fe.__boundDepthTexture!==qe){if(qe!==null&&v.has(qe)&&(x.width!==qe.image.width||x.height!==qe.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");N.setupDepthRenderbuffer(x)}}let Se=x.texture;(Se.isData3DTexture||Se.isDataArrayTexture||Se.isCompressedArrayTexture)&&(de=!0);let De=v.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(De[P])?V=De[P][z]:V=De[P],k=!0):x.samples>0&&N.useMultisampledRTT(x)===!1?V=v.get(x).__webglMultisampledFramebuffer:Array.isArray(De)?V=De[z]:V=De,U.copy(x.viewport),O.copy(x.scissor),Z=x.scissorTest}else U.copy(X).multiplyScalar(Je).floor(),O.copy(re).multiplyScalar(Je).floor(),Z=ce;if(z!==0&&(V=Kw),we.bindFramebuffer(A.FRAMEBUFFER,V)&&we.drawBuffers(x,V),we.viewport(U),we.scissor(O),we.setScissorTest(Z),k){let fe=v.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_CUBE_MAP_POSITIVE_X+P,fe.__webglTexture,z)}else if(de){let fe=P;for(let Se=0;Se<x.textures.length;Se++){let De=v.get(x.textures[Se]);A.framebufferTextureLayer(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0+Se,De.__webglTexture,z,fe)}}else if(x!==null&&z!==0){let fe=v.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,fe.__webglTexture,z)}G=-1},this.readRenderTargetPixels=function(x,P,z,V,k,de,ve,fe=0){if(!(x&&x.isWebGLRenderTarget)){Ue("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let Se=v.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&ve!==void 0&&(Se=Se[ve]),Se){we.bindFramebuffer(A.FRAMEBUFFER,Se);try{let De=x.textures[fe],qe=De.format,tt=De.type;if(x.textures.length>1&&A.readBuffer(A.COLOR_ATTACHMENT0+fe),!At.textureFormatReadable(qe)){Ue("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!At.textureTypeReadable(tt)){Ue("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}P>=0&&P<=x.width-V&&z>=0&&z<=x.height-k&&A.readPixels(P,z,V,k,se.convert(qe),se.convert(tt),de)}finally{let De=H!==null?v.get(H).__webglFramebuffer:null;we.bindFramebuffer(A.FRAMEBUFFER,De)}}},this.readRenderTargetPixelsAsync=async function(x,P,z,V,k,de,ve,fe=0){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let Se=v.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&ve!==void 0&&(Se=Se[ve]),Se)if(P>=0&&P<=x.width-V&&z>=0&&z<=x.height-k){we.bindFramebuffer(A.FRAMEBUFFER,Se);let De=x.textures[fe],qe=De.format,tt=De.type;if(x.textures.length>1&&A.readBuffer(A.COLOR_ATTACHMENT0+fe),!At.textureFormatReadable(qe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!At.textureTypeReadable(tt))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Ae=A.createBuffer();A.bindBuffer(A.PIXEL_PACK_BUFFER,Ae),A.bufferData(A.PIXEL_PACK_BUFFER,de.byteLength,A.STREAM_READ),A.readPixels(P,z,V,k,se.convert(qe),se.convert(tt),0);let St=H!==null?v.get(H).__webglFramebuffer:null;we.bindFramebuffer(A.FRAMEBUFFER,St);let Vt=A.fenceSync(A.SYNC_GPU_COMMANDS_COMPLETE,0);return A.flush(),await dw(A,Vt,4),A.bindBuffer(A.PIXEL_PACK_BUFFER,Ae),A.getBufferSubData(A.PIXEL_PACK_BUFFER,0,de),A.deleteBuffer(Ae),A.deleteSync(Vt),de}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(x,P=null,z=0){let V=Math.pow(2,-z),k=Math.floor(x.image.width*V),de=Math.floor(x.image.height*V),ve=P!==null?P.x:0,fe=P!==null?P.y:0;N.setTexture2D(x,0),A.copyTexSubImage2D(A.TEXTURE_2D,z,0,0,ve,fe,k,de),we.unbindTexture()};let Qw=A.createFramebuffer(),eC=A.createFramebuffer();this.copyTextureToTexture=function(x,P,z=null,V=null,k=0,de=0){let ve,fe,Se,De,qe,tt,Ae,St,Vt,Ft=x.isCompressedTexture?x.mipmaps[de]:x.image;if(z!==null)ve=z.max.x-z.min.x,fe=z.max.y-z.min.y,Se=z.isBox3?z.max.z-z.min.z:1,De=z.min.x,qe=z.min.y,tt=z.isBox3?z.min.z:0;else{let an=Math.pow(2,-k);ve=Math.floor(Ft.width*an),fe=Math.floor(Ft.height*an),x.isDataArrayTexture?Se=Ft.depth:x.isData3DTexture?Se=Math.floor(Ft.depth*an):Se=1,De=0,qe=0,tt=0}V!==null?(Ae=V.x,St=V.y,Vt=V.z):(Ae=0,St=0,Vt=0);let wt=se.convert(P.format),pn=se.convert(P.type),Ce;P.isData3DTexture?(N.setTexture3D(P,0),Ce=A.TEXTURE_3D):P.isDataArrayTexture||P.isCompressedArrayTexture?(N.setTexture2DArray(P,0),Ce=A.TEXTURE_2D_ARRAY):(N.setTexture2D(P,0),Ce=A.TEXTURE_2D),A.pixelStorei(A.UNPACK_FLIP_Y_WEBGL,P.flipY),A.pixelStorei(A.UNPACK_PREMULTIPLY_ALPHA_WEBGL,P.premultiplyAlpha),A.pixelStorei(A.UNPACK_ALIGNMENT,P.unpackAlignment);let qn=A.getParameter(A.UNPACK_ROW_LENGTH),ft=A.getParameter(A.UNPACK_IMAGE_HEIGHT),hi=A.getParameter(A.UNPACK_SKIP_PIXELS),Pi=A.getParameter(A.UNPACK_SKIP_ROWS),_o=A.getParameter(A.UNPACK_SKIP_IMAGES);A.pixelStorei(A.UNPACK_ROW_LENGTH,Ft.width),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,Ft.height),A.pixelStorei(A.UNPACK_SKIP_PIXELS,De),A.pixelStorei(A.UNPACK_SKIP_ROWS,qe),A.pixelStorei(A.UNPACK_SKIP_IMAGES,tt);let _s=x.isDataArrayTexture||x.isData3DTexture,It=P.isDataArrayTexture||P.isData3DTexture;if(x.isDepthTexture){let an=v.get(x),Rr=v.get(P),tn=v.get(an.__renderTarget),Nr=v.get(Rr.__renderTarget);we.bindFramebuffer(A.READ_FRAMEBUFFER,tn.__webglFramebuffer),we.bindFramebuffer(A.DRAW_FRAMEBUFFER,Nr.__webglFramebuffer);for(let xs=0;xs<Se;xs++)_s&&(A.framebufferTextureLayer(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,v.get(x).__webglTexture,k,tt+xs),A.framebufferTextureLayer(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,v.get(P).__webglTexture,de,Vt+xs)),A.blitFramebuffer(De,qe,ve,fe,Ae,St,ve,fe,A.DEPTH_BUFFER_BIT,A.NEAREST);we.bindFramebuffer(A.READ_FRAMEBUFFER,null),we.bindFramebuffer(A.DRAW_FRAMEBUFFER,null)}else if(k!==0||x.isRenderTargetTexture||v.has(x)){let an=v.get(x),Rr=v.get(P);we.bindFramebuffer(A.READ_FRAMEBUFFER,Qw),we.bindFramebuffer(A.DRAW_FRAMEBUFFER,eC);for(let tn=0;tn<Se;tn++)_s?A.framebufferTextureLayer(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,an.__webglTexture,k,tt+tn):A.framebufferTexture2D(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,an.__webglTexture,k),It?A.framebufferTextureLayer(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,Rr.__webglTexture,de,Vt+tn):A.framebufferTexture2D(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,Rr.__webglTexture,de),k!==0?A.blitFramebuffer(De,qe,ve,fe,Ae,St,ve,fe,A.COLOR_BUFFER_BIT,A.NEAREST):It?A.copyTexSubImage3D(Ce,de,Ae,St,Vt+tn,De,qe,ve,fe):A.copyTexSubImage2D(Ce,de,Ae,St,De,qe,ve,fe);we.bindFramebuffer(A.READ_FRAMEBUFFER,null),we.bindFramebuffer(A.DRAW_FRAMEBUFFER,null)}else It?x.isDataTexture||x.isData3DTexture?A.texSubImage3D(Ce,de,Ae,St,Vt,ve,fe,Se,wt,pn,Ft.data):P.isCompressedArrayTexture?A.compressedTexSubImage3D(Ce,de,Ae,St,Vt,ve,fe,Se,wt,Ft.data):A.texSubImage3D(Ce,de,Ae,St,Vt,ve,fe,Se,wt,pn,Ft):x.isDataTexture?A.texSubImage2D(A.TEXTURE_2D,de,Ae,St,ve,fe,wt,pn,Ft.data):x.isCompressedTexture?A.compressedTexSubImage2D(A.TEXTURE_2D,de,Ae,St,Ft.width,Ft.height,wt,Ft.data):A.texSubImage2D(A.TEXTURE_2D,de,Ae,St,ve,fe,wt,pn,Ft);A.pixelStorei(A.UNPACK_ROW_LENGTH,qn),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,ft),A.pixelStorei(A.UNPACK_SKIP_PIXELS,hi),A.pixelStorei(A.UNPACK_SKIP_ROWS,Pi),A.pixelStorei(A.UNPACK_SKIP_IMAGES,_o),de===0&&P.generateMipmaps&&A.generateMipmap(Ce),we.unbindTexture()},this.initRenderTarget=function(x){v.get(x).__webglFramebuffer===void 0&&N.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?N.setTextureCube(x,0):x.isData3DTexture?N.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?N.setTexture2DArray(x,0):N.setTexture2D(x,0),we.unbindTexture()},this.resetState=function(){T=0,F=0,H=null,we.reset(),ie.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Di}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=ct._getDrawingBufferColorSpace(e),t.unpackColorSpace=ct._getUnpackColorSpace()}};var Sp=class extends gp{constructor(e){super(e)}load(e,t,i,r){let o=this,s=new _l(this.manager);s.setPath(this.path),s.setResponseType("arraybuffer"),s.setRequestHeader(this.requestHeader),s.setWithCredentials(this.withCredentials),s.load(e,function(a){try{t(o.parse(a))}catch(c){r?r(c):console.error(c),o.manager.itemError(e)}},i,r)}parse(e){function t(l){let u=new DataView(l),d=32/8*3+32/8*3*3+16/8,f=u.getUint32(80,!0);if(80+32/8+f*d===u.byteLength)return!0;let p=[115,111,108,105,100];for(let y=0;y<5;y++)if(i(p,u,y))return!1;return!0}function i(l,u,d){for(let f=0,h=l.length;f<h;f++)if(l[f]!==u.getUint8(d+f))return!1;return!0}function r(l){let u=new DataView(l),d=u.getUint32(80,!0),f,h,p,y=!1,g,m,E,S,w;for(let T=0;T<70;T++)u.getUint32(T,!1)==1129270351&&u.getUint8(T+4)==82&&u.getUint8(T+5)==61&&(y=!0,g=new Float32Array(d*3*3),m=u.getUint8(T+6)/255,E=u.getUint8(T+7)/255,S=u.getUint8(T+8)/255,w=u.getUint8(T+9)/255);let D=84,C=50,I=new bn,_=new Float32Array(d*3*3),b=new Float32Array(d*3*3),W=new nt;for(let T=0;T<d;T++){let F=D+T*C,H=u.getFloat32(F,!0),G=u.getFloat32(F+4,!0),B=u.getFloat32(F+8,!0);if(y){let U=u.getUint16(F+48,!0);(U&32768)===0?(f=(U&31)/31,h=(U>>5&31)/31,p=(U>>10&31)/31):(f=m,h=E,p=S)}for(let U=1;U<=3;U++){let O=F+U*12,Z=T*3*3+(U-1)*3;_[Z]=u.getFloat32(O,!0),_[Z+1]=u.getFloat32(O+4,!0),_[Z+2]=u.getFloat32(O+8,!0),b[Z]=H,b[Z+1]=G,b[Z+2]=B,y&&(W.setRGB(f,h,p,Fn),g[Z]=W.r,g[Z+1]=W.g,g[Z+2]=W.b)}}return I.setAttribute("position",new fn(_,3)),I.setAttribute("normal",new fn(b,3)),y&&(I.setAttribute("color",new fn(g,3)),I.hasColors=!0,I.alpha=w),I}function o(l){let u=new bn,d=/solid([\s\S]*?)endsolid/g,f=/facet([\s\S]*?)endfacet/g,h=/solid\s(.+)/,p=0,y=/[\s]+([+-]?(?:\d*)(?:\.\d*)?(?:[eE][+-]?\d+)?)/.source,g=new RegExp("vertex"+y+y+y,"g"),m=new RegExp("normal"+y+y+y,"g"),E=[],S=[],w=[],D=new L,C,I=0,_=0,b=0;for(;(C=d.exec(l))!==null;){_=b;let W=C[0],T=(C=h.exec(W))!==null?C[1]:"";for(w.push(T);(C=f.exec(W))!==null;){let G=0,B=0,U=C[0];for(;(C=m.exec(U))!==null;)D.x=parseFloat(C[1]),D.y=parseFloat(C[2]),D.z=parseFloat(C[3]),B++;for(;(C=g.exec(U))!==null;)E.push(parseFloat(C[1]),parseFloat(C[2]),parseFloat(C[3])),S.push(D.x,D.y,D.z),G++,b++;B!==1&&console.error("THREE.STLLoader: Something isn't right with the normal of face number "+p),G!==3&&console.error("THREE.STLLoader: Something isn't right with the vertices of face number "+p),p++}let F=_,H=b-_;u.userData.groupNames=w,u.addGroup(F,H,I),I++}return u.setAttribute("position",new Xt(E,3)),u.setAttribute("normal",new Xt(S,3)),u}function s(l){return typeof l!="string"?new TextDecoder().decode(l):l}function a(l){if(typeof l=="string"){let u=new Uint8Array(l.length);for(let d=0;d<l.length;d++)u[d]=l.charCodeAt(d)&255;return u.buffer||u}else return l}let c=a(e);return t(c)?r(c):o(s(e))}};var lk=["canvas"],Ga=Math.PI/180,uk=-.6,dk=1,fk=14,hk=.15,pk=1.55,Gw=4,jw=40,mk=.03,Ww=.008,gk=4,vk=659225,$w=1976635,yk=3900150,_k=16777215,qw={installed:2278750,in_progress:16096779,not_installed:6583435},wp=class n{canvasRef;state=J($i);injector=J(In);renderer;scene;camera;raycaster=new Ml;mouseNDC=new lt;meshByNodeId=new Map;animFrameId=0;camTheta=uk;camPhi=dk;camRadius=fk;isDragging=!1;dragStartX=0;dragStartY=0;dragLastX=0;dragLastY=0;ngOnInit(){this.setupRenderer(),this.setupSceneAndCamera(),this.addLighting(),this.addGroundGrid(),this.subscribeToMouseEvents(),this.startRenderLoop(),this.loadAllModels().then(()=>{this.watchNodeStatuses(),this.watchSelectedNode()})}ngOnDestroy(){cancelAnimationFrame(this.animFrameId),this.renderer.dispose(),this.unsubscribeFromMouseEvents()}setupRenderer(){let e=this.canvasRef.nativeElement;this.renderer=new Ep({canvas:e,antialias:!0}),this.renderer.shadowMap.enabled=!0,this.renderer.setClearColor(vk);let t=e.clientWidth||800,i=e.clientHeight||500;this.renderer.setSize(t,i,!1),this.renderer.setPixelRatio(window.devicePixelRatio)}setupSceneAndCamera(){this.scene=new ll;let e=this.canvasRef.nativeElement,t=e.clientWidth||800,i=e.clientHeight||500,r=t/i;this.camera=new En(45,r,.1,200),this.applyCameraPosition()}addLighting(){let e=new bl(16777215,.45);this.scene.add(e);let t=new Fa(16772829,1.4);t.position.set(15,20,10),t.castShadow=!0,this.scene.add(t);let i=new Fa(9158655,.25);i.position.set(-10,5,-8),this.scene.add(i)}addGroundGrid(){let e=new Sl(30,30,$w,$w);this.scene.add(e)}async loadAllModels(){let e=new Sp,t=this.state.nodes();for(let i of t){let r=await this.loadSTLFile(e,`/models/${i.stlFile}`);r.computeBoundingBox();let o=new L;r.boundingBox.getCenter(o),r.translate(-o.x,-o.y,-o.z),r.scale(.001,.001,.001);let s=new vl({color:qw[i.status],roughness:.55,metalness:.25,transparent:!0,opacity:.92}),a=new Wn(r,s);a.position.set(i.position.x,i.position.y,i.position.z),a.rotation.set(i.rotation.rx*Ga,i.rotation.ry*Ga,i.rotation.rz*Ga),a.castShadow=!0,a.receiveShadow=!0,a.userData.nodeId=i.id;let c=new Pa(new ml(r),new fs({color:_k,transparent:!0,opacity:.08}));c.raycast=()=>{},a.add(c),this.scene.add(a),this.meshByNodeId.set(i.id,a)}}loadSTLFile(e,t){return new Promise((i,r)=>{e.load(t,o=>i(o),void 0,o=>r(o))})}applyCameraPosition(){let e=this.camRadius,t=this.camPhi,i=this.camTheta;this.camera.position.x=e*Math.sin(t)*Math.sin(i),this.camera.position.y=e*Math.cos(t),this.camera.position.z=e*Math.sin(t)*Math.cos(i),this.camera.lookAt(1.5,1.5,3)}startRenderLoop(){let e=()=>{this.animFrameId=requestAnimationFrame(e),this.updateRendererSizeIfNeeded(),this.renderer.render(this.scene,this.camera)};e()}updateRendererSizeIfNeeded(){let e=this.canvasRef.nativeElement,t=e.clientWidth,i=e.clientHeight,r=this.renderer.domElement.width!==t,o=this.renderer.domElement.height!==i;(r||o)&&(this.renderer.setSize(t,i,!1),this.camera.aspect=t/i,this.camera.updateProjectionMatrix())}watchNodeStatuses(){Xu(()=>{let e=this.state.nodes();for(let t of e){let i=this.meshByNodeId.get(t.id);if(i===void 0)continue;i.material.color.setHex(qw[t.status]),i.position.set(t.position.x,t.position.y,t.position.z),i.rotation.set(t.rotation.rx*Ga,t.rotation.ry*Ga,t.rotation.rz*Ga)}},{injector:this.injector})}watchSelectedNode(){Xu(()=>{let e=this.state.selected();this.meshByNodeId.forEach((t,i)=>{let r=t.material;i===e?(r.emissive.setHex(yk),r.emissiveIntensity=.45,r.opacity=1):(r.emissive.setHex(0),r.emissiveIntensity=0,r.opacity=.92)})},{injector:this.injector})}onMouseDown=e=>{this.isDragging=!0,this.dragStartX=e.clientX,this.dragStartY=e.clientY,this.dragLastX=e.clientX,this.dragLastY=e.clientY};onMouseMove=e=>{if(!this.isDragging)return;let t=e.clientX-this.dragLastX,i=e.clientY-this.dragLastY;this.camTheta=this.camTheta-t*Ww;let r=this.camPhi+i*Ww;this.camPhi=Math.max(hk,Math.min(pk,r)),this.dragLastX=e.clientX,this.dragLastY=e.clientY,this.applyCameraPosition()};onMouseUp=e=>{let t=e.clientX-this.dragStartX,i=e.clientY-this.dragStartY;Math.sqrt(t*t+i*i)<gk&&this.handleClick(e),this.isDragging=!1};onMouseWheel=e=>{this.camRadius=this.camRadius+e.deltaY*mk,this.camRadius<Gw&&(this.camRadius=Gw),this.camRadius>jw&&(this.camRadius=jw),this.applyCameraPosition(),e.preventDefault()};handleClick(e){let i=this.canvasRef.nativeElement.getBoundingClientRect();this.mouseNDC.x=(e.clientX-i.left)/i.width*2-1,this.mouseNDC.y=-((e.clientY-i.top)/i.height)*2+1,this.raycaster.setFromCamera(this.mouseNDC,this.camera);let r=Array.from(this.meshByNodeId.values()),o=this.raycaster.intersectObjects(r);if(o.length===0){this.state.select(null);return}let a=o[0].object.userData.nodeId;a===this.state.selected()?this.state.select(null):this.state.select(a)}subscribeToMouseEvents(){let e=this.canvasRef.nativeElement;e.addEventListener("mousedown",this.onMouseDown),e.addEventListener("wheel",this.onMouseWheel,{passive:!1}),window.addEventListener("mousemove",this.onMouseMove),window.addEventListener("mouseup",this.onMouseUp)}unsubscribeFromMouseEvents(){let e=this.canvasRef.nativeElement;e.removeEventListener("mousedown",this.onMouseDown),e.removeEventListener("wheel",this.onMouseWheel),window.removeEventListener("mousemove",this.onMouseMove),window.removeEventListener("mouseup",this.onMouseUp)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Zn({type:n,selectors:[["app-viewer"]],viewQuery:function(t,i){if(t&1&&Ud(lk,7),t&2){let r;av(r=cv())&&(i.canvasRef=r.first)}},decls:12,vars:0,consts:[["canvas",""],[1,"wrap"],[1,"legend"],[1,"dot","installed"],[1,"dot","in_progress"],[1,"dot","not_installed"],[1,"hint"]],template:function(t,i){t&1&&(te(0,"div",1),Gn(1,"canvas",null,0),te(3,"div",2),Gn(4,"span",3),me(5,"\u0423\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D "),Gn(6,"span",4),me(7,"\u0412 \u0440\u0430\u0431\u043E\u0442\u0435 "),Gn(8,"span",5),me(9,"\u041D\u0435 \u0443\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D "),le(),te(10,"div",6),me(11,"\u041F\u0435\u0440\u0435\u0442\u0430\u0449\u0438 \xB7 \u0421\u043A\u0440\u043E\u043B\u043B"),le()())},styles:[".wrap[_ngcontent-%COMP%]{position:relative;width:100%;height:100%}canvas[_ngcontent-%COMP%]{width:100%!important;height:100%!important;display:block;border-radius:10px}.legend[_ngcontent-%COMP%]{position:absolute;bottom:10px;left:10px;display:flex;gap:10px;align-items:center;background:#0a0f19cc;-webkit-backdrop-filter:blur(6px);backdrop-filter:blur(6px);border-radius:8px;padding:5px 12px;font-size:11px;color:#94a3b8}.dot[_ngcontent-%COMP%]{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:4px}.dot.installed[_ngcontent-%COMP%]{background:#22c55e}.dot.in_progress[_ngcontent-%COMP%]{background:#f59e0b}.dot.not_installed[_ngcontent-%COMP%]{background:#64748b}.hint[_ngcontent-%COMP%]{position:absolute;top:10px;right:12px;font-size:11px;color:#334155;background:#0a0f1999;border-radius:6px;padding:3px 9px}"],changeDetection:0})};var Cp=class n{transform(e){let t=0;for(let i of e)i.done&&(t=t+1);return t}static \u0275fac=function(t){return new(t||n)};static \u0275pipe=Od({name:"doneCount",type:n,pure:!0})};var xk=(n,e)=>e.id;function Ek(n,e){n&1&&(te(0,"div",3)(1,"span"),me(2,"\u2B21"),le(),te(3,"p"),me(4,"\u0412\u044B\u0431\u0435\u0440\u0438\u0442\u0435 \u044D\u043B\u0435\u043C\u0435\u043D\u0442"),Gn(5,"br"),me(6,"\u043D\u0430 3D-\u043C\u043E\u0434\u0435\u043B\u0438"),le()())}function bk(n,e){if(n&1){let t=yn();te(0,"button",28),dt("click",function(){let r=ht(t).$implicit,o=Ie(),s=Ie();return pt(s.state.patch(o.id,{status:r}))}),me(1),le()}if(n&2){let t=e.$implicit,i=Ie(),r=Ie();mr(t),zi("on",i.status===t),Oe(),Gi(" ",r.statusLabel(t)," ")}}function Mk(n,e){if(n&1){let t=yn();te(0,"button",28),dt("click",function(){let r=ht(t).$implicit,o=Ie(),s=Ie();return pt(s.state.patch(o.id,{quality:r}))}),me(1),le()}if(n&2){let t=e.$implicit,i=Ie(),r=Ie();mr(Xr("grade ",t)),zi("on",i.quality===t),Oe(),Gi(" ",r.qualityLabel(t)," ")}}function Sk(n,e){if(n&1){let t=yn();te(0,"button",29),dt("click",function(){ht(t);let r=Ie(),o=Ie();return pt(o.state.patch(r.id,{quality:null}))}),me(1," \u2715 \u0421\u0431\u0440\u043E\u0441\u0438\u0442\u044C "),le()}}function wk(n,e){if(n&1&&(te(0,"div"),me(1),le()),n&2){let t=Ie(),i=Ie();mr(Xr("quality-badge ",t.quality)),Oe(),rn(i.qualityLabel(t.quality))}}function Ck(n,e){if(n&1){let t=yn();te(0,"input",36,2),dt("keydown.enter",function(){ht(t);let r=Zo(1),o=Ie().$implicit,s=Ie(),a=Ie();return pt(a.saveEdit(s.id,o.id,r.value))})("keydown.escape",function(){ht(t);let r=Ie(3);return pt(r.cancelEdit())})("blur",function(){ht(t);let r=Zo(1),o=Ie().$implicit,s=Ie(),a=Ie();return pt(a.saveEdit(s.id,o.id,r.value))}),le()}if(n&2){let t=Ie().$implicit;Jn("value",t.text)}}function Tk(n,e){if(n&1&&(te(0,"span",37),me(1),le()),n&2){let t=Ie().$implicit;zi("done",t.done),Oe(),rn(t.text)}}function Dk(n,e){if(n&1){let t=yn();te(0,"button",38),dt("click",function(){ht(t);let r=Ie().$implicit,o=Ie(2);return pt(o.startEdit(r.id))}),me(1,"\u270E"),le()}}function Ik(n,e){if(n&1){let t=yn();te(0,"div",17)(1,"button",30),dt("click",function(){let r=ht(t).$implicit,o=Ie(),s=Ie();return pt(s.state.toggleChecklistItem(o.id,r.id))}),me(2),le(),Nn(3,Ck,2,1,"input",31)(4,Tk,2,3,"span",32),te(5,"div",33),Nn(6,Dk,2,0,"button",34),te(7,"button",35),dt("click",function(){let r=ht(t).$implicit,o=Ie(),s=Ie();return pt(s.state.deleteChecklistItem(o.id,r.id))}),me(8,"\u2715"),le()()()}if(n&2){let t=e.$implicit,i=Ie(2);Oe(),zi("checked",t.done),Oe(),Gi(" ",t.done?"\u2713":""," "),Oe(),Pn(i.editingItemId()===t.id?3:4),Oe(3),Pn(i.editingItemId()!==t.id?6:-1)}}function Ak(n,e){if(n&1){let t=yn();Gn(0,"img",39),te(1,"button",40),dt("click",function(r){ht(t);let o=Ie(2);return r.stopPropagation(),pt(o.clearPreview())}),me(2,"\u2715"),le()}if(n&2){let t=Ie(2);Jn("src",t.previewDataUrl(),bc)}}function Rk(n,e){n&1&&(te(0,"div",25),me(1,"\u{1F4F7} \u041D\u0430\u0436\u043C\u0438\u0442\u0435 \u0438\u043B\u0438 \u043F\u0435\u0440\u0435\u0442\u0430\u0449\u0438\u0442\u0435"),Gn(2,"br"),te(3,"small"),me(4,"JPG / PNG \u0434\u043E 10 \u041C\u0411"),le()())}function Nk(n,e){if(n&1){let t=yn();te(0,"button",41),dt("click",function(){ht(t);let r=Ie(),o=Ie();return pt(o.openAiStub(r.id))}),me(1," \u{1F50D} \u041E\u0442\u043F\u0440\u0430\u0432\u0438\u0442\u044C \u043D\u0430 AI-\u0430\u043D\u0430\u043B\u0438\u0437 "),le()}}function Pk(n,e){if(n&1){let t=yn();te(0,"img",43),dt("click",function(){let r=ht(t).$implicit,o=Ie(3);return pt(o.zoomPhotoUrl.set(r))}),le()}if(n&2){let t=e.$implicit;Jn("src",t,bc)}}function Ok(n,e){if(n&1&&(te(0,"div",8)(1,"div",9),me(2),le(),te(3,"div",42),$r(4,Pk,1,1,"img",39,Cc),le()()),n&2){let t=Ie();Oe(2),Gi("\u0424\u043E\u0442\u043E\u0430\u0440\u0445\u0438\u0432 (",t.photos.length,")"),Oe(2),qr(t.photos)}}function Lk(n,e){if(n&1){let t=yn();te(0,"div",4)(1,"div",6)(2,"div")(3,"div",7),me(4),le(),te(5,"h2"),me(6),le()(),te(7,"span"),me(8),le()(),te(9,"div",8)(10,"div",9),me(11,"\u0421\u0442\u0430\u0442\u0443\u0441 \u043C\u043E\u043D\u0442\u0430\u0436\u0430"),le(),te(12,"div",10),$r(13,bk,2,5,"button",11,Cc),le()(),te(15,"div",8)(16,"div",9),me(17,"\u041E\u0446\u0435\u043D\u043A\u0430 \u043A\u0430\u0447\u0435\u0441\u0442\u0432\u0430"),le(),te(18,"div",10),$r(19,Mk,2,6,"button",11,Cc),Nn(21,Sk,2,0,"button",12),le(),Nn(22,wk,2,4,"div",13),le(),te(23,"div",8)(24,"div",14)(25,"div",9),me(26,"\u0427\u0435\u043A\u043B\u0438\u0441\u0442 \u0440\u0430\u0431\u043E\u0442"),le(),te(27,"span",15),me(28),lv(29,"doneCount"),le()(),te(30,"div",16),$r(31,Ik,9,5,"div",17,xk),te(33,"div",18)(34,"input",19,0),dt("keydown.enter",function(){let r=ht(t),o=Zo(35),s=Ie();return pt(s.addItem(r.id,o))}),le(),te(36,"button",20),dt("click",function(){let r=ht(t),o=Zo(35),s=Ie();return pt(s.addItem(r.id,o))}),me(37,"+"),le()()()(),te(38,"div",8)(39,"div",9),me(40,"\u041F\u043E\u043B\u043E\u0436\u0435\u043D\u0438\u0435 (\u043C\u0435\u0442\u0440\u044B)"),le(),te(41,"div",21)(42,"label"),me(43,"X "),te(44,"input",22),dt("change",function(r){let o=ht(t),s=Ie();return pt(s.updatePosition(o.id,"x",r))}),le()(),te(45,"label"),me(46,"Y "),te(47,"input",22),dt("change",function(r){let o=ht(t),s=Ie();return pt(s.updatePosition(o.id,"y",r))}),le()(),te(48,"label"),me(49,"Z "),te(50,"input",22),dt("change",function(r){let o=ht(t),s=Ie();return pt(s.updatePosition(o.id,"z",r))}),le()()()(),te(51,"div",8)(52,"div",9),me(53,"\u041F\u043E\u0432\u043E\u0440\u043E\u0442 (\u0433\u0440\u0430\u0434\u0443\u0441\u044B)"),le(),te(54,"div",21)(55,"label"),me(56,"RX "),te(57,"input",23),dt("change",function(r){let o=ht(t),s=Ie();return pt(s.updateRotation(o.id,"rx",r))}),le()(),te(58,"label"),me(59,"RY "),te(60,"input",23),dt("change",function(r){let o=ht(t),s=Ie();return pt(s.updateRotation(o.id,"ry",r))}),le()(),te(61,"label"),me(62,"RZ "),te(63,"input",23),dt("change",function(r){let o=ht(t),s=Ie();return pt(s.updateRotation(o.id,"rz",r))}),le()()()(),te(64,"div",8)(65,"div",9),me(66,"\u0424\u043E\u0442\u043E-\u043A\u043E\u043D\u0442\u0440\u043E\u043B\u044C"),le(),te(67,"div",24),dt("click",function(){ht(t);let r=Zo(71);return pt(r.click())})("dragover",function(r){return r.preventDefault()})("drop",function(r){ht(t);let o=Ie();return pt(o.onFileDrop(r))}),Nn(68,Ak,3,1)(69,Rk,5,0,"div",25),le(),te(70,"input",26,1),dt("change",function(r){ht(t);let o=Ie();return pt(o.onFileSelected(r))}),le(),Nn(72,Nk,2,0,"button",27),le(),Nn(73,Ok,6,1,"div",8),le()}if(n&2){let t=e,i=Ie();Oe(4),rn(t.stlFile),Oe(2),rn(t.name),Oe(),mr(Xr("pill ",t.status)),Oe(),rn(i.statusLabel(t.status)),Oe(5),qr(i.allStatuses),Oe(6),qr(i.allGrades),Oe(2),Pn(t.quality!==null?21:-1),Oe(),Pn(t.quality!==null?22:-1),Oe(6),Bd(" ",uv(29,21,t.checklist)," / ",t.checklist.length," "),Oe(3),qr(t.checklist),Oe(13),Jn("value",t.position.x),Oe(3),Jn("value",t.position.y),Oe(3),Jn("value",t.position.z),Oe(7),Jn("value",t.rotation.rx),Oe(3),Jn("value",t.rotation.ry),Oe(3),Jn("value",t.rotation.rz),Oe(4),zi("filled",i.previewDataUrl()!==null),Oe(),Pn(i.previewDataUrl()!==null?68:69),Oe(4),Pn(i.previewDataUrl()!==null?72:-1),Oe(),Pn(t.photos.length>0?73:-1)}}function Fk(n,e){if(n&1){let t=yn();te(0,"div",44),dt("click",function(){ht(t);let r=Ie();return pt(r.closeAiStub())}),te(1,"div",45),dt("click",function(r){return r.stopPropagation()}),te(2,"div",46),me(3,"\u{1F916}"),le(),te(4,"h3"),me(5,"AI-\u0430\u043D\u0430\u043B\u0438\u0437 \u043A\u0430\u0447\u0435\u0441\u0442\u0432\u0430"),le(),te(6,"p"),me(7,"\u0417\u0434\u0435\u0441\u044C \u0431\u0443\u0434\u0435\u0442 \u043E\u0442\u043F\u0440\u0430\u0432\u043A\u0430 \u0444\u043E\u0442\u043E\u0433\u0440\u0430\u0444\u0438\u0438 \u0432 \u043C\u043E\u0434\u0435\u043B\u044C \u043A\u043E\u043C\u043F\u044C\u044E\u0442\u0435\u0440\u043D\u043E\u0433\u043E \u0437\u0440\u0435\u043D\u0438\u044F:"),le(),te(8,"ul")(9,"li"),me(10,"\u0412\u044B\u044F\u0432\u043B\u0435\u043D\u0438\u0435 \u0434\u0435\u0444\u0435\u043A\u0442\u043E\u0432 \u043C\u043E\u043D\u0442\u0430\u0436\u0430"),le(),te(11,"li"),me(12,"\u0410\u0432\u0442\u043E\u043C\u0430\u0442\u0438\u0447\u0435\u0441\u043A\u0430\u044F \u043E\u0446\u0435\u043D\u043A\u0430 \u043A\u0430\u0447\u0435\u0441\u0442\u0432\u0430"),le(),te(13,"li"),me(14,"\u0420\u0435\u043A\u043E\u043C\u0435\u043D\u0434\u0430\u0446\u0438\u0438 \u043F\u043E \u0443\u0441\u0442\u0440\u0430\u043D\u0435\u043D\u0438\u044E"),le()(),te(15,"div",47),me(16,"\u0424\u0443\u043D\u043A\u0446\u0438\u044F \u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0430 \u043F\u043E\u0441\u043B\u0435 \u043F\u043E\u0434\u043A\u043B\u044E\u0447\u0435\u043D\u0438\u044F API"),le(),te(17,"div",48)(18,"button",49),dt("click",function(){ht(t);let r=Ie();return pt(r.closeAiStub())}),me(19,"\u041E\u0442\u043C\u0435\u043D\u0430"),le(),te(20,"button",50),dt("click",function(){ht(t);let r=Ie();return pt(r.savePhotoToArchive())}),me(21,"\u0421\u043E\u0445\u0440\u0430\u043D\u0438\u0442\u044C \u0444\u043E\u0442\u043E"),le()()()()}}function kk(n,e){if(n&1){let t=yn();te(0,"div",44),dt("click",function(){ht(t);let r=Ie();return pt(r.zoomPhotoUrl.set(null))}),Gn(1,"img",51),le()}if(n&2){let t=Ie();Oe(),Jn("src",t.zoomPhotoUrl(),bc)}}var Uk={installed:"\u0423\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D",in_progress:"\u0412 \u0440\u0430\u0431\u043E\u0442\u0435",not_installed:"\u041D\u0435 \u0443\u0441\u0442\u0430\u043D\u043E\u0432\u043B\u0435\u043D"},Bk={approved:"\u041E\u0434\u043E\u0431\u0440\u0435\u043D\u043E",rework:"\u0422\u0440\u0435\u0431\u0443\u0435\u0442\u0441\u044F \u0434\u043E\u0440\u0430\u0431\u043E\u0442\u043A\u0430",defect:"\u0411\u0440\u0430\u043A"},Tp=class n{state=J($i);allStatuses=["installed","in_progress","not_installed"];allGrades=["approved","rework","defect"];statusLabel(e){return Uk[e]}qualityLabel(e){return Bk[e]}previewDataUrl=Hn(null);zoomPhotoUrl=Hn(null);showAiModal=Hn(!1);editingItemId=Hn(null);pendingPhotoNodeId="";startEdit(e){this.editingItemId.set(e)}cancelEdit(){this.editingItemId.set(null)}saveEdit(e,t,i){this.state.editChecklistItem(e,t,i),this.editingItemId.set(null)}addItem(e,t){let i=t.value;this.state.addChecklistItem(e,i),t.value="",t.focus()}updatePosition(e,t,i){let r=i.target,o=parseFloat(r.value),s=this.state.selectedNode();if(s===null||isNaN(o))return;let a=it(ae({},s.position),{[t]:o});this.state.patch(e,{position:a})}updateRotation(e,t,i){let r=i.target,o=parseFloat(r.value),s=this.state.selectedNode();if(s===null||isNaN(o))return;let a=it(ae({},s.rotation),{[t]:o});this.state.patch(e,{rotation:a})}clearPreview(){this.previewDataUrl.set(null)}onFileSelected(e){let i=e.target.files?.[0];i!==void 0&&this.readFileAsDataUrl(i)}onFileDrop(e){e.preventDefault();let t=e.dataTransfer?.files?.[0];t!==void 0&&this.readFileAsDataUrl(t)}readFileAsDataUrl(e){if(e.size>10485760)return;let i=new FileReader;i.onload=r=>{let o=r.target.result;this.previewDataUrl.set(o)},i.readAsDataURL(e)}openAiStub(e){this.pendingPhotoNodeId=e,this.showAiModal.set(!0)}closeAiStub(){this.showAiModal.set(!1)}savePhotoToArchive(){let e=this.previewDataUrl();e!==null&&this.pendingPhotoNodeId!==""&&(this.state.addPhoto(this.pendingPhotoNodeId,e),this.clearPreview()),this.closeAiStub()}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Zn({type:n,selectors:[["app-node-panel"]],decls:4,vars:4,consts:[["newItemInput",""],["fileInput",""],["editInput",""],[1,"empty"],[1,"panel"],[1,"overlay"],[1,"row"],[1,"tag"],[1,"section"],[1,"lbl"],[1,"btns"],[3,"class","on"],[1,"grade","reset"],[3,"class"],[1,"lbl-row"],[1,"cl-counter"],[1,"cl-list"],[1,"cl-item"],[1,"cl-add"],["placeholder","\u0414\u043E\u0431\u0430\u0432\u0438\u0442\u044C \u043F\u0443\u043D\u043A\u0442\u2026",1,"cl-input",3,"keydown.enter"],[1,"cl-btn-add",3,"click"],[1,"coords"],["type","number","step","0.01",3,"change","value"],["type","number","step","1",3,"change","value"],[1,"drop",3,"click","dragover","drop"],[1,"hint"],["type","file","accept","image/*","capture","environment",2,"display","none",3,"change"],[1,"analyze"],[3,"click"],[1,"grade","reset",3,"click"],[1,"cl-check",3,"click"],[1,"cl-input",3,"value"],[1,"cl-text",3,"done"],[1,"cl-actions"],["title","\u0420\u0435\u0434\u0430\u043A\u0442\u0438\u0440\u043E\u0432\u0430\u0442\u044C",1,"cl-btn","edit"],["title","\u0423\u0434\u0430\u043B\u0438\u0442\u044C",1,"cl-btn","del",3,"click"],[1,"cl-input",3,"keydown.enter","keydown.escape","blur","value"],[1,"cl-text"],["title","\u0420\u0435\u0434\u0430\u043A\u0442\u0438\u0440\u043E\u0432\u0430\u0442\u044C",1,"cl-btn","edit",3,"click"],[3,"src"],[1,"x",3,"click"],[1,"analyze",3,"click"],[1,"gallery"],[3,"click","src"],[1,"overlay",3,"click"],[1,"modal",3,"click"],[1,"modal-icon"],[1,"modal-note"],[1,"modal-btns"],[1,"btn-cancel",3,"click"],[1,"btn-ok",3,"click"],[1,"zoom-img",3,"src"]],template:function(t,i){if(t&1&&(Nn(0,Ek,7,0,"div",3),Nn(1,Lk,74,23,"div",4),Nn(2,Fk,22,0,"div",5),Nn(3,kk,2,1,"div",5)),t&2){let r;Pn(i.state.selectedNode()?-1:0),Oe(),Pn((r=i.state.selectedNode())?1:-1,r),Oe(),Pn(i.showAiModal()?2:-1),Oe(),Pn(i.zoomPhotoUrl()!==null?3:-1)}},dependencies:[Jr,Cp],styles:['@charset "UTF-8";[_nghost-%COMP%]{display:flex;flex-direction:column;height:100%;overflow-y:auto}.empty[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#334155;gap:10px}.empty[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:36px;opacity:.3}.empty[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{font-size:13px;color:#475569;text-align:center;line-height:1.6}.panel[_ngcontent-%COMP%]{padding:16px;display:flex;flex-direction:column;gap:14px}.row[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:flex-start;gap:8px}.tag[_ngcontent-%COMP%]{font-size:10px;color:#38bdf8;background:#38bdf81a;border-radius:4px;padding:2px 7px;width:fit-content;margin-bottom:4px;font-family:monospace}h2[_ngcontent-%COMP%]{margin:0;font-size:16px;color:#f1f5f9}.pill[_ngcontent-%COMP%]{font-size:11px;padding:3px 10px;border-radius:20px;white-space:nowrap;flex-shrink:0}.pill.installed[_ngcontent-%COMP%]{background:#22c55e26;color:#22c55e}.pill.in_progress[_ngcontent-%COMP%]{background:#f59e0b26;color:#f59e0b}.pill.not_installed[_ngcontent-%COMP%]{background:#64748b26;color:#94a3b8}.section[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:7px}.lbl[_ngcontent-%COMP%]{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#475569}.btns[_ngcontent-%COMP%]{display:flex;gap:5px;flex-wrap:wrap}.btns[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{font-size:11px;padding:4px 10px;border-radius:6px;border:1px solid #1e293b;background:transparent;color:#64748b;cursor:pointer;transition:all .15s;font-family:inherit}.btns[_ngcontent-%COMP%]   button.installed.on[_ngcontent-%COMP%]{color:#22c55e;background:#22c55e1a;border-color:#22c55e44}.btns[_ngcontent-%COMP%]   button.in_progress.on[_ngcontent-%COMP%]{color:#f59e0b;background:#f59e0b1a;border-color:#f59e0b44}.btns[_ngcontent-%COMP%]   button.not_installed.on[_ngcontent-%COMP%]{color:#94a3b8;background:#64748b1a;border-color:#64748b44}.btns[_ngcontent-%COMP%]   button.grade.approved.on[_ngcontent-%COMP%]{color:#22c55e;background:#22c55e1a;border-color:#22c55e44}.btns[_ngcontent-%COMP%]   button.grade.rework.on[_ngcontent-%COMP%]{color:#f59e0b;background:#f59e0b1a;border-color:#f59e0b44}.btns[_ngcontent-%COMP%]   button.grade.defect.on[_ngcontent-%COMP%]{color:#ef4444;background:#ef44441a;border-color:#ef444444}.btns[_ngcontent-%COMP%]   button.grade.reset[_ngcontent-%COMP%]{color:#475569;font-size:10px}.quality-badge[_ngcontent-%COMP%]{font-size:12px;font-weight:600;padding:5px 12px;border-radius:8px;width:fit-content}.quality-badge.approved[_ngcontent-%COMP%]{background:#22c55e1a;color:#22c55e}.quality-badge.rework[_ngcontent-%COMP%]{background:#f59e0b1a;color:#f59e0b}.quality-badge.defect[_ngcontent-%COMP%]{background:#ef44441a;color:#ef4444}.lbl-row[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center}.cl-counter[_ngcontent-%COMP%]{font-size:11px;color:#38bdf8;background:#38bdf81a;border-radius:10px;padding:1px 8px}.cl-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:4px}.cl-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;padding:6px 8px;background:#0f172a;border-radius:8px;border:1px solid #1e293b;transition:border-color .15s}.cl-item[_ngcontent-%COMP%]:hover{border-color:#334155}.cl-item[_ngcontent-%COMP%]:hover   .cl-actions[_ngcontent-%COMP%]{opacity:1}.cl-check[_ngcontent-%COMP%]{width:20px;height:20px;border-radius:5px;border:1.5px solid #334155;background:transparent;color:#22c55e;font-size:13px;font-weight:700;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;transition:all .15s}.cl-check.checked[_ngcontent-%COMP%]{background:#22c55e26;border-color:#22c55e44}.cl-text[_ngcontent-%COMP%]{flex:1;font-size:12px;color:#cbd5e1;line-height:1.4}.cl-text.done[_ngcontent-%COMP%]{color:#334155;text-decoration:line-through}.cl-input[_ngcontent-%COMP%]{flex:1;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:5px 8px;color:#f1f5f9;font-size:12px;font-family:inherit}.cl-input[_ngcontent-%COMP%]:focus{outline:none;border-color:#38bdf8}.cl-input[_ngcontent-%COMP%]::placeholder{color:#334155}.cl-actions[_ngcontent-%COMP%]{display:flex;gap:4px;opacity:0;transition:opacity .15s}.cl-btn[_ngcontent-%COMP%]{width:22px;height:22px;border-radius:5px;border:none;background:transparent;cursor:pointer;font-size:12px;display:flex;align-items:center;justify-content:center;transition:background .15s}.cl-btn.edit[_ngcontent-%COMP%]{color:#64748b}.cl-btn.edit[_ngcontent-%COMP%]:hover{background:#1e293b;color:#38bdf8}.cl-btn.del[_ngcontent-%COMP%]{color:#64748b}.cl-btn.del[_ngcontent-%COMP%]:hover{background:#ef44441a;color:#ef4444}.cl-add[_ngcontent-%COMP%]{display:flex;gap:6px;margin-top:4px}.cl-btn-add[_ngcontent-%COMP%]{width:32px;height:32px;border-radius:8px;border:1px solid #334155;background:transparent;color:#38bdf8;font-size:18px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;transition:all .15s}.cl-btn-add[_ngcontent-%COMP%]:hover{background:#38bdf81a;border-color:#38bdf8}.coords[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}.coords[_ngcontent-%COMP%]   label[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:3px;font-size:10px;color:#475569;text-transform:uppercase;letter-spacing:.5px}.coords[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{background:#1e293b;border:1px solid #334155;border-radius:6px;padding:6px 8px;color:#f1f5f9;font-size:12px;font-family:monospace;width:100%}.coords[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]:focus{outline:none;border-color:#38bdf8}.drop[_ngcontent-%COMP%]{border:2px dashed #1e293b;border-radius:10px;cursor:pointer;min-height:80px;display:flex;align-items:center;justify-content:center;overflow:hidden;position:relative;transition:border-color .2s}.drop[_ngcontent-%COMP%]:hover{border-color:#38bdf8}.drop.filled[_ngcontent-%COMP%]{border-style:solid;border-color:#334155}.drop[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{width:100%;max-height:160px;object-fit:cover;display:block}.hint[_ngcontent-%COMP%]{text-align:center;color:#475569;padding:14px;font-size:12px}.hint[_ngcontent-%COMP%]   small[_ngcontent-%COMP%]{color:#334155}.x[_ngcontent-%COMP%]{position:absolute;top:6px;right:6px;background:#0009;border:none;color:#fff;width:22px;height:22px;border-radius:50%;cursor:pointer;font-size:12px}.analyze[_ngcontent-%COMP%]{width:100%;padding:10px;background:linear-gradient(135deg,#0ea5e9,#6366f1);border:none;border-radius:8px;color:#fff;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit}.gallery[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:5px}.gallery[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{width:100%;aspect-ratio:1;object-fit:cover;border-radius:6px;cursor:pointer}.overlay[_ngcontent-%COMP%]{position:fixed;inset:0;background:#000000bf;-webkit-backdrop-filter:blur(4px);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;z-index:1000}.modal[_ngcontent-%COMP%]{background:#0f172a;border:1px solid #1e293b;border-radius:14px;width:380px;max-width:92vw;padding:28px 24px;display:flex;flex-direction:column;align-items:center;gap:12px;text-align:center}.modal-icon[_ngcontent-%COMP%]{font-size:40px}.modal[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:18px;color:#f1f5f9}.modal[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:0;font-size:13px;color:#94a3b8;line-height:1.6}.modal[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0;padding-left:18px;text-align:left;font-size:13px;color:#64748b;line-height:1.8}.modal-note[_ngcontent-%COMP%]{font-size:12px;color:#334155;background:#1e293b;border-radius:8px;padding:8px 14px;width:100%}.modal-btns[_ngcontent-%COMP%]{display:flex;gap:10px;width:100%}.btn-cancel[_ngcontent-%COMP%]{flex:1;padding:9px;background:transparent;border:1px solid #334155;border-radius:8px;color:#94a3b8;cursor:pointer;font-family:inherit;font-size:13px}.btn-ok[_ngcontent-%COMP%]{flex:1;padding:9px;background:linear-gradient(135deg,#0ea5e9,#6366f1);border:none;border-radius:8px;color:#fff;font-weight:600;cursor:pointer;font-family:inherit;font-size:13px}.zoom-img[_ngcontent-%COMP%]{max-width:90vw;max-height:88vh;border-radius:10px;object-fit:contain}'],changeDetection:0})};var Dp=class n{static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Zn({type:n,selectors:[["app-root"]],decls:6,vars:0,consts:[[1,"shell"]],template:function(t,i){t&1&&(Wo(0,"div",0),Yo(1,"app-header"),Wo(2,"main"),Yo(3,"app-sidebar")(4,"app-viewer")(5,"app-node-panel"),Tc()())},dependencies:[vf,yf,wp,Tp],styles:['@charset "UTF-8";[_nghost-%COMP%]{display:block;height:100vh;width:100vw;overflow:hidden}.shell[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:100%}main[_ngcontent-%COMP%]{flex:1;display:grid;grid-template-columns:210px 1fr 310px;overflow:hidden;min-height:0}app-sidebar[_ngcontent-%COMP%]{border-right:1px solid #1e293b}app-viewer[_ngcontent-%COMP%]{padding:12px}app-node-panel[_ngcontent-%COMP%]{border-left:1px solid #1e293b}@media(max-width:900px){main[_ngcontent-%COMP%]{grid-template-columns:1fr 280px}app-sidebar[_ngcontent-%COMP%]{display:none}}@media(max-width:600px){main[_ngcontent-%COMP%]{grid-template-columns:1fr;grid-template-rows:260px 1fr}app-node-panel[_ngcontent-%COMP%]{display:none}}']})};Sv(Dp,tS).catch(n=>console.error(n));
