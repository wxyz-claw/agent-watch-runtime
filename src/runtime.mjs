import { normalizeObservation, validatePredicate } from './sdk.mjs';
import { deliverOnce } from './delivery.mjs';

const contracts = Object.freeze({until:'durable_ready', changes:'observe_change', emitted:'source_emitted'});
const fields = new Set(['source_id','consumer_id','request_id','deadline_ms','interval_ms','predicate']);
const id = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const integer = (value,low,high) => Number.isSafeInteger(value) && value >= low && value <= high;
const isId = (value) => typeof value === 'string' && id.test(value);

function unavailable(mode, code) {
  return {version:1,contract:contracts[mode],coverage:'failed',authority:'advisory',code};
}

export class WatchRuntime {
  constructor({store,sources,routes,now = Date.now,collectTimeoutMs = 3000,deliveryTimeoutMs = 3000,
    intervalMs = 1000,parallelism = 4} = {}) {
    if (!store || !(sources instanceof Map) || !(routes instanceof Map) || typeof now !== 'function' ||
        !integer(intervalMs,20,86400000) || !integer(parallelism,1,32) ||
        !integer(collectTimeoutMs,1,60000) || !integer(deliveryTimeoutMs,1,30000)) throw Error('invalid_request');
    for (const [name,read] of sources) {
      if (!isId(name) || (typeof read !== 'function' && read !== null)) throw Error('invalid_request');
    }
    for (const name of routes.keys()) if (!isId(name)) throw Error('invalid_request');
    Object.assign(this,{store,sources,routes,now,collectTimeoutMs,deliveryTimeoutMs,intervalMs,parallelism});
    this.busySources = new Set();
    this.abandoned = new Map();
    this.reads = new Set();
    this.sends = new Set();
    this.lastError = null;
    this.timer = null;
  }

  definition(mode,args) {
    if (!Object.hasOwn(contracts,mode) || !args || typeof args !== 'object' || Array.isArray(args) ||
        Object.keys(args).some(k => !fields.has(k)) || ![args.source_id,args.consumer_id,args.request_id].every(isId) ||
        ('predicate' in args) !== (mode === 'until')) throw Error('invalid_request');
    if (!this.sources.has(args.source_id)) throw Error('source_not_found');
    if (!this.routes.has(args.consumer_id)) throw Error('consumer_not_found');
    if (this.sources.get(args.source_id) === null && mode !== 'emitted') throw Error('invalid_request');
    const interval = args.interval_ms === undefined ? this.intervalMs : args.interval_ms;
    const deadline = args.deadline_ms === undefined ? null : args.deadline_ms;
    if (!integer(interval,20,86400000) || (args.deadline_ms !== undefined && !integer(deadline,0,Number.MAX_SAFE_INTEGER))) {
      throw Error('invalid_request');
    }
    const def = {mode,source_id:args.source_id,consumer_id:args.consumer_id,request_id:args.request_id,
      interval_ms:interval,deadline_ms:deadline};
    if (mode === 'until') {
      try { def.predicate = validatePredicate(args.predicate); }
      catch { throw Error('invalid_request'); }
    }
    return def;
  }

  async create(mode, args) {
    const definition = this.definition(mode,args);
    const now = this.now();
    const scheduled = typeof this.sources.get(definition.source_id) === 'function';
    const {watch_id} = this.store.create(definition,now,scheduled);
    this.store.expire(now,watch_id);
    if (scheduled && this.reads.size < this.parallelism && !this.busySources.has(definition.source_id)) {
      const claim = this.store.claimObservation(now,{watchId:watch_id,leaseMs:this.collectionLease()});
      if (claim) await this.launchRead(claim);
    }
    return this.get(watch_id);
  }

  get(watchId) {
    if (!isId(watchId)) throw Error('invalid_request');
    this.store.expire(this.now(),watchId);
    return this.store.get(watchId);
  }

  cancel(watchId) {
    if (!isId(watchId)) throw Error('invalid_request');
    return this.store.cancel(watchId,this.now());
  }

  // Trusted in-process source entry point. A null reader registers an event-driven source.
  ingest(sourceId, watchId, observation) {
    if (!this.sources.has(sourceId) || this.sources.get(sourceId) !== null) throw Error('source_not_found');
    const status = this.get(watchId);
    if (status.source_id !== sourceId || status.mode !== 'emitted') throw Error('invalid_request');
    if (status.state !== 'active') return status;
    const claim = this.store.claimObservation(this.now(),{watchId,force:true});
    if (!claim) throw Error('store_error');
    this.store.apply(claim,this.normalize('emitted',observation),this.now(),false);
    return this.get(watchId);
  }

  collectionLease() { return Math.max(this.store.leaseMs,this.collectTimeoutMs + 1000); }

  normalize(mode, observation) {
    try { return normalizeObservation(observation,contracts[mode]); }
    catch { return unavailable(mode,'invalid_observation'); }
  }

  async sample(claim) {
    const {source_id,mode,checkpoint} = claim;
    const source = this.sources.get(source_id);
    if (typeof source !== 'function') return unavailable(mode,'source_unavailable');
    if (this.abandoned.has(source_id)) return unavailable(mode,'source_timeout');

    const controller = new AbortController();
    let timer;
    let timedOut = false;
    const pending = Promise.resolve().then(() => source({checkpoint,signal:controller.signal,now:this.now(),mode}))
      .then(value => this.normalize(mode,value), () => unavailable(mode,'source_error'));
    const timeout = new Promise(resolve => {
      timer = setTimeout(() => {
        timedOut = true;
        this.abandoned.set(source_id,pending);
        resolve(unavailable(mode,'source_timeout'));
        controller.abort();
      },this.collectTimeoutMs);
    });
    try { return await Promise.race([pending,timeout]); }
    finally {
      clearTimeout(timer);
      if (timedOut) {
        // At most one non-cooperative promise can survive a timeout for each registered source.
        void pending.then(() => {
          if (this.abandoned.get(source_id) === pending) this.abandoned.delete(source_id);
        });
      }
    }
  }

  launchRead(claim) {
    this.busySources.add(claim.source_id);
    const task = this.sample(claim).then(obs => this.store.apply(claim,obs,this.now()))
      .finally(() => { this.busySources.delete(claim.source_id); this.reads.delete(task); });
    this.reads.add(task);
    return task;
  }

  pulse() {
    const now = this.now();
    this.store.expire(now);
    const started = [];
    while (this.reads.size < this.parallelism) {
      const claim = this.store.claimObservation(now,{skipSources:[...this.busySources],leaseMs:this.collectionLease()});
      if (!claim) break;
      started.push(this.launchRead(claim));
    }
    const sendSlots = this.parallelism - this.sends.size;
    for (let slot = 0; slot < sendSlots; slot++) {
      const task = deliverOnce(this.store,this.routes,{now:this.now,timeoutMs:this.deliveryTimeoutMs})
        .finally(() => this.sends.delete(task));
      this.sends.add(task);
      started.push(task);
    }
    return started;
  }

  async tick() { await Promise.all(this.pulse()); }

  start({tickMs = 100} = {}) {
    if (!integer(tickMs,10,60000)) throw Error('invalid_request');
    if (this.timer) return;
    const run = () => {
      try {
        for (const task of this.pulse()) task.catch(() => { this.lastError = 'store_error'; });
      } catch { this.lastError = 'store_error'; }
    };
    this.timer = setInterval(run,tickMs);
    run();
  }

  async stop() {
    clearInterval(this.timer);
    this.timer = null;
    await Promise.allSettled([...this.reads,...this.sends]);
  }
}
