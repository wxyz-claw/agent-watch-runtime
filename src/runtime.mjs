import { evaluatePredicate, isAuthoritativeObservation, normalizeObservation, validatePredicate } from './sdk.mjs';
import { configSummary, normalizeConfig, normalizePatch } from './configuration.mjs';
import { deliverOnce, syncDeliveryRoutes } from './delivery.mjs';

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
    intervalMs = 1000,parallelism = 4,retentionMs = 7 * 86400000,pruneIntervalMs = 60000} = {}) {
    if (!store || (retentionMs !== null && !integer(retentionMs,0,Number.MAX_SAFE_INTEGER)) ||
        !integer(pruneIntervalMs,1,86400000) || !(sources instanceof Map) || !(routes instanceof Map) || typeof now !== 'function' ||
        !integer(intervalMs,20,86400000) || !integer(parallelism,1,32) ||
        !integer(collectTimeoutMs,1,60000) || !integer(deliveryTimeoutMs,1,30000)) throw Error('invalid_request');
    for (const [name,read] of sources) {
      if (!isId(name) || (typeof read !== 'function' && read !== null)) throw Error('invalid_request');
    }
    for (const name of routes.keys()) if (!isId(name)) throw Error('invalid_request');
    Object.assign(this,{store,sources,routes,now,collectTimeoutMs,deliveryTimeoutMs,intervalMs,parallelism,
      retentionMs,pruneIntervalMs});
    this.nextPruneMs = 0;
    this.busySources = new Set();
    this.previews = new Map();
    this.busyConsumers = new Set();
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
    this.registered({...args,mode});
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
    return this.createDefinition(this.definition(mode,args));
  }

  registered(config) {
    if (!this.sources.has(config.source_id)) throw Error('source_not_found');
    if (!this.routes.has(config.consumer_id)) throw Error('consumer_not_found');
    if (this.sources.get(config.source_id) === null && config.mode !== 'emitted') throw Error('invalid_request');
    return config;
  }

  async createConfigured(input,requestId) {
    if (!isId(requestId)) throw Error('invalid_request');
    const {version,delivery,...definition} = this.registered(normalizeConfig(input,this.intervalMs));
    // Keep the original idempotency signature compatible with unversioned creation calls.
    if (!delivery.enabled) definition.delivery = delivery;
    return this.createDefinition({...definition,request_id:requestId});
  }

  async createDefinition(definition) {
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

  async preview(input,watchId) {
    const config = this.registered(normalizeConfig(input,this.intervalMs));
    const {mode,source_id} = config;
    let basis, observation;
    if (watchId !== undefined) {
      if (!isId(watchId)) throw Error('invalid_request');
      // This query reads committed evidence even at an elapsed deadline. It never runs a scheduler step.
      const row = this.store.controlRow(watchId);
      if (row.mode !== mode || row.source_id !== source_id || row.consumer_id !== config.consumer_id) {
        throw Error('invalid_request');
      }
      const checkpoint = JSON.parse(row.checkpoint);
      basis = 'checkpoint';
      observation = {contract:contracts[mode],coverage:row.coverage ?? 'failed',authority:row.authority ?? 'advisory',
        code:row.coverage === null ? 'preview_unavailable' : null};
      if (Object.hasOwn(checkpoint,'value')) observation.value = checkpoint.value;
    } else {
      const read = this.sources.get(source_id)?.preview;
      if (typeof read !== 'function' || this.previews.has(source_id) || this.previews.size >= this.parallelism) {
        basis = 'unavailable';
        observation = unavailable(mode,'preview_unavailable');
      } else {
        basis = 'sample';
        observation = await this.previewSample(read,source_id,mode);
      }
    }
    const truth = mode === 'until' && isAuthoritativeObservation(observation)
      ? evaluatePredicate(observation.value,config.predicate) : 'UNKNOWN';
    return {configuration:configSummary(config),basis,truth,coverage:observation.coverage,
      authority:observation.authority,code:observation.code ?? null};
  }

  async previewSample(read,sourceId,mode) {
    const controller = new AbortController();
    const pending = Promise.resolve().then(() => read({mode,checkpoint:{reads:0,cursor:null},
      signal:controller.signal,now:this.now()})).then(
      value => this.normalize(mode,value), () => unavailable(mode,'source_error'));
    // A noncooperative adapter retains its preview slot until it settles. Collection uses separate slots.
    this.previews.set(sourceId,pending);
    void pending.then(() => this.previews.delete(sourceId));
    let timer;
    const timeout = new Promise(resolve => {
      timer = setTimeout(() => {
        resolve(unavailable(mode,'source_timeout'));
        controller.abort();
      },this.collectTimeoutMs);
    });
    try { return await Promise.race([pending,timeout]); }
    finally { clearTimeout(timer); }
  }

  list(options = {}) {
    if (options === null || typeof options !== 'object' || Array.isArray(options) ||
        Object.keys(options).some(key => !['after','limit','state'].includes(key)) ||
        (options.after !== undefined && !isId(options.after))) throw Error('invalid_request');
    return this.store.list(options);
  }

  scheduledWatch(watchId,revision) {
    if (!isId(watchId) || !integer(revision,1,Number.MAX_SAFE_INTEGER)) throw Error('invalid_request');
    const row = this.store.one('SELECT source_id FROM watches WHERE watch_id=?',watchId);
    if (!row) throw Error('watch_not_found');
    return typeof this.sources.get(row.source_id) === 'function';
  }

  pause(watchId,revision) {
    return this.store.setPaused(watchId,revision,true,this.scheduledWatch(watchId,revision),this.now());
  }

  resume(watchId,revision) {
    return this.store.setPaused(watchId,revision,false,this.scheduledWatch(watchId,revision),this.now());
  }

  update(watchId,revision,patch) {
    return this.store.edit(watchId,revision,normalizePatch(patch),this.scheduledWatch(watchId,revision),this.now());
  }

  async checkNow(watchId,revision,seq) {
    if (!integer(seq,1,Number.MAX_SAFE_INTEGER)) throw Error('invalid_request');
    const scheduled = this.scheduledWatch(watchId,revision);
    const now = this.now();
    const op = this.store.queueCheck(watchId,revision,seq,scheduled,now);
    if (op.state !== 'queued' && op.state !== 'running') return op;
    if (this.reads.size < this.parallelism) {
      const claim = this.store.claimObservation(now,{watchId,skipSources:[...this.busySources],leaseMs:this.collectionLease()});
      if (claim) await this.launchRead(claim);
    }
    const result = this.store.manualCheck(watchId);
    if (result.operation_seq !== seq) throw Error('operation_conflict');
    return result;
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

  history(watchId, options) {
    if (!isId(watchId)) throw Error('invalid_request');
    return this.store.history(watchId,options);
  }

  resumeDelivery(consumerId) {
    if (!this.routes.has(consumerId)) throw Error('consumer_not_found');
    return this.store.resumeDelivery(consumerId,this.now());
  }

  // Operator recovery: return exhausted deliveries on one watch to the queue with a fresh budget.
  redrive(watchId, {event_id = null} = {}) {
    if (!isId(watchId) || (event_id !== null && !isId(event_id))) throw Error('invalid_request');
    return this.store.redrive(watchId,this.now(),event_id);
  }

  // Bounded retention pass. pulse() runs it every pruneIntervalMs unless retention is disabled.
  prune({limit = 256} = {}) {
    if (this.retentionMs === null) return {watches:0,events:0};
    return this.store.prune(this.now(),this.retentionMs,limit);
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
    catch (err) { return unavailable(mode,err?.code === 'unsupported_contract' ? 'unsupported_contract' : 'invalid_observation'); }
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
    if (this.retentionMs !== null && now >= this.nextPruneMs) {
      this.nextPruneMs = now + this.pruneIntervalMs;
      this.store.prune(now,this.retentionMs);
    }
    const started = [];
    while (this.reads.size < this.parallelism) {
      const claim = this.store.claimObservation(now,{skipSources:[...this.busySources],leaseMs:this.collectionLease()});
      if (!claim) break;
      started.push(this.launchRead(claim));
    }
    const snapshots = syncDeliveryRoutes(this.store,this.routes,now);
    while (this.sends.size < this.parallelism) {
      const claim = this.store.claimDelivery(now,[...this.busyConsumers]);
      if (!claim) break;
      this.busyConsumers.add(claim.consumer_id);
      const task = deliverOnce(this.store,this.routes,{now:this.now,timeoutMs:this.deliveryTimeoutMs,claim,snapshots})
        .finally(() => { this.busyConsumers.delete(claim.consumer_id); this.sends.delete(task); });
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
