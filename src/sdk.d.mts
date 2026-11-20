export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type ObservationContract = 'durable_ready' | 'observe_change' | 'source_emitted';
export type Coverage = 'complete' | 'partial' | 'failed';
export type Authority = 'authoritative' | 'advisory';
export type Truth = 'TRUE' | 'FALSE' | 'UNKNOWN';

export type DiagnosticCode =
  | 'source_unavailable' | 'source_timeout' | 'source_invalid' | 'source_stale' | 'source_error'
  | 'coverage_partial' | 'coverage_failed' | 'advisory' | 'history_gap' | 'unsupported_contract'
  | 'invalid_observation' | 'invalid_predicate' | 'deadline_reached' | 'canceled' | 'capacity';

export interface ObservationEvidence {
  snapshot_digest?: string;
  diff_digest?: string;
  snapshot_bytes?: number;
  diff_bytes?: number;
  history_count?: number;
  history_complete?: false;
}

interface ObservationMetadata {
  version: 1;
  cursor?: string;
  evidence?: ObservationEvidence;
  code?: DiagnosticCode;
}

type Snapshot = {
  contract: 'durable_ready' | 'observe_change';
  source_event_id?: never;
} & (
  | { coverage: 'complete'; authority: 'authoritative'; value: JsonValue }
  | { coverage: 'complete'; authority: 'advisory'; value?: JsonValue }
  | { coverage: 'partial' | 'failed'; authority: Authority; value?: JsonValue }
);

type Emission = {
  contract: 'source_emitted';
  authority: Authority;
  value?: JsonValue;
} & (
  | { coverage: 'complete'; source_event_id: string }
  | { coverage: 'partial' | 'failed'; source_event_id?: string }
);

export type Observation = ObservationMetadata & (Snapshot | Emission);

export interface SourceContext {
  mode: 'until' | 'changes' | 'emitted';
  checkpoint: { reads: number; cursor: string | null; value?: JsonValue };
  now: number;
  signal: AbortSignal;
}

export type Source = (context: SourceContext) => Observation | Promise<Observation>;
export type Predicate = { path: string } & (
  | { equals: JsonValue; contains?: never }
  | { contains: string; equals?: never }
);

export type ChangedPath = { op: 'add' | 'remove' | 'replace' } & (
  | { path: string; path_digest?: never }
  | { path_digest: string; path?: never }
);

export interface DiffEvidence {
  before_digest: string;
  after_digest: string;
  changes: ChangedPath[];
  truncated: boolean;
}

export interface EventEvidence {
  source_digest?: string;
  source_event_digest?: string;
  observation?: ObservationEvidence;
  diff?: DiffEvidence;
  code?: DiagnosticCode;
}

export interface EventFields {
  version: 1;
  event_id: string;
  watch_id: string;
  sequence: number;
  kind: 'ready' | 'changed' | 'emitted' | 'deadline' | 'canceled' | 'error';
  observed_at: number;
  evidence: EventEvidence;
}

type Frozen<T> = T extends object ? { readonly [K in keyof T]: Frozen<T[K]> } : T;
export type WatchEvent = Frozen<EventFields & { digest: string }>;

export const MAX_EVENT_BYTES: 32768;
export const MAX_VALUE_BYTES: 65536;
export function canonical(value: JsonValue): string;
export function digest(value: JsonValue): string;
export function normalizeObservation(raw: unknown, expectedContract?: ObservationContract): Observation;
export function isAuthoritativeObservation(observation: Observation): boolean;
export function validatePredicate(raw: unknown): Predicate;
export function evaluatePredicate(value: unknown, predicate: Predicate): Truth;
export function diffEvidence(before: JsonValue, after: JsonValue, options?: { publicPaths?: readonly string[] | ReadonlySet<string> }): DiffEvidence;
export function sealEvent(fields: EventFields): WatchEvent;
export function validateEvent(raw: unknown): WatchEvent;
