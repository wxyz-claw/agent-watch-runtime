import type { Predicate } from './sdk.mjs';

export type WatchMode = 'until' | 'changes' | 'emitted';
export interface DeliveryConfig {
  readonly enabled: boolean;
}

type Selection =
  | { readonly mode: 'until'; readonly predicate: Readonly<Predicate> }
  | { readonly mode: 'changes' | 'emitted'; readonly predicate?: never };

interface Identity {
  readonly version: 1;
  readonly source_id: string;
  readonly consumer_id: string;
}

interface Controls {
  readonly interval_ms: number;
  readonly deadline_ms: number | null;
  readonly delivery: DeliveryConfig;
}

export type WatchConfig = Identity & Selection & Partial<Controls>;
export type NormalizedWatchConfig = Identity & Selection & Controls;

type Editable = Controls & { readonly predicate: Readonly<Predicate> };
type WithAtLeastOne<T> = {
  [K in keyof T]: Pick<T, K> & Partial<Omit<T, K>>
}[keyof T];

export type WatchPatch = { readonly version: 1 } & WithAtLeastOne<Editable>;

export type ConfigSummary = Identity & Controls & { readonly config_digest: string } & (
  | { readonly mode: 'until'; readonly predicate_digest: string }
  | { readonly mode: 'changes' | 'emitted'; readonly predicate_digest?: never }
);

/** Returns a detached, deeply frozen configuration with all runtime defaults applied. */
export function normalizeConfig(raw: unknown, defaultInterval?: number): NormalizedWatchConfig;
/** Retains exactly the requested edits. A null deadline clears the absolute deadline. */
export function normalizePatch(raw: unknown): WatchPatch;
/** Returns public control metadata and canonical digests. */
export function configSummary(config: unknown): ConfigSummary;
