import { Aimd } from './aimd.js';
import { FixedRate } from './fixed-rate.js';

export const REGISTRY = Object.fromEntries([FixedRate, Aimd].map((c) => [c.ccName, c]));

export function makeCC(name, params) {
  const Cls = REGISTRY[name];
  if (!Cls) throw new Error(`unknown CC ${name}; have ${Object.keys(REGISTRY)}`);
  return new Cls(params ?? {});
}
