declare module "pg-copy-streams" {
  import type { Submittable } from "pg";
  import type { Duplex } from "node:stream";

  export type CopyStream = Submittable & Duplex;

  export function from(sql: string): CopyStream;
  export function to(sql: string): CopyStream;
  export function both(sql: string): CopyStream;
}
