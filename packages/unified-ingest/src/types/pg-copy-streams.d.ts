declare module "pg-copy-streams" {
  import type { PoolClient } from "pg";
  export function from(sql: string): NodeJS.ReadWriteStream;
  export function to(sql: string): NodeJS.ReadWriteStream;
}
