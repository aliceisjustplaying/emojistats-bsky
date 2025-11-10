import { fetchPdses } from "../backfill/util/fetch.js";

declare global {
  namespace NodeJS {
    interface ProcessEnv {
      BGS_HOSTNAME: string;
      BGS_ADMIN_KEY: string;
    }
  }
}

for (const envVar of ["BGS_HOSTNAME", "BGS_ADMIN_KEY"]) {
  if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
  const pdses = (await fetchPdses()).map((url) => new URL(url));

  const bgs = "https://" + process.env.BGS_HOSTNAME;
  const Authorization =
    "Basic " +
    Buffer.from("admin:" + process.env.BGS_ADMIN_KEY).toString("base64");

  await using _enableSubs =
    (await enableSubscriptions(bgs, Authorization)) ?? null;
  await using _increaseLimit =
    (await increaseLimit(bgs, Authorization)) ?? null;

  let crawled = 0;
  process.stdout.write(`Crawling... ${crawled}/${pdses.length}\r`);
  await Promise.all(
    pdses.map(async (url) => {
      try {
        const res = await fetch(`${bgs}/admin/pds/requestCrawl`, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization },
          body: JSON.stringify({
            hostname: "https://" + url.hostname,
            per_second: 200,
            per_hour: 150 * 60 * 60,
            per_day: 120 * 60 * 60 * 24,
            crawl_rate: 50,
            repo_limit: 1_000_000,
          }),
        });
        if (!res.ok) {
          await logErrorRes(res, `Error requesting crawl for ${url.hostname}`);
        }
      } catch (err) {
        console.error(
          `Network error requesting crawl for ${url.hostname}: ${err}`,
        );
      } finally {
        crawled++;
        process.stdout.write(`Crawling... ${crawled}/${pdses.length}\r`);
      }
    }),
  );
  console.log("Done crawling!");
}

async function enableSubscriptions(
  bgs: string,
  Authorization: string,
): Promise<AsyncDisposable | void> {
  const enabledRes = await fetch(`${bgs}/admin/subs/getEnabled`, {
    method: "GET",
    headers: { "Content-Type": "application/json", Authorization },
  });
  if (!enabledRes.ok) {
    await logErrorRes(enabledRes, "Error getting subscriptions enabled status");
    return;
  }
  const { enabled } = (await enabledRes.json()) as { enabled: boolean };

  if (enabled) {
    console.log("Subscriptions already enabled");
    return;
  }

  const enableRes = await fetch(`${bgs}/admin/subs/setEnabled?enabled=true`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization },
  });
  if (!enableRes.ok) {
    await logErrorRes(enableRes, "Error enabling subscriptions");
    return;
  }

  return {
    [Symbol.asyncDispose]: async () => {
      const disableRes = await fetch(
        `${bgs}/admin/subs/setEnabled?enabled=false`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization },
        },
      );
      if (!disableRes.ok) {
        await logErrorRes(disableRes, "Error re-disabling subscriptions");
      }
    },
  };
}

async function increaseLimit(bgs: string, Authorization: string) {
  const limitRes = await fetch(`${bgs}/admin/subs/perDayLimit`, {
    method: "GET",
    headers: { "Content-Type": "application/json", Authorization },
  });
  if (!limitRes.ok) {
    await logErrorRes(limitRes, "Error getting per day limit");
    return;
  }
  const { limit } = (await limitRes.json()) as { limit: number };

  const increaseRes = await fetch(
    `${bgs}/admin/subs/setPerDayLimit?limit=999999`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization },
    },
  );
  if (!increaseRes.ok) {
    await logErrorRes(increaseRes, "Error increasing per day limit");
    return;
  }

  return {
    [Symbol.asyncDispose]: async () => {
      const decreaseRes = await fetch(
        `${bgs}/admin/subs/setPerDayLimit?limit=${limit}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization },
        },
      );
      if (!decreaseRes.ok) {
        await logErrorRes(decreaseRes, "Error resetting per day limit");
      }
    },
  };
}

async function logErrorRes(res: Response, msg: string) {
  console.error(
    `${msg}: ${res.status} ${res.statusText} — ${await res
      .json()
      .then((r: any) => r?.error || "unknown error")}`,
  );
}
void main();
