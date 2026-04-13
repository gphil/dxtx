import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { supportedChains } from "../chains.js";
import { logLine } from "../log.js";
import type { Chain } from "../types.js";

const currentDir = dirname(fileURLToPath(import.meta.url));
const workerEntryPath = join(currentDir, "..", "sync-flows.js");
const maxRecentLines = 20;
const restartScheduleMs = [5_000, 10_000, 30_000, 60_000, 120_000];
const oomPattern = /Out of Memory Error|could not allocate block/i;
const restartDelayMs = (attempt: number) =>
  restartScheduleMs[Math.min(attempt, restartScheduleMs.length - 1)] ?? 120_000;
const normalizeEnvName = (chain: Chain) => chain.toUpperCase();
const chainEnvKey = (baseKey: string, chain: Chain) => `${baseKey}_${normalizeEnvName(chain)}`;
const parsePositiveInt = (value: string | undefined) => {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
};
const configuredMaxChunks = (chain: Chain) =>
  parsePositiveInt(process.env[chainEnvKey("FLOW_SYNC_MAX_CHUNKS", chain)] ?? process.env.FLOW_SYNC_MAX_CHUNKS) ?? 32;
const configuredThreads = (chain: Chain) =>
  parsePositiveInt(process.env[chainEnvKey("ANALYTICS_THREADS", chain)] ?? process.env.ANALYTICS_THREADS);

type WorkerOverrides = {
  maxChunks?: number;
  threads?: number;
};

const splitArgs = (values: string[]) =>
  values.flatMap((value) => value.split(",")).map((value) => value.trim()).filter(Boolean);

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const requestedChains = (args: string[]) => {
  const cliChains = [...new Set(splitArgs(args).filter(isChain))];

  if (cliChains.length > 0) {
    return cliChains;
  }

  const envChains = [...new Set(splitArgs([process.env.CACHE_CHAINS || ""]).filter(isChain))];

  return envChains.length > 0 ? envChains : supportedChains;
};

const writePrefixedLines = (
  chain: Chain,
  output: NodeJS.WriteStream,
  chunk: string,
  pending: string,
) => {
  const combined = `${pending}${chunk}`;
  const lines = combined.split(/\r?\n/);
  const nextPending = lines.pop() ?? "";

  lines
    .filter(Boolean)
    .forEach((line) => output.write(`[${chain}] ${line}\n`));

  return nextPending;
};

const pipeChildOutput = (chain: Chain, child: ReturnType<typeof spawn>) => {
  let recentLines: string[] = [];
  const recordLine = (line: string) => {
    recentLines = [...recentLines.slice(-(maxRecentLines - 1)), line];
  };
  const attach = (
    stream: NodeJS.ReadableStream | null,
    output: NodeJS.WriteStream,
  ) => {
    if (!stream) {
      return;
    }

    let pending = "";
    stream.setEncoding("utf8");
    stream.on("data", (chunk) => {
      const combined = `${pending}${chunk}`;
      const lines = combined.split(/\r?\n/);
      pending = lines.pop() ?? "";

      lines
        .filter(Boolean)
        .forEach((line) => {
          const prefixed = `[${chain}] ${line}`;
          output.write(`${prefixed}\n`);
          recordLine(prefixed);
        });
    });
    stream.on("end", () => {
      if (pending) {
        const prefixed = `[${chain}] ${pending}`;
        output.write(`${prefixed}\n`);
        recordLine(prefixed);
      }
    });
  };

  attach(child.stdout, process.stdout);
  attach(child.stderr, process.stderr);

  return () => recentLines;
};

type ChildExit = {
  code: number | null;
  signal: NodeJS.Signals | null;
  recentLines: string[];
};

const recentFailureLine = (recentLines: string[]) => [...recentLines].reverse()[0];

const childEnv = (chain: Chain, overrides: WorkerOverrides) => ({
  ...process.env,
  ...(overrides.maxChunks === undefined
    ? {}
    : { [chainEnvKey("FLOW_SYNC_MAX_CHUNKS", chain)]: String(overrides.maxChunks) }),
  ...(overrides.threads === undefined
    ? {}
    : { [chainEnvKey("ANALYTICS_THREADS", chain)]: String(overrides.threads) }),
});

const nextOverridesOnOom = (chain: Chain, overrides: WorkerOverrides): WorkerOverrides => {
  const currentMaxChunks = overrides.maxChunks ?? configuredMaxChunks(chain);
  const currentThreads = overrides.threads ?? configuredThreads(chain) ?? 1;

  return {
    maxChunks: Math.max(1, Math.floor(currentMaxChunks / 2)),
    threads: currentThreads > 1 ? 1 : currentThreads,
  };
};

const runChainOnce = (chain: Chain, overrides: WorkerOverrides = {}) =>
  new Promise<ChildExit>((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--require=dotenv/config", workerEntryPath, chain],
      {
        cwd: process.cwd(),
        env: childEnv(chain, overrides),
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    const getRecentLines = pipeChildOutput(chain, child);

    child.on("error", reject);
    child.on("close", (code, signal) => {
      resolve({
        code,
        signal,
        recentLines: getRecentLines(),
      });
    });
  });

const runChain = async (chain: Chain) => {
  const result = await runChainOnce(chain);

  if (result.code === 0) {
    return;
  }

  throw new Error(`chain=${chain} exited with code=${result.code ?? "null"} signal=${result.signal ?? "null"}`);
};

const runChainLoop = async (chain: Chain) => {
  let restartCount = 0;
  let overrides: WorkerOverrides = {};

  while (true) {
    const result = await runChainOnce(chain, overrides);

    if (result.signal === "SIGINT" || result.signal === "SIGTERM") {
      return;
    }

    const nextOverrides = oomPattern.test(result.recentLines.join("\n"))
      ? nextOverridesOnOom(chain, overrides)
      : overrides;
    const overridesChanged =
      nextOverrides.maxChunks !== overrides.maxChunks || nextOverrides.threads !== overrides.threads;
    overrides = nextOverrides;

    const backoffMs = restartDelayMs(restartCount);
    restartCount += 1;

    logLine("restarting flow sync worker", {
      chain,
      attempt: restartCount,
      retry_in_sec: Math.round(backoffMs / 1_000),
      exit_code: result.code ?? undefined,
      signal: result.signal ?? undefined,
      error: recentFailureLine(result.recentLines),
      auto_max_chunks: overrides.maxChunks,
      auto_threads: overrides.threads,
      oom_backoff: overridesChanged ? 1 : undefined,
    });

    await sleep(backoffMs);
  }
};

const oneShotConcurrency = () => {
  const configured = Number.parseInt(process.env.FLOW_SYNC_CONCURRENCY || "2", 10);
  return Number.isFinite(configured) && configured > 0 ? configured : 2;
};

const runBatch = async (chains: Chain[], concurrency: number) => {
  const queue = [...chains];

  const worker = async () => {
    while (true) {
      const chain = queue.shift();

      if (!chain) {
        return;
      }

      await runChain(chain);
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(concurrency, chains.length) }, () => worker()),
  );
};

const main = async () => {
  const chains = requestedChains(process.argv.slice(2));
  const loop = process.env.FLOW_SYNC_LOOP === "1" || process.env.FLOW_SYNC_LOOP === "true";
  const concurrency = loop ? chains.length : oneShotConcurrency();

  logLine("starting flow sync workers", {
    chains: chains.join(","),
    loop: loop ? 1 : undefined,
    concurrency,
  });

  if (loop) {
    await Promise.all(chains.map((chain) => runChainLoop(chain)));
    return;
  }

  await runBatch(chains, concurrency);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
