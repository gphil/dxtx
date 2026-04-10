import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { supportedChains } from "../chains.js";
import { logLine } from "../log.js";
import type { Chain } from "../types.js";

const currentDir = dirname(fileURLToPath(import.meta.url));
const workerEntryPath = join(currentDir, "..", "sync-flows.js");

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
      pending = writePrefixedLines(chain, output, chunk, pending);
    });
    stream.on("end", () => {
      if (pending) {
        output.write(`[${chain}] ${pending}\n`);
      }
    });
  };

  attach(child.stdout, process.stdout);
  attach(child.stderr, process.stderr);
};

const runChain = (chain: Chain) =>
  new Promise<void>((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--require=dotenv/config", workerEntryPath, chain],
      {
        cwd: process.cwd(),
        env: process.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    pipeChildOutput(chain, child);

    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`chain=${chain} exited with code=${code ?? "null"} signal=${signal ?? "null"}`));
    });
  });

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
    await Promise.all(chains.map((chain) => runChain(chain)));
    return;
  }

  await runBatch(chains, concurrency);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
