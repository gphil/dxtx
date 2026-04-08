import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { loadDuneUniverseTokenMetadata } from "../dune-tokens.js";
import { resolveRpcUrl, resolveSqdUrl } from "../chains.js";
import { logLine } from "../log.js";
import { loadPublishTargetsByChain, selectedChains } from "../publish/run.js";
import type { Chain } from "../types.js";

const currentDir = dirname(fileURLToPath(import.meta.url));
const childEntryPath = (chain: Chain) => join(currentDir, "..", chain, "publish.js");

const writeSharedMetadata = async (chains: Chain[]) => {
  const targetsByChain = await loadPublishTargetsByChain(chains);
  const scopes = chains.map((chain) => ({
    chain,
    addresses: (targetsByChain.get(chain) ?? []).map((target) => target.address),
  }));
  const tempDir = await mkdtemp(join(tmpdir(), "dxtx-shared-metadata-"));
  const metadataPath = join(tempDir, "tokens.json");
  const tokens = await loadDuneUniverseTokenMetadata({
    scopes,
  }).catch((error) => {
    logLine("skipped shared dune token metadata", {
      chains: chains.join(","),
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  });

  logLine("resolved shared dune token metadata", {
    chains: chains.join(","),
    targets: scopes.reduce((sum, scope) => sum + scope.addresses.length, 0),
    resolved: tokens.length,
  });

  await writeFile(metadataPath, JSON.stringify(tokens), "utf8");
  return metadataPath;
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

const runChain = (chain: Chain, sharedMetadataPath: string) =>
  new Promise<void>((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--require=dotenv/config", childEntryPath(chain)],
      {
        cwd: process.cwd(),
        env: {
          ...process.env,
          DXTX_SHARED_METADATA_PATH: sharedMetadataPath,
          FORCE_PRETTY_LOGGER: process.env.FORCE_PRETTY_LOGGER ?? "1",
        },
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

const runnableChains = (chains: Chain[]) =>
  chains.filter((chain) => {
    try {
      resolveSqdUrl(chain);
      resolveRpcUrl(chain);
      return true;
    } catch (error) {
      logLine("skipped chain publisher", {
        chain,
        error: error instanceof Error ? error.message : String(error),
      });
      return false;
    }
  });

const main = async () => {
  const chains = runnableChains(selectedChains());

  if (chains.length === 0) {
    logLine("skipped all chain publishers", {
      error: "no runnable chains configured",
    });
    return;
  }

  const sharedMetadataPath = await writeSharedMetadata(chains);

  logLine("starting all chain publishers", {
    chains: chains.join(","),
  });

  try {
    const results = await Promise.allSettled(chains.map((chain) => runChain(chain, sharedMetadataPath)));
    const failures = results.flatMap((result, index) =>
      result.status === "rejected"
        ? [{ chain: chains[index], error: result.reason instanceof Error ? result.reason.message : String(result.reason) }]
        : [],
    );

    if (failures.length > 0) {
      failures.forEach((failure) =>
        logLine("chain publisher failed", {
          chain: failure.chain,
          error: failure.error,
        }),
      );
      throw new Error(`failed chain publishers: ${failures.map((failure) => failure.chain).join(",")}`);
    }
  } finally {
    await rm(sharedMetadataPath, { force: true }).catch(() => undefined);
    await rm(dirname(sharedMetadataPath), { recursive: true, force: true }).catch(() => undefined);
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
