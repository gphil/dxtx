import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { refreshEnrichedLeaderboards } from "../refresh-enriched-leaderboards.js";
import { logLine } from "../log.js";
import { parseArgValue, parseBool } from "../serving.js";
import { ensureServingSchema } from "../serving-schema.js";
import { syncLabels } from "../sync-labels.js";
import { refreshPrices } from "../sync-prices.js";

const currentDir = dirname(fileURLToPath(import.meta.url));
const flowWorkerPath = join(currentDir, "sync-flows.js");

const metadataIntervalMinutes = () => {
  const value = Number.parseInt(process.env.METADATA_SYNC_INTERVAL_MINUTES || "60", 10);
  return Number.isFinite(value) && value > 0 ? value : 60;
};

const labelIntervalMinutes = () => {
  const value = Number.parseInt(process.env.LABEL_SYNC_INTERVAL_MINUTES || "1440", 10);
  return Number.isFinite(value) && value > 0 ? value : 1440;
};

const forwardFlowArgs = (args: string[]) => args.filter((arg) => !arg.startsWith("--"));

const createShutdownController = () => {
  let shuttingDown = false;
  let resolveShutdown: () => void = () => {};
  const shutdownPromise = new Promise<void>((resolve) => {
    resolveShutdown = resolve;
  });

  const requestShutdown = (signal: NodeJS.Signals) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    logLine("received shutdown signal", { signal });
    resolveShutdown();
  };

  process.once("SIGINT", () => requestShutdown("SIGINT"));
  process.once("SIGTERM", () => requestShutdown("SIGTERM"));

  return {
    isShuttingDown: () => shuttingDown,
    waitForShutdown: () => shutdownPromise,
  };
};

const createLabelSyncRunner = () => {
  const configuredInterval = Number.parseInt(parseArgValue("label-interval-minutes") || "", 10);
  const intervalMinutes =
    Number.isFinite(configuredInterval) && configuredInterval > 0
      ? configuredInterval
      : labelIntervalMinutes();
  const enabled = !parseBool(parseArgValue("skip-labels"));
  let nextRunAt = 0;

  return async () => {
    if (!enabled || Date.now() < nextRunAt) {
      return;
    }

    const startedAt = performance.now();

    try {
      await syncLabels();
      logLine("completed label sync cycle", {
        duration_ms: Math.round(performance.now() - startedAt),
        label_interval_minutes: intervalMinutes,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logLine("label sync cycle failed", {
        duration_ms: Math.round(performance.now() - startedAt),
        error: message,
      });
    }

    nextRunAt = Date.now() + intervalMinutes * 60 * 1000;
  };
};

const runMetadataCycle = async (runLabelSyncIfDue: () => Promise<void>) => {
  const startedAt = performance.now();
  await runLabelSyncIfDue();
  await refreshPrices();
  await refreshEnrichedLeaderboards();
  logLine("completed metadata sync cycle", {
    duration_ms: Math.round(performance.now() - startedAt),
  });
};

const runFlowSyncOnce = async ({
  args,
  isShuttingDown,
  waitForShutdown,
}: {
  args: string[];
  isShuttingDown: () => boolean;
  waitForShutdown: () => Promise<void>;
}) =>
  new Promise<void>((resolve, reject) => {
    const child = spawn(process.execPath, ["--require=dotenv/config", flowWorkerPath, ...args], {
      cwd: process.cwd(),
      env: { ...process.env, SKIP_POSTGRES_SCHEMA: "1" },
      stdio: "inherit",
    });

    void waitForShutdown().then(() => {
      child.kill("SIGTERM");
    });

    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (isShuttingDown() && (signal === "SIGINT" || signal === "SIGTERM")) {
        resolve();
        return;
      }

      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`sync:flows exited code=${code ?? "null"} signal=${signal ?? "null"}`));
    });
  });

const runFlowSyncLoop = ({
  args,
  isShuttingDown,
  waitForShutdown,
}: {
  args: string[];
  isShuttingDown: () => boolean;
  waitForShutdown: () => Promise<void>;
}) => {
  const child = spawn(process.execPath, ["--require=dotenv/config", flowWorkerPath, ...args], {
    cwd: process.cwd(),
    env: { ...process.env, FLOW_SYNC_LOOP: "1", SKIP_POSTGRES_SCHEMA: "1" },
    stdio: "inherit",
  });

  void waitForShutdown().then(() => {
    child.kill("SIGTERM");
  });

  return new Promise<void>((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (isShuttingDown() && (signal === "SIGINT" || signal === "SIGTERM")) {
        resolve();
        return;
      }

      reject(new Error(`sync:flows loop exited code=${code ?? "null"} signal=${signal ?? "null"}`));
    });
  });
};

const main = async () => {
  const once = parseBool(parseArgValue("once"));
  const configuredInterval = Number.parseInt(parseArgValue("metadata-interval-minutes") || "", 10);
  const metadataMinutes =
    Number.isFinite(configuredInterval) && configuredInterval > 0
      ? configuredInterval
      : metadataIntervalMinutes();
  const flowArgs = forwardFlowArgs(process.argv.slice(2));
  const runLabelSyncIfDue = createLabelSyncRunner();
  const shutdown = createShutdownController();

  logLine("ensuring serving schema", {});
  await ensureServingSchema();
  logLine("serving schema ready", {});

  if (once) {
    await runFlowSyncOnce({
      args: flowArgs,
      isShuttingDown: shutdown.isShuttingDown,
      waitForShutdown: shutdown.waitForShutdown,
    });

    if (!shutdown.isShuttingDown()) {
      await runMetadataCycle(runLabelSyncIfDue);
    }

    return;
  }

  const metadataLoop = async () => {
    while (!shutdown.isShuttingDown()) {
      await runMetadataCycle(runLabelSyncIfDue);
      await Promise.race([sleep(metadataMinutes * 60 * 1000), shutdown.waitForShutdown()]);
    }
  };

  logLine("starting sync-all worker", {
    metadata_interval_minutes: metadataMinutes,
    label_interval_minutes: Number.parseInt(parseArgValue("label-interval-minutes") || "", 10) || labelIntervalMinutes(),
    labels_enabled: !parseBool(parseArgValue("skip-labels")) ? "true" : "false",
  });

  await Promise.all([
    runFlowSyncLoop({
      args: flowArgs,
      isShuttingDown: shutdown.isShuttingDown,
      waitForShutdown: shutdown.waitForShutdown,
    }),
    metadataLoop(),
  ]);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
