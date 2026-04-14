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
  await ensureServingSchema();
  await runLabelSyncIfDue();
  await refreshPrices();
  await refreshEnrichedLeaderboards();
  logLine("completed metadata sync cycle", {
    duration_ms: Math.round(performance.now() - startedAt),
  });
};

const runFlowSyncOnce = async (args: string[]) =>
  new Promise<void>((resolve, reject) => {
    const child = spawn(process.execPath, ["--require=dotenv/config", flowWorkerPath, ...args], {
      cwd: process.cwd(),
      env: process.env,
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`sync:flows exited code=${code ?? "null"} signal=${signal ?? "null"}`));
    });
  });

const runFlowSyncLoop = (args: string[]) => {
  const child = spawn(process.execPath, ["--require=dotenv/config", flowWorkerPath, ...args], {
    cwd: process.cwd(),
    env: { ...process.env, FLOW_SYNC_LOOP: "1" },
    stdio: "inherit",
  });

  const stop = () => {
    child.kill("SIGTERM");
  };

  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);

  return new Promise<never>((_, reject) => {
    child.on("error", reject);
    child.on("close", (code, signal) => {
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

  if (once) {
    await runFlowSyncOnce(flowArgs);
    await runMetadataCycle(runLabelSyncIfDue);
    return;
  }

  const metadataLoop = async () => {
    while (true) {
      await runMetadataCycle(runLabelSyncIfDue);
      await sleep(metadataMinutes * 60 * 1000);
    }
  };

  logLine("starting sync-all worker", {
    metadata_interval_minutes: metadataMinutes,
    label_interval_minutes: Number.parseInt(parseArgValue("label-interval-minutes") || "", 10) || labelIntervalMinutes(),
    labels_enabled: !parseBool(parseArgValue("skip-labels")) ? "true" : "false",
  });

  await Promise.all([runFlowSyncLoop(flowArgs), metadataLoop()]);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
