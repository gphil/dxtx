import { probeFileStoreDest } from "./cache.js";
import { supportedChains } from "./chains.js";
import { logLine } from "./log.js";
import type { Chain } from "./types.js";

const isChain = (value: string): value is Chain => supportedChains.includes(value as Chain);

const resolveChain = (value: string | undefined) => {
  if (!value) {
    throw new Error(`missing chain argument; expected one of ${supportedChains.join(",")}`);
  }

  if (!isChain(value)) {
    throw new Error(`unsupported chain: ${value}; expected one of ${supportedChains.join(",")}`);
  }

  return value;
};

const main = async () => {
  const chain = resolveChain(process.argv[2]);
  await probeFileStoreDest({ chain });
  logLine("file-store probe passed", { chain });
};

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logLine("file-store probe failed", { error: message });
  process.exitCode = 1;
});
