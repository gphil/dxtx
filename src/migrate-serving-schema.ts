import { logLine } from "./log.js";
import { isDirectRun } from "./serving.js";
import { migrateServingSchemaLegacy } from "./serving-schema.js";

export const migrateServingSchema = async () => {
  logLine("starting legacy serving schema migration", {});
  await migrateServingSchemaLegacy();
  logLine("completed legacy serving schema migration", {});
};

if (isDirectRun(import.meta.url)) {
  migrateServingSchema().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
