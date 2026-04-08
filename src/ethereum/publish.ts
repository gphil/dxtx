import { runChainPublish } from "../publish/run.js";

runChainPublish("ethereum").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
