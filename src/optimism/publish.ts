import { runChainPublish } from "../publish/run.js";

runChainPublish("optimism").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
