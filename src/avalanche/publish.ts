import { runChainPublish } from "../publish/run.js";

runChainPublish("avalanche").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
