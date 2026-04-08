import { runChainPublish } from "../publish/run.js";

runChainPublish("bsc").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
