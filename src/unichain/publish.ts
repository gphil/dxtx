import { runChainPublish } from "../publish/run.js";

runChainPublish("unichain").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
