import { runChainPublish } from "../publish/run.js";

runChainPublish("base").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
