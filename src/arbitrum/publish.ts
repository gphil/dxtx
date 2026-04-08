import { runChainPublish } from "../publish/run.js";

runChainPublish("arbitrum").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
