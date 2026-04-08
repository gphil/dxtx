import { runChainPublish } from "../publish/run.js";

runChainPublish("polygon").catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
