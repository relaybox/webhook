import "dotenv/config";

import { startWorker, stopWorker } from "@/module/worker";
import { getLogger } from "@/util/logger.util";
import { cleanupRedisClient } from "@/lib/redis";
import { cleanupPgPool } from "@/lib/pg";

const logger = getLogger("webhook-service");

startWorker();

async function shutdown(signal: string): Promise<void> {
  logger.info(`${signal} received, shutting down webhook worker`);

  const shutdownTimeout = setTimeout(() => {
    logger.error("Graceful shutdown timed out, forcing exit");
    process.exit(1);
  }, 10000);

  try {
    await Promise.all([stopWorker(), cleanupRedisClient(), cleanupPgPool()]);

    clearTimeout(shutdownTimeout);

    logger.info("Graceful shutdown completed");
    process.exit(0);
  } catch (err) {
    logger.error("Error during graceful shutdown", { err });
    process.exit(1);
  }
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
