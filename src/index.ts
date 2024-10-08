import 'dotenv/config';

import { startProcessWorker, stopProcessWorker } from '@/module/workers/process';
import { startDispatchWorker, stopDispatchWorker } from '@/module/workers/dispatch';
import { getLogger } from '@/util/logger.util';
import { cleanupRedisClient, getRedisClient } from '@/lib/redis';
import { cleanupPgPool, getPgPool } from '@/lib/pg';

const logger = getLogger('webhook-service');
const pgPool = getPgPool();
const redisClient = getRedisClient();

async function initializeConnections(): Promise<void> {
  if (!pgPool) {
    throw new Error('Failed to initialize PG pool');
  }

  if (!redisClient) {
    throw new Error('Failed to initialize Redis client');
  }

  try {
    await redisClient.connect();
  } catch (err) {
    logger.error('Failed to connect to Redis', { err });
    throw err;
  }
}

async function start() {
  await initializeConnections();

  if (!pgPool || !redisClient) {
    throw new Error('Database connections not initialized');
  }

  startProcessWorker(pgPool, redisClient);
  startDispatchWorker(pgPool, redisClient);
}

async function shutdown(signal: string): Promise<void> {
  logger.info(`${signal} received, shutting down webhook worker`);

  const shutdownTimeout = setTimeout(() => {
    logger.error('Graceful shutdown timed out, forcing exit');
    process.exit(1);
  }, 10000);

  try {
    await Promise.all([
      stopProcessWorker(),
      stopDispatchWorker(),
      cleanupRedisClient(),
      cleanupPgPool()
    ]);

    clearTimeout(shutdownTimeout);

    logger.info('Graceful shutdown completed');
    process.exit(0);
  } catch (err) {
    logger.error('Error during graceful shutdown', { err });
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start();
