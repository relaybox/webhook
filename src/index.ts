import 'dotenv/config';

import { Worker } from 'bullmq';
import { getLogger } from '@/util/logger.util';
import { cleanupRedisClient, getRedisClient } from '@/lib/redis';
import { cleanupPgPool, getPgPool } from '@/lib/pg';
import { startWorker, stopWorker } from './module/worker';

const logger = getLogger('webhook');
const pgPool = getPgPool();
const redisClient = getRedisClient();

const PROCESS_QUEUE_NAME = 'webhook-process';
const DISPATCH_QUEUE_NAME = 'webhook-dispatch';
const LOGGING_QUEUE_NAME = 'webhook-logging';

let processWorker: Worker | null = null;
let dispatchWorker: Worker | null = null;
let loggingWorker: Worker | null = null;

async function startService() {
  await initializeConnections();

  processWorker = startWorker(logger, pgPool, redisClient, PROCESS_QUEUE_NAME, 10);
  dispatchWorker = startWorker(logger, pgPool, redisClient, DISPATCH_QUEUE_NAME);
  loggingWorker = startWorker(logger, pgPool, redisClient, LOGGING_QUEUE_NAME, 1);
}

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

async function shutdown(signal: string): Promise<void> {
  logger.info(`${signal} received, shutting down webhook worker`);

  const shutdownTimeout = setTimeout(() => {
    logger.error('Graceful shutdown timed out, forcing exit');
    process.exit(1);
  }, 10000);

  try {
    await Promise.all([
      processWorker ? stopWorker(logger, processWorker) : Promise.resolve(),
      dispatchWorker ? stopWorker(logger, dispatchWorker) : Promise.resolve(),
      loggingWorker ? stopWorker(logger, loggingWorker) : Promise.resolve(),
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

startService();
