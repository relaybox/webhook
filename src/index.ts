import 'dotenv/config';

import { getLogger } from '@/util/logger.util';
import { cleanupRedisClient, getRedisClient } from '@/lib/redis';
import { cleanupPgPool, getPgPool } from '@/lib/pg';
import { startWorker, stopWorker } from './module/worker';
import { startWebhookLogger } from './module/service';
import { WEBHOOK_DISPATCH_QUEUE_NAME, WEBHOOK_LOGGER_QUEUE_NAME } from './module/queues';
import { ServiceWorker } from './module/types';

const logger = getLogger('webhook');
const pgPool = getPgPool();
const redisClient = getRedisClient();

const WEBHOOK_PROCESS_QUEUE_NAME = 'webhook-process';

let processWorker: ServiceWorker;
let dispatchWorker: ServiceWorker;
let loggerWorker: ServiceWorker;

async function startService() {
  await initializeConnections();

  startWebhookLogger(logger);

  processWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_PROCESS_QUEUE_NAME, 10);
  dispatchWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_DISPATCH_QUEUE_NAME);
  loggerWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_LOGGER_QUEUE_NAME);
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
      loggerWorker ? stopWorker(logger, loggerWorker) : Promise.resolve(),
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
