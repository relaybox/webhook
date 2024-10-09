import 'dotenv/config';

import { getLogger } from '@/util/logger.util';
import { cleanupRedisClient, getRedisClient } from '@/lib/redis';
import { cleanupPgPool, getPgPool } from '@/lib/pg';
import { startWorker, stopWorker } from './module/worker';
import { WEBHOOK_DISPATCH_QUEUE_NAME } from './module/queues';
import { ServiceWorker } from './module/types';
import { startLogStreamConsumer } from './module/consumer';
import { StreamConsumer } from './lib/stream-consumer';

const logger = getLogger('webhook');
const pgPool = getPgPool();
const redisClient = getRedisClient();

const WEBHOOK_PROCESS_QUEUE_NAME = 'webhook-process';

let processWorker: ServiceWorker;
let dispatchWorker: ServiceWorker;
let logStreamConsumer: StreamConsumer;

export const LOG_STREAM_KEY = 'webhook:log-stream';
export const LOG_STREAM_GROUP_NAME = 'webhook-log-group';
export const LOG_STREAM_CONSUMER_NAME = 'webhook-log-consumer';

async function startService() {
  await initializeConnections();

  processWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_PROCESS_QUEUE_NAME, 10);
  dispatchWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_DISPATCH_QUEUE_NAME);
  logStreamConsumer = await startLogStreamConsumer(
    logger,
    pgPool,
    redisClient,
    LOG_STREAM_KEY,
    LOG_STREAM_CONSUMER_NAME
  );
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
      stopWorker(logger, processWorker),
      stopWorker(logger, dispatchWorker),
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
