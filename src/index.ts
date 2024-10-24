import 'dotenv/config';

import { getLogger } from '@/util/logger.util';
import { cleanupRedisClient, connectionOptions, getRedisClient } from '@/lib/redis';
import { cleanupPgPool, getPgPool } from '@/lib/pg';
import { startWorker } from './module/worker';
import { ServiceWorker } from './module/types';
import { startLogStreamConsumer, startLogStreamMonitor } from './module/consumer';
import StreamConsumer from './lib/streams/stream-consumer';
import { WEBHOOK_DISPATCH_QUEUE_NAME } from './module/queues/dispatch';
import { WEBHOOK_PROCESS_QUEUE_NAME } from './module/queues/process';
import StreamMonitor from './lib/streams/stream-monitor';
import { WEBHOOK_PERSIST_QUEUE_NAME } from './module/queues/persist';

// FORCE v2

const logger = getLogger('webhook-service');
const pgPool = getPgPool();
const redisClient = getRedisClient();

let processWorker: ServiceWorker;
let dispatchWorker: ServiceWorker;
let persistWorker: ServiceWorker;
let logStreamConsumer: StreamConsumer;
let logStreamMonitor: StreamMonitor;

async function startService() {
  await initializeConnections();

  processWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_PROCESS_QUEUE_NAME, 10);
  dispatchWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_DISPATCH_QUEUE_NAME);
  persistWorker = startWorker(logger, pgPool, redisClient, WEBHOOK_PERSIST_QUEUE_NAME);
  logStreamConsumer = await startLogStreamConsumer(logger, pgPool, redisClient, connectionOptions);
  logStreamMonitor = await startLogStreamMonitor(logger, redisClient, connectionOptions);
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
  }, 20000);

  try {
    await Promise.all([
      processWorker?.close(),
      dispatchWorker?.close(),
      persistWorker?.close(),
      logStreamConsumer.disconnect(),
      logStreamMonitor.disconnect(),
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
