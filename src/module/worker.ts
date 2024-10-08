import { Job, Worker } from 'bullmq';
import { getLogger } from '@/util/logger.util';
import { connectionOptionsIo, RedisClient } from '@/lib/redis';
import { Pool } from 'pg';
import { JobName, router } from './router';

const QUEUE_NAME = 'webhook-dispatch';

const logger = getLogger(QUEUE_NAME);

let worker: Worker | null = null;

async function handler(pgPool: Pool, redisClient: RedisClient, job: Job) {
  const { id, name, data } = job;

  logger.info(`Processing job ${id} (${name})`, { data });

  await router(pgPool, redisClient, name as JobName, data);

  logger.debug(`Job ${id} processed`);
}

export function startWorker(pgPool: Pool, redisClient: RedisClient): Worker {
  worker = new Worker(QUEUE_NAME, (job: Job) => handler(pgPool, redisClient, job), {
    connection: connectionOptionsIo,
    prefix: 'queue'
  });

  worker.on('failed', (job: Job<any, void, string> | undefined, err: Error, prev: string) => {
    logger.error(`Failed to process job ${job?.id}`, { err });
  });

  worker.on('ready', () => {
    logger.info(`${QUEUE_NAME} worker ready`);
  });

  worker.on('active', () => {
    logger.debug(`${QUEUE_NAME} worker active`);
  });

  return worker;
}

export async function stopWorker(): Promise<void> {
  if (!worker) {
    throw new Error('Worker not initialized');
  }

  try {
    logger.info('Closing dispatch worker');
    await worker.close();
  } catch (err) {
    logger.error('Failed to close worker', { err });
  } finally {
    worker = null;
  }
}
