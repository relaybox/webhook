import { Job, Worker } from 'bullmq';
import { getLogger } from '@/util/logger.util';
import { connectionOptionsIo, RedisClient } from '@/lib/redis';
import { JobName, router } from '../router';
import { Pool } from 'pg';

const QUEUE_NAME = 'webhook-process';

const logger = getLogger(QUEUE_NAME);

let worker: Worker | null = null;

async function handler(pgPool: Pool, redisClient: RedisClient, job: Job) {
  const { id, name, data } = job;

  logger.info(`Processing job ${id} (${name})`, { data });

  await router(pgPool, redisClient, name as JobName, data);

  logger.debug(`Job ${id} processed`);
}

export async function startProcessWorker(pgPool: Pool, redisClient: RedisClient) {
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
}

export async function stopProcessWorker(): Promise<void> {
  if (!worker) {
    throw new Error('Worker not initialized');
  }

  try {
    logger.info('Closing process worker');
    await worker.close();
  } catch (err) {
    logger.error('Failed to close worker', { err });
  } finally {
    worker = null;
  }
}
