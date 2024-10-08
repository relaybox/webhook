import { Pool } from 'pg';
import { Job, Worker } from 'bullmq';
import { Logger } from 'winston';
import { connectionOptionsIo, RedisClient } from '@/lib/redis';
import { JobName, router } from './router';

async function handler(logger: Logger, pgPool: Pool, redisClient: RedisClient, job: Job) {
  const { id, name, data } = job;

  await router(pgPool, redisClient, name as JobName, data);

  logger.debug(`Job ${id} processed successfully`);
}

export function startWorker(
  logger: Logger,
  pgPool: Pool,
  redisClient: RedisClient,
  queueName: string
): Worker {
  const worker = new Worker(queueName, (job: Job) => handler(logger, pgPool, redisClient, job), {
    connection: connectionOptionsIo,
    prefix: 'queue'
  });

  worker.on('failed', (job: Job | undefined, err: Error, prev: string) => {
    logger.error(`${worker.name} failed to process job ${job?.id}`, { err });
  });

  worker.on('ready', () => {
    logger.info(`${worker.name} worker ready`);
  });

  worker.on('active', (job: Job) => {
    logger.debug(`${worker.name} worker active, processing ${job.id}`);
  });

  return worker;
}

export async function stopWorker(logger: Logger, worker: Worker | null): Promise<void> {
  if (!worker) {
    throw new Error(`Worker not initialized`);
  }

  try {
    logger.info(`Closing worker ${worker.name}`);
    await worker.close();
  } catch (err) {
    logger.error('Failed to close worker', { err });
  }
}
