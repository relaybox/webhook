import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { handler as webhookProcessHandler } from '@/handlers/webhook-process';
import { handler as webhookDispatchHandler } from '@/handlers/webhook-dispatch';
import { handler as webhookLoggerHandler } from '@/handlers/webhook-logger';

export enum JobName {
  WEBHOOK_PROCESS = 'webhook:process',
  WEBHOOK_DISPATCH = 'webhook:dispatch',
  WEBHOOK_LOGGER = 'webhook:logger'
}

const handlerMap = {
  [JobName.WEBHOOK_PROCESS]: webhookProcessHandler,
  [JobName.WEBHOOK_DISPATCH]: webhookDispatchHandler,
  [JobName.WEBHOOK_LOGGER]: webhookLoggerHandler
};

export async function router(
  pgPool: Pool,
  redisClient: RedisClient,
  jobName: JobName,
  data: any
): Promise<void> {
  return handlerMap[jobName](pgPool, redisClient, data);
}
