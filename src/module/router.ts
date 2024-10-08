import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { handler as webhookProcessHandler } from '@/handlers/webhook-process';
import { handler as webhookDispatchHandler } from '@/handlers/webhook-dispatch';

export enum JobName {
  WEBHOOK_PROCESS = 'webhook:process',
  WEBHOOK_DISPATCH = 'webhook:dispatch'
}

const handlerMap = {
  [JobName.WEBHOOK_PROCESS]: webhookProcessHandler,
  [JobName.WEBHOOK_DISPATCH]: webhookDispatchHandler
};

export async function router(
  pgPool: Pool,
  redisClient: RedisClient,
  jobName: JobName,
  data: any
): Promise<void> {
  return handlerMap[jobName](pgPool, redisClient, data);
}
