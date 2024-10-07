import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { handler as webhookDispatchHandler } from '@/handlers/webhook-dispatch';

export enum JobName {
  WEBHOOK_DISPATCH = 'webhook:dispatch'
}

const handlerMap = {
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
