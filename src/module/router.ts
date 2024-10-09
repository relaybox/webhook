import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { handler as webhookProcessHandler } from '@/handlers/webhook-process';
import { handler as webhookDispatchHandler } from '@/handlers/webhook-dispatch';
import { handler as webhookLoggingWriteHandler } from '@/handlers/webhook-logging';

export enum JobName {
  WEBHOOK_PROCESS = 'webhook:process',
  WEBHOOK_DISPATCH = 'webhook:dispatch',
  WEBHOOK_LOGGING_WRITE = 'webhook:logging:write'
}

const handlerMap = {
  [JobName.WEBHOOK_PROCESS]: webhookProcessHandler,
  [JobName.WEBHOOK_DISPATCH]: webhookDispatchHandler,
  [JobName.WEBHOOK_LOGGING_WRITE]: webhookLoggingWriteHandler
};

export async function router(
  pgPool: Pool,
  redisClient: RedisClient,
  jobName: JobName,
  data: any
): Promise<void> {
  return handlerMap[jobName](pgPool, redisClient, data);
}
