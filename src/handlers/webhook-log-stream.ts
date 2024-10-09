import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { logWebhookEvent } from '@/module/service';
import { RegisteredWebhook, WebhookResponse } from '@/module/types';

const logger = getLogger('webhook-logger');

interface JobData {
  webhook: RegisteredWebhook;
  webhookResponse: WebhookResponse;
}

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  messageData: JobData[]
): Promise<void> {
  const pgClient = await pgPool.connect();

  try {
    console.log('CRON TASK RUNNING', messageData);
  } catch (err: unknown) {
    logger.error(`Failed to log webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
