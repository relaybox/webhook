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
  jobData: JobData
): Promise<void> {
  const pgClient = await pgPool.connect();

  const { webhook, webhookResponse } = jobData;

  logger.info(`Logging webhook`, { webhook });

  try {
    // await logWebhookEvent(logger, pgClient, webhook, webhookResponse);
    console.log('CRON TASK RUNNING');
  } catch (err: unknown) {
    logger.error(`Failed to log webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
