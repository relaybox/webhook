import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { dispatchWebhook } from '@/module/service';

const logger = getLogger('webhook-dispatch');

export async function handler(pgPool: Pool, redisClient: RedisClient, jobData: any): Promise<void> {
  const { webhook, payload } = jobData;

  try {
    const response = await dispatchWebhook(logger, webhook, payload);
    logger.info(`Webhook dispatched successfully`, { response });
  } catch (err: any) {
    logger.error(`Failed to dispatch webhook event`, { err });
    throw err;
  }
}
