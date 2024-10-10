import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { RegisteredWebhook, WebhookResponse } from '@/module/types';

const logger = getLogger('webhook-logger');

interface messageData {
  streamId: string;
  webhook: RegisteredWebhook;
  webhookResponse: WebhookResponse;
}

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  messageData: messageData[]
): Promise<void> {
  const pgClient = await pgPool.connect();

  try {
    console.log('MESSSAGES FROM LOG STREAM', messageData);
  } catch (err: unknown) {
    logger.error(`Failed to log webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
