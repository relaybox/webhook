import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { WebhookPayload } from '@/module/types';
import { getRegisteredWebhooksByAppPid } from '@/module/service';

const logger = getLogger('webhook-dispatch');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  payload: WebhookPayload
): Promise<void> {
  const pgClient = await pgPool.connect();

  try {
    const { data, session } = payload;
    const { appPid } = session;

    const registeredWebhooks = await getRegisteredWebhooksByAppPid(logger, pgClient, appPid);
  } catch (err: any) {
    logger.error(`Failed to dispatch webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
