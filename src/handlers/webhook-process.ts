import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { WebhookPayload } from '@/module/types';
import { enqueueRegisteredWebhooks, getRegisteredWebhooksByEvent } from '@/module/service';

const logger = getLogger('webhook-process');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  payload: WebhookPayload
): Promise<void> {
  const pgClient = await pgPool.connect();

  try {
    const { data, session, event, filterAttributes } = payload;
    const { appPid } = session;

    const registeredWebhooks = await getRegisteredWebhooksByEvent(logger, pgClient, appPid, event);

    if (!registeredWebhooks) {
      logger.debug(`No registered webhooks found for app: ${appPid}`);
      return;
    }

    logger.debug(`Registered webhooks found for app: ${appPid}`, { registeredWebhooks });

    await enqueueRegisteredWebhooks(logger, registeredWebhooks, data);
  } catch (err: any) {
    logger.error(`Failed to process webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
