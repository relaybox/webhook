import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { WebhookPayload } from '@/module/types';
import { enqueueRegisteredWebhooks, getWebhooksByAppAndEvent } from '@/module/service';

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

    logger.info(`Processing webhook for app ${appPid}, ${event}`, {
      appPid,
      event,
      session
    });

    const registeredWebhooks = await getWebhooksByAppAndEvent(logger, pgClient, appPid, event);

    if (!registeredWebhooks?.length) {
      logger.debug(`No registered webhooks found for app ${appPid}`);
      return;
    }

    logger.info(`${registeredWebhooks.length} webhook(s) found for app ${appPid}, ${event}`, {
      registeredWebhooks
    });

    await enqueueRegisteredWebhooks(logger, registeredWebhooks, payload);
  } catch (err: any) {
    logger.error(`Failed to process webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
