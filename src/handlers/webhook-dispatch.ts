import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { dispatchWebhook, enqueueWebhookLog, logWebhookEvent } from '@/module/service';
import { RegisteredWebhook, WebhookResponse } from '@/module/types';

const logger = getLogger('webhook-dispatch');

interface JobData {
  webhook: RegisteredWebhook;
  payload: any;
}

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  jobData: JobData
): Promise<void> {
  const { webhook, payload } = jobData;

  const pgClient = await pgPool.connect();

  let webhookResponse: WebhookResponse | null = null;

  logger.info(`Dispatching webhook`, { webhook });

  try {
    webhookResponse = await dispatchWebhook(logger, pgClient, webhook, payload);
  } catch (err: unknown) {
    logger.error(`Failed to dispatch webhook event`, { err });

    const statusText = err instanceof Error ? err.message : 'Unable to dispatch webhook';

    webhookResponse = {
      id: payload.id,
      status: 500,
      statusText
    };

    throw err;
  } finally {
    if (webhookResponse) {
      enqueueWebhookLog(logger, webhook, webhookResponse);
    }

    pgClient.release();
  }
}
