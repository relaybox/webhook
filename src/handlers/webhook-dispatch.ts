import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { dispatchWebhook, enqueueWebhookLog, parseWebhookPayload } from '@/module/service';
import { Webhook, WebhookResponse } from '@/module/types';

const logger = getLogger('webhook-dispatch');

interface JobData {
  webhook: Webhook;
  payload: any;
}

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  jobData: JobData
): Promise<void> {
  const { webhook, payload } = jobData;

  let webhookResponse: WebhookResponse | null = null;

  logger.info(`Dispatching webhook`, { webhook });

  try {
    const parsedPayload = parseWebhookPayload(logger, payload);
    webhookResponse = await dispatchWebhook(logger, webhook, parsedPayload);
  } catch (err: unknown) {
    logger.error(`Failed to dispatch webhook event`, { webhook, err });

    const statusText = err instanceof Error ? err.message : 'Unable to dispatch webhook';

    webhookResponse = {
      id: payload.id,
      status: 500,
      statusText,
      timestamp: Date.now()
    };

    throw err;
  } finally {
    if (webhookResponse) {
      enqueueWebhookLog(logger, redisClient, webhook, webhookResponse);
    }
  }
}
