import { Logger } from 'winston';
import { Job } from 'bullmq';
import { PoolClient } from 'pg';
import { createHmac } from 'crypto';
import * as db from './db';
import { RegisteredWebhook, WebhookEvent, WebhookPayload, WebhookResponse } from './types';
import webhookDispatchQueue, {
  defaultJobConfig as dispatchQueueDefaultJobConfig,
  WebhookDispatchJobName
} from './queues/dispatch';
import webhookLoggingQueue, {
  defaultJobConfig as loggingQueueDefaultJobConfig,
  WebhookLoggingJobName
} from './queues/logging';

const SIGNATURE_HASHING_ALGORITHM = 'sha256';
const SIGNATURE_BUFFER_ENCODING = 'utf-8';
const SIGNTURE_DIGEST = 'hex';
const DEFAULT_REQUEST_TIMEOUT_MS = 100;
const RETRYABLE_ERROR_CODES = [500, 429];

export function generateRequestSignature(stringToSign: string, signingKey: string): string {
  if (!stringToSign || !signingKey) {
    throw new Error(`Please provide string to sign and signing key`);
  }

  try {
    const buffer = Buffer.from(stringToSign, SIGNATURE_BUFFER_ENCODING);
    const signature = createHmac(SIGNATURE_HASHING_ALGORITHM, signingKey)
      .update(buffer)
      .digest(SIGNTURE_DIGEST);

    return signature;
  } catch (err: any) {
    throw new Error(`Failed to generate signature, ${err.message}`);
  }
}

export async function getWebhooksByAppAndEvent(
  logger: Logger,
  pgClient: PoolClient,
  appPid: string,
  event: WebhookEvent
): Promise<RegisteredWebhook[] | undefined> {
  try {
    const { rows: webhooks } = await db.getWebhooksByAppAndEvent(pgClient, appPid, event);

    if (!webhooks.length) {
      logger.debug(`No registered webhooks found for appPid ${appPid}, ${event}`);
    }

    return webhooks;
  } catch (err: any) {
    throw new Error(`Failed to get registered webhooks, ${err.message}`);
  }
}

export async function enqueueRegisteredWebhooks(
  logger: Logger,
  registeredWebhooks: RegisteredWebhook[],
  payload: WebhookPayload
): Promise<Job[]> {
  logger.debug(`Enqueuing ${registeredWebhooks.length} webhook(s)`);

  return Promise.all(
    registeredWebhooks.map(async (webhook: RegisteredWebhook) => {
      const jobData = {
        webhook,
        payload
      };

      return webhookDispatchQueue.add(
        WebhookDispatchJobName.WEBHOOK_DISPATCH,
        jobData,
        dispatchQueueDefaultJobConfig
      );
    })
  );
}

export async function dispatchWebhook(
  logger: Logger,
  pgClient: PoolClient,
  webhook: RegisteredWebhook,
  payload: WebhookPayload
): Promise<WebhookResponse> {
  logger.debug(`Dispatching webhook`, { webhook });

  const { id, data } = payload;
  const { url, signingKey } = webhook;

  try {
    const stringToSign = JSON.stringify(data);
    const requestSignature = generateRequestSignature(stringToSign, signingKey);

    const headers = {
      'Content-Type': 'application/json',
      'X-Relaybox-Webhook-Signature': requestSignature
    };

    const requestOptions = {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(DEFAULT_REQUEST_TIMEOUT_MS)
    };

    const response = await fetch(url, requestOptions);

    if (!response.ok && RETRYABLE_ERROR_CODES.includes(response.status)) {
      throw new Error(`Retryable HTTP error, status: ${response.status}`);
    }

    return {
      id,
      status: response.status,
      statusText: response.statusText
    };
  } catch (err: unknown) {
    logger.error(`Failed to dispatch webhook to ${url}`, { err });
    throw err;
  }
}

export function enqueueWebhookLog(
  logger: Logger,
  webhook: RegisteredWebhook,
  webhookResponse: WebhookResponse
): Promise<Job> {
  logger.debug(`Enqueuing webhook log`, { webhook, webhookResponse });

  const jobData = {
    webhook,
    webhookResponse
  };

  return webhookLoggingQueue.add(
    WebhookLoggingJobName.WEBHOOK_LOGGING_WRITE,
    jobData,
    loggingQueueDefaultJobConfig
  );
}

export async function logWebhookEvent(
  logger: Logger,
  pgClient: PoolClient,
  webhook: RegisteredWebhook,
  webhookResponse: WebhookResponse
): Promise<void> {
  logger.debug(`Logging webhook event`, { webhook, webhookResponse });

  try {
    const { id: webhookId, appId, appPid } = webhook;
    const { id: webhookRequestId, status, statusText } = webhookResponse;
    await db.logWebhookEvent(
      pgClient,
      appId,
      appPid,
      webhookId,
      webhookRequestId,
      status,
      statusText
    );
  } catch (err: unknown) {
    logger.error(`Failed to log webhook event`, { err });
  }
}
