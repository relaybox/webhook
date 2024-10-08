import { Logger } from 'winston';
import { Job } from 'bullmq';
import { PoolClient } from 'pg';
import { createHmac } from 'crypto';
import * as db from './db';
import { RegisteredWebhook, WebhookEvent, WebhookPayload, WebhookResponse } from './types';
import webhookDispatchQueue, { defaultJobConfig, WebhookDispatchJobName } from './queues/dispatch';

const SIGNATURE_HASHING_ALGORITHM = 'sha256';
const SIGNATURE_BUFFER_ENCODING = 'utf-8';
const SIGNTURE_DIGEST = 'hex';
const DEFAULT_REQUEST_TIMEOUT_MS = 10000;
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

export async function getRegisteredWebhooksByEvent(
  logger: Logger,
  pgClient: PoolClient,
  appPid: string,
  event: WebhookEvent
): Promise<RegisteredWebhook[] | undefined> {
  return [
    {
      id: '1',
      event: WebhookEvent.ROOM_JOIN,
      signingKey: '123',
      url: 'http://localhost:4000/webhook/event'
    },
    {
      id: '00000000-0000-0000-000000000000',
      event: WebhookEvent.ROOM_JOIN,
      signingKey: '123',
      url: 'http://localhost:4000/webhook/event'
    }
  ];

  try {
    const { rows: registeredWebhooks } = await db.getRegisteredWebhooksByEvent(
      pgClient,
      appPid,
      event
    );

    if (!registeredWebhooks.length) {
      logger.debug(`No registered webhooks found for appPid ${appPid}`);
    }

    return registeredWebhooks;
  } catch (err: any) {
    throw new Error(`Failed to get registered webhooks, ${err.message}`);
  }
}

export async function enqueueRegisteredWebhooks(
  logger: Logger,
  registeredWebhooks: RegisteredWebhook[],
  data: WebhookPayload
): Promise<Job[]> {
  logger.debug(`Enqueuing ${registeredWebhooks.length} webhook(s)`);

  return Promise.all(
    registeredWebhooks.map(async (webhook: RegisteredWebhook) => {
      const jobConfig = {
        ...defaultJobConfig
        // jobId: webhook.id
      };

      const jobData = {
        webhook,
        data
      };

      return webhookDispatchQueue.add(WebhookDispatchJobName.WEBHOOK_DISPATCH, jobData, jobConfig);
    })
  );
}

export async function dispatchWebhook(
  logger: Logger,
  webhook: RegisteredWebhook,
  data: WebhookPayload
): Promise<WebhookResponse> {
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
      body: JSON.stringify(data),
      signal: AbortSignal.timeout(DEFAULT_REQUEST_TIMEOUT_MS)
    };

    const response = await fetch(url, requestOptions);

    if (!response.ok) {
      if (RETRYABLE_ERROR_CODES.includes(response.status)) {
        throw new Error(`Retryable HTTP error, status: ${response.status}`);
      }
    }

    return {
      status: response.status,
      statusText: response.statusText
    };
  } catch (err: any) {
    logger.error(`Failed to dispatch webhook ${url}`, { err });
    throw err;
  }
}
