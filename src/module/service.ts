import { Logger } from 'winston';
import { PoolClient } from 'pg';
import { createHmac } from 'crypto';
import * as db from './db';
import { RegisteredWebhook, WebhookEvent, WebhookPayload, WebhookResponse } from './types';

const SIGNATURE_HASHING_ALGORITHM = 'sha256';
const SIGNATURE_BUFFER_ENCODING = 'utf-8';
const SIGNTURE_DIGEST = 'hex';
const DEFAULT_REQUEST_TIMEOUT_MS = 10000;
const DEFAULT_REQUEST_ERROR_CODE = 500;
const MAX_RETRIES = 10;
const BASE_DELAY_MS = 100; // Initial delay of 100ms
const MAX_DELAY_MS = 10000; // Maximum delay of 10 seconds

export function generateHmacSignature(stringToSign: string, signingKey: string): string {
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
      id: '2',
      event: WebhookEvent.ROOM_JOIN,
      signingKey: '123',
      url: 'http://localhost:4012/webhook/events'
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function dispatchRegisteredWebhooks(
  logger: Logger,
  registeredWebhooks: RegisteredWebhook[],
  data: WebhookPayload
): Promise<WebhookResponse[]> {
  return Promise.all(
    registeredWebhooks.map(async (webhook: RegisteredWebhook) =>
      dispatchWebhook(logger, webhook, data)
    )
  );
}

export async function dispatchWebhook(
  logger: Logger,
  webhook: RegisteredWebhook,
  data: WebhookPayload,
  retryCount = 0
): Promise<WebhookResponse> {
  let response = {} as Response;

  const { url, signingKey } = webhook;

  try {
    const stringToSign = JSON.stringify(data);
    const requestSignature = generateHmacSignature(stringToSign, signingKey);

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

    response = await fetch(url, requestOptions);

    return {
      status: response.status,
      statusText: response.statusText
    };
  } catch (err: any) {
    logger.error(`Failed to dispatch webhook ${url}`);

    if (retryCount < MAX_RETRIES) {
      return registerDispatchRetry(logger, webhook, data, retryCount);
    }

    return {
      status: err?.response?.status || DEFAULT_REQUEST_ERROR_CODE,
      statusText: err.message || 'Unknown error'
    };
  }
}

export async function registerDispatchRetry(
  logger: Logger,
  webhook: RegisteredWebhook,
  data: any,
  retryCount: number
): Promise<WebhookResponse> {
  let delay = Math.min(BASE_DELAY_MS * 2 ** retryCount, MAX_DELAY_MS);

  const jitter = delay * 0.5 * Math.random();

  delay = retryCount % 2 === 0 ? delay - jitter : delay + jitter;

  logger.info(
    `Retrying webhook ${webhook.url} in ${Math.round(delay)}ms (Attempt ${retryCount + 2}/${
      MAX_RETRIES + 1
    })`
  );

  await sleep(delay);

  return dispatchWebhook(logger, webhook, data, retryCount + 1);
}
