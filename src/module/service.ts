import { Logger } from 'winston';
import { PoolClient } from 'pg';
import { createHmac } from 'crypto';
import * as db from './db';
import { RegisteredWebhook, WebhookEvent, WebhookPayload } from './types';

const SIGNATURE_HASHING_ALGORITHM = 'sha256';
const SIGNATURE_BUFFER_ENCODING = 'utf-8';
const SIGNTURE_DIGEST = 'hex';
const DEFAULT_REQUEST_TIMEOUT = 10000;
const DEFAULT_REQUEST_ERROR_CODE = 500;

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
      url: 'http://localhost:4000/webhook/events'
    },
    {
      id: '2',
      event: WebhookEvent.ROOM_JOIN,
      signingKey: '123',
      url: 'http://localhost:4012/webhook/event'
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

export async function dispatchRegisteredWebhooks(
  logger: Logger,
  registeredWebhooks: RegisteredWebhook[],
  data: WebhookPayload
): Promise<any> {
  return Promise.all(
    registeredWebhooks.map(async (webhook: RegisteredWebhook) => {
      const { url, signingKey } = webhook;

      let response: Response | null = null;

      try {
        const headers = {
          'Content-Type': 'application/json',
          'X-Relaybox-Webhook-Signature': generateHmacSignature(JSON.stringify(data), signingKey)
        };

        const requestOptions = {
          method: 'POST',
          headers: headers,
          body: JSON.stringify(data),
          signal: AbortSignal.timeout(DEFAULT_REQUEST_TIMEOUT)
        };

        response = await fetch(url, requestOptions);

        return {
          status: response.status,
          statusText: response.statusText
        };
      } catch (err: any) {
        logger.error(`Failed to dispatch webhook ${url}`, { err });

        return {
          status: response?.status || DEFAULT_REQUEST_ERROR_CODE,
          statusText: err.message
        };
      }
    })
  );
}
