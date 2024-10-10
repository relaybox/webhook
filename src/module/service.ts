import { Logger } from 'winston';
import { Job } from 'bullmq';
import { Pool, PoolClient } from 'pg';
import { createHmac } from 'crypto';
import * as db from './db';
import * as repository from './repository';
import {
  LogStreamMessageData,
  RegisteredWebhook,
  StreamConsumerData,
  StreamConsumerMessageData,
  WebhookEvent,
  WebhookPayload,
  WebhookResponse
} from './types';
import webhookDispatchQueue, {
  defaultJobConfig as dispatchQueueDefaultJobConfig,
  WebhookDispatchJobName
} from './queues/dispatch';
import { RedisClient } from '@/lib/redis';
import { LOG_STREAM_KEY } from '..';

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
      statusText: response.statusText,
      timestamp: Date.now()
    };
  } catch (err: unknown) {
    logger.error(`Failed to dispatch webhook to ${url}`, { err });
    throw err;
  }
}

export async function enqueueWebhookLog(
  logger: Logger,
  redisClient: RedisClient,
  webhook: RegisteredWebhook,
  webhookResponse: WebhookResponse
): Promise<void> {
  logger.debug(`Enqueuing webhook log`, { webhook, webhookResponse });

  const logData = {
    webhook,
    webhookResponse
  };

  await repository.addMessageToLogStream(redisClient, LOG_STREAM_KEY, logData);
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

export function parseRawLogStream(
  logger: Logger,
  streams: StreamConsumerData[],
  streamKey: string
): LogStreamMessageData[] {
  logger.debug(`Parsing ${streams.length} log stream message(s)`);

  const stream = streams.find((stream: StreamConsumerData) => stream.name === streamKey);

  if (!stream) {
    throw new Error(`Stream not found for key ${streamKey}`);
  }

  return stream?.messages.map((streamMessageData: StreamConsumerMessageData) => ({
    streamId: streamMessageData.id,
    ...JSON.parse(streamMessageData.message.data)
  }));
}

export function parseBufferedLogStream(
  logger: Logger,
  messages: StreamConsumerMessageData[]
): LogStreamMessageData[] {
  return messages.map((streamMessageData: StreamConsumerMessageData) => ({
    streamId: streamMessageData.id,
    ...JSON.parse(streamMessageData.message.data)
  }));
}

export function parseLogStreamMessageData(
  logger: Logger,
  logStreamMessageData: LogStreamMessageData[]
): (string | number)[][] {
  logger.debug(`Parsing ${logStreamMessageData.length} log stream message(s)`);

  return logStreamMessageData.map((messageData) => [
    messageData.webhook.appId,
    messageData.webhook.appPid,
    messageData.webhook.id,
    messageData.webhookResponse.id,
    messageData.webhookResponse.status,
    messageData.webhookResponse.statusText,
    new Date(messageData.webhookResponse.timestamp).toISOString()
  ]);
}

export async function bulkInsertWebhookLogs(
  logger: Logger,
  pgClient: PoolClient,
  rows: (string | number)[][]
): Promise<void> {
  logger.debug(`Bulk inserting ${rows.length} webhook log(s)`);

  try {
    const placeholdersPerRow = rows[0].length;

    const queryPlaceholders = rows.map((_, i) => {
      const baseIndex = i * placeholdersPerRow + 1;
      const arrayParams = { length: placeholdersPerRow };
      const placeholders = Array.from(arrayParams, (_, j) => `$${baseIndex + j}`);

      return `(${placeholders.join(', ')})`;
    });

    const values = rows.flat();

    await db.bulkInsertWebhookLogs(pgClient, queryPlaceholders, values);
  } catch (err: unknown) {
    logger.error(`Failed to bulk insert webhook logs`, { err });
  }
}

export async function acknowledgeLogStreamMessageData(
  logger: Logger,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  logStreamMessageData: LogStreamMessageData[]
): Promise<void> {
  try {
    const ids = logStreamMessageData.map((messageData) => messageData.streamId);

    logger.debug(`Acknowledging ${ids.length} messages`);

    await repository.acknowledgeLogStreamMessagesById(redisClient, streamKey, groupName, ids);
  } catch (err) {
    logger.error('Error processing messages:', err);
  }
}
