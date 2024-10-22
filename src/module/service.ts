import { Logger } from 'winston';
import { Job } from 'bullmq';
import { PoolClient } from 'pg';
import { createHmac } from 'crypto';
import * as db from './db';
import * as repository from './repository';
import {
  LogStreamMessage,
  Webhook,
  webhookLogsDbEntry,
  WebhookEvent,
  WebhookPayload,
  WebhookResponse,
  WebhookHeadersKeyPair
} from './types';
import webhookDispatchQueue, {
  defaultJobConfig as dispatchQueueDefaultJobConfig,
  WebhookDispatchJobName
} from './queues/dispatch';
import { RedisClient } from '@/lib/redis';
import { StreamConsumerMessage } from '@/lib/streams/stream-consumer';
import { LOG_STREAM_KEY } from './consumer';
import { canonicalize, canonicalizeEx } from 'json-canonicalize';

const SIGNATURE_HASHING_ALGORITHM = 'sha256';
const SIGNATURE_BUFFER_ENCODING = 'utf-8';
const SIGNTURE_DIGEST = 'hex';
const DEFAULT_REQUEST_TIMEOUT_MS = 5000;
const RETRYABLE_ERROR_CODES = [429, 500, 502];

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
): Promise<Webhook[] | undefined> {
  try {
    const { rows: webhooks } = await db.getWebhooksByAppAndEvent(pgClient, appPid, event);

    return webhooks;
  } catch (err: any) {
    throw new Error(`Failed to get registered webhooks, ${err.message}`);
  }
}

export async function enqueueRegisteredWebhooks(
  logger: Logger,
  registeredWebhooks: Webhook[],
  payload: WebhookPayload
): Promise<Job[]> {
  logger.debug(`Enqueuing ${registeredWebhooks.length} webhook(s)`);

  return Promise.all(
    registeredWebhooks.map(async (webhook: Webhook) => {
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

export function parseWebhookHeaders(
  logger: Logger,
  headers?: WebhookHeadersKeyPair[]
): Record<string, string> {
  try {
    if (!headers?.length) {
      return {};
    }

    return headers.reduce<Record<string, string>>((acc, { keyName, value }) => {
      acc[keyName] = value.toString();
      return acc;
    }, {});
  } catch (err: unknown) {
    logger.error(`Failed to parse webhook headers`, { err });
    throw err;
  }
}

export function parseWebhookPayload(logger: Logger, payload: WebhookPayload): WebhookPayload {
  try {
    const timestamp = new Date().toISOString();

    const { id, event, data, session } = payload;

    return { id, event, data, session, timestamp };
  } catch (err: unknown) {
    logger.error(`Failed to parse webhook payload`, { err });
    throw err;
  }
}

export async function dispatchWebhook(
  logger: Logger,
  webhook: Webhook,
  payload: WebhookPayload
): Promise<WebhookResponse> {
  logger.debug(`Dispatching webhook`, { webhook });

  const { id } = payload;
  const { url, signingKey, headers: webhookHeaders } = webhook;

  try {
    const parsedWebhookHeaders = parseWebhookHeaders(logger, webhookHeaders);
    const stringToSign = serializeWebhookData(logger, payload);
    const requestSignature = generateRequestSignature(stringToSign, signingKey);

    const headers = {
      ...parsedWebhookHeaders,
      'Content-Type': 'application/json',
      'X-RelayBox-Webhook-Signature': requestSignature,
      'X-RelayBox-Webhook-Id': webhook.id
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
  webhook: Webhook,
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
  webhook: Webhook,
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

export function parseStreamConsumerMessage(
  logger: Logger,
  message: StreamConsumerMessage
): LogStreamMessage {
  try {
    const parsedData = JSON.parse(message.message.data);

    return {
      streamId: message.id,
      ...parsedData
    };
  } catch (err: unknown) {
    logger.error(`Failed to parse stream consumer message`, { err });
    throw err;
  }
}

export function parseStreamConsumerMessages(
  logger: Logger,
  messages: StreamConsumerMessage[]
): LogStreamMessage[] {
  logger.debug(`Parsing ${messages.length} buffered log stream message(s)`);

  return messages.reduce<LogStreamMessage[]>((acc, msg) => {
    try {
      const parsedStreamConsumerMessage = parseStreamConsumerMessage(logger, msg);
      acc.push(parsedStreamConsumerMessage);
    } catch (err: unknown) {
      logger.error(`Failed to parse stream consumer message`, { err });
    }

    return acc;
  }, []);
}

export function parseWebhookLogsDbEntries(
  logger: Logger,
  logStreamMessages: LogStreamMessage[]
): webhookLogsDbEntry[] {
  logger.debug(`Parsing ${logStreamMessages.length} log stream message(s)`);

  return logStreamMessages.reduce<webhookLogsDbEntry[]>((acc, messageData) => {
    const { webhook, webhookResponse } = messageData;

    try {
      acc.push([
        webhook.appId,
        webhook.appPid,
        webhook.id,
        webhook.webhookEventId,
        webhookResponse.id,
        webhookResponse.status,
        webhookResponse.statusText,
        new Date(webhookResponse.timestamp).toISOString()
      ]);
    } catch (err: unknown) {
      logger.error(`Failed to parse log stream message`, { err, webhook, webhookResponse });
    }

    return acc;
  }, []);
}

export async function bulkInsertWebhookLogs(
  logger: Logger,
  pgClient: PoolClient,
  parsedLogStreamDbEntries: webhookLogsDbEntry[]
): Promise<void> {
  logger.debug(`Bulk inserting ${parsedLogStreamDbEntries.length} webhook log(s)`);

  try {
    const placeholdersPerRow = parsedLogStreamDbEntries[0].length;

    const queryPlaceholders = parsedLogStreamDbEntries.map((_, i) => {
      const baseIndex = i * placeholdersPerRow + 1;
      const arrayParams = { length: placeholdersPerRow };
      const placeholders = Array.from(arrayParams, (_, j) => `$${baseIndex + j}`);

      return `(${placeholders.join(', ')})`;
    });

    const values = parsedLogStreamDbEntries.flat();

    await db.bulkInsertWebhookLogs(pgClient, queryPlaceholders, values);
  } catch (err: unknown) {
    logger.error(`Failed to bulk insert webhook logs`, { err });
    throw err;
  }
}

export async function insertWebhookLog(
  logger: Logger,
  pgClient: PoolClient,
  message: LogStreamMessage
): Promise<void> {
  logger.debug(`Inserting webhook log db entry`, { message });

  const { webhook, webhookResponse } = message;

  try {
    await db.insertWebhookLogsDbEntry(pgClient, webhook, webhookResponse);
  } catch (err: unknown) {
    logger.error(`Failed to insert webhook log db entry`, { err });
    throw err;
  }
}

export async function ackStreamMessages(
  logger: Logger,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  logStreamMessages: LogStreamMessage[]
): Promise<void> {
  try {
    const ids = logStreamMessages.map((message) => message.streamId);

    logger.debug(`Acknowledging ${ids.length} stream message(s)`, { streamKey, groupName });

    await repository.ackStreamMessages(redisClient, streamKey, groupName, ids);

    logger.info(`Acknowledged ${ids.length} log stream message(s)`);
  } catch (err) {
    logger.error('Error processing messages:', err);
  }
}

export function serializeWebhookData(logger: Logger, data: any): string {
  logger.debug(`Serializing webhook data`, { data });

  try {
    const canonicalizedData = canonicalize(data);
    const serializedData = JSON.stringify(canonicalizedData);
    return serializedData;
  } catch (err: unknown) {
    logger.error(`Failed to serialize webhook data`, { err });
    throw err;
  }
}
