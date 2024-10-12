import { RedisClient } from '@/lib/redis';
import StreamConsumer, { StreamConsumerMessage } from '@/lib/streams/stream-consumer';
import { Pool } from 'pg';
import { Logger } from 'winston';
import { handler as webhookLogStreamHandler } from '@/handlers/webhook-stream';
import { RedisClientOptions } from 'redis';
import StreamMonitor from '@/lib/streams/stream-monitor';
import webhookPersistQueue, { defaultJobConfig, WebhookPersistJobName } from './queues/persist';

const LOG_STREAM_DEFAULT_BUFFER_MAX_LENGTH = Number(
  process.env.LOG_STREAM_DEFAULT_BUFFER_MAX_LENGTH
);

export const LOG_STREAM_KEY = 'logs:webhook';
export const LOG_STREAM_GROUP_NAME = 'webhook:log-group';
export const LOG_STREAM_CONSUMER_NAME = 'webhook:log-consumer';

export async function startLogStreamConsumer(
  logger: Logger,
  pgPool: Pool,
  redisClient: RedisClient,
  connectionOptions: RedisClientOptions
): Promise<StreamConsumer> {
  logger.info('Starting log stream consumer');

  const streamConsumer = new StreamConsumer({
    connectionOptions,
    streamKey: LOG_STREAM_KEY,
    groupName: LOG_STREAM_GROUP_NAME,
    bufferMaxLength: LOG_STREAM_DEFAULT_BUFFER_MAX_LENGTH
  });

  streamConsumer.on('data', async (messages: StreamConsumerMessage[]) => {
    logger.debug(`Processing ${messages.length} log stream message(s) from consumer`);

    try {
      await webhookLogStreamHandler(
        pgPool,
        redisClient,
        LOG_STREAM_KEY,
        LOG_STREAM_GROUP_NAME,
        messages
      );
    } catch (err: unknown) {
      logger.error('Error processing log stream message data', { err });
    }
  });

  return streamConsumer.connect();
}

export async function startLogStreamMonitor(
  logger: Logger,
  redisClient: RedisClient,
  connectionOptions: RedisClientOptions
): Promise<StreamMonitor> {
  logger.info('Starting log stream monitor');

  const streamMonitor = new StreamMonitor({
    connectionOptions,
    streamKey: LOG_STREAM_KEY,
    groupName: LOG_STREAM_GROUP_NAME,
    consumerMaxIdleTimeMs: 30000
  });

  streamMonitor.on('data', async (messages: StreamConsumerMessage[]) => {
    logger.debug(`Processing ${messages.length} log stream message(s) from monitor`);

    try {
      const jobs = messages.map((message) => {
        return {
          name: WebhookPersistJobName.WEBHOOK_PERSIST,
          data: message,
          opts: defaultJobConfig
        };
      });

      webhookPersistQueue.addBulk(jobs);
    } catch (err: unknown) {
      logger.error(`Failed to add webhook log stream data`, { err });
    }
  });

  return streamMonitor.connect();
}
