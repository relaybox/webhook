import { RedisClient } from '@/lib/redis';
import StreamConsumer, { StreamConsumerMessage } from '@/lib/stream-consumer';
import { Pool } from 'pg';
import { Logger } from 'winston';
import { handler as webhookLogStreamHandler } from '@/handlers/webhook-log-stream';
import { parseBufferedLogStream } from './service';

const LOG_STREAM_DEFAULT_BUFFER_MAX_LENGTH = Number(
  process.env.LOG_STREAM_DEFAULT_BUFFER_MAX_LENGTH
);

export async function startLogStreamConsumer(
  logger: Logger,
  pgPool: Pool,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string
): Promise<StreamConsumer> {
  logger.info('Starting log stream consumer');

  const streamConsumer = new StreamConsumer({
    redisClient,
    streamKey,
    groupName,
    blocking: true,
    bufferMaxLength: LOG_STREAM_DEFAULT_BUFFER_MAX_LENGTH
  });

  streamConsumer.on('data', (messages: StreamConsumerMessage[]) => {
    logger.debug(`Processing ${messages.length} log stream message(s)`);

    try {
      const parsedMessages = parseBufferedLogStream(logger, messages);
      webhookLogStreamHandler(pgPool, redisClient, streamKey, groupName, parsedMessages);
    } catch (err: unknown) {
      logger.error('Error processing log stream message data', { err });
    }
  });

  await streamConsumer.connect();

  return streamConsumer;
}
