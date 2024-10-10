import { RedisClient } from '@/lib/redis';
import StreamConsumer from '@/lib/stream-consumer';
import { Pool } from 'pg';
import { Logger } from 'winston';
import { handler as webhookLogStreamHandler } from '@/handlers/webhook-log-stream';
import { parseBufferedLogStream } from './service';

const LOG_STREAM_DEFAULT_MAX_LEN = Number(process.env.LOG_STREAM_DEFAULT_MAX_LEN);
const LOG_STREAM_DEFAULT_TRIM_INTERVAL_MS = Number(process.env.LOG_STREAM_DEFAULT_TRIM_INTERVAL_MS);
const LOG_STREAM_DEFAULT_MAX_BUFFER_LENGTH = Number(
  process.env.LOG_STREAM_DEFAULT_MAX_BUFFER_LENGTH
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
    streamMaxLen: LOG_STREAM_DEFAULT_MAX_LEN,
    bufferMaxLength: LOG_STREAM_DEFAULT_MAX_BUFFER_LENGTH
  });

  streamConsumer.on('data', (data: any) => {
    logger.debug(`Processing ${data.length} log stream message(s)`);

    try {
      // const messages = parseRawLogStream(logger, data, streamKey);
      const messages = parseBufferedLogStream(logger, data);
      webhookLogStreamHandler(pgPool, redisClient, streamKey, groupName, messages);
    } catch (err: unknown) {
      logger.error('Error processing log stream message data', { err });
    }
  });

  await streamConsumer.connect();

  return streamConsumer;
}
