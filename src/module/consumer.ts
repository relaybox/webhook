import { RedisClient } from '@/lib/redis';
import { StreamConsumer } from '@/lib/stream-consumer';
import { Pool } from 'pg';
import { Logger } from 'winston';
import { handler as webhookLogStreamHandler } from '@/handlers/webhook-log-stream';
import { parseBufferedLogStream, parseRawLogStream } from './service';

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
    // pollingTimeoutMs: 3000,
    streamMaxLen: 1000
  });

  await streamConsumer.connect();

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

  return streamConsumer;
}
