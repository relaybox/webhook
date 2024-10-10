import { RedisClient } from '@/lib/redis';
import { StreamConsumer } from '@/lib/stream-consumer';
import { Pool } from 'pg';
import { Logger } from 'winston';
import { handler as webhookLogStreamHandler } from '@/handlers/webhook-log-stream';
import { LogStreamMessageData } from './types';

interface StreamConsumerData {
  name: string;
  messages: string;
}

interface StreamConsumerMessageData {
  id: string;
  message: {
    data: string;
  };
}

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
    blocking: false,
    pollingTimeoutMs: 3000
  });

  await streamConsumer.connect();

  streamConsumer.on('data', (streams: any) => {
    logger.debug(`Processing ${streams.length} log stream message(s)`);

    try {
      const stream = streams.find((stream: StreamConsumerData) => stream.name === streamKey);

      const messages: LogStreamMessageData[] = stream.messages.map(
        (streamMessageData: StreamConsumerMessageData) => ({
          streamId: streamMessageData.id,
          ...JSON.parse(streamMessageData.message.data)
        })
      );

      webhookLogStreamHandler(pgPool, redisClient, streamKey, groupName, messages);
    } catch (err: unknown) {
      logger.error('Error processing log stream message data', { err });
    }
  });

  return streamConsumer;
}
