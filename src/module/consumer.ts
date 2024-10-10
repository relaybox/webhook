import { RedisClient } from '@/lib/redis';
import { StreamConsumer } from '@/lib/stream-consumer';
import { Pool } from 'pg';
import { RedisClientOptions } from 'redis';
import { Logger } from 'winston';
import { handler as webhookLogStreamHandler } from '@/handlers/webhook-log-stream';

interface StreamConsumerData {
  name: string;
  messages: string;
}

interface StreamMessageData {
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
    const stream = streams.find((stream: StreamConsumerData) => stream.name === streamKey);

    const messages = stream.messages.map((streamMessageData: StreamMessageData) => ({
      streamId: streamMessageData.id,
      ...JSON.parse(streamMessageData.message.data)
    }));

    webhookLogStreamHandler(pgPool, redisClient, messages);
  });

  return streamConsumer;
}
