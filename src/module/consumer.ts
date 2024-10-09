import { RedisClient } from '@/lib/redis';
import { StreamConsumer } from '@/lib/stream-consumer';
import { Pool } from 'pg';
import { Logger } from 'winston';

interface StreamMessage {
  id: string;
  message: string;
}

export async function startLogStreamConsumer(
  logger: Logger,
  pgPool: Pool,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string
): Promise<StreamConsumer> {
  logger.info('Starting log stream consumer');

  const streamConsumer = new StreamConsumer(redisClient, streamKey, groupName);

  await streamConsumer.connect();

  streamConsumer.on('data', (data: any) => {
    // const messages = message.map(({ id, message }: StreamMessage) => {
    //   console.log('MESSAGE', startLogStreamConsumer);
    // });

    console.log('STREAM DATA', JSON.stringify(data, null, 2));
  });

  return streamConsumer;
}
