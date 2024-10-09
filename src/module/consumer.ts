import { RedisClient } from '@/lib/redis';
import { Pool } from 'pg';
import { Logger } from 'winston';

export async function startConsumer(
  logger: Logger,
  pgPool: Pool,
  redisStreamClient: RedisClient,
  streamKey: string,
  groupName: string,
  consumerName: string
): Promise<void> {
  logger.info('Starting consumer');

  await createConsumerGroup(logger, redisStreamClient, streamKey, groupName);

  consumeMessages(logger, pgPool, redisStreamClient, streamKey, groupName, consumerName);
}

async function createConsumerGroup(
  logger: Logger,
  redisStreamClient: RedisClient,
  streamKey: string,
  groupName: string
): Promise<void> {
  logger.info(`Creating consumer group "${groupName}"`);

  try {
    await redisStreamClient.xGroupCreate(streamKey, groupName, '0', { MKSTREAM: true });
  } catch (err: unknown) {
    if (err instanceof Error && err.message.includes('BUSYGROUP')) {
      logger.info(`Consumer group "${groupName}" already exists.`);
    } else {
      logger.error(`Error creating consumer group "${groupName}":`, { err });
      throw err;
    }
  }
}

async function consumeMessages(
  logger: Logger,
  pgPool: Pool,
  redisStreamClient: RedisClient,
  streamKey: string,
  groupName: string,
  consumerName: string
): Promise<void> {
  logger.info(`Consumer '${consumerName}' started consuming messages from stream '${streamKey}'`);

  const streams = [
    {
      id: '>',
      key: streamKey
    }
  ];

  const options = {
    BLOCK: 0,
    COUNT: 10
  };

  while (true) {
    try {
      const result = await redisStreamClient.xReadGroup(groupName, consumerName, streams, options);

      console.log('STREAMS >>>>>', JSON.stringify(result, null, 2));

      if (result) {
        const messages = result.flatMap((stream) => stream.messages);

        await processMessages(logger, pgPool, redisStreamClient, streamKey, groupName, messages);
      }
    } catch (err: unknown) {
      logger.error('Error consuming messages from stream:', err);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

async function processMessages(
  logger: Logger,
  pgPool: Pool,
  redisStreamClient: RedisClient,
  streamKey: string,
  groupName: string,
  messages: { id: string; message: { [x: string]: string } }[]
): Promise<void> {
  const logs = messages.map(({ id, message }) => {
    const logData = JSON.parse(message.message);
    return { id, logData };
  });

  try {
    console.log('Saving to PG >>>>>>>>>>>>>>>>');

    const ids = logs.map((log) => log.id);

    await redisStreamClient.xAck(streamKey, groupName, ids);
    logger.info(`Acknowledged ${ids.length} messages`);
  } catch (err) {
    logger.error('Error processing messages:', err);
  }
}

export function stopConsumer(logger: Logger, redisStreamClient: RedisClient): void {
  logger.info('Stopping consumer');
}
