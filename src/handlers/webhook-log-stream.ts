import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import {
  ackStreamMessages,
  bulkInsertWebhookLogs,
  parseStreamConsumerMessages,
  parseLogStreamMessages
} from '@/module/service';
import { StreamConsumerMessage } from '@/lib/streams/stream-consumer';
import { LogStreamMessage } from '@/module/types';

const logger = getLogger('webhook-log-stream');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  streamConsumerMessages: StreamConsumerMessage[]
): Promise<void> {
  logger.info(`Handling webhook log stream data`, {
    streamKey,
    groupName,
    batchSize: streamConsumerMessages.length
  });

  const pgClient = await pgPool.connect();

  let parsedStreamConsumerMessages: LogStreamMessage[] | null = null;

  try {
    parsedStreamConsumerMessages = parseStreamConsumerMessages(logger, streamConsumerMessages);
    const parsedLogStreamMessages = parseLogStreamMessages(logger, parsedStreamConsumerMessages);

    await bulkInsertWebhookLogs(logger, pgClient, parsedLogStreamMessages);
  } catch (err: unknown) {
    logger.error(`Failed to perist webhook log stream data`, { err });
    throw err;
  } finally {
    pgClient.release();

    if (parsedStreamConsumerMessages) {
      ackStreamMessages(logger, redisClient, streamKey, groupName, parsedStreamConsumerMessages);
      logger.info(`Acknowledged ${parsedStreamConsumerMessages.length} log stream message(s)`);
    }
  }
}
