import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import {
  ackStreamMessages,
  bulkInsertWebhookLogs,
  parseBufferedLogStream,
  parseLogStreamMessages
} from '@/module/service';
import { StreamConsumerMessage } from '@/lib/stream-consumer';

const logger = getLogger('webhook-log-stream');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  messages: StreamConsumerMessage[]
): Promise<void> {
  logger.info(`Handling webhook log stream data`, {
    streamKey,
    groupName,
    batchSize: messages.length
  });

  const pgClient = await pgPool.connect();

  const parsedMessages = parseBufferedLogStream(logger, messages);

  try {
    const parsedMessageData = parseLogStreamMessages(logger, parsedMessages);

    await bulkInsertWebhookLogs(logger, pgClient, parsedMessageData);
  } catch (err: unknown) {
    logger.error(`Failed to perist webhook log stream data`, { err });
    throw err;
  } finally {
    pgClient.release();
    ackStreamMessages(logger, redisClient, streamKey, groupName, parsedMessages);
    logger.info(`Acknowledged ${parsedMessages.length} log stream message(s)`);
  }
}
