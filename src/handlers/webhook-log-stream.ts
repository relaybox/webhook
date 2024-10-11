import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { LogStreamMessage } from '@/module/types';
import { ackStreamMessages, bulkInsertWebhookLogs, parseLogStreamMessages } from '@/module/service';

const logger = getLogger('webhook-log-stream');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  logStreamMessages: LogStreamMessage[]
): Promise<void> {
  logger.info(`Handling webhook log stream data`, {
    streamKey,
    groupName,
    batchSize: logStreamMessages.length
  });

  const pgClient = await pgPool.connect();

  try {
    const parsedMessageData = parseLogStreamMessages(logger, logStreamMessages);

    await bulkInsertWebhookLogs(logger, pgClient, parsedMessageData);
  } catch (err: unknown) {
    logger.error(`Failed to perist webhook log stream data`, { err });
    throw err;
  } finally {
    pgClient.release();
    ackStreamMessages(logger, redisClient, streamKey, groupName, logStreamMessages);
    logger.info(`Acknowledged ${logStreamMessages.length} log stream message(s)`);
  }
}
