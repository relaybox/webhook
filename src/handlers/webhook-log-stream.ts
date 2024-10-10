import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { LogStreamMessageData } from '@/module/types';
import { bulkInsertWebhookLogs, parseLogStreamMessageData } from '@/module/service';

const logger = getLogger('webhook-logger');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  logStreamMessageData: LogStreamMessageData[]
): Promise<void> {
  const pgClient = await pgPool.connect();

  try {
    const parsedMessageData = parseLogStreamMessageData(logger, logStreamMessageData);
    await bulkInsertWebhookLogs(logger, pgClient, parsedMessageData);
  } catch (err: unknown) {
    logger.error(`Failed to log webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
