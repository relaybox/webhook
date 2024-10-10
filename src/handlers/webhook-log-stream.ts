import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { LogStreamMessageData } from '@/module/types';
import {
  acknowledgeLogStreamMessageData,
  bulkInsertWebhookLogs,
  parseLogStreamMessageData
} from '@/module/service';

const logger = getLogger('webhook-logger');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  logStreamMessageData: LogStreamMessageData[]
): Promise<void> {
  const pgClient = await pgPool.connect();

  console.log(logStreamMessageData);

  try {
    const parsedMessageData = parseLogStreamMessageData(logger, logStreamMessageData);

    await bulkInsertWebhookLogs(logger, pgClient, parsedMessageData);

    await acknowledgeLogStreamMessageData(
      logger,
      redisClient,
      streamKey,
      groupName,
      logStreamMessageData
    );
  } catch (err: unknown) {
    logger.error(`Failed to log webhook event`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
