import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import {
  ackStreamMessages,
  bulkInsertWebhookLogs,
  parseStreamConsumerMessages,
  parseLogStreamDbEntries
} from '@/module/service';
import { StreamConsumerMessage } from '@/lib/streams/stream-consumer';

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

  try {
    const parsedStreamConsumerMessages = parseStreamConsumerMessages(
      logger,
      streamConsumerMessages
    );

    const parsedLogStreamDbEntries = parseLogStreamDbEntries(logger, parsedStreamConsumerMessages);

    await bulkInsertWebhookLogs(logger, pgClient, parsedLogStreamDbEntries);

    await ackStreamMessages(
      logger,
      redisClient,
      streamKey,
      groupName,
      parsedStreamConsumerMessages
    );
  } catch (err: unknown) {
    logger.error(`Failed to perist webhook log stream data`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
