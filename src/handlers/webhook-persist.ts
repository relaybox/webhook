import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';
import { StreamConsumerMessage } from '@/lib/streams/stream-consumer';
import { insertWebhookLog, parseStreamConsumerMessage } from '@/module/service';

const logger = getLogger('webhook-persist');

export async function handler(
  pgPool: Pool,
  redisClient: RedisClient,
  streamConsumerMessage: StreamConsumerMessage
): Promise<void> {
  logger.info(`Handling webhook log stream data`, { streamConsumerMessage });

  const pgClient = await pgPool.connect();

  try {
    const parsedStreamConsumerMessage = parseStreamConsumerMessage(logger, streamConsumerMessage);

    console.log(parsedStreamConsumerMessage);

    await insertWebhookLog(logger, pgClient, parsedStreamConsumerMessage);
  } catch (err: unknown) {
    logger.error(`Failed to perist webhook log stream data`, { err });
    throw err;
  } finally {
    pgClient.release();
  }
}
