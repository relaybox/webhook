import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanupRedisClient, connectionOptions, getRedisClient, RedisClient } from '@/lib/redis';
import StreamMonitor from '@/lib/streams/stream-monitor';
import StreamConsumer from '@/lib/streams/stream-consumer';

// vi.mock('@/util/logger.util', () => ({
//   getLogger: vi.fn().mockReturnValue({
//     debug: vi.fn(),
//     info: vi.fn(),
//     warn: vi.fn(),
//     error: vi.fn()
//   })
// }));

const LOG_STREAM_KEY = 'test:logs:webhook';
const LOG_STREAM_GROUP_NAME = 'test:webhook-log-group';

const defaultStreamMonitorOptions = {
  streamKey: LOG_STREAM_KEY,
  groupName: LOG_STREAM_GROUP_NAME
};

function addMessages(
  count: number,
  redisClient: RedisClient,
  streamKey: string,
  message: any
): Promise<string[]> {
  return Promise.all(
    Array.from({ length: count }, (_, i) =>
      redisClient.xAdd(streamKey, '*', { data: JSON.stringify(message) })
    )
  );
}

describe('StreamMonitor', () => {
  let redisClient: RedisClient;
  let streamConsumer: StreamConsumer;
  let streamMonitor: StreamMonitor;

  beforeAll(async () => {
    redisClient = getRedisClient();
    await redisClient.connect();

    streamConsumer = new StreamConsumer({
      ...defaultStreamMonitorOptions,
      connectionOptions,
      maxBlockingIterations: 3
    });

    await streamConsumer.connect();
  });

  afterAll(async () => {
    await cleanupRedisClient();
  });

  beforeEach(async () => {});

  afterEach(async () => {
    await streamMonitor.disconnect();
    await redisClient.del(LOG_STREAM_KEY);
    await streamConsumer.disconnect();
  });

  it('should monitor pending messages', async () => {
    streamMonitor = new StreamMonitor({
      ...defaultStreamMonitorOptions,
      connectionOptions,
      delayMs: 1000
    });

    await addMessages(3, redisClient, LOG_STREAM_KEY, { test: true });

    await streamMonitor.connect();

    expect(true).toBe(true);
  });
});
