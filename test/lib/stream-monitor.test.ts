import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanupRedisClient, connectionOptions, getRedisClient, RedisClient } from '@/lib/redis';
import StreamMonitor from '@/lib/streams/stream-monitor';
import StreamConsumer, { StreamConsumerMessage } from '@/lib/streams/stream-consumer';

vi.mock('@/util/logger.util', () => ({
  getLogger: vi.fn().mockReturnValue({
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn()
  })
}));

const LOG_STREAM_KEY = 'test:logs:webhook';
const LOG_STREAM_GROUP_NAME = 'test:webhook-log-group';

const defaultStreamMonitorOptions = {
  streamKey: LOG_STREAM_KEY,
  groupName: LOG_STREAM_GROUP_NAME
};

function addMessagesToStream(
  count: number,
  redisClient: RedisClient,
  streamKey: string,
  message: any
): Promise<string[]> {
  return Promise.all(
    Array.from({ length: count }, (_, i) => redisClient.xAdd(streamKey, '*', message))
  );
}

describe('StreamMonitor', () => {
  let redisClient: RedisClient;
  let streamConsumer: StreamConsumer;
  let streamMonitor: StreamMonitor;

  beforeAll(async () => {
    redisClient = getRedisClient();
    await redisClient.connect();
  });

  afterAll(async () => {
    await cleanupRedisClient();
  });

  beforeEach(async () => {
    streamConsumer = new StreamConsumer({
      ...defaultStreamMonitorOptions,
      connectionOptions,
      maxBlockingIterations: 3
    });

    await streamConsumer.connect();
  });

  afterEach(async () => {
    await streamMonitor.disconnect();
    await redisClient.del(LOG_STREAM_KEY);
    await streamConsumer.disconnect();
  });

  it('should claim, acknowledge and emit pending messages', async () => {
    const consumerMaxIdleTimeMs = 300;
    const messageCount = 3;

    streamMonitor = new StreamMonitor({
      ...defaultStreamMonitorOptions,
      connectionOptions,
      delayMs: 1000,
      consumerMaxIdleTimeMs
    });

    const mockMessage = {
      test: true
    };

    await addMessagesToStream(messageCount, redisClient, LOG_STREAM_KEY, {
      data: JSON.stringify(mockMessage)
    });

    await new Promise((resolve) => setTimeout(resolve, consumerMaxIdleTimeMs));

    const pendingMessagesBeforeMonitor = await redisClient.xPending(
      LOG_STREAM_KEY,
      LOG_STREAM_GROUP_NAME
    );

    const dataPromise = new Promise<void>((resolve, reject) => {
      streamMonitor.on('data', async (messages: StreamConsumerMessage[]) => {
        expect(messages).toHaveLength(messageCount);
        expect(messages).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              id: expect.any(String),
              message: expect.objectContaining({
                data: JSON.stringify(mockMessage)
              })
            })
          ])
        );

        resolve();
      });
    });

    await streamMonitor.connect();

    await Promise.race([dataPromise, new Promise((_, reject) => setTimeout(reject, 5000))]);

    const pendingMessagesAfterMonitor = await redisClient.xPending(
      LOG_STREAM_KEY,
      LOG_STREAM_GROUP_NAME
    );

    expect(pendingMessagesBeforeMonitor).not.toEqual(pendingMessagesAfterMonitor);
    expect(pendingMessagesBeforeMonitor.pending).toEqual(3);
    expect(pendingMessagesAfterMonitor.pending).toEqual(0);
  });

  it('should cleanup hanging consumers', async () => {
    const consumerMaxIdleTimeMs = 300;

    streamMonitor = new StreamMonitor({
      ...defaultStreamMonitorOptions,
      connectionOptions,
      delayMs: 1000,
      consumerMaxIdleTimeMs
    });

    await new Promise((resolve) => setTimeout(resolve, consumerMaxIdleTimeMs));

    const consumersBeforeMonitor = await redisClient.xInfoConsumers(
      LOG_STREAM_KEY,
      LOG_STREAM_GROUP_NAME
    );

    await streamMonitor.connect();

    const consumersAfterMonitor = await redisClient.xInfoConsumers(
      LOG_STREAM_KEY,
      LOG_STREAM_GROUP_NAME
    );

    expect(consumersBeforeMonitor).not.toEqual(consumersAfterMonitor);
    expect(consumersBeforeMonitor.length).toEqual(1);
    expect(consumersAfterMonitor.length).toEqual(0);
  });
});
