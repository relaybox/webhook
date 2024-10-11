import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { cleanupRedisClient, getRedisClient, RedisClient } from '@/lib/redis';
import StreamConsumer, { StreamConsumerMessage } from '@/lib/stream-consumer';

vi.mock('@/util/logger.util', () => ({
  getLogger: vi.fn().mockReturnValue({
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn()
  })
}));

const LOG_STREAM_DEFAULT_MAX_LEN = 10;
const LOG_STREAM_DEFAULT_MAX_BUFFER_LENGTH = 1;
const LOG_STREAM_KEY = 'test:logs:webhook';
const LOG_STREAM_GROUP_NAME = 'test:webhook-log-group';

const defaultStreamConsumerOptions = {
  streamKey: LOG_STREAM_KEY,
  groupName: LOG_STREAM_GROUP_NAME,
  blocking: true,
  streamMaxLen: LOG_STREAM_DEFAULT_MAX_LEN,
  bufferMaxLength: LOG_STREAM_DEFAULT_MAX_BUFFER_LENGTH,
  consumerIdleTimeoutMs: 1000,
  maxBlockingIterations: 1
};

describe('StreamConsumer', () => {
  let redisClient: RedisClient;
  let streamConsumer: StreamConsumer;

  beforeAll(async () => {
    redisClient = getRedisClient();
    await redisClient.connect();
  });

  afterAll(async () => {
    await cleanupRedisClient();
  });

  afterEach(async () => {
    vi.useRealTimers();
    await streamConsumer.disconnect();
    await redisClient.del(LOG_STREAM_KEY);
  });

  it('should add and consume a single stream message', async () => {
    streamConsumer = new StreamConsumer({
      ...defaultStreamConsumerOptions,
      redisClient,
      maxBlockingIterations: 1
    });

    const mockMessage = {
      test: true
    };

    const onDataPromise = new Promise<void>((resolve, reject) => {
      streamConsumer.on('data', async (messages: StreamConsumerMessage[]) => {
        const messageIds = [];

        for (const message of messages) {
          messageIds.push(message.id);
          expect(JSON.parse(message.message.data)).toEqual(mockMessage);
        }

        expect(messages).toHaveLength(1);

        await redisClient.xAck(LOG_STREAM_KEY, LOG_STREAM_GROUP_NAME, messageIds);

        resolve();
      });
    });

    await streamConsumer.connect();
    await redisClient.xAdd(LOG_STREAM_KEY, '*', { data: JSON.stringify(mockMessage) });
    await onDataPromise;
  });

  it.only('should claim pending messages from closed consumers', async () => {
    const streamConsumerOne = new StreamConsumer({
      ...defaultStreamConsumerOptions,
      redisClient,
      consumerName: `test-consumer-${Date.now()}`,
      maxBlockingIterations: 1
    });

    const mockMessage = {
      test: true
    };

    await streamConsumerOne.connect();
    await redisClient.xAdd(LOG_STREAM_KEY, '*', { data: JSON.stringify(mockMessage) });
    await streamConsumerOne.disconnect();

    streamConsumer = new StreamConsumer({
      ...defaultStreamConsumerOptions,
      redisClient,
      consumerIdleTimeoutMs: 0,
      // blockingTimeoutMs: 1,
      consumerName: `test-consumer-${Date.now()}`,
      maxBlockingIterations: 1
    });

    const onDataPromise = new Promise<void>((resolve, reject) => {
      streamConsumer.on('data', async (messages: StreamConsumerMessage[]) => {
        const messageIds = [];

        for (const message of messages) {
          messageIds.push(message.id);
          expect(JSON.parse(message.message.data)).toEqual(mockMessage);
        }

        expect(messages).toHaveLength(1);

        // await redisClient.xAck(LOG_STREAM_KEY, LOG_STREAM_GROUP_NAME, messageIds);

        resolve();
      });
    });

    await streamConsumer.connect();
    await redisClient.xAdd(LOG_STREAM_KEY, '*', { data: JSON.stringify(mockMessage) });

    await onDataPromise;
  });
});
