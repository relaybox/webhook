import EventEmitter from 'events';
import { Logger } from 'winston';
import { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from 'redis';
import { getLogger } from '@/util/logger.util';

const DEFAULT_CONSUMER_NAME = `consumer-${process.pid}`;
const DEFAULT_POLLING_TIMEOUT = 10000;
const DEFAULT_MAX_LEN = 1000;
const DEFAULT_TRIM_INTERVAL = 60000;
const DEFAULT_MAX_BUFFER_LENGTH = 10;

type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;

export interface StreamConsumerData {
  name: string;
  messages: StreamConsumerMessage[];
}

export interface StreamConsumerMessage {
  id: string;
  message: {
    [x: string]: string;
  };
}

export interface StreamConsumerOptions {
  redisClient: RedisClient;
  streamKey: string;
  groupName: string;
  consumerName?: string;
  blocking?: boolean;
  pollingTimeoutMs?: number;
  streamMaxLen?: number;
  bufferMaxLength?: number;
}

export default class StreamConsumer extends EventEmitter {
  private redisClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private consumerName: string;
  private logger: Logger;
  private isConsuming = true;
  private blocking: boolean = false;
  private pollingTimeoutMs: number;
  private pollTimeout: NodeJS.Timeout;
  private streamMaxLen: number;
  private trimInterval: NodeJS.Timeout;
  private messageBuffer: any = [];
  private bufferMaxLength: number;

  constructor(opts: StreamConsumerOptions) {
    super();

    this.redisClient = opts.redisClient.duplicate();
    this.streamKey = opts.streamKey;
    this.groupName = opts.groupName;
    this.consumerName = opts.consumerName || DEFAULT_CONSUMER_NAME;
    this.pollingTimeoutMs = opts.pollingTimeoutMs || DEFAULT_POLLING_TIMEOUT;
    this.blocking = opts.blocking || true;
    this.streamMaxLen = opts.streamMaxLen || DEFAULT_MAX_LEN;
    this.bufferMaxLength = opts.bufferMaxLength || DEFAULT_MAX_BUFFER_LENGTH;
    this.trimInterval = setInterval(() => this.trimStream(), DEFAULT_TRIM_INTERVAL);

    this.logger = getLogger(`stream-consumer:${this.consumerName}`);
  }

  async connect(): Promise<StreamConsumer> {
    this.logger.info(`Creating stream consumer`);

    const logData = {
      streamKey: this.streamKey,
      groupName: this.groupName
    };

    this.redisClient.on('connect', () => {
      this.logger.info(`Redis stream consumer client connected`, logData);
    });

    this.redisClient.on('error', (err) => {
      this.logger.error(`Redis stream consumer client connection error`, { ...logData, err });
    });

    this.redisClient.on('ready', () => {
      this.logger.info(`Redis stream consumer client is ready`, logData);
    });

    this.redisClient.on('end', () => {
      this.logger.info(`Redis stream consumer client disconnected`, logData);
    });

    await this.redisClient.connect();

    await this.createConsumerGroup();
    await this.checkPendingMessages();

    if (this.blocking) {
      this.startBlockingConsumer();
    } else {
      this.readStream();
    }

    this.logger.info(`Stream consumer is ready`);

    return this;
  }

  private async createConsumerGroup(): Promise<void> {
    this.logger.debug(`Creating consumer group "${this.groupName}"`);

    try {
      await this.redisClient.xGroupCreate(this.streamKey, this.groupName, '0', {
        MKSTREAM: true
      });
    } catch (err: unknown) {
      if (err instanceof Error && err.message.includes('BUSYGROUP')) {
        this.logger.debug(`Consumer group "${this.groupName}" already exists.`);
      } else {
        this.logger.error(`Error creating consumer group "${this.groupName}":`, { err });
        throw err;
      }
    }
  }

  private async startBlockingConsumer(): Promise<void> {
    this.logger.debug(`Starting consumer`);

    const streams = [
      {
        id: '>',
        key: this.streamKey
      }
    ];

    const options = {
      BLOCK: 10000,
      COUNT: 10
    };

    while (this.isConsuming) {
      try {
        const data = await this.redisClient.xReadGroup(
          this.groupName,
          this.consumerName,
          streams,
          options
        );

        if (data) {
          const messages = data.flatMap((stream: StreamConsumerData) => stream.messages);
          this.pushToMessageBuffer(messages);
        } else {
          this.flushMessageBuffer();
        }
      } catch (err: unknown) {
        this.logger.error('Error consuming messages from stream:', err);
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  private pushToMessageBuffer(messages: (StreamConsumerMessage | null)[]): void {
    this.logger.debug(`Buffering stream data`);

    try {
      this.messageBuffer = this.messageBuffer.concat(messages);

      if (this.messageBuffer.length >= this.bufferMaxLength) {
        this.flushMessageBuffer();
      }
    } catch (err: unknown) {
      this.logger.error('Stream message buffering failed');
    }
  }

  private flushMessageBuffer(): void {
    if (!this.messageBuffer.length) {
      this.logger.debug(`No messages in buffer`);
      return;
    }

    this.logger.debug(`Flushing stream data buffer`);

    try {
      this.emit('data', this.messageBuffer);
      this.messageBuffer = [];
    } catch (err: unknown) {
      this.logger.error('Stream message buffering failed');
    }
  }

  private async readStream(): Promise<void> {
    this.logger.debug(`Polling ${this.streamKey} stream`);

    const streams = [
      {
        id: '>',
        key: this.streamKey
      }
    ];

    const options = {
      COUNT: 10
    };

    try {
      const data = await this.redisClient.xReadGroup(
        this.groupName,
        this.consumerName,
        streams,
        options
      );

      if (data) {
        this.emit('data', data);
      }
    } catch (err: unknown) {
      this.logger.error('Error consuming messages from stream:', err);
    }

    this.pollTimeout = setTimeout(() => this.readStream(), this.pollingTimeoutMs);
  }

  private trimStream(): void {
    this.logger.debug(`Trimming stream`);

    try {
      this.redisClient.xTrim(this.streamKey, 'MAXLEN', this.streamMaxLen, {
        strategyModifier: '~'
      });
    } catch (err: unknown) {
      this.logger.error('Error trimming stream:', err);
    }
  }

  private async checkPendingMessages(): Promise<void> {
    this.logger.info(`Checking pending messages`);

    try {
      const pendingMessages = await this.redisClient.xPending(this.streamKey, this.groupName);

      if (!pendingMessages.pending) {
        this.logger.info(`No pending messages found`);
        return;
      }

      const { pending, firstId, lastId, consumers } = pendingMessages;

      if (pending > 0 && firstId) {
        await this.claimPendingMessages(pending, firstId, lastId);
      }
    } catch (err: unknown) {
      this.logger.error('Error checking pending messages:', err);
    }
  }

  private async claimPendingMessages(
    pending: number,
    firstId: string,
    lastId: string | null
  ): Promise<void> {
    this.logger.debug(
      `Attempting to claim ${pending} pending messages in range ${firstId} - ${lastId}`
    );

    try {
      const claimed = await this.redisClient.xAutoClaim(
        this.streamKey,
        this.groupName,
        this.consumerName,
        10000,
        firstId,
        {
          COUNT: pending
        }
      );

      this.logger.debug(`Claimed ${claimed.messages.length} idle message(s)`);

      if (claimed.messages.length) {
        this.pushToMessageBuffer(claimed.messages);
      }
    } catch (err) {
      this.logger.error('Error claiming pending messages:', err);
    }
  }

  public async disconnect(): Promise<void> {
    this.logger.info('Closing stream consumer');

    try {
      this.isConsuming = false;
      this.flushMessageBuffer();
      await this.redisClient.quit();
      clearTimeout(this.pollTimeout);
      clearInterval(this.trimInterval);
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
    }
  }
}
