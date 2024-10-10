import EventEmitter from 'events';
import { Logger } from 'winston';
import { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from 'redis';
import { getLogger } from '@/util/logger.util';
import { StreamConsumerData, StreamConsumerMessageData } from '@/module/types';

const DEFAULT_CONSUMER_NAME = `consumer-${process.pid}`;
const DEFAULT_POLLING_TIMEOUT = 10000;
const DEFAULT_MAX_LEN = 1000;
const DEFAULT_TRIM_INTERVAL = 60000;
const DEFAULT_MAX_BUFFER_LENGTH = 10;

type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;

interface StreamConsumerOptions {
  redisClient: RedisClient;
  streamKey: string;
  groupName: string;
  consumerName?: string;
  blocking?: boolean;
  pollingTimeoutMs?: number;
  streamMaxLen?: number;
  maxBufferLength?: number;
}

export class StreamConsumer extends EventEmitter {
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
  private maxBufferLength: number = DEFAULT_MAX_BUFFER_LENGTH;

  constructor(opts: StreamConsumerOptions) {
    super();

    this.redisClient = opts.redisClient.duplicate();
    this.streamKey = opts.streamKey;
    this.groupName = opts.groupName;
    this.consumerName = opts?.consumerName || DEFAULT_CONSUMER_NAME;
    this.pollingTimeoutMs = opts.pollingTimeoutMs || DEFAULT_POLLING_TIMEOUT;
    this.blocking = opts.blocking || false;
    this.streamMaxLen = opts.streamMaxLen || DEFAULT_MAX_LEN;
    this.trimInterval = setInterval(() => this.trimStream(), DEFAULT_TRIM_INTERVAL);

    this.logger = getLogger(`stream-consumer:${this.consumerName}`);
  }

  async connect(): Promise<StreamConsumer> {
    this.logger.info(`Creating stream consumer`);

    this.redisClient.on('connect', () => {
      this.logger.info(`Redis stream consumer client connected`, {
        streamKey: this.streamKey,
        groupName: this.groupName
      });
    });

    this.redisClient.on('error', (err) => {
      this.logger.error(`Redis stream consumer client connection error`, {
        err,
        streamKey: this.streamKey,
        groupName: this.groupName
      });
    });

    this.redisClient.on('ready', () => {
      this.logger.info(`Redis stream consumer client is ready`, {
        streamKey: this.streamKey,
        groupName: this.groupName
      });
    });

    this.redisClient.on('end', () => {
      this.logger.info(`Redis stream consumer client disconnected`, {
        streamKey: this.streamKey,
        groupName: this.groupName
      });
    });

    await this.redisClient.connect();

    await this.createConsumerGroup();

    this.checkPendingMessages();

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
          this.pushToStreamDataBuffer(messages);
        } else {
          this.flushStreamDataBuffer();
        }
      } catch (err: unknown) {
        this.logger.error('Error consuming messages from stream:', err);
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  private pushToStreamDataBuffer(messages: (StreamConsumerMessageData | null)[]): void {
    this.logger.debug(`Buffering stream data`);

    try {
      this.messageBuffer = this.messageBuffer.concat(messages);

      if (this.messageBuffer.length >= this.maxBufferLength) {
        this.flushStreamDataBuffer();
      }
    } catch (err: unknown) {
      this.logger.error('Stream message buffering failed');
    }
  }

  private flushStreamDataBuffer(): void {
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
        this.logger.info(`${pending} pending message(s) found, reading PEL`);

        await this.claimPendingMessages(pending, firstId, lastId);

        // POTENTIALLY DELETE CONSUMERS WITH PENDING MESSAGES HERE...
        // console.log(consumers);
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
    this.logger.debug(`Claiming pending messages in range ${firstId} - ${lastId}`);

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

      this.logger.debug(`Claiming ${claimed.messages.length} idle message(s)`);

      if (claimed.messages.length) {
        this.pushToStreamDataBuffer(claimed.messages);
      }
    } catch (err) {
      this.logger.error('Error claiming pending messages:', err);
    }
  }

  public async disconnect(): Promise<void> {
    this.logger.info('Closing stream consumer');

    try {
      this.isConsuming = false;
      this.flushStreamDataBuffer();
      // this.redisClient.xGroupDelConsumer(this.streamKey, this.groupName, this.consumerName);
      await this.redisClient.quit();
      clearTimeout(this.pollTimeout);
      clearInterval(this.trimInterval);
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
    }
  }
}
