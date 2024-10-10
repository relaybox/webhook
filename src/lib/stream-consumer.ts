import EventEmitter from 'events';
import { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from 'redis';
import os from 'os';
import { getLogger } from '@/util/logger.util';
import { Logger } from 'winston';

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
  maxLen?: number;
  maxBufferLength?: number;
}

export class StreamConsumer extends EventEmitter {
  private redisClient: RedisClient;
  private redisBlockingClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private consumerName: string;
  private logger: Logger;
  private isConsuming = true;
  private blocking: boolean = false;
  private pollingTimeoutMs: number;
  private pollTimeout: NodeJS.Timeout;
  private maxLen: number;
  private trimInterval: NodeJS.Timeout;
  private messageBuffer: any = [];
  private maxBufferLength: number = DEFAULT_MAX_BUFFER_LENGTH;

  constructor(opts: StreamConsumerOptions) {
    super();

    this.redisClient = opts.redisClient;
    this.streamKey = opts.streamKey;
    this.groupName = opts.groupName;
    this.consumerName = opts?.consumerName || DEFAULT_CONSUMER_NAME;
    this.pollingTimeoutMs = opts.pollingTimeoutMs || DEFAULT_POLLING_TIMEOUT;
    this.blocking = opts.blocking || false;
    this.maxLen = opts.maxLen || DEFAULT_MAX_LEN;

    if (opts.blocking) {
      this.redisBlockingClient = this.createBlockingClient();
    }

    this.logger = getLogger(`stream-consumer:${this.consumerName}`);

    this.trimInterval = setInterval(() => this.trimStream(), DEFAULT_TRIM_INTERVAL);
  }

  async connect(): Promise<StreamConsumer> {
    this.logger.info(`Creating stream consumer`);

    await this.createConsumerGroup();

    if (this.blocking) {
      await this.redisBlockingClient.connect();
      this.startBlockingConsumer();
    } else {
      this.readStream();
    }

    this.logger.info(`Stream consumer is ready`);

    return this;
  }

  private createBlockingClient(): RedisClient {
    const client = this.redisClient.duplicate();

    client.on('connect', () => {
      this.logger.info('Redis blocking client connected');
    });

    client.on('error', (err) => {
      this.logger.error(`Redis blocking client connection error`, { err });
    });

    client.on('ready', () => {
      this.logger.info('Redis blocking client is ready');
    });

    client.on('end', () => {
      this.logger.info('Redis blocking client disconnected');
    });

    return client;
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
        const data = await this.redisBlockingClient.xReadGroup(
          this.groupName,
          this.consumerName,
          streams,
          options
        );

        if (data) {
          this.bufferStreamData(data);
        } else {
          this.flushStreamDataBuffer();
        }
      } catch (err: unknown) {
        this.logger.error('Error consuming messages from stream:', err);
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  private bufferStreamData(data: any): void {
    this.logger.debug(`Buffering stream data`);

    try {
      const messages = data.flatMap((stream: any) => stream.messages);

      this.messageBuffer = this.messageBuffer.concat(messages);

      if (this.messageBuffer.length >= this.maxBufferLength) {
        this.flushStreamDataBuffer();
      }
    } catch (err: unknown) {
      this.logger.error('Stream message buffering failed');
    }
  }

  private flushStreamDataBuffer(): void {
    this.logger.debug(`Flushing stream data buffer`);
    try {
      if (!this.messageBuffer.length) {
        this.logger.debug(`No messages in buffer`);
        return;
      }

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
      this.redisClient.xTrim(this.streamKey, 'MAXLEN', this.maxLen, {
        strategyModifier: '~'
      });
    } catch (err: unknown) {
      this.logger.error('Error trimming stream:', err);
    }
  }

  public async disconnect(): Promise<void> {
    this.logger.info('Closing stream consumer');

    try {
      this.isConsuming = false;
      (await this.redisBlockingClient?.quit()) || Promise.resolve();
      clearTimeout(this.pollTimeout);
      clearInterval(this.trimInterval);
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
    }
  }
}
