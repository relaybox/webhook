import EventEmitter from 'events';
import { Logger } from 'winston';
import { createClient, RedisClientOptions } from 'redis';
import { getLogger } from '@/util/logger.util';
import { RedisClient } from './types';

const DEFAULT_CONSUMER_NAME = `consumer-${process.pid}`;
const DEFAULT_BUFFER_MAX_LENGTH = 10;
const DEFAULT_CONSUMER_BLOCKING_TIMEOUT_MS = 10000;

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
  connectionOptions: RedisClientOptions;
  streamKey: string;
  groupName: string;
  consumerName?: string;
  blockingTimeoutMs?: number;
  maxBlockingIterations?: number;
  bufferMaxLength?: number;
}

export default class StreamConsumer extends EventEmitter {
  private redisClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private consumerName: string;
  private logger: Logger;
  private blockingTimeoutMs: number;
  private maxBlockingIterations: number | null;
  private currentBlockingIteration: number = 0;
  private pollTimeout: NodeJS.Timeout;
  private messageBuffer: any = [];
  private bufferMaxLength: number;
  private isConsuming = true;

  constructor(opts: StreamConsumerOptions) {
    super();

    this.redisClient = createClient(opts.connectionOptions);
    this.streamKey = opts.streamKey;
    this.groupName = opts.groupName;
    this.consumerName = opts.consumerName || DEFAULT_CONSUMER_NAME;
    this.blockingTimeoutMs = opts.blockingTimeoutMs || DEFAULT_CONSUMER_BLOCKING_TIMEOUT_MS;
    this.maxBlockingIterations = opts.maxBlockingIterations || null;
    this.bufferMaxLength = opts.bufferMaxLength || DEFAULT_BUFFER_MAX_LENGTH;

    this.logger = getLogger(`stream-consumer:${this.consumerName}`);

    this.logger.info(`Stream consumer ready`);
  }

  async connect(): Promise<StreamConsumer> {
    this.logger.info(`Connecting stream consumer client`);

    const logData = {
      streamKey: this.streamKey,
      groupName: this.groupName
    };

    this.redisClient.on('connect', () => {
      this.logger.info(`Stream consumer client connected`, logData);
    });

    this.redisClient.on('error', (err) => {
      this.logger.error(`Stream consumer client error`, { ...logData, err });
    });

    this.redisClient.on('ready', () => {
      this.logger.info(`Stream consumer client ready`, logData);
    });

    this.redisClient.on('end', () => {
      this.logger.info(`Stream consumer client disconnected`, logData);
    });

    await this.redisClient.connect();

    await this.createConsumerGroup();

    this.startConsumer();

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

  private async startConsumer(): Promise<void> {
    this.logger.debug(`Starting consumer`);

    const streams = [
      {
        id: '>',
        key: this.streamKey
      }
    ];

    const options = {
      BLOCK: this.blockingTimeoutMs,
      COUNT: this.bufferMaxLength
    };

    while (this.isConsuming) {
      if (
        this.maxBlockingIterations &&
        this.currentBlockingIteration >= this.maxBlockingIterations
      ) {
        this.logger.debug(`Blocking max iterations reached, breaking blocking loop`);
        break;
      }

      this.currentBlockingIteration++;

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
        this.emit('error', err);
      }
    }
  }

  private pushToMessageBuffer(messages: (StreamConsumerMessage | null)[]): void {
    this.logger.debug(`Buffering ${messages.length} message(s)`);

    try {
      this.messageBuffer = this.messageBuffer.concat(messages);

      if (this.messageBuffer.length >= this.bufferMaxLength) {
        this.flushMessageBuffer();
      }
    } catch (err: unknown) {
      this.logger.error('Push to message buffer failed', { err });
      this.emit('error', err);
    }
  }

  private flushMessageBuffer(): void {
    this.logger.debug(`Attempting to flush message buffer`);

    if (!this.messageBuffer.length) {
      this.logger.debug(`No buffered messages`);
      return;
    }

    this.logger.info(`Flushing ${this.messageBuffer.length} buffered message(s)`);

    try {
      this.emit('data', this.messageBuffer);
      this.messageBuffer = [];
    } catch (err: unknown) {
      this.logger.error('Flush message buffer failed', { err });
      this.emit('error', err);
    }
  }

  public async disconnect(): Promise<void> {
    this.logger.info('Closing stream consumer');

    try {
      this.isConsuming = false;
      this.flushMessageBuffer();
      clearTimeout(this.pollTimeout);

      if (this.redisClient.isOpen) {
        this.redisClient.removeAllListeners();
        await this.redisClient.quit();
      }
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
      this.emit('error', err);
    }
  }
}
