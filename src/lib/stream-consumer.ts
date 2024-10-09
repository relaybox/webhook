import EventEmitter from 'events';
import { RedisClient } from './redis';
import os from 'os';
import { getLogger } from '@/util/logger.util';
import { Logger } from 'winston';

const DEFAULT_CONSUMER_NAME = os.hostname();

export class StreamConsumer extends EventEmitter {
  private redisClient: RedisClient;
  private redisBlockingClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private consumerName: string = DEFAULT_CONSUMER_NAME;
  private logger: Logger;

  constructor(redisClient: RedisClient, streamKey: string, groupName: string) {
    super();

    this.redisClient = redisClient;
    this.redisBlockingClient = redisClient.duplicate();
    this.streamKey = streamKey;
    this.groupName = groupName;

    this.logger = getLogger(`stream-consumer:${this.consumerName}`);
  }

  async connect(): Promise<StreamConsumer> {
    this.logger.info(`Creating stream consumer`);

    await this.redisBlockingClient.connect();
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
      BLOCK: 0,
      COUNT: 10
    };

    while (true) {
      try {
        const result = await this.redisBlockingClient.xReadGroup(
          this.groupName,
          this.consumerName,
          streams,
          options
        );

        if (result) {
          this.emit('message', result);
        }
      } catch (err: unknown) {
        this.logger.error('Error consuming messages from stream:', err);
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }
}
