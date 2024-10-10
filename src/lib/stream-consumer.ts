import EventEmitter from 'events';
import {
  createClient,
  RedisClientOptions,
  RedisClientType,
  RedisFunctions,
  RedisModules,
  RedisScripts
} from 'redis';
import os from 'os';
import { getLogger } from '@/util/logger.util';
import { Logger } from 'winston';

const DEFAULT_CONSUMER_NAME = os.hostname();

type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;

export class StreamConsumer extends EventEmitter {
  private redisClient: RedisClient;
  private redisBlockingClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private consumerName: string;
  private logger: Logger;
  private isConsuming = true;

  constructor(
    connectionOptions: RedisClientOptions,
    streamKey: string,
    groupName: string,
    consumerName: string = DEFAULT_CONSUMER_NAME
  ) {
    super();

    this.redisClient = this.createClient(connectionOptions);
    this.redisBlockingClient = this.createClient(connectionOptions);
    this.streamKey = streamKey;
    this.groupName = groupName;
    this.consumerName = consumerName;

    this.logger = getLogger(`stream-consumer:${this.consumerName}`);
  }

  async connect(): Promise<StreamConsumer> {
    this.logger.info(`Creating stream consumer`);

    await this.connectClients();
    await this.createConsumerGroup();

    this.startConsumer();

    this.logger.info(`Stream consumer is ready`);

    return this;
  }

  private connectClients(): Promise<RedisClient[]> {
    return Promise.all([this.redisClient.connect(), this.redisBlockingClient.connect()]);
  }

  private disconnectClients(): Promise<string[]> {
    return Promise.all([this.redisClient.quit(), this.redisBlockingClient.quit()]);
  }

  private createClient(connectionOptions: RedisClientOptions): RedisClient {
    const client = createClient(connectionOptions);

    client.on('connect', () => {
      this.logger.info('Redis stream client connected');
    });

    client.on('error', (err) => {
      this.logger.error(`Redis stream client connection error`, { err });
    });

    client.on('ready', () => {
      this.logger.info('Redis stream client is ready');
    });

    client.on('end', () => {
      this.logger.info('Redis stream client disconnected');
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

    while (this.isConsuming) {
      try {
        const data = await this.redisBlockingClient.xReadGroup(
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
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  public async disconnect(): Promise<void> {
    this.logger.info('Closing stream consumer');

    try {
      this.isConsuming = false;
      await this.disconnectClients();
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
    }
  }
}
