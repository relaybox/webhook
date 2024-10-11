import EventEmitter from 'events';
import { getLogger } from '@/util/logger.util';
import { Logger } from 'winston';
import { RedisClient } from '../redis';
import { createClient, RedisClientOptions } from 'redis';

export interface StreamMonitorOptions {
  connectionOptions: RedisClientOptions;
  streamKey: string;
  groupName: string;
  delayMs?: number;
}

export default class StreamMonitor extends EventEmitter {
  private logger: Logger;
  private redisClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private delayMs: number;
  private delayTimeout: NodeJS.Timeout;

  constructor(opts: StreamMonitorOptions) {
    super();

    this.redisClient = createClient(opts.connectionOptions);
    this.streamKey = opts.streamKey;
    this.groupName = opts.groupName;
    this.delayMs = opts.delayMs || 5000;

    this.logger = getLogger(`stream-monitor`);

    this.logger.info(`Stream monitor ready`);
  }

  async connect(): Promise<StreamMonitor> {
    this.logger.info(`Connecting stream monitor client`);

    const logData = {
      streamKey: this.streamKey
    };

    this.redisClient.on('connect', () => {
      this.logger.info(`Stream monitor client connected`, logData);
    });

    this.redisClient.on('error', (err) => {
      this.logger.error(`Stream monitor client error`, { ...logData, err });
    });

    this.redisClient.on('ready', () => {
      this.logger.info(`Stream monitor client ready`, logData);
    });

    this.redisClient.on('end', () => {
      this.logger.info(`Stream monitor client disconnected`, logData);
    });

    await this.redisClient.connect();

    await this.startMonitor();

    return this;
  }

  private async startMonitor(): Promise<void> {
    this.logger.debug(`Starting monitor`);

    const pendingMessages = await this.redisClient.xPending(this.streamKey, this.groupName);

    console.log(pendingMessages);

    this.delayTimeout = setTimeout(() => this.startMonitor(), this.delayMs);
  }

  async disconnect(): Promise<void> {
    this.logger.info('Closing stream monitor');

    try {
      clearTimeout(this.delayTimeout);

      if (this.redisClient.isOpen) {
        await this.redisClient.quit();
      }
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
    }
  }
}
