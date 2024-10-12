import EventEmitter from 'events';
import { getLogger } from '@/util/logger.util';
import { Logger } from 'winston';
import { createClient, RedisClientOptions } from 'redis';
import { StreamConsumerMessage } from './stream-consumer';
import { RedisClient } from './types';

export interface StreamMonitorOptions {
  connectionOptions: RedisClientOptions;
  streamKey: string;
  groupName: string;
  consumerMaxIdleTimeMs?: number;
  delayMs?: number;
}

export interface StreamMonitorData {
  nextId: string;
  messages: StreamConsumerMessage[];
}

export default class StreamMonitor extends EventEmitter {
  private logger: Logger;
  private redisClient: RedisClient;
  private streamKey: string;
  private groupName: string;
  private delayMs: number;
  private delayTimeout: NodeJS.Timeout | null;
  private consumerMaxIdleTimeMs: number;

  constructor(opts: StreamMonitorOptions) {
    super();

    this.redisClient = createClient(opts.connectionOptions);
    this.streamKey = opts.streamKey;
    this.groupName = opts.groupName;
    this.delayMs = opts.delayMs || 5000;
    this.consumerMaxIdleTimeMs = opts.consumerMaxIdleTimeMs ?? 10000;

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

    try {
      await this.handlePendingMessages();
      await this.handleHangingConsumers();
    } catch (err: unknown) {
      this.logger.error(`Error monitoring stream`, { err });
    }

    this.delayTimeout = setTimeout(() => this.startMonitor(), this.delayMs);
  }

  private async handlePendingMessages(): Promise<void> {
    this.logger.debug(`Handling pending messages`);

    const { pending, firstId, consumers } = await this.redisClient.xPending(
      this.streamKey,
      this.groupName
    );

    if (!pending || !consumers?.length) {
      this.logger.debug(`No pending messages found`);
      return;
    }

    for (const consumer of consumers) {
      const claimedMessages = await this.claimPendingMessages(consumer.name, firstId);

      if (claimedMessages?.length) {
        this.logger.debug(
          `Claimed ${claimedMessages.length} pending messages from ${consumer.name}`,
          { consumer }
        );

        await this.acknowledgeClaimedMessages(claimedMessages);

        this.emit('data', claimedMessages);
      }
    }
  }

  private async handleHangingConsumers(): Promise<void> {
    this.logger.debug(`Handling hanging consumers`);

    const consumers = await this.redisClient.xInfoConsumers(this.streamKey, this.groupName);

    for (const consumer of consumers) {
      if (consumer.idle > this.consumerMaxIdleTimeMs) {
        this.logger.info(`Consumer ${consumer.name} idle for ${consumer.idle}ms`, {
          consumer
        });

        await this.cleanConsumer(consumer.name);
      }
    }
  }

  private async claimPendingMessages(
    consumerName: string,
    firstId: string | null
  ): Promise<(StreamConsumerMessage | null)[]> {
    this.logger.debug(`Attempting to claim pending messages from ${consumerName}`);

    const data = await this.redisClient.xAutoClaim(
      this.streamKey,
      this.groupName,
      consumerName,
      this.consumerMaxIdleTimeMs,
      firstId ?? '0'
    );

    return data.messages || [];
  }

  private async acknowledgeClaimedMessages(
    messages: (StreamConsumerMessage | null)[]
  ): Promise<number> {
    const ids = messages.reduce<string[]>((acc, message) => {
      if (message) {
        acc.push(message.id);
      }

      return acc;
    }, []);

    this.logger.debug(`ack'ing ${ids.length} claimed message(s)`, { ids });

    return this.redisClient.xAck(this.streamKey, this.groupName, ids);
  }

  private async cleanConsumer(consumerName: string): Promise<void> {
    this.logger.debug(`Cleaning consumer ${consumerName}`);

    try {
      await this.redisClient.xGroupDelConsumer(this.streamKey, this.groupName, consumerName);
    } catch (err: unknown) {
      this.logger.error(`Error cleaning consumer`, { err, consumerName });
      throw err;
    }
  }

  async disconnect(): Promise<void> {
    this.logger.info('Closing stream monitor');

    try {
      if (this.delayTimeout) {
        clearTimeout(this.delayTimeout);
        this.delayTimeout = null;
      }

      if (this.redisClient.isOpen) {
        this.redisClient.removeAllListeners();
        await this.redisClient.quit();
      }
    } catch (err) {
      this.logger.error('Error disconnecting Redis client', { err });
    }
  }
}
