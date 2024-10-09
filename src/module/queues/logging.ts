import { JobsOptions, Queue } from 'bullmq';
import { connectionOptionsIo } from '@/lib/redis';

const WEBHOOK_LOGGING_QUEUE_NAME = 'webhook-logging';

const defaultQueueConfig = {
  streams: {
    events: {
      maxLen: 100
    }
  }
};

export enum WebhookLoggingJobName {
  WEBHOOK_LOGGING_WRITE = 'webhook:logging:write'
}

export const defaultJobConfig: JobsOptions = {
  removeOnComplete: true,
  removeOnFail: true
};

const webhookLoggingQueue = new Queue(WEBHOOK_LOGGING_QUEUE_NAME, {
  connection: connectionOptionsIo,
  prefix: 'queue',
  ...defaultQueueConfig
});

export default webhookLoggingQueue;
