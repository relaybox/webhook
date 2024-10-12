import { JobsOptions, Queue } from 'bullmq';
import { connectionOptionsIo } from '@/lib/redis';

export const WEBHOOK_PERSIST_QUEUE_NAME = 'webhook-persist';

const WEBHOOK_PERSIST_QUEUE_PREFIX = 'queue';
const RETRY_BACKOFF_RATE_MS = 500;
const RETRY_MAX_ATTEMPTS = 1;
const RETRY_BACKOFF_TYPE = 'exponential';

const defaultQueueConfig = {
  streams: {
    events: {
      maxLen: 100
    }
  }
};

export enum WebhookPersistJobName {
  WEBHOOK_PERSIST = 'webhook:persist'
}

export const defaultJobConfig: JobsOptions = {
  attempts: RETRY_MAX_ATTEMPTS,
  backoff: {
    type: RETRY_BACKOFF_TYPE,
    delay: RETRY_BACKOFF_RATE_MS
  },
  removeOnComplete: true,
  removeOnFail: false
};

const webhookPersistQueue = new Queue(WEBHOOK_PERSIST_QUEUE_NAME, {
  connection: connectionOptionsIo,
  prefix: WEBHOOK_PERSIST_QUEUE_PREFIX,
  ...defaultQueueConfig
});

export default webhookPersistQueue;
