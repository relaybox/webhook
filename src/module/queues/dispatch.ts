import { JobsOptions, Queue } from 'bullmq';
import { connectionOptionsIo } from '@/lib/redis';

export const WEBHOOK_DISPATCH_QUEUE_NAME = 'webhook-dispatch';
const RETRY_BACKOFF_RATE_MS = 500;
const RETRY_MAX_ATTEMPTS = 5;
const RETRY_BACKOFF_TYPE = 'exponential';

const defaultQueueConfig = {
  streams: {
    events: {
      maxLen: 100
    }
  }
};

export enum WebhookDispatchJobName {
  WEBHOOK_DISPATCH = 'webhook:dispatch'
}

export const defaultJobConfig: JobsOptions = {
  attempts: RETRY_MAX_ATTEMPTS,
  backoff: {
    type: RETRY_BACKOFF_TYPE,
    delay: RETRY_BACKOFF_RATE_MS
  },
  removeOnComplete: true,
  removeOnFail: true
};

const webhookDispatchQueue = new Queue(WEBHOOK_DISPATCH_QUEUE_NAME, {
  connection: connectionOptionsIo,
  prefix: 'queue',
  ...defaultQueueConfig
});

export default webhookDispatchQueue;
