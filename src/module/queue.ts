import { Queue } from 'bullmq';
import { connectionOptionsIo } from '@/lib/redis';

const WEBHOOK_DISPATCH_QUEUE_NAME = 'webhook-dispatch';
const FAILED_JOB_BACKOFF_RATE_MS = 200;

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

export const defaultJobConfig = {
  attempts: 10,
  backoff: {
    type: 'exponential',
    delay: FAILED_JOB_BACKOFF_RATE_MS
  },
  removeOnComplete: true,
  removeOnFail: true
};

console.log('init qeueu');

const webhookDispatchQueue = new Queue(WEBHOOK_DISPATCH_QUEUE_NAME, {
  connection: connectionOptionsIo,
  prefix: 'queue',
  ...defaultQueueConfig
});

export default webhookDispatchQueue;
