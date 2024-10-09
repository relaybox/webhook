import { Queue } from 'bullmq';
import { connectionOptionsIo } from '@/lib/redis';

export const WEBHOOK_LOGGER_QUEUE_NAME = 'webhook-logger';
const WEBHOOK_LOGGER_CRON_JOB_ID = 'webhook:logger';
const CRON_SCHEDULE_MINS = process.env.CRON_SCHEDULE_MINS;

export const defaultJobConfig = {
  jobId: WEBHOOK_LOGGER_CRON_JOB_ID,
  repeat: {
    pattern: `0 */${CRON_SCHEDULE_MINS} * * * *`
  },
  removeOnComplete: true,
  removeOnFail: { count: 5 }
};

const webhookLoggerQueue = new Queue(WEBHOOK_LOGGER_QUEUE_NAME, {
  connection: connectionOptionsIo,
  prefix: 'queue'
});

export default webhookLoggerQueue;
