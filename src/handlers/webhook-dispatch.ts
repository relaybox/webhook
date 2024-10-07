import { Pool } from 'pg';
import { RedisClient } from '@/lib/redis';
import { getLogger } from '@/util/logger.util';

const logger = getLogger('webhook-dispatch');

export async function handler(pgPool: Pool, redisClient: RedisClient, data: any): Promise<void> {
  try {
    console.log(1, data);
  } catch (err: any) {
    logger.error(`Failed to set session active`, { err });
    throw err;
  }
}
