import { RedisClient } from '@/lib/redis';

export function addMessageToLogStream(
  redisClient: RedisClient,
  streamKey: string,
  messageData: Record<string, unknown>,
  id: string = '*'
): Promise<string> {
  const messageDataToString = JSON.stringify(messageData);
  return redisClient.xAdd(streamKey, id, { data: messageDataToString });
}

export async function acknowledgeLogStreamMessages(
  redisClient: RedisClient,
  streamKey: string,
  groupName: string,
  ids: string[]
): Promise<void> {
  await redisClient.xAck(streamKey, groupName, ids);
}
