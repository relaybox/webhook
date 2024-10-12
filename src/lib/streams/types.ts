import { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from 'redis';

export type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;
