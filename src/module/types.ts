import { Worker } from 'bullmq';

export type ServiceWorker = Worker | null;

export enum WebhookEvent {
  ROOM_JOIN = 'room:join',
  ROOM_LEAVE = 'room:leave',
  ROOM_PUBLISH = 'room:publish'
}

export interface Webhook {
  id: string;
  event: WebhookEvent;
  signingKey: string;
  url: string;
  appId: string;
  appPid: string;
  webhookEventId: string;
}

export interface WebhookPayload {
  id: string;
  event: WebhookEvent;
  data: any;
  session: Session;
  filterAttributes?: Record<string, unknown>;
}

export interface WebhookResponse {
  id: string;
  status: number;
  statusText: string;
  timestamp: number;
}

export type webhookLogsDbEntry = (string | number)[];

export interface AuthUser {
  id: string;
  clientId: string;
  createdAt: string;
  updatedAt: string;
  username: string;
  orgId: string;
  isOnline: boolean;
  lastOnline: string;
  appId: string;
  blockedAt: string | null;
}

export interface Session {
  uid: string;
  appPid: string;
  keyId: string;
  clientId: string;
  exp: number;
  timestamp: string;
  connectionId: string;
  socketId: string;
  user?: AuthUser;
}

export interface LogStreamMessage {
  streamId: string;
  webhook: Webhook;
  webhookResponse: WebhookResponse;
}
