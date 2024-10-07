export enum WebhookEvent {
  ROOM_JOIN = 'room:join',
  ROOM_LEAVE = 'room:leave',
  ROOM_PUBLISH = 'room:publish'
}

export interface WebhookPayload {
  id: string;
  event: WebhookEvent;
  data: any;
  session: Session;
  filterAttributes?: Record<string, unknown>;
}

export interface RegisteredWebhook {
  id: string;
  event: WebhookEvent;
  signingKey: string;
  url: string;
}

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
