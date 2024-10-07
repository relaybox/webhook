export interface ReducedSession {
  appPid: string;
  keyId: string;
  uid: string;
  clientId: string;
  connectionId: string;
  socketId: string;
  requestId?: string;
}

export interface LatencyLog {
  createdAt: string;
  receivedAt: string;
}
