import { Session, Webhook, WebhookEvent, WebhookPayload, WebhookResponse } from '@/module/types';
import { v4 as uuid } from 'uuid';

export const mockWebhookEndpoint = 'http://localhost:4000/webhook/event';

export function getMockWebhook(event?: WebhookEvent): Webhook {
  const id = uuid();
  const appId = uuid();
  const webhookEventId = uuid();
  const appPid = Math.random().toString(36).slice(-12);

  return {
    id,
    event: event || WebhookEvent.ROOM_JOIN,
    signingKey: '123',
    url: mockWebhookEndpoint,
    appId,
    appPid,
    webhookEventId
  };
}

export function getMockWebhookPayload(
  id?: string,
  event?: WebhookEvent,
  data: any = { test: true }
): WebhookPayload {
  const session = {} as Session;

  return {
    id: id || uuid(),
    event: event || WebhookEvent.ROOM_JOIN,
    data,
    session
  };
}

export function getMockWebhookResponse(
  id?: string,
  status?: number,
  statusText?: string
): WebhookResponse {
  return {
    id: id || uuid(),
    status: status || 200,
    statusText: statusText || 'OK',
    timestamp: Date.now()
  };
}
