import { PoolClient, QueryResult } from 'pg';
import { WebhookEvent } from './types';

export function getWebhooksByAppAndEvent(
  pgClient: PoolClient,
  appPid: string,
  event: WebhookEvent
): Promise<QueryResult> {
  const query = `
    SELECT aw."appId", aw."appPid", aw.id, aw.url, aw."signingKey" 
    FROM webhook_events we
    INNER JOIN application_webhooks aw
    ON we.id = aw."webhookEventId" AND aw."appPid" = $1
    WHERE we.type = $2;
  `;

  return pgClient.query(query, [appPid, event]);
}

export function logWebhookEvent(
  pgClient: PoolClient,
  appId: string,
  appPid: string,
  webhookId: string,
  status: number,
  statusText: string
): Promise<QueryResult> {
  const now = new Date().toISOString();

  const query = `
    INSERT INTO application_webhook_logs (
      "appId", "appPid", "webhookId", status, "statusText", "createdAt"
    )
    VALUES ($1, $2, $3, $4, $5, $6);
  `;

  return pgClient.query(query, [appId, appPid, webhookId, status, statusText, now]);
}
