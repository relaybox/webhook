import { PoolClient, QueryResult } from 'pg';
import { Webhook, WebhookEvent, WebhookResponse } from './types';

export function getWebhooksByAppAndEvent(
  pgClient: PoolClient,
  appPid: string,
  event: WebhookEvent
): Promise<QueryResult> {
  const query = `
    SELECT aw."appId", aw."appPid", aw.id, aw.url, aw."signingKey", aw.headers, awe."webhookEventId"
    FROM webhook_events we
    INNER JOIN application_webhook_events awe
    ON we.id = awe."webhookEventId" AND awe."appPid" = $1 
    INNER JOIN application_webhooks aw 
    ON awe."webhookId" = aw.id AND aw.enabled IS TRUE AND aw."deletedAt" IS NULL
    WHERE we.type = $2;
  `;

  return pgClient.query(query, [appPid, event]);
}

export function logWebhookEvent(
  pgClient: PoolClient,
  appId: string,
  appPid: string,
  webhookId: string,
  webhookRequestId: string,
  status: number,
  statusText: string
): Promise<QueryResult> {
  const now = new Date().toISOString();

  const query = `
    INSERT INTO application_webhook_logs (
      "appId", "appPid", "webhookId", "webhookRequestId", status, "statusText", "createdAt"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7);
  `;

  return pgClient.query(query, [
    appId,
    appPid,
    webhookId,
    webhookRequestId,
    status,
    statusText,
    now
  ]);
}

export function insertWebhookLogsDbEntry(
  pgClient: PoolClient,
  webhook: Webhook,
  webhookResponse: WebhookResponse
): Promise<QueryResult> {
  const query = `
    INSERT INTO application_webhook_logs (
      "appId", "appPid", "webhookId", "webhookRequestId", status, "statusText", "createdAt"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7);
  `;

  return pgClient.query(query, [
    webhook.appId,
    webhook.appPid,
    webhook.id,
    webhookResponse.id,
    webhookResponse.status,
    webhookResponse.statusText,
    new Date(webhookResponse.timestamp).toISOString()
  ]);
}

export function bulkInsertWebhookLogs(
  pgClient: PoolClient,
  queryPlaceholders: string[],
  values: (string | number)[]
): Promise<QueryResult> {
  const query = `
    INSERT INTO application_webhook_logs (
      "appId", "appPid", "webhookId", "webhookEventId", "webhookRequestId", status, "statusText", "createdAt"
    )
    VALUES ${queryPlaceholders} 
    ON CONFLICT 
    DO NOTHING;
  `;

  return pgClient.query(query, values);
}
