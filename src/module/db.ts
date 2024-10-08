import { PoolClient, QueryResult } from 'pg';
import { WebhookEvent } from './types';

export async function getWebhooksByAppAndEvent(
  pgClient: PoolClient,
  appPid: string,
  event: WebhookEvent
): Promise<QueryResult> {
  const query = `
    SELECT aw.url, aw."signingKey" FROM webhook_events we
    INNER JOIN application_webhooks aw
    ON we.id = aw."webhookEventId" AND aw."appPid" = $1
    WHERE we.type = $2;
  `;

  return pgClient.query(query, [appPid, event]);
}
