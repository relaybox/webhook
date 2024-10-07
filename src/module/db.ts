import { PoolClient, QueryResult } from 'pg';
import { WebhookEvent } from './types';

export async function getRegisteredWebhooksByEvent(
  pgClient: PoolClient,
  appPid: string,
  event: WebhookEvent
): Promise<QueryResult> {
  const query = `
    SELECT * FROM application_webhooks aw
    LEFT JOIN application_webhooks_filters awf ON aw.id = awf."webhookId"
    WHERE "appPid" = $1 
    AND event = $2;
  `;

  return pgClient.query(query, [appPid, event]);
}
