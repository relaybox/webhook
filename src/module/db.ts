import { PoolClient, QueryResult } from 'pg';
import { Logger } from 'winston';

export async function getRegisteredWebhooksByAppPid(
  pgClient: PoolClient,
  appPid: string
): Promise<QueryResult> {
  const query = `
    SELECT * FROM application_webhooks WHERE "appPid" = $1;
  `;

  return pgClient.query(query, [appPid]);
}
