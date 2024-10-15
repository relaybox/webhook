import { mockQueue } from 'test/__mocks__/external/bullmq';
import {
  getMockWebhook,
  getMockWebhookPayload,
  getMockWebhookResponse,
  mockWebhookEndpoint
} from 'test/__mocks__/internal/webhook.mock';
import { describe, expect, vi, it, afterEach, beforeAll, afterAll } from 'vitest';
import { WebhookDispatchJobName } from '@/module/queues/dispatch';
import {
  bulkInsertWebhookLogs,
  dispatchWebhook,
  enqueueRegisteredWebhooks,
  generateRequestSignature,
  parseStreamConsumerMessage,
  parseWebhookHeaders,
  parseWebhookLogsDbEntries
} from '@/module/service';
import { getLogger } from '@/util/logger.util';
import { setupServer } from 'msw/node';
import { HttpResponse, http } from 'msw';
import { PoolClient } from 'pg';

const logger = getLogger('webhook-service');

const server = setupServer();

// const mockRepository = vi.hoisted(() => ({
//   getCachedRooms: vi.fn()
// }));

// vi.mock('@/module/repository', () => mockRepository);

const mockDb = vi.hoisted(() => ({
  bulkInsertWebhookLogs: vi.fn()
}));

vi.mock('@/module/db', () => mockDb);

describe('service', () => {
  afterEach(() => {
    vi.clearAllMocks();
    vi.resetAllMocks();
  });

  describe('generateRequestSignature', () => {
    it('should generate a valid 256 bit request signature', () => {
      const stringToSign = JSON.stringify({ test: true });
      const signingKey = '12345';

      const signatureOne = generateRequestSignature(stringToSign, signingKey);
      const signatureTwo = generateRequestSignature(stringToSign, signingKey);

      expect(signatureOne).toHaveLength(64);
      expect(signatureOne).toEqual(signatureTwo);
    });
  });

  describe('enqueueRegisteredWebhooks', async () => {
    it('should enqueue webhooks with associated payloads', async () => {
      const mockWebhookOne = getMockWebhook();
      const mockWebhookTwo = getMockWebhook();
      const mockPayload = getMockWebhookPayload();

      await enqueueRegisteredWebhooks(logger, [mockWebhookOne, mockWebhookTwo], mockPayload);

      expect(mockQueue.add).toHaveBeenCalledTimes(2);
      expect(mockQueue.add).toHaveBeenCalledWith(
        WebhookDispatchJobName.WEBHOOK_DISPATCH,
        expect.objectContaining({
          webhook: mockWebhookOne || mockWebhookTwo,
          payload: mockPayload
        }),
        expect.any(Object)
      );
    });
  });

  describe('parseWebhookHeaders', () => {
    it('should parse webhook headers successfully', () => {
      const mockHeaders = [
        {
          keyName: 'X-Test-Header-One',
          value: 'test-value'
        },
        {
          keyName: 'X-Test-Header-Two',
          value: 'test-value'
        }
      ];

      const parsedHeaders = parseWebhookHeaders(logger, mockHeaders);
      expect(parsedHeaders).toEqual({
        'X-Test-Header-One': 'test-value',
        'X-Test-Header-Two': 'test-value'
      });
    });
  });

  describe('parseStreamConsumerMessage', () => {
    it('should parse a stream consumer message successfully', () => {
      const mockMessage = {
        id: '123',
        message: {
          data: JSON.stringify({
            test: true
          })
        }
      };

      const parsedMessage = parseStreamConsumerMessage(logger, mockMessage);

      expect(parsedMessage).toEqual({
        streamId: '123',
        test: true
      });
    });
  });

  describe('parseWebhookLogsDbEntries', () => {
    it('should parse webhook logs db entries successfully', () => {
      const webhook = getMockWebhook();
      const webhookResponse = getMockWebhookResponse();

      const logStreamMessages = [
        {
          streamId: '123',
          webhook,
          webhookResponse
        },
        {
          streamId: '456',
          webhook,
          webhookResponse
        }
      ];

      const parsedWebhookLogsDbEntries = parseWebhookLogsDbEntries(logger, logStreamMessages);

      expect(parsedWebhookLogsDbEntries).toHaveLength(2);
      expect(parsedWebhookLogsDbEntries[0]).toEqual([
        webhook.appId,
        webhook.appPid,
        webhook.id,
        webhook.webhookEventId,
        webhookResponse.id,
        webhookResponse.status,
        webhookResponse.statusText,
        new Date(webhookResponse.timestamp).toISOString()
      ]);
    });
  });

  describe('bulkInsertWebhookLogs', () => {
    it('should bulk insert webhook logs successfully', async () => {
      const webhook = getMockWebhook();
      const webhookResponse = getMockWebhookResponse();

      const logStreamMessages = [
        {
          streamId: '123',
          webhook,
          webhookResponse
        }
      ];

      const parsedWebhookLogsDbEntries = parseWebhookLogsDbEntries(logger, logStreamMessages);

      await bulkInsertWebhookLogs(logger, {} as PoolClient, parsedWebhookLogsDbEntries);

      expect(mockDb.bulkInsertWebhookLogs).toHaveBeenCalledTimes(1);
    });
  });

  describe('dispatchWebhook', async () => {
    beforeAll(() => server.listen());

    afterEach(() => server.resetHandlers());

    afterAll(() => {
      server.close();
      vi.restoreAllMocks();
    });

    it('should dispatch a webhook successfully', async () => {
      server.use(
        http.post(mockWebhookEndpoint, () =>
          HttpResponse.json({
            status: 200
          })
        )
      );

      const webhook = getMockWebhook();
      const payload = getMockWebhookPayload();
      const response = await dispatchWebhook(logger, webhook, payload);

      expect(response).toEqual(
        expect.objectContaining({
          id: payload.id,
          status: 200,
          timestamp: expect.any(Number)
        })
      );
    });

    it('should return standard webhook response from non-retryable error', async () => {
      server.use(
        http.post(mockWebhookEndpoint, () => {
          return new HttpResponse(null, {
            status: 404
          });
        })
      );

      const webhook = getMockWebhook();
      const payload = getMockWebhookPayload();
      const response = await dispatchWebhook(logger, webhook, payload);

      expect(response).toEqual(
        expect.objectContaining({
          id: payload.id,
          status: 404,
          statusText: 'Not Found',
          timestamp: expect.any(Number)
        })
      );
    });

    it('should throw an error if retryable error occurs (causing retry)', async () => {
      server.use(
        http.post(mockWebhookEndpoint, () => {
          return new HttpResponse(null, {
            status: 500
          });
        })
      );

      const webhook = getMockWebhook();
      const payload = getMockWebhookPayload();

      await expect(() => dispatchWebhook(logger, webhook, payload)).rejects.toThrow(
        /Retryable HTTP error/
      );
    });

    it('should throw an error if retryable error occurs (causing retry)', async () => {
      server.use(
        http.post(mockWebhookEndpoint, () => {
          return HttpResponse.error();
        })
      );

      const webhook = getMockWebhook();
      const payload = getMockWebhookPayload();

      await expect(() => dispatchWebhook(logger, webhook, payload)).rejects.toThrow(
        /Failed to fetch/
      );
    });
  });
});
