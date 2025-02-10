// test/module/service.spec.ts

import { describe, it, expect, beforeAll, afterEach, afterAll, vi } from 'vitest';
import { HttpResponse, http } from 'msw';
import { setupServer } from 'msw/node';
import { dispatchWebhook } from '@/module/service';
import {
  getMockWebhook,
  getMockWebhookPayload,
  mockWebhookEndpoint
} from 'test/__mocks__/internal/webhook.mock';
import { getLogger } from '@/util/logger.util';

const logger = getLogger('webhook-dispatch');
const server = setupServer();

describe('dispatchWebhook', () => {
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

  it('should throw an error if network error occurs', async () => {
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
