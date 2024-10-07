import { Logger } from 'winston';
import { RedisClient } from '@/lib/redis';
import { PoolClient } from 'pg';
import { createHmac } from 'crypto';

const SIGNATURE_HASHING_ALGORITHM = 'sha256';
const SIGNATURE_BUFFER_ENCODING = 'utf-8';
const SIGNTURE_DIGEST = 'hex';

export function generateHmacSignature(stringToSign: string, signingKey: string): string {
  if (!stringToSign || !signingKey) {
    throw new Error(`Please provide string to sign and signing key`);
  }

  try {
    const buffer = Buffer.from(stringToSign, SIGNATURE_BUFFER_ENCODING);
    const signature = createHmac(SIGNATURE_HASHING_ALGORITHM, signingKey)
      .update(buffer)
      .digest(SIGNTURE_DIGEST);

    return signature;
  } catch (err: any) {
    throw new Error(`Failed to generate signature, ${err.message}`);
  }
}
