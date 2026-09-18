/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as assert from 'assert';
import * as crypto from 'crypto';
import * as path from 'path';
import * as mm from 'mm';
import { ClientWorker, ServerListManager, Snapshot } from '../src';
import { HttpAgent } from '../src/http_agent';
import { ConfigCipher, IKmsClient } from '../src/cipher';
import { createDefaultConfiguration } from './utils';

const { rimraf } = require('mz-modules');

const cacheDir = path.join(__dirname, '.cache_cipher');

/**
 * In-memory stand-in for the Alibaba Cloud KMS client. Deterministic data keys keep the
 * tests hermetic (no network) while exercising the real envelope encrypt/decrypt flow.
 */
class FakeKmsClient implements IKmsClient {
  private keyStore = new Map<string, string>();
  generateDataKeyCalls: Array<{ keyId: string; keySpec: string }> = [];
  decryptCalls: string[] = [];

  async generateDataKey(keyId: string, keySpec: string): Promise<{ plaintext: string; ciphertextBlob: string }> {
    this.generateDataKeyCalls.push({ keyId, keySpec });
    const byteLength = keySpec === 'AES_256' ? 32 : 16;
    const plaintext = Buffer.alloc(byteLength, byteLength).toString('base64');
    const ciphertextBlob = `edk-${keySpec}-${this.generateDataKeyCalls.length}`;
    this.keyStore.set(ciphertextBlob, plaintext);
    return { plaintext, ciphertextBlob };
  }

  async decrypt(ciphertextBlob: string): Promise<string> {
    this.decryptCalls.push(ciphertextBlob);
    const plaintext = this.keyStore.get(ciphertextBlob);
    if (!plaintext) {
      throw new Error(`FakeKmsClient: unknown ciphertextBlob ${ciphertextBlob}`);
    }
    return plaintext;
  }
}

function createCipherClient(kmsClient: IKmsClient): ClientWorker {
  const configuration = createDefaultConfiguration({
    serverAddr: '127.0.0.1:8848',
    namespace: '',
    cacheDir,
    kmsClient,
  });
  const snapshot = new Snapshot({ configuration });
  const serverMgr = new ServerListManager({ configuration });
  const httpAgent = new HttpAgent({ configuration });
  configuration.merge({ snapshot, serverMgr, httpAgent });
  return new ClientWorker({ configuration });
}

// Independent AES/ECB/PKCS5 helper used to build/verify ciphertext outside of ConfigCipher.
function aesEcb(plaintext: string, base64Key: string, algorithm: string): string {
  const key = Buffer.from(base64Key, 'base64');
  const cipher = crypto.createCipheriv(algorithm, key, null);
  return Buffer.concat([ cipher.update(Buffer.from(plaintext, 'utf8')), cipher.final() ]).toString('base64');
}

function newCipher(config?: any, kmsClient?: IKmsClient): ConfigCipher {
  return new ConfigCipher(createDefaultConfiguration(config || {}), kmsClient);
}

describe('test/cipher.test.ts', () => {

  afterEach(async () => {
    mm.restore();
    await rimraf(cacheDir);
  });

  describe('ConfigCipher', () => {

    it('should detect cipher dataIds by prefix', () => {
      const cipher = newCipher();
      assert(cipher.isCipherDataId('cipher-kms-aes-128-x') === true);
      assert(cipher.isCipherDataId('cipher-kms-aes-256-x') === true);
      assert(cipher.isCipherDataId('cipher-custom-x') === true);
      assert(cipher.isCipherDataId('plain-x') === false);
      assert(cipher.isCipherDataId('') === false);
    });

    it('should encrypt then decrypt back to the original plaintext (AES-128)', async () => {
      const cipher = newCipher({}, new FakeKmsClient());
      const dataId = 'cipher-kms-aes-128-my-config';
      const plaintext = 'db.password=s3cr3t 你好';
      const encrypted = await cipher.encryptIfNeeded(dataId, plaintext);
      assert(encrypted.content !== plaintext);
      assert(typeof encrypted.encryptedDataKey === 'string' && encrypted.encryptedDataKey!.length > 0);
      const decrypted = await cipher.decryptIfNeeded(dataId, encrypted.content, encrypted.encryptedDataKey);
      assert(decrypted === plaintext);
    });

    it('should encrypt then decrypt back to the original plaintext (AES-256)', async () => {
      const cipher = newCipher({}, new FakeKmsClient());
      const dataId = 'cipher-kms-aes-256-my-config';
      const plaintext = 'a-longer-secret-value-to-span-multiple-aes-blocks-1234567890';
      const encrypted = await cipher.encryptIfNeeded(dataId, plaintext);
      const decrypted = await cipher.decryptIfNeeded(dataId, encrypted.content, encrypted.encryptedDataKey);
      assert(decrypted === plaintext);
    });

    it('should produce ciphertext byte-identical to a manual AES/ECB/PKCS5 pass', async () => {
      const cipher = newCipher({}, new FakeKmsClient());
      const plaintext = 'wire-format-check';
      const encrypted = await cipher.encryptIfNeeded('cipher-kms-aes-128-wire', plaintext);
      // FakeKmsClient returns a deterministic 16-byte key for AES_128; recompute independently.
      const expected = aesEcb(plaintext, Buffer.alloc(16, 16).toString('base64'), 'aes-128-ecb');
      assert(encrypted.content === expected);
    });

    it('should request the matching keySpec and default keyId from KMS', async () => {
      const kms = new FakeKmsClient();
      const cipher = newCipher({}, kms);
      await cipher.encryptIfNeeded('cipher-kms-aes-128-a', 'x');
      await cipher.encryptIfNeeded('cipher-kms-aes-256-b', 'y');
      assert(kms.generateDataKeyCalls.length === 2);
      assert(kms.generateDataKeyCalls[0].keySpec === 'AES_128');
      assert(kms.generateDataKeyCalls[1].keySpec === 'AES_256');
      assert(kms.generateDataKeyCalls[0].keyId === 'alias/acs/mse');
    });

    it('should honor a custom kmsKeyId', async () => {
      const kms = new FakeKmsClient();
      const cipher = newCipher({ kmsKeyId: 'alias/my-cmk' }, kms);
      await cipher.encryptIfNeeded('cipher-kms-aes-128-a', 'x');
      assert(kms.generateDataKeyCalls[0].keyId === 'alias/my-cmk');
    });

    it('should pass non-cipher dataIds through untouched without any KMS call', async () => {
      const kms = new FakeKmsClient();
      const cipher = newCipher({}, kms);
      const encrypted = await cipher.encryptIfNeeded('plain-data-id', 'secret=value');
      assert(encrypted.content === 'secret=value');
      assert(encrypted.encryptedDataKey === undefined);
      assert(kms.generateDataKeyCalls.length === 0);
      const decrypted = await cipher.decryptIfNeeded('plain-data-id', 'secret=value');
      assert(decrypted === 'secret=value');
    });

    it('should pass empty content through', async () => {
      const cipher = newCipher({}, new FakeKmsClient());
      const encrypted = await cipher.encryptIfNeeded('cipher-kms-aes-128-empty', '');
      assert(encrypted.content === '' && encrypted.encryptedDataKey === undefined);
      const decrypted = await cipher.decryptIfNeeded('cipher-kms-aes-128-empty', '', 'edk-x');
      assert(decrypted === '');
    });

    it('should return content as-is when encryptedDataKey is absent (plaintext failover)', async () => {
      const kms = new FakeKmsClient();
      const cipher = newCipher({}, kms);
      const decrypted = await cipher.decryptIfNeeded('cipher-kms-aes-128-fo', 'plain-failover-value');
      assert(decrypted === 'plain-failover-value');
      assert(kms.decryptCalls.length === 0);
    });

    it('should cache the plaintext data key so repeated reads skip KMS decrypt', async () => {
      const kms = new FakeKmsClient();
      const cipher = newCipher({}, kms);
      const dataId = 'cipher-kms-aes-128-cached';
      const encrypted = await cipher.encryptIfNeeded(dataId, 'v1');
      const first = await cipher.decryptIfNeeded(dataId, encrypted.content, encrypted.encryptedDataKey);
      const second = await cipher.decryptIfNeeded(dataId, encrypted.content, encrypted.encryptedDataKey);
      assert(first === 'v1' && second === 'v1');
      assert(kms.decryptCalls.length === 0, 'encrypt seeded the cache, so decrypt must not call KMS');
    });

    it('should call KMS decrypt once for an unknown data key then cache it', async () => {
      const kms = new FakeKmsClient();
      const dataId = 'cipher-kms-aes-256-remote';
      // Simulate content encrypted by another client: seed the fake KMS with a data key.
      const { plaintext, ciphertextBlob } = await kms.generateDataKey('alias/acs/mse', 'AES_256');
      const content = aesEcb('remote-value', plaintext, 'aes-256-ecb');
      const freshCipher = newCipher({}, kms);
      const first = await freshCipher.decryptIfNeeded(dataId, content, ciphertextBlob);
      const second = await freshCipher.decryptIfNeeded(dataId, content, ciphertextBlob);
      assert(first === 'remote-value' && second === 'remote-value');
      assert(kms.decryptCalls.length === 1, 'second read should hit the in-memory data-key cache');
    });

    it('should throw on an unsupported cipher dataId prefix', async () => {
      const cipher = newCipher({}, new FakeKmsClient());
      let threw = false;
      try {
        await cipher.encryptIfNeeded('cipher-unknown-algo-x', 'v');
      } catch (err) {
        threw = true;
      }
      assert(threw === true);
    });
  });

  describe('ClientWorker KMS integration (HTTP carriers)', () => {

    it('should decrypt a cipher config read from the server via the Encrypted-Data-Key header', async () => {
      const kms = new FakeKmsClient();
      const client = createCipherClient(kms);
      const dataId = 'cipher-kms-aes-128-http-get';
      const plaintext = 'db.password=s3cr3t';
      const { plaintext: dataKey, ciphertextBlob } = await kms.generateDataKey('alias/acs/mse', 'AES_128');
      const ciphertext = aesEcb(plaintext, dataKey, 'aes-128-ecb');

      let capturedOptions: any;
      mm(client.httpAgent, 'request', async (routePath, options) => {
        capturedOptions = options;
        return { data: ciphertext, headers: { 'encrypted-data-key': ciphertextBlob } };
      });

      const content = await client.getConfig(dataId, 'DEFAULT_GROUP');
      assert(content === plaintext);
      assert(capturedOptions.withHeaders === true, 'cipher dataIds must request response headers');
    });

    it('should read the header case-insensitively, persist ciphertext + edk, and decrypt from snapshot on failure', async () => {
      const kms = new FakeKmsClient();
      const client = createCipherClient(kms);
      const dataId = 'cipher-kms-aes-128-snap';
      const group = 'DEFAULT_GROUP';
      const plaintext = 'cached-secret=1';
      const { plaintext: dataKey, ciphertextBlob } = await kms.generateDataKey('alias/acs/mse', 'AES_128');
      const ciphertext = aesEcb(plaintext, dataKey, 'aes-128-ecb');

      mm(client.httpAgent, 'request', async () => {
        return { data: ciphertext, headers: { 'Encrypted-Data-Key': ciphertextBlob } };
      });
      assert(await client.getConfig(dataId, group) === plaintext);

      const snapshotKey = (client as any).getSnapshotKeyEncoded(dataId, group);
      const edkKey = (client as any).getEncryptedDataKeySnapshotKey(dataId, group);
      assert(await client.snapshot.get(snapshotKey) === ciphertext, 'snapshot must store ciphertext, never plaintext');
      assert(await client.snapshot.get(edkKey) === ciphertextBlob);

      // Server now fails → getConfigInner falls back to the snapshot and still decrypts.
      mm.restore();
      mm(client.httpAgent, 'request', async () => { throw new Error('server down'); });
      assert(await client.getConfig(dataId, group) === plaintext);
    });

    it('should keep the plaintext path returning the raw body without requesting headers', async () => {
      const client = createCipherClient(new FakeKmsClient());
      let capturedOptions: any;
      mm(client.httpAgent, 'request', async (routePath, options) => {
        capturedOptions = options;
        return 'plain-body';
      });
      const content = await client.getConfig('plain-data-id', 'DEFAULT_GROUP');
      assert(content === 'plain-body');
      assert(capturedOptions.withHeaders === false, 'non-cipher reads must not change existing behavior');
    });

    it('should encrypt on publish and carry encryptedDataKey as a form param', async () => {
      const kms = new FakeKmsClient();
      const client = createCipherClient(kms);
      const dataId = 'cipher-kms-aes-256-http-pub';
      let capturedOptions: any;
      mm(client.httpAgent, 'request', async (routePath, options) => {
        capturedOptions = options;
        return 'true';
      });

      await client.publishSingle(dataId, 'DEFAULT_GROUP', 'token=abc');
      const sent = capturedOptions.data;
      assert(sent.content !== 'token=abc', 'published content must be ciphertext');
      assert(typeof sent.encryptedDataKey === 'string' && sent.encryptedDataKey.length > 0);
      // The carried data key must decrypt the published ciphertext back to the original.
      const key = Buffer.from(await kms.decrypt(sent.encryptedDataKey), 'base64');
      const decipher = crypto.createDecipheriv('aes-256-ecb', key, null);
      const plain = Buffer.concat([ decipher.update(Buffer.from(sent.content, 'base64')), decipher.final() ]).toString('utf8');
      assert(plain === 'token=abc');
    });

    it('should publish plaintext unchanged for non-cipher dataIds', async () => {
      const kms = new FakeKmsClient();
      const client = createCipherClient(kms);
      let capturedOptions: any;
      mm(client.httpAgent, 'request', async (routePath, options) => {
        capturedOptions = options;
        return 'true';
      });
      await client.publishSingle('plain-id', 'DEFAULT_GROUP', 'a=b');
      assert(capturedOptions.data.content === 'a=b');
      assert(capturedOptions.data.encryptedDataKey === undefined);
      assert(kms.generateDataKeyCalls.length === 0);
    });
  });
});
