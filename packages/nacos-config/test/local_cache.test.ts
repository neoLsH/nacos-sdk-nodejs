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

import { ClientWorker, ServerListManager, Snapshot } from '../src';
import { DataClient } from '../src/client';
import { HttpAgent } from '../src/http_agent';
import { GrpcConnection } from 'nacos-common';
import { createDefaultConfiguration } from './utils';
import * as path from 'path';
import * as mm from 'mm';
import * as assert from 'assert';

const fs = require('mz/fs');
const rawFs = require('fs');
const { mkdirp, rimraf, sleep } = require('mz-modules');

const cacheDir = path.join(__dirname, '.cache_local');

function createClient(): ClientWorker {
  const configuration = createDefaultConfiguration({
    serverAddr: '127.0.0.1:8848',
    namespace: '',
    cacheDir,
  });
  const snapshot = new Snapshot({ configuration });
  const serverMgr = new ServerListManager({ configuration });
  const httpAgent = new HttpAgent({ configuration });
  configuration.merge({ snapshot, serverMgr, httpAgent });
  return new ClientWorker({ configuration });
}

describe('test/local_cache.test.ts', () => {

  afterEach(async () => {
    mm.restore();
    await rimraf(cacheDir);
  });

  describe('getConfig with failover', () => {

    it('should return failover content without calling server', async () => {
      const client = createClient();
      const snapshotKey = (client as any).getSnapshotKeyEncoded('fo-data-id', 'fo-group');
      const failoverFile = (client.snapshot as any).getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'emergency-config');

      let serverCalled = false;
      mm(client.httpAgent, 'request', async () => {
        serverCalled = true;
        return 'server-config';
      });
      const content = await client.getConfig('fo-data-id', 'fo-group');
      assert(content === 'emergency-config');
      assert(serverCalled === false);
    });

    it('should fall back to server when failover file absent', async () => {
      const client = createClient();
      mm(client.httpAgent, 'request', async () => 'server-config');
      const content = await client.getConfig('no-fo-data-id', 'fo-group');
      assert(content === 'server-config');
    });

    it('should ignore non-file failover path (directory)', async () => {
      const client = createClient();
      const snapshotKey = (client as any).getSnapshotKeyEncoded('dir-data-id', 'fo-group');
      const failoverFile = (client.snapshot as any).getFailoverFile(snapshotKey);
      await mkdirp(failoverFile);

      const calls = { server: false };
      mm(client.httpAgent, 'request', async () => {
        calls.server = true;
        return 'server-config';
      });
      const content = await client.getConfig('dir-data-id', 'fo-group');
      assert(content === 'server-config');
      assert(calls.server === true);
    });
  });

  describe('snapshot lifecycle', () => {

    it('should delete snapshot when server responds 404', async () => {
      const client = createClient();
      const snapshotKey = (client as any).getSnapshotKeyEncoded('gone-data-id', 'fo-group');
      // 预置一份过期快照
      await client.snapshot.save(snapshotKey, 'stale-content');
      assert(await client.snapshot.get(snapshotKey) === 'stale-content');

      mm(client.httpAgent, 'request', async () => null);
      const content = await client.getConfig('gone-data-id', 'fo-group');
      assert(content === null);
      assert(await client.snapshot.get(snapshotKey) === null);
    });

    it('should delete snapshot on remove()', async () => {
      const client = createClient();
      const snapshotKey = (client as any).getSnapshotKeyEncoded('rm-data-id', 'fo-group');
      await client.snapshot.save(snapshotKey, 'to-be-removed');

      mm(client.httpAgent, 'request', async () => 'true');
      await client.remove('rm-data-id', 'fo-group');
      assert(await client.snapshot.get(snapshotKey) === null);
    });
  });

  describe('checkLocalFailover (subscription hot switch)', () => {

    function seedSubscription(client: ClientWorker, dataId: string, group: string) {
      const key = (client as any).formatKey({ dataId, group });
      (client as any).subscriptions.set(key, { dataId, group, md5: null, content: null });
      return key;
    }

    function nextEmit(client: ClientWorker, key: string): Promise<string> {
      return new Promise(resolve => client.once(key, resolve));
    }

    it('should switch to failover content when file is created', async () => {
      const client = createClient();
      const key = seedSubscription(client, 'hot-data-id', 'fo-group');
      const snapshotKey = (client as any).getSnapshotKeyEncoded('hot-data-id', 'fo-group');
      const failoverFile = (client.snapshot as any).getFailoverFile(snapshotKey);

      await (client as any).checkLocalFailover();
      const item = (client as any).subscriptions.get(key);
      assert(item.useFailover !== true);

      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      const emitted = nextEmit(client, key);
      await (client as any).checkLocalFailover();
      assert(await emitted === 'failover-v1');
      assert(item.useFailover === true);
      assert(item.content === 'failover-v1');
    });

    it('should reload failover content when file changes', async () => {
      const client = createClient();
      const key = seedSubscription(client, 'hot-data-id2', 'fo-group');
      const snapshotKey = (client as any).getSnapshotKeyEncoded('hot-data-id2', 'fo-group');
      const failoverFile = (client.snapshot as any).getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      await (client as any).checkLocalFailover();

      await fs.writeFile(failoverFile, 'failover-v2');
      // 强制 mtime 前进，避免毫秒级写入落在同一时刻
      const future = new Date(Date.now() + 5000);
      rawFs.utimesSync(failoverFile, future, future);

      const emitted = nextEmit(client, key);
      await (client as any).checkLocalFailover();
      assert(await emitted === 'failover-v2');
      const item = (client as any).subscriptions.get(key);
      assert(item.content === 'failover-v2');
    });

    it('should switch back to server mode when file is deleted', async () => {
      const client = createClient();
      const key = seedSubscription(client, 'hot-data-id3', 'fo-group');
      const snapshotKey = (client as any).getSnapshotKeyEncoded('hot-data-id3', 'fo-group');
      const failoverFile = (client.snapshot as any).getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      await (client as any).checkLocalFailover();
      const item = (client as any).subscriptions.get(key);
      assert(item.useFailover === true);

      await rimraf(failoverFile);
      await (client as any).checkLocalFailover();
      assert(item.useFailover === false);
      assert(item.failoverVersion === null);
    });

    it('should exclude failover-mode keys from server probe', async () => {
      const client = createClient();
      const failoverKey = seedSubscription(client, 'probe-skip-data-id', 'fo-group');
      seedSubscription(client, 'probe-keep-data-id', 'fo-group');
      // 通过真实 failover 文件进入 failover 模式
      const snapshotKey = (client as any).getSnapshotKeyEncoded('probe-skip-data-id', 'fo-group');
      const failoverFile = (client.snapshot as any).getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-content');

      let captured;
      mm(client.httpAgent, 'request', async (path, options) => {
        captured = options;
        return '';
      });
      await (client as any).checkServerConfigInfo();

      assert((client as any).subscriptions.get(failoverKey).useFailover === true);
      const probing = captured.data['Listening-Configs'];
      assert(probing.includes('probe-keep-data-id'));
      assert(!probing.includes('probe-skip-data-id'));
    });
  });

  describe('gRPC transport disaster recovery', () => {
    // gRPC 是默认传输（Nacos 3.x 唯一传输），构造 DataClient 会真实 connect()。
    // 这里 mock 掉 connect 及 proxy 的网络方法，做纯离线的本地缓存 / 容灾单测。
    const grpcClients: DataClient[] = [];

    function createGrpcClient(): DataClient {
      mm(GrpcConnection.prototype, 'connect', async () => {});
      const client = new DataClient({
        appName: 'test',
        serverAddr: '127.0.0.1:8848',
        namespace: '',
        cacheDir,
      } as any);
      // 服务端异常回退快照时会 emit 'error'，挂空监听避免未捕获错误中断测试
      client.on('error', () => {});
      grpcClients.push(client);
      return client;
    }

    // 直接种入订阅与容灾状态，绕开异步 subscribe 初始化，确定性地驱动热切换检查
    function seedGrpcSubscription(client: DataClient, dataId: string, group: string) {
      const key = `${dataId}@@${group}`;
      const received: string[] = [];
      const anyClient = client as any;
      if (!anyClient._grpcSubscribers) { anyClient._grpcSubscribers = new Map(); }
      anyClient._grpcSubscribers.set(key, [ (content: string) => { received.push(content); } ]);
      if (!anyClient._grpcFailoverState) { anyClient._grpcFailoverState = new Map(); }
      anyClient._grpcFailoverState.set(key, { dataId, group, useFailover: false, failoverVersion: null, content: null });
      return { key, received };
    }

    afterEach(() => {
      for (const client of grpcClients) {
        client.close();
      }
      grpcClients.length = 0;
    });

    it('should return failover content without calling server', async () => {
      const client = createGrpcClient();
      let serverCalled = false;
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => {
        serverCalled = true;
        return 'server-config';
      });
      const snapshotKey = (client as any)._getSnapshotKey('fo-data-id', 'fo-group');
      const failoverFile = (client as any).snapshot.getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'emergency-config');

      const content = await client.getConfig('fo-data-id', 'fo-group');
      assert(content === 'emergency-config');
      assert(serverCalled === false);
    });

    it('should save snapshot when server responds', async () => {
      const client = createGrpcClient();
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => 'server-config');
      const snapshotKey = (client as any)._getSnapshotKey('snap-data-id', 'fo-group');

      const content = await client.getConfig('snap-data-id', 'fo-group');
      assert(content === 'server-config');
      assert(await (client as any).snapshot.get(snapshotKey) === 'server-config');
    });

    it('should fall back to snapshot when server errors', async () => {
      const client = createGrpcClient();
      const snapshotKey = (client as any)._getSnapshotKey('fb-data-id', 'fo-group');
      await (client as any).snapshot.save(snapshotKey, 'cached-config');
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => {
        throw new Error('server down');
      });

      const content = await client.getConfig('fb-data-id', 'fo-group');
      assert(content === 'cached-config');
    });

    it('should throw when server errors and no snapshot exists', async () => {
      const client = createGrpcClient();
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => {
        throw new Error('server down');
      });

      let threw = false;
      try {
        await client.getConfig('nofb-data-id', 'fo-group');
      } catch (err) {
        threw = true;
      }
      assert(threw === true);
    });

    it('should not persist snapshot when server returns blank content', async () => {
      const client = createGrpcClient();
      const snapshotKey = (client as any)._getSnapshotKey('blank-data-id', 'fo-group');
      await (client as any).snapshot.save(snapshotKey, 'stale-content');
      assert(await (client as any).snapshot.get(snapshotKey) === 'stale-content');
      // gRPC getConfig 对缺失配置返回空串，空内容应按删除处理（对齐 Java saveSnapshot(null)）
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => '');

      const content = await client.getConfig('blank-data-id', 'fo-group');
      assert(content === '');
      assert(await (client as any).snapshot.get(snapshotKey) === null);
    });

    it('should delete snapshot on remove()', async () => {
      const client = createGrpcClient();
      const snapshotKey = (client as any)._getSnapshotKey('rm-data-id', 'fo-group');
      await (client as any).snapshot.save(snapshotKey, 'to-be-removed');
      mm((client as any)._grpcConfigProxy, 'remove', async () => true);

      await client.remove('rm-data-id', 'fo-group');
      assert(await (client as any).snapshot.get(snapshotKey) === null);
    });

    it('should switch to failover content when file is created', async () => {
      const client = createGrpcClient();
      const { key, received } = seedGrpcSubscription(client, 'hot-data-id', 'fo-group');
      const snapshotKey = (client as any)._getSnapshotKey('hot-data-id', 'fo-group');
      const failoverFile = (client as any).snapshot.getFailoverFile(snapshotKey);

      await (client as any)._checkGrpcLocalFailover();
      assert((client as any)._grpcFailoverState.get(key).useFailover === false);

      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      await (client as any)._checkGrpcLocalFailover();
      const state = (client as any)._grpcFailoverState.get(key);
      assert(state.useFailover === true);
      assert(state.content === 'failover-v1');
      assert(received.includes('failover-v1'));
    });

    it('should reload failover content when file changes', async () => {
      const client = createGrpcClient();
      const { key, received } = seedGrpcSubscription(client, 'hot-data-id2', 'fo-group');
      const snapshotKey = (client as any)._getSnapshotKey('hot-data-id2', 'fo-group');
      const failoverFile = (client as any).snapshot.getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      await (client as any)._checkGrpcLocalFailover();

      await fs.writeFile(failoverFile, 'failover-v2');
      const future = new Date(Date.now() + 5000);
      rawFs.utimesSync(failoverFile, future, future);
      await (client as any)._checkGrpcLocalFailover();
      const state = (client as any)._grpcFailoverState.get(key);
      assert(state.content === 'failover-v2');
      assert(received.includes('failover-v2'));
    });

    it('should switch back to server content when file is deleted', async () => {
      const client = createGrpcClient();
      const { key, received } = seedGrpcSubscription(client, 'hot-data-id3', 'fo-group');
      const snapshotKey = (client as any)._getSnapshotKey('hot-data-id3', 'fo-group');
      const failoverFile = (client as any).snapshot.getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      await (client as any)._checkGrpcLocalFailover();
      assert((client as any)._grpcFailoverState.get(key).useFailover === true);

      mm((client as any)._grpcConfigProxy, 'getConfig', async () => 'server-config');
      await rimraf(failoverFile);
      await (client as any)._checkGrpcLocalFailover();
      const state = (client as any)._grpcFailoverState.get(key);
      assert(state.useFailover === false);
      assert(state.failoverVersion === null);
      assert(state.content === 'server-config');
      assert(received.includes('server-config'));
    });

    it('should deliver server push when not in failover mode', async () => {
      const client = createGrpcClient();
      let serverContent = 'server-config';
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => serverContent);
      mm((client as any)._grpcConfigProxy, 'addListener', async () => {});
      const received: string[] = [];
      client.subscribe({ dataId: 'push-ok-data-id', group: 'fo-group' }, (content: string) => received.push(content));
      await sleep(100);

      serverContent = 'server-config-v2';
      (client as any)._grpcConfigProxy.emit('configChanged', { dataId: 'push-ok-data-id', group: 'fo-group', tenant: '' });
      await sleep(100);
      assert(received.includes('server-config-v2'));
    });

    it('should ignore server push while in failover mode', async () => {
      const client = createGrpcClient();
      let serverContent = 'server-config';
      mm((client as any)._grpcConfigProxy, 'getConfig', async () => serverContent);
      mm((client as any)._grpcConfigProxy, 'addListener', async () => {});
      const received: string[] = [];
      client.subscribe({ dataId: 'push-data-id', group: 'fo-group' }, (content: string) => received.push(content));
      await sleep(100);

      const snapshotKey = (client as any)._getSnapshotKey('push-data-id', 'fo-group');
      const failoverFile = (client as any).snapshot.getFailoverFile(snapshotKey);
      await mkdirp(path.dirname(failoverFile));
      await fs.writeFile(failoverFile, 'failover-v1');
      await (client as any)._checkGrpcLocalFailover();
      assert((client as any)._grpcFailoverState.get('push-data-id@@fo-group').useFailover === true);

      serverContent = 'server-config-v2';
      (client as any)._grpcConfigProxy.emit('configChanged', { dataId: 'push-data-id', group: 'fo-group', tenant: '' });
      await sleep(100);
      assert(!received.includes('server-config-v2'));
    });
  });
});
