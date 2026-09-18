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
import {
  BaseClient,
  ClientOptionKeys,
  ClientOptions,
  IClientWorker,
  IConfiguration,
  IServerListManager,
  ISnapshot,
  UnitOptions,
} from './interface';
import { ServerListManager } from './server_list_mgr';
import { ClientWorker } from './client_worker';
import { Snapshot } from './snapshot';
import { CURRENT_UNIT, DEFAULT_OPTIONS } from './const';
import { checkParameters } from './utils';
import { HttpAgent } from './http_agent';
import { Configuration } from './configuration';
import { GrpcConfigProxy, ConfigQueryResult } from './grpc_config_proxy';
import { ConfigCipher, createConfigCipher } from './cipher';
import { readConfigWithFailover } from './disaster_recovery';
import { GrpcConnection, GrpcTransportClient } from 'nacos-common';
import * as assert from 'assert';
import * as path from 'path';

const Base = require('sdk-base');

/** gRPC 模式下本地容灾文件的轮询检查间隔（毫秒）。 */
const FAILOVER_CHECK_INTERVAL = 10000;

/** gRPC 订阅项的本地容灾状态（对齐 Java SDK CacheData 的 isUseLocalConfigInfo / localConfigLastModified）。 */
interface GrpcFailoverState {
  dataId: string;
  group: string;
  useFailover: boolean;
  failoverVersion: number | null;
  content: string | null;
}

export class DataClient extends Base implements BaseClient {

  private clients: Map<string, IClientWorker>;
  private configuration: IConfiguration;
  private cipher: ConfigCipher;
  protected snapshot: ISnapshot;
  protected serverMgr: IServerListManager | null;
  protected httpAgent;
  private _transport: 'grpc' | 'http';
  private _grpcConnection: GrpcConnection | null;
  private _grpcTransportClient: GrpcTransportClient | null;
  private _grpcConfigProxy: GrpcConfigProxy | null;
  private _grpcSubscribers: Map<string, Function[]> | null;
  private _grpcFailoverState: Map<string, GrpcFailoverState> | null;
  private _failoverWatcher: any;

  constructor(options: ClientOptions) {
    if(!options.endpoint && !options.serverAddr) {
      assert(options.endpoint, '[Client] options.endpoint or options.serverAddr is required');
    }

    options = Object.assign({}, DEFAULT_OPTIONS, options);
    super(options);
    this.configuration = this.options.configuration = new Configuration(options);
    this._transport = (options.transport === 'http') ? 'http' : 'grpc';
    this._grpcConnection = null;
    this._grpcTransportClient = null;
    this._grpcConfigProxy = null;
    this._grpcSubscribers = null;
    this._grpcFailoverState = null;
    this._failoverWatcher = null;

    this.snapshot = this.getSnapshot();
    (<any>this.snapshot).on('error', err => this.throwError(err));

    if (this._transport === 'grpc') {
      // gRPC mode: skip ServerListManager and HttpAgent; set up gRPC stack
      this.serverMgr = null;
      this.httpAgent = null;
      // gRPC 模式下 KMS 信封加解密由 DataClient 直接负责（HTTP 模式在 ClientWorker 内部完成）
      this.cipher = createConfigCipher(this.configuration);

      this.configuration.merge({
        snapshot: this.snapshot,
      });

      // Normalise serverAddr to a string array
      const rawAddr = options.serverAddr;
      let serverList: string[];
      if (Array.isArray(rawAddr)) {
        serverList = rawAddr;
      } else if (rawAddr) {
        serverList = [ rawAddr ];
      } else {
        serverList = [ `${options.endpoint || 'localhost'}:8848` ];
      }

      const logger = (this.options as any).logger || console;
      this._grpcConnection = new GrpcConnection({
        serverList,
        namespace: options.namespace || 'public',
        ssl: options.ssl,
        logger,
        accessKey: options.accessKey,
        secretKey: options.secretKey,
        username: options.username,
        password: options.password,
        labels: { source: 'sdk', module: 'config' },
      });

      this._grpcTransportClient = new GrpcTransportClient(this._grpcConnection);

      this._grpcConfigProxy = new GrpcConfigProxy({
        transportClient: this._grpcTransportClient,
        namespace: options.namespace,
        logger,
      });

      this._grpcConfigProxy.on('configChanged', ({ dataId, group, tenant }) => {
        // Emit so subscribers can be notified
        this.emit('configChanged', { dataId, group, tenant });
      });
    } else {
      // HTTP mode: original initialization
      this.serverMgr = this.getServerListManager();
      (<any>this.serverMgr).on('error', err => this.throwError(err));

      const CustomHttpAgent = this.configuration.get(ClientOptionKeys.HTTP_AGENT);
      this.httpAgent = CustomHttpAgent ? new CustomHttpAgent({ configuration: this.configuration }) : new HttpAgent({ configuration: this.configuration });

      this.configuration.merge({
        snapshot: this.snapshot,
        serverMgr: this.serverMgr,
        httpAgent: this.httpAgent,
      });
    }

    this.clients = new Map();

    if (this._transport === 'grpc') {
      this._grpcConnection!.connect().then(() => {
        this.ready(true);
      }).catch(err => {
        this.throwError(err);
        this.ready(true);
      });
    } else {
      this.ready(true);
    }
  }

  get appName() {
    return this.configuration.get(ClientOptionKeys.APPNAME);
  }

  get httpclient() {
    return this.configuration.get(ClientOptionKeys.HTTPCLIENT);
  }

  /**
   * 获取当前机器所在机房
   * @return {String} currentUnit
   */
  async getCurrentUnit() {
    if (!this.serverMgr) {
      return 'gRPC';
    }
    return await this.serverMgr.getCurrentUnit();
  }

  /**
   * 获取所有单元信息
   * @return {Array} units
   */
  async getAllUnits() {
    if (!this.serverMgr) {
      return [];
    }
    return await this.serverMgr.fetchUnitLists();
  }

  /**
   * 订阅
   * @param {Object} info
   *   - {String} dataId - id of the data you want to subscribe
   *   - {String} [group] - group name of the data
   *   - {String} [unit] - which unit you want to connect, default is current unit
   * @param {Function} listener - listener
   * @return {DataClient} self
   */
  subscribe(info, listener) {
    const { dataId, group } = info;
    checkParameters(dataId, group);

    if (this._grpcConfigProxy) {
      const key = `${dataId}@@${group}`;
      if (!this._grpcSubscribers) {
        this._grpcSubscribers = new Map();
        this._grpcConfigProxy.on('configChanged', async (evt) => {
          const evtKey = `${evt.dataId}@@${evt.group}`;
          const listeners = this._grpcSubscribers!.get(evtKey);
          if (!listeners || listeners.length === 0) {
            return;
          }
          // 处于本地容灾模式的 key 忽略服务端变更推送（对齐 Java：local config wins）
          const state = this._grpcFailoverState!.get(evtKey);
          if (state && state.useFailover) {
            return;
          }
          try {
            // 带容灾读取：成功落快照，服务端异常回退快照
            const { content, encryptedDataKey } = await this._getConfigWithCache(evt.dataId, evt.group);
            // 用户边界：变更通知的密文解密后再回调监听器
            const plainContent = await this.cipher.decryptIfNeeded(evt.dataId, content, encryptedDataKey);
            if (state) {
              state.content = plainContent;
            }
            for (const fn of listeners) { fn(plainContent); }
          } catch (err) {
            this.throwError(err);
          }
        });
      }
      const listeners = this._grpcSubscribers.get(key) || [];
      listeners.push(listener);
      this._grpcSubscribers.set(key, listeners);
      // 登记本地容灾状态并启动 failover 文件热切换轮询（对齐 Java checkLocalConfig）
      if (!this._grpcFailoverState) {
        this._grpcFailoverState = new Map();
      }
      if (!this._grpcFailoverState.has(key)) {
        this._grpcFailoverState.set(key, { dataId, group, useFailover: false, failoverVersion: null, content: null });
      }
      this._startFailoverWatcher();
      // 带容灾拉取当前内容（failover > 服务端 > 快照）：回调监听器并注册 gRPC 监听
      this._getConfigWithCache(dataId, group).then(async ({ content, encryptedDataKey }) => {
        // 用户边界：初始内容解密后再回调监听器
        const plainContent = await this.cipher.decryptIfNeeded(dataId, content, encryptedDataKey);
        if (plainContent) listener(plainContent);
        const state = this._grpcFailoverState!.get(key);
        if (state) {
          state.content = plainContent;
          // 存在容灾文件则立即进入 failover 模式，否则等下一轮 watcher 探测
          const mtime = await this.snapshot.getFailoverMtime(this._getSnapshotKey(dataId, group));
          if (mtime !== null) {
            state.useFailover = true;
            state.failoverVersion = mtime;
          }
        }
        // md5 基于服务端原文（可能是密文）计算，与服务端监听探针保持一致，避免误判变更
        const crypto = require('crypto');
        const md5 = content ? crypto.createHash('md5').update(content).digest('hex') : '';
        this._grpcConfigProxy!.addListener(dataId, group, md5).catch(() => {});
      }).catch(() => {});
      return this;
    }

    const client = this.getClient(info);
    client.subscribe({ dataId, group }, listener);
    return this;
  }

  unSubscribe(info, listener) {
    const { dataId, group } = info;
    checkParameters(dataId, group);

    if (this._grpcConfigProxy) {
      const key = `${dataId}@@${group}`;
      if (this._grpcSubscribers) {
        if (listener) {
          const listeners = this._grpcSubscribers.get(key) || [];
          const idx = listeners.indexOf(listener);
          if (idx >= 0) listeners.splice(idx, 1);
          if (listeners.length === 0) {
            this._grpcSubscribers.delete(key);
            if (this._grpcFailoverState) { this._grpcFailoverState.delete(key); }
            this._grpcConfigProxy.removeListener(dataId, group).catch(() => {});
          }
        } else {
          this._grpcSubscribers.delete(key);
          if (this._grpcFailoverState) { this._grpcFailoverState.delete(key); }
          this._grpcConfigProxy.removeListener(dataId, group).catch(() => {});
        }
        // 没有订阅项后停止 failover 文件轮询
        if (this._grpcSubscribers.size === 0) {
          this._stopFailoverWatcher();
        }
      }
      return this;
    }

    const client = this.getClient(info);
    client.unSubscribe({ dataId, group }, listener);
    return this;
  }

  /**
   * 获取配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {String} value
   */
  async getConfig(dataId, group, options?) {
    checkParameters(dataId, group);
    if (this._grpcConfigProxy) {
      // 带本地容灾读取：failover > 服务端 > 快照（对齐 Java SDK 读优先级）
      const { content, encryptedDataKey } = await this._getConfigWithCache(dataId, group, this.configuration.get(ClientOptionKeys.NAMESPACE));
      // 用户边界：cipher dataId 解密后返回明文
      return await this.cipher.decryptIfNeeded(dataId, content, encryptedDataKey);
    }
    const client = this.getClient(options);
    return await client.getConfig(dataId, group);
  }

  /**
   * 查询租户下的所有的配置
   * @return {Array} config
   */
  async getConfigs() {
    const client = this.getClient();
    return await client.getConfigs();
  }


  /**
   * 发布配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {String} content - config value
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   *   - {String} type - config type, e.g., 'text', 'json', 'xml', 'html', 'properties', 'yaml', etc.
   * @return {Boolean} success
   */
  async publishSingle(dataId, group, content, options?: UnitOptions) {
    checkParameters(dataId, group);
    if (this._grpcConfigProxy) {
      // 用户边界：cipher dataId 先加密，密文与 encryptedDataKey 经 additionMap 发布
      const encryptResult = await this.cipher.encryptIfNeeded(dataId, content);
      return await this._grpcConfigProxy.publishSingle(
        dataId, group,
        this.configuration.get(ClientOptionKeys.NAMESPACE),
        encryptResult.content,
        options && options.type,
        encryptResult.encryptedDataKey
      );
    }
    const client = this.getClient(options);
    return await client.publishSingle(dataId, group, content, options);
  }

  /**
   * 删除配置
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Boolean} success
   */
  async remove(dataId, group, options?) {
    checkParameters(dataId, group);
    if (this._grpcConfigProxy) {
      const removed = await this._grpcConfigProxy.remove(dataId, group, this.configuration.get(ClientOptionKeys.NAMESPACE));
      // 同步清理本地快照与 encryptedDataKey 缓存，避免服务端已删除的配置残留在缓存里（与 HTTP 模式一致）
      await this.snapshot.delete(this._getSnapshotKey(dataId, group));
      await this.snapshot.delete(this._getEncryptedDataKeySnapshotKey(dataId, group));
      return removed;
    }
    const client = this.getClient(options);
    return await client.remove(dataId, group);
  }

  /**
   * @deprecated This API is not implemented and will be removed in a future version.
   * Nacos server does not support batch configuration retrieval operations.
   * Please use individual getConfig() calls instead.
   * 批量获取配置
   * @param {Array} dataIds - data id array
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Array} result
   */
  async batchGetConfig(dataIds, group, options) {
    checkParameters(dataIds, group);
    const client = this.getClient(options);
    return await client.batchGetConfig(dataIds, group);
  }

  /**
   * @deprecated This API is not implemented and will be removed in a future version.
   * Nacos server does not support batch query operations.
   * Please use individual query methods instead.
   * 批量查询
   * @param {Array} dataIds - data id array
   * @param {String} group - group name of the data
   * @param {Object} options
   *   - {Stirng} unit - which unit you want to connect, default is current unit
   * @return {Object} result
   */
  async batchQuery(dataIds, group, options) {
    checkParameters(dataIds, group);
    const client = this.getClient(options);
    return await client.batchQuery(dataIds, group);
  }

  /**
   * 将配置发布到所有单元
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @param {String} content - config value
   * @return {Boolean} success
   */
  async publishToAllUnit(dataId, group, content) {
    checkParameters(dataId, group);
    const units = await this.getAllUnits();
    await units.map(unit => this.getClient({ unit }).publishSingle(dataId, group, content));
    return true;
  }

  /**
   * 将配置从所有单元中删除
   * @param {String} dataId - id of the data
   * @param {String} group - group name of the data
   * @return {Boolean} success
   */
  async removeToAllUnit(dataId, group) {
    checkParameters(dataId, group);
    const units = await this.getAllUnits();
    await units.map(unit => this.getClient({ unit }).remove(dataId, group));
    return true;
  }

  /**
   * @deprecated This API is not implemented and will be removed in a future version
   */
  async publishAggr(dataId, group, datumId, content, options) {
    checkParameters(dataId, group, datumId);
    const client = this.getClient(options);
    return await client.publishAggr(dataId, group, datumId, content);
  }

  /**
   * @deprecated This API is not implemented and will be removed in a future version
   */
  async removeAggr(dataId, group, datumId, options) {
    checkParameters(dataId, group, datumId);
    const client = this.getClient(options);
    return await client.removeAggr(dataId, group, datumId);
  }

  close() {
    this._stopFailoverWatcher();
    if (this._grpcConfigProxy) {
      this._grpcConfigProxy.close();
    }
    if (this._grpcConnection) {
      this._grpcConnection.close();
    }
    if (this.serverMgr) {
      this.serverMgr.close();
    }
    for (const client of this.clients.values()) {
      client.close();
    }
    this.clients.clear();
  }

  protected getClient(options: { unit?: string; group?; dataId? } = {}): IClientWorker {
    if (!options.unit) {
      options.unit = CURRENT_UNIT;
    }
    const { unit } = options;
    let client = this.clients.get(unit);
    if (!client) {
      client = this.getClientWorker(Object.assign({}, {
        configuration: this.configuration.attach({ unit })
      }));
      client.on('error', err => {
        this.throwError(err);
      });
      this.clients.set(unit, client);
    }
    return client;
  }

  /**
   * 默认异常处理
   * @param {Error} err - 异常
   * @return {void}
   * @private
   */
  private throwError(err) {
    if (err) {
      setImmediate(() => this.emit('error', err));
    }
  }

  /**
   * 供其他包覆盖
   * @param options
   */
  protected getClientWorker(options): IClientWorker {
    return new ClientWorker(options);
  }

  protected getServerListManager(): IServerListManager {
    return new ServerListManager(this.options);
  }

  protected getSnapshot(): ISnapshot {
    return new Snapshot(this.options);
  }

  /**
   * 与 ClientWorker.getSnapshotKeyEncoded 一致的快照 key 编码，
   * 保证 HTTP / gRPC 两种传输共享同一份本地缓存与容灾文件。
   */
  private _getSnapshotKey(dataId: string, group: string): string {
    const tenant = this.configuration.get(ClientOptionKeys.NAMESPACE) || 'default_tenant';
    const unit = this.configuration.get(ClientOptionKeys.UNIT) || CURRENT_UNIT;
    return path.join(
      'config',
      encodeURIComponent(unit),
      encodeURIComponent(tenant),
      encodeURIComponent(group),
      encodeURIComponent(dataId)
    );
  }

  /**
   * gRPC 模式带本地容灾的读取（对齐 Java SDK 读优先级 failover > server > snapshot）：
   * - 用户手工维护的 failover 文件存在时优先返回；
   * - 否则查询服务端，成功后落快照（空/缺失内容由 Snapshot.save 统一按删除处理，等价 Java saveSnapshot(null)）；
   * - 服务端异常时回退本地快照，快照也没有才抛错。
   */
  private async _getConfigWithCache(dataId: string, group: string, tenant?: string): Promise<ConfigQueryResult> {
    const key = this._getSnapshotKey(dataId, group);
    const edkKey = this._getEncryptedDataKeySnapshotKey(dataId, group);
    const result = await readConfigWithFailover({
      snapshotKey: key,
      encryptedDataKeySnapshotKey: edkKey,
      isCipher: this.cipher.isCipherDataId(dataId),
      snapshot: this.snapshot,
      fetchFromServer: () => this._grpcConfigProxy!.getConfig(dataId, group, tenant),
      readSnapshotFallback: () => this.snapshot.get(key),
      onServerError: err => this.throwError(err),
    });
    return { content: result.content || '', encryptedDataKey: result.encryptedDataKey };
  }

  /**
   * encryptedDataKey 的本地持久化 key，与内容快照（'config/' 前缀）并行的独立命名空间
   * （'edk/' 前缀），与 ClientWorker.getEncryptedDataKeySnapshotKey 保持一致，HTTP / gRPC 共享。
   */
  private _getEncryptedDataKeySnapshotKey(dataId: string, group: string): string {
    return path.join('edk', this._getSnapshotKey(dataId, group));
  }

  /**
   * gRPC 模式下的本地容灾文件热切换（对齐 Java SDK ClientWorker.checkLocalConfig）：
   * - 文件新建/变更 → 切到容灾内容并通知监听器；
   * - 文件删除 → 回退服务端内容并通知监听器。
   * gRPC 无长轮询循环，由 _failoverWatcher 定时驱动。
   */
  private async _checkGrpcLocalFailover(): Promise<void> {
    if (!this._grpcSubscribers || !this._grpcFailoverState) {
      return;
    }
    for (const [key, listeners] of this._grpcSubscribers.entries()) {
      const state = this._grpcFailoverState.get(key);
      if (!state || listeners.length === 0) {
        continue;
      }
      const snapshotKey = this._getSnapshotKey(state.dataId, state.group);
      const mtime = await this.snapshot.getFailoverMtime(snapshotKey);

      if (mtime === null) {
        // 容灾文件被删除：切回服务端内容
        if (state.useFailover) {
          state.useFailover = false;
          state.failoverVersion = null;
          try {
            const { content, encryptedDataKey } = await this._getConfigWithCache(state.dataId, state.group);
            // 用户边界：切回服务端内容时解密后再比较/通知
            const plainContent = await this.cipher.decryptIfNeeded(state.dataId, content, encryptedDataKey);
            if (plainContent !== state.content) {
              state.content = plainContent;
              for (const fn of listeners) { fn(plainContent); }
            }
          } catch (err) {
            this.throwError(err);
          }
        }
        continue;
      }

      if (!state.useFailover || state.failoverVersion !== mtime) {
        const content = await this.snapshot.getFailover(snapshotKey);
        if (content === null) {
          continue;
        }
        state.useFailover = true;
        state.failoverVersion = mtime;
        if (content !== state.content) {
          state.content = content;
          for (const fn of listeners) { fn(content); }
        }
      }
    }
  }

  private _startFailoverWatcher(): void {
    if (this._failoverWatcher || this._transport !== 'grpc') {
      return;
    }
    this._failoverWatcher = setInterval(() => {
      this._checkGrpcLocalFailover().catch(err => this.throwError(err));
    }, FAILOVER_CHECK_INTERVAL);
    // 不阻止进程正常退出
    if (this._failoverWatcher.unref) {
      this._failoverWatcher.unref();
    }
  }

  private _stopFailoverWatcher(): void {
    if (this._failoverWatcher) {
      clearInterval(this._failoverWatcher);
      this._failoverWatcher = null;
    }
  }

}
