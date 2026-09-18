import { ISnapshot } from './interface';

/** 本地容灾读取的结果：原始内容（cipher dataId 为密文）与配套的 encryptedDataKey。 */
export interface ConfigCacheResult {
  /** 配置内容；服务端返回“配置不存在”时为 null */
  content: string | null;
  /** KMS 加密数据密钥；仅 cipher dataId 且服务端下发时存在 */
  encryptedDataKey?: string;
}

/**
 * 本地容灾读取所需的钩子。HTTP（ClientWorker）与 gRPC（DataClient）两条传输的容灾
 * 主流程完全一致，仅“从服务端取值”“快照回退读取”“异常上报”三处实现不同，由调用方注入
 * （两个客户端类各自 extends Base，故用组合而非继承共享该骨架）。
 */
export interface DisasterRecoveryHooks {
  /** 内容快照的统一存储 key（各传输自行编码后传入） */
  snapshotKey: string;
  /** encryptedDataKey 快照的存储 key（与内容快照并行的 'edk/' 命名空间） */
  encryptedDataKeySnapshotKey: string;
  /** 是否为 cipher dataId：决定是否持久化/回退 encryptedDataKey */
  isCipher: boolean;
  /** 快照与容灾文件的持久化实现 */
  snapshot: ISnapshot;
  /**
   * 从服务端拉取配置及其 encryptedDataKey。配置不存在时 content 约定：HTTP 返回 null、
   * gRPC 返回空串；两者最终都会清除本地快照，差异仅在返回值。
   */
  fetchFromServer: () => Promise<ConfigCacheResult>;
  /** 服务端不可用时的快照回退读取（HTTP 带 legacy key 迁移，gRPC 直读） */
  readSnapshotFallback: () => Promise<string | null>;
  /** 服务端异常但命中快照回退时的错误上报（HTTP _error / gRPC throwError） */
  onServerError: (err: Error) => void;
}

/**
 * 本地容灾读取骨架，对齐 Java SDK 的读优先级：failover > server > snapshot。
 * - failover 是用户手工维护的应急容灾配置，命中则优先返回，按明文处理，不携带 encryptedDataKey；
 * - 服务端异常时回退本地快照（cipher dataId 一并回退 edk 以便解密），快照也没有才抛出原始错误；
 * - 服务端返回“配置不存在”（null）时删除内容快照与 edk，避免故障回退读到过期空内容；
 * - 快照存原始内容（cipher dataId 为密文），encryptedDataKey 单独持久化供离线解密。
 */
export async function readConfigWithFailover(hooks: DisasterRecoveryHooks): Promise<ConfigCacheResult> {
  const { snapshotKey, encryptedDataKeySnapshotKey, isCipher, snapshot } = hooks;

  const failover = await snapshot.getFailover(snapshotKey);
  if (failover !== null) {
    return { content: failover };
  }

  let content: string | null;
  let encryptedDataKey: string | undefined;
  try {
    const result = await hooks.fetchFromServer();
    content = result.content;
    encryptedDataKey = result.encryptedDataKey;
  } catch (err) {
    const cache = await hooks.readSnapshotFallback();
    if (cache !== null) {
      hooks.onServerError(err);
      const cachedDataKey = isCipher ? await snapshot.get(encryptedDataKeySnapshotKey) : null;
      return { content: cache, encryptedDataKey: cachedDataKey || undefined };
    }
    throw err;
  }

  if (content === null) {
    await snapshot.delete(snapshotKey);
    await snapshot.delete(encryptedDataKeySnapshotKey);
    return { content: null };
  }

  // 落原始内容快照（空内容由 Snapshot.save 统一按删除处理）
  await snapshot.save(snapshotKey, content || '');
  // encryptedDataKey 单独持久化；无 edk 时清理旧值避免误用
  if (isCipher && encryptedDataKey) {
    await snapshot.save(encryptedDataKeySnapshotKey, encryptedDataKey);
  } else {
    await snapshot.delete(encryptedDataKeySnapshotKey);
  }
  return { content, encryptedDataKey };
}
