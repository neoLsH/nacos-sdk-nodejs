import { ISnapshot } from './interface';

/**
 * 本地容灾读取所需的钩子。HTTP（ClientWorker）与 gRPC（DataClient）两条传输的容灾
 * 主流程完全一致，仅“从服务端取值”“快照回退读取”“异常上报”三处实现不同，由调用方注入
 * （两个客户端类各自 extends Base，故用组合而非继承共享该骨架）。
 */
export interface DisasterRecoveryHooks {
  /** 快照与容灾文件的统一存储 key（各传输自行编码后传入） */
  snapshotKey: string;
  /** 快照与容灾文件的持久化实现 */
  snapshot: ISnapshot;
  /**
   * 从服务端拉取配置。配置不存在时各传输沿用既有语义：HTTP 返回 null，gRPC 返回空串；
   * 两者最终都会清除本地快照，差异仅在返回值。
   */
  fetchFromServer: () => Promise<string | null>;
  /** 服务端不可用时的快照回退读取（HTTP 带 legacy key 迁移，gRPC 直读） */
  readSnapshotFallback: () => Promise<string | null>;
  /** 服务端异常但命中快照回退时的错误上报（HTTP _error / gRPC throwError） */
  onServerError: (err: Error) => void;
}

/**
 * 本地容灾读取骨架，对齐 Java SDK 的读优先级：failover > server > snapshot。
 * - failover 是用户手工维护的应急容灾配置，命中则优先于服务端与快照返回；
 * - 服务端异常时回退本地快照，快照也没有才抛出原始错误；
 * - 服务端返回“配置不存在”（null）时删除本地快照，避免故障回退读到过期空内容。
 */
export async function readConfigWithFailover(hooks: DisasterRecoveryHooks): Promise<string | null> {
  const { snapshotKey, snapshot } = hooks;

  const failover = await snapshot.getFailover(snapshotKey);
  if (failover !== null) {
    return failover;
  }

  let content: string | null;
  try {
    content = await hooks.fetchFromServer();
  } catch (err) {
    const cache = await hooks.readSnapshotFallback();
    if (cache !== null) {
      hooks.onServerError(err);
      return cache;
    }
    throw err;
  }

  if (content === null) {
    await snapshot.delete(snapshotKey);
    return null;
  }

  // 落快照（空内容由 Snapshot.save 统一按删除处理）
  await snapshot.save(snapshotKey, content || '');
  return content;
}
