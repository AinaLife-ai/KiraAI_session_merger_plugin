from __future__ import annotations

"""
硬重开：对合并组内 sid 做「保留最近 keep 轮 + 摘要写回」。
仅在 merge_reset_mode=hard 时使用。

v2（借鉴 ContextCondensation）：
- 写穿模式（默认）：不 delete_session，直接 write_memory 覆写，
  保留会话 title/description 等元信息；
- 头部摘要剥离：旧摘要不参与 keep 轮数计算，也不重复进入 dropped；
- 融合布局：新摘要融合进第一个保留 chunk，不占窗口槽位、位置恒定，
  利于命中模型上下文缓存。
"""

from typing import List, Optional

from .summarizer import SUMMARY_MARKER, extract_summary_text
from .memory_access import safe_fetch_memory
from .session_mirror import defer_or_recreate, defer_or_write


def _clean_and_chunk(flat: List[dict]) -> List[List[dict]]:
    start_idx = 0
    for i, msg in enumerate(flat):
        if (msg.get("role") if isinstance(msg, dict) else None) == "user":
            start_idx = i
            break
    else:
        return []
    cleaned = flat[start_idx:]
    chunks: List[List[dict]] = []
    cur: List[dict] = []
    for msg in cleaned:
        if not isinstance(msg, dict):
            continue
        if msg.get("role") == "user":
            if cur:
                chunks.append(cur)
            cur = [msg]
        else:
            cur.append(msg)
    if cur:
        chunks.append(cur)
    return chunks


def _flatten(chunks: List[List[dict]]) -> List[dict]:
    out: List[dict] = []
    for c in chunks:
        out.extend(c)
    return out


def _split_head_summary(flat: List[dict]) -> tuple:
    """剥离扁平记忆首条的摘要消息，返回 (摘要正文, 剩余消息)。"""
    if flat and isinstance(flat[0], dict):
        first = flat[0]
        if first.get("role") == "user":
            content = str(first.get("content", "") or "")
            if content.startswith(SUMMARY_MARKER):
                return extract_summary_text(content), list(flat[1:])
    return "", list(flat)


def _norm_messages(flat) -> List[dict]:
    norm: List[dict] = []
    for m in flat or []:
        if isinstance(m, dict):
            norm.append(m)
        elif hasattr(m, "to_dict"):
            norm.append(m.to_dict())
    return norm


def build_fused_chunks(
    kept_chunks: List[List[dict]], summary_chunk: Optional[list]
) -> List[List[dict]]:
    """缓存友好布局：摘要融合进第一个保留 chunk（不占窗口槽位）。"""
    chunks = [list(c) for c in kept_chunks]
    if summary_chunk:
        summary_msg = list(summary_chunk)[0]
        if chunks:
            chunks[0] = [summary_msg] + chunks[0]
        else:
            chunks = [[summary_msg]]
    return chunks


def read_reset_parts(session_mgr, sid: str, keep_turns: int) -> dict:
    """
    只读计算重开分区：{"head_text", "chunks", "kept", "dropped"}。

    旧头部摘会被剥离：不计入 keep 轮数，也不进 dropped
    （累计模式下它由 CumulativeSummaryStore 接管）。
    """
    result = {"head_text": "", "chunks": [], "kept": [], "dropped": []}
    if not session_mgr or not sid:
        return result
    keep_turns = max(1, int(keep_turns or 1))
    try:
        # 双世代只读（与 hard_reset_session 同一路径）：
        #   2.x 直读 live chat_memory；3.0 走 SessionMirror（由异步入口 refresh_mirror 预热）。
        # ⚠️ 不得直读 session_mgr.chat_memory —— 3.0 无该属性，会静默拿到空结果
        #    （症状：reset 照跑但 summary=no，摘要永远不生成）。
        flat: List[dict] = _norm_messages(safe_fetch_memory(session_mgr, sid) or [])
        if not flat:
            return result
        head_text, rest = _split_head_summary(flat)
        chunks = _clean_and_chunk(rest)
        kept = chunks[-keep_turns:] if len(chunks) > keep_turns else chunks[:]
        dropped = _flatten(chunks[:-keep_turns]) if len(chunks) > keep_turns else []
        result.update(
            {"head_text": head_text, "chunks": chunks, "kept": kept, "dropped": dropped}
        )
        return result
    except Exception:
        return result


def compute_dropped_flat(session_mgr, sid: str, keep_turns: int) -> List[dict]:
    """
    只读计算：硬重开将丢弃的消息（keep 轮之前的部分）。
    供 async 层在真正重开前生成摘要用；不写盘。
    """
    return read_reset_parts(session_mgr, sid, keep_turns)["dropped"]


def hard_reset_session(
    session_mgr,
    sid: str,
    keep_turns: int,
    logger=None,
    summary_chunk: Optional[list] = None,
    write_through: bool = True,
    defer: Optional[list] = None,
) -> int:
    """
    对单个 sid：读记忆 → 剥离旧摘要 → 保留最近 keep_turns 轮 → 融合摘要写回。

    write_through=True（默认）：直接 write_memory 覆写，保留 title/description；
    False：旧路径 delete + 重建。返回保留的 chunk 数。失败返回 -1。
    """
    if not session_mgr or not sid:
        return -1
    keep_turns = max(1, int(keep_turns or 1))
    try:
        old_flat = _norm_messages(safe_fetch_memory(session_mgr, sid) or [])
        _head, rest = _split_head_summary(old_flat)
        chunks = _clean_and_chunk(rest)
        if not chunks:
            kept = 0
            kept_chunks: List[List[dict]] = []
        else:
            kept_chunks = chunks[-keep_turns:] if len(chunks) > keep_turns else chunks[:]
            kept = len(kept_chunks)

        to_write = build_fused_chunks(kept_chunks, summary_chunk)

        # 双世代写回（特征识别，不绑版本号）：
        #   2.x：直写（write_memory / delete+重建，本函数在 to_thread 线程区运行）；
        #   3.0：异步接口不能在此 await —— 写操作入 defer 队列，由异步调用方冲刷。
        if write_through:
            defer_or_write(session_mgr, sid, to_write, defer, logger=logger)
        else:
            defer_or_recreate(session_mgr, sid, to_write, defer, logger=logger)

        if logger:
            logger.warning(
                "[MERGER hard] reset sid=%s keep_chunks=%d summary=%s mode=%s",
                sid,
                kept,
                "yes" if summary_chunk else "no",
                "write-through" if write_through else "delete",
            )
        return kept
    except Exception as e:
        if logger:
            logger.error("[MERGER hard] reset failed sid=%s: %s", sid, e)
        return -1


def hard_reset_members(
    session_mgr,
    sids: List[str],
    keep_turns: int,
    logger=None,
    summary_chunks=None,
    write_through: bool = True,
    defer: Optional[list] = None,
) -> dict:
    """summary_chunks: sid -> summary_chunk 映射（可选，重开前摘要）。"""
    results = {}
    for sid in sids:
        sc = (summary_chunks or {}).get(sid)
        results[sid] = hard_reset_session(
            session_mgr, sid, keep_turns, logger, summary_chunk=sc,
            write_through=write_through, defer=defer,
        )
    return results
