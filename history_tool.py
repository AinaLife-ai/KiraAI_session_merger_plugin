from __future__ import annotations

import re
import time
import traceback
from typing import Any, Dict, List, Optional

import httpx

from core.chat.message_utils import KiraMessageBatchEvent


# Placeholder raw_message produced by some OneBot implementations (e.g.
# SnowLuma) when the reply segment conversion fails - the message content
# is actually empty and must be rebuilt from segments or get_msg.
# NOTE: raw_message is a CQ-coded string, so literal "[", "]", ",", "&" in
# text arrive escaped as "&#91;", "&#93;", "&#44;", "&amp;" (SnowLuma
# helper/cq.ts cqEscape). Every placeholder comparison must unescape first,
# otherwise SnowLuma's "[引用消息]" placeholder arrives as "&#91;引用消息&#93;"
# and slips through the filter.
_PLACEHOLDER_TOKENS = {"[引用消息]", "[空消息]", "[引用]", "[转发消息]"}
_PLACEHOLDER_RAW = _PLACEHOLDER_TOKENS | {""}
_CQ_ENTITIES = (("&#91;", "["), ("&#93;", "]"), ("&#44;", ","), ("&amp;", "&"))


def cq_unescape(text: str) -> str:
    """Decode OneBot CQ entities; "&amp;" must be last (see SnowLuma cq.ts)."""
    if not text:
        return text
    for entity, char in _CQ_ENTITIES:
        text = text.replace(entity, char)
    return text
# Segment types whose source (url/file) may be missing in stored history
# and needs a get_msg refresh (SnowLuma refreshes image URLs on get_msg).
_MEDIA_TYPES = {"image", "record", "video"}
# Max messages to refresh per call (get_msg is one round-trip each).
_MAX_REFRESH = 10


class HistoryToolService:
    """
    跨会话历史查询（对齐新版 history_plugin v1.3.2 强解析能力）。

    - WS 通道优先（复用适配器连接，与转发/撤回同一 ID 命名空间，
      SnowLuma 下 get_msg 可反查），HTTP 通道兜底。
    - 强解析：raw_message 为占位（如 SnowLuma 的 [引用消息]）时改用
      message 段数组；reply 段显示 [引用 msg_id:xxx]；媒体缺源标记待刷新。
    - get_msg 批量刷新（最多 10 条/次）恢复媒体 URL。
    - 空引用占位消息渲染后判定过滤，不污染 LLM 上下文。
    - 保留 KSM 特有：全局熔断、同回合调用限制、缓存、权限控制、截断。
    任何失败只 return str，绝不抛异常。
    """

    # 本地 OneBot 拉取历史消息（尤其群聊大 count）可能耗时数秒，
    # 参考可用的 history_plugin 显式 timeout=10，这里按阶段拆分并留足余量。
    CONNECT_TIMEOUT = 3.0
    READ_TIMEOUT = 15.0
    ERROR_CACHE_TTL = 90.0
    # 同一 agent 回合内：同一目标会话最多成功返回几次（再调用直接拒绝，不塞大段历史）
    MAX_CALLS_PER_TARGET_PER_EVENT = 1
    # 同一 agent 回合内：历史工具总调用上限（含被拒绝的）
    MAX_CALLS_PER_EVENT = 2
    # 单次返回正文最大字符，避免 tool_result 把上下文撑爆
    MAX_RESULT_CHARS = 3500

    def __init__(
        self,
        http_host: str = "localhost",
        http_port: int = 3000,
        access_token: str = "",
        master_id: str = "",
        allowed_users: Optional[List[str]] = None,
        restricted_groups: Optional[List[str]] = None,
        cache_ttl_sec: int = 120,
        circuit_fail_threshold: int = 2,
        circuit_open_sec: float = 60.0,
        use_ws: bool = True,
        ctx=None,
        logger=None,
    ):
        self.http_host = http_host or "localhost"
        self.http_port = int(http_port or 3000)
        self.base_url = f"http://{self.http_host}:{self.http_port}"
        self.access_token = access_token or ""
        self.master_id = str(master_id or "").strip()
        self.allowed_users = [str(u).strip() for u in (allowed_users or []) if str(u).strip()]
        self.restricted_groups = [str(g).strip() for g in (restricted_groups or []) if str(g).strip()]
        self.cache_ttl_sec = max(0, int(cache_ttl_sec or 0))
        self.circuit_fail_threshold = max(1, int(circuit_fail_threshold or 2))
        self.circuit_open_sec = max(0.0, float(circuit_open_sec or 60.0))
        self.use_ws = bool(use_ws)
        # Plugin context: needed by _get_client to resolve the adapter's WS
        # client (same ID namespace as the adapter, so message IDs work).
        self.ctx = ctx
        self.logger = logger
        self._call_cache: Dict[str, Dict[str, Any]] = {}
        self._fail_streak = 0
        self._circuit_open_until = 0.0

    def _check_permission(self, user_id: str, session_type: str, session_id: str) -> bool:
        if not self.master_id:
            return True
        if user_id == self.master_id:
            return True
        if user_id in self.allowed_users:
            if session_type == "gm" and session_id in self.restricted_groups:
                return False
            return True
        if session_type == "dm":
            return session_id == user_id
        if session_type == "gm":
            return session_id not in self.restricted_groups
        return False

    @staticmethod
    def parse_session_ref(session_id: str, session_type: Optional[str] = None) -> Dict[str, str]:
        sid = (session_id or "").strip()
        st = (session_type or "").strip().lower()

        if ":" in sid:
            parts = sid.split(":", 2)
            adapter = parts[0] if len(parts) >= 1 else "qq"
            typ = parts[1] if len(parts) >= 2 else "dm"
            entity = parts[2] if len(parts) >= 3 else sid
            if typ in ("group", "g"):
                typ = "gm"
            if typ in ("private", "p", "friend"):
                typ = "dm"
            return {"adapter": adapter, "session_type": typ, "session_id": entity, "full": sid}

        if st in ("group", "gm", "g"):
            typ = "gm"
        elif st in ("private", "dm", "p", "friend"):
            typ = "dm"
        else:
            typ = "dm"
        return {"adapter": "qq", "session_type": typ, "session_id": sid, "full": f"qq:{typ}:{sid}"}

    # ---------- 强解析（对齐 history_plugin v1.3.2） ----------

    @staticmethod
    def _segments_to_text(msg_segments) -> str:
        """Render message segments to text, keeping media URLs and reply IDs."""
        parts = []
        for seg in msg_segments:
            seg_type = seg.get("type")
            seg_data = seg.get("data", {})
            if seg_type == "text":
                parts.append(seg_data.get("text", ""))
            elif seg_type == "at":
                parts.append(f"@{seg_data.get('qq', 'someone')}")
            elif seg_type == "face":
                parts.append("[表情]")
            elif seg_type == "image":
                img_url = seg_data.get("url", "")
                if img_url:
                    parts.append(f"[图片]({img_url})")
                else:
                    parts.append("[图片]")
            elif seg_type == "video":
                parts.append("[视频]")
            elif seg_type == "file":
                file_name = seg_data.get("name", "文件")
                parts.append(f"[文件]{file_name}")
            elif seg_type == "reply":
                rid = seg_data.get("id", "")
                parts.append(f"[引用 msg_id:{rid}]" if rid else "[引用]")
            elif seg_type == "forward":
                parts.append("[转发消息]")
            else:
                parts.append(f"[{seg_type}]")
        return " ".join(parts)

    def _message_to_text(self, msg: dict) -> str:
        """Convert a message to formatted text. Uses raw_message only when it
        is real content; placeholder raw_message (e.g. SnowLuma's
        "[引用消息]") falls back to the segment array."""
        raw = cq_unescape((msg.get("raw_message") or "").strip())
        if raw and raw not in _PLACEHOLDER_RAW:
            content = raw
        else:
            msg_segments = msg.get("message", [])
            if not msg_segments:
                content = "[空消息]"
            else:
                content = self._segments_to_text(msg_segments)

        msg_id = msg.get("message_id")
        if msg_id:
            content += f" (msg_id:{msg_id})"
        return content

    def _is_placeholder(self, msg: dict) -> bool:
        """True when the message renders as a placeholder (empty quote) and
        carries no real content - SnowLuma stores reply-conversion failures
        as such (raw_message = "[引用消息]" with empty/placeholder segments).
        Filtering these keeps the LLM context clean."""
        raw = cq_unescape((msg.get("raw_message") or "").strip())
        segs = msg.get("message") or []
        # Placeholder raw_message (non-empty) marks a conversion failure.
        # raw_message is CQ-escaped, hence the cq_unescape above.
        if raw and raw in _PLACEHOLDER_RAW:
            return True
        # Empty raw_message is normal for segment-based messages - only
        # filter when there is genuinely no content at all.
        if not raw and not segs:
            return True
        # Render the content; if it is empty or a pure placeholder after
        # stripping the trailing (msg_id:xxx), the message is not real.
        content = self._message_to_text(msg)
        content = re.sub(r"\s*\(msg_id:-?\d+\)\s*$", "", content).strip()
        if not content or content in _PLACEHOLDER_TOKENS:
            return True
        # Second channel: render from the segment array (already unescaped)
        # so an escaped placeholder raw_message cannot hide a fake message.
        seg_text = self._segments_to_text(segs).strip() if segs else ""
        if seg_text and seg_text in _PLACEHOLDER_TOKENS:
            return True
        # SnowLuma's synthetic backfill event: user_id 0 + a single
        # "[引用消息]" text segment (message-actions.ts buildBackfillEvent).
        uid = str(msg.get("user_id")
                  or (msg.get("sender") or {}).get("user_id") or "").strip()
        if uid in ("", "0") and seg_text in _PLACEHOLDER_TOKENS:
            return True
        return False

    def _needs_refresh(self, msg: dict) -> bool:
        """True when the message needs a get_msg refresh: placeholder
        raw_message, or media segments without a usable source."""
        raw = cq_unescape((msg.get("raw_message") or "").strip())
        if raw in _PLACEHOLDER_RAW:
            return True
        for seg in msg.get("message") or []:
            if seg.get("type") in _MEDIA_TYPES:
                data = seg.get("data") or {}
                if not (data.get("url") or data.get("file") or data.get("file_id")):
                    return True
        return False

    # ---------- 通道 ----------

    def _get_client(self, event):
        """Get the adapter WS client from the event (same ID namespace as
        the adapter itself, so message IDs are usable by get_msg / forward)."""
        try:
            info = getattr(event, "adapter", None)
            if info is None:
                return None
            name = getattr(info, "name", None) or getattr(info, "adapter_id", None)
            if not name:
                return None
            adapter = self.ctx.adapter_mgr.get_adapter(name)
            if adapter is None:
                return None
            return adapter.get_client()
        except Exception as e:
            if self.logger:
                self.logger.error(f"[history_tool] get client failed: {e}")
            return None

    async def _fetch_ws(self, client, session_type: str, session_id: str, count: int):
        """Fetch history via the WS channel (adapter's own OneBot connection)."""
        try:
            if session_type == "gm":
                resp = await client.send_action(
                    "get_group_msg_history",
                    {"group_id": int(session_id), "count": count},
                    timeout=15,
                )
            else:
                resp = await client.send_action(
                    "get_friend_msg_history",
                    {"user_id": int(session_id), "count": count},
                    timeout=15,
                )
            if isinstance(resp, dict) and resp.get("status") == "ok":
                return resp.get("data", {}).get("messages") or []
        except Exception as e:
            if self.logger:
                self.logger.error(f"[history_tool] WS history failed: {e}")
        return None

    async def _fetch_http(self, session_type: str, session_id: str, count: int):
        """Fetch history via the HTTP service (legacy channel)."""
        try:
            if session_type == "gm":
                api = "get_group_msg_history"
                params = {"group_id": int(session_id), "count": count}
            else:
                api = "get_friend_msg_history"
                params = {"user_id": int(session_id), "count": count}

            headers = {}
            if self.access_token:
                headers["Authorization"] = f"Bearer {self.access_token}"

            timeout = httpx.Timeout(
                connect=self.CONNECT_TIMEOUT,
                read=self.READ_TIMEOUT,
                write=self.READ_TIMEOUT,
                pool=self.CONNECT_TIMEOUT,
            )

            url = f"{self.base_url}/{api}"
            if self.logger:
                self.logger.info("[history_tool] fetching %s params=%s", url, params)

            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(url, json=params, headers=headers)
                if resp.status_code >= 400:
                    err = (
                        f"Error: HTTP {resp.status_code} from {url}/{api}. "
                        "OneBot HTTP 不可用。请勿再次调用 get_session_history，"
                        "请基于当前对话上下文回答。"
                    )
                    if self.logger:
                        self.logger.error("Error fetching history: HTTP %s body=%s", resp.status_code, resp.text[:200])
                    return None, err
                try:
                    result = resp.json()
                except Exception as e:
                    err = f"Error: invalid JSON from OneBot ({e}) body={resp.text[:200]}"
                    return None, err

            if result.get("status") != "ok":
                err = f"Failed: {result.get('message', 'unknown error')}"
                return None, err
            return result.get("data", {}).get("messages", []), None
        except Exception as e:
            err = f"{type(e).__name__}: {str(e) or '(no message)'}"
            return None, err

    async def _get_msg_ws(self, client, message_id) -> dict | None:
        """Fetch a single message via get_msg (SnowLuma refreshes image URLs
        on get_msg, so this recovers media sources missing from history)."""
        try:
            resp = await client.send_action(
                "get_msg", {"message_id": message_id}, timeout=15
            )
            if isinstance(resp, dict) and resp.get("status") == "ok":
                return resp.get("data") or {}
        except Exception as e:
            if self.logger:
                self.logger.error(f"[history_tool] get_msg({message_id}) failed: {e}")
        return None

    # ---------- 缓存 / 熔断 / 限制（KSM 保留） ----------

    def _cache_put(self, key: str, count: int, data: str, is_error: bool = False):
        self._call_cache[key] = {
            "count": count,
            "data": data,
            "timestamp": time.time(),
            "is_error": is_error,
        }
        if len(self._call_cache) > 100:
            now = time.time()
            for k in [k for k, v in self._call_cache.items() if now - v.get("timestamp", 0) > 300]:
                del self._call_cache[k]

    def _cache_get_hit(self, key: str, count: int) -> Optional[str]:
        cached = self._call_cache.get(key)
        if not cached:
            return None
        now = time.time()
        is_error = bool(cached.get("is_error"))
        ttl = self.ERROR_CACHE_TTL if is_error else self.cache_ttl_sec
        if ttl <= 0:
            return None
        if (now - cached.get("timestamp", 0)) >= ttl:
            return None
        if (not is_error) and count > cached.get("count", 0):
            return None
        data = cached["data"]
        if is_error:
            return data
        # 缓存命中：只回短拒，不再把整段历史塞进 tool_result（否则每步 +数千 token）
        return (
            "Rejected: 该会话历史本回合已查询过（结果在上文 tool 记录中）。"
            "请直接基于当前对话上下文回复，禁止再次调用 get_session_history。"
        )

    @staticmethod
    def _event_extra(event) -> dict:
        try:
            extra = getattr(event, "extra", None)
            if not isinstance(extra, dict):
                extra = {}
                try:
                    event.extra = extra
                except Exception:
                    return {}
            return extra
        except Exception:
            return {}

    def _track_and_limit(self, event, target_key: str) -> Optional[str]:
        """
        同一 KiraMessageBatchEvent（一次 agent 回合）内限制历史工具调用。
        返回非 None 则应直接 return 该字符串，不再打 HTTP。
        """
        extra = self._event_extra(event)
        total = int(extra.get("merger_hist_total", 0) or 0)
        by_target = extra.get("merger_hist_by_target")
        if not isinstance(by_target, dict):
            by_target = {}
            extra["merger_hist_by_target"] = by_target

        if total >= self.MAX_CALLS_PER_EVENT:
            return (
                "Rejected: 本回合 get_session_history 调用次数已达上限。"
                "请直接回复，禁止再查历史。"
            )
        n = int(by_target.get(target_key, 0) or 0)
        if n >= self.MAX_CALLS_PER_TARGET_PER_EVENT:
            return (
                f"Rejected: 本回合已查询过 {target_key} 的历史。"
                "请直接基于上下文回复，禁止再次 get_session_history。"
            )

        by_target[target_key] = n + 1
        extra["merger_hist_total"] = total + 1
        return None

    def _truncate_result(self, text: str) -> str:
        if not text or len(text) <= self.MAX_RESULT_CHARS:
            return text
        # 保留末尾（更新）
        cut = text[-self.MAX_RESULT_CHARS :]
        return "…(truncated older)…\n" + cut

    def _note_failure(self):
        """记录失败并进入熔断。

        关键修复：熔断窗口期内再次失败时，不再把窗口重新续到未来
        （否则会像「永远打不开」）。窗口结束后 _fail_streak 重置。
        """
        now = time.time()
        if now >= self._circuit_open_until:
            # 窗口已结束，说明这是一次新的失败序列，重置计数
            self._fail_streak = 0
        self._fail_streak += 1
        if self._fail_streak >= self.circuit_fail_threshold:
            # 只在首次进入熔断时设置窗口；窗口期内不再延长
            if now >= self._circuit_open_until:
                self._circuit_open_until = now + self.circuit_open_sec
                if self.logger:
                    self.logger.warning(
                        "history circuit OPEN for %.0fs after %d failures",
                        self.circuit_open_sec,
                        self._fail_streak,
                    )

    def _note_success(self):
        self._fail_streak = 0
        self._circuit_open_until = 0.0

    def _circuit_blocked(self) -> Optional[str]:
        now = time.time()
        if now < self._circuit_open_until:
            left = int(self._circuit_open_until - now)
            return (
                f"Error: OneBot HTTP circuit open ({left}s left). "
                "请勿再次调用 get_session_history，请基于当前对话上下文回答。"
            )
        return None

    async def get_session_history(
        self,
        event: KiraMessageBatchEvent,
        session_id: str,
        count: int = 20,
        session_type: Optional[str] = None,
        *,
        merge_enabled: bool = False,
    ) -> str:
        try:
            blocked = self._circuit_blocked()
            if blocked:
                return blocked

            user_id = "unknown"
            if event.messages and event.messages[0].sender:
                user_id = str(event.messages[0].sender.user_id)

            ref = self.parse_session_ref(session_id, session_type)
            st = ref["session_type"]
            entity = ref["session_id"]

            if not self._check_permission(user_id, st, entity):
                return "抱歉，您没有权限查看此会话的历史消息。"

            try:
                count = int(count)
            except Exception:
                count = 20
            # 与 history_plugin 对齐：最少 5，最多 80，默认 20
            if count < 5:
                count = 5
            elif count > 80:
                count = 80

            cache_key = f"{st}:{entity}"
            target_key = cache_key

            # 本回合调用次数硬限制（在缓存命中之前也计数，防止刷拒绝）
            limited = self._track_and_limit(event, target_key)
            if limited:
                return limited

            hit = self._cache_get_hit(cache_key, count)
            if hit is not None:
                return hit

            # ---------- 拉取历史：WS 通道优先，HTTP 兜底 ----------
            # Over-fetch so placeholder rows (which sit at the newest end)
            # cannot crowd real messages out of the returned window.
            fetch_count = min(80, max(count, count * 3))
            messages = None
            err = None
            client = None
            if self.use_ws:
                client = self._get_client(event)
                if client is not None:
                    messages = await self._fetch_ws(client, st, entity, fetch_count)
                    if messages is None:
                        if self.logger:
                            self.logger.warning(
                                "[history_tool] WS fetch failed for %s; falling back to HTTP",
                                cache_key,
                            )
            if messages is None:
                messages, err = await self._fetch_http(st, entity, fetch_count)

            if err is not None:
                self._cache_put(cache_key, 80, err, is_error=True)
                self._note_failure()
                return err

            if not messages:
                empty = "该会话暂无历史消息。"
                self._cache_put(cache_key, count, empty, is_error=False)
                self._note_success()
                return empty

            # ---------- get_msg 批量刷新（最多 10 条/次） ----------
            if client is not None:
                target = messages[-fetch_count:]
                refreshed = 0
                for i, m in enumerate(target):
                    if refreshed >= _MAX_REFRESH:
                        break
                    if self._needs_refresh(m):
                        mid = m.get("message_id")
                        if mid is not None:
                            fresh = await self._get_msg_ws(client, mid)
                            if fresh and fresh.get("message"):
                                target[i] = fresh
                                refreshed += 1
                if refreshed and self.logger:
                    self.logger.info(
                        "[history_tool] refreshed %d messages via get_msg", refreshed
                    )

            # ---------- 格式化 + 空引用占位过滤 ----------
            # NOTE: iterate `target` (the refreshed slice) - the old code
            # formatted `messages[-count:]` again, so every get_msg refresh
            # was silently discarded.
            formatted = []
            skipped = 0
            real = []
            for msg in (target if client is not None else messages[-fetch_count:]):
                if self._is_placeholder(msg):
                    skipped += 1
                    continue
                real.append(msg)
            for msg in real[-count:]:
                sender = msg.get("sender", {}).get("nickname", "Unknown")
                content = self._message_to_text(msg)
                formatted.append(f"{sender}: {content}")

            if skipped and self.logger:
                self.logger.info(
                    "[history_tool] filtered %d unresolvable placeholder messages",
                    skipped,
                )

            if not formatted:
                empty = "该会话暂无有效历史消息。"
                self._cache_put(cache_key, count, empty, is_error=False)
                self._note_success()
                return empty

            result_text = self._truncate_result("\n".join(formatted))
            self._cache_put(cache_key, count, result_text, is_error=False)
            self._note_success()
            return result_text

        except Exception as e:
            tb = traceback.format_exc()
            err_msg = f"{type(e).__name__}: {str(e) or '(no message)'}"
            if self.logger:
                self.logger.error("Error fetching history: %s\n%s", err_msg, tb)
            err = (
                f"Error: {err_msg}. "
                "请勿再次调用 get_session_history，请基于当前对话上下文回答。"
            )
            try:
                ref = self.parse_session_ref(session_id, session_type)
                self._cache_put(
                    f"{ref['session_type']}:{ref['session_id']}",
                    80,
                    err,
                    is_error=True,
                )
            except Exception:
                pass
            self._note_failure()
            return err
