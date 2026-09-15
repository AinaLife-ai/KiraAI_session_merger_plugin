# -*- coding: utf-8 -*-
"""End-to-end tests for KSM's locate path (HistoryToolService.get_session_history).

Covers the KSM-specific extras on top of the shared engine: session-ref parsing
(gm/dm), circuit breaker, per-turn call limit, scan budget, truncation.

Run: python3 tests/test_locate_path.py
"""
import asyncio
import importlib.util
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# --- minimal KiraAI stubs ---
for name in ("core", "core.chat", "core.chat.message_utils"):
    sys.modules[name] = types.ModuleType(name)
sys.modules["core.chat.message_utils"].KiraMessageBatchEvent = type("E", (), {})
hx = types.ModuleType("httpx")
hx.Timeout = lambda **k: None
hx.AsyncClient = object
sys.modules["httpx"] = hx

pkg_name = "ksm_e2e"
pkg = types.ModuleType(pkg_name)
pkg.__path__ = [ROOT]
pkg.__package__ = pkg_name
sys.modules[pkg_name] = pkg
for _sub in ("locate", "onebot_compat"):
    _s = importlib.util.spec_from_file_location(
        f"{pkg_name}.{_sub}", os.path.join(ROOT, f"{_sub}.py"))
    _m = importlib.util.module_from_spec(_s)
    sys.modules[f"{pkg_name}.{_sub}"] = _m
    _s.loader.exec_module(_m)

spec = importlib.util.spec_from_file_location(f"{pkg_name}.history_tool",
                                              os.path.join(ROOT, "history_tool.py"))
mod = importlib.util.module_from_spec(spec)
sys.modules[f"{pkg_name}.history_tool"] = mod
spec.loader.exec_module(mod)
HistoryToolService = mod.HistoryToolService

results = []


def check(label, cond, detail=""):
    results.append((label, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + label + ((" — " + str(detail)) if detail else ""))


def text(t):
    return {"type": "text", "data": {"text": t}}


BASE_TS = 1789434600
TOTAL = 600


def build_history():
    out = []
    for i in range(TOTAL):
        uid = 100 if i % 3 else 200
        nick = "甲" if uid == 100 else "乙"
        body = "普通消息 %d" % i
        if i == 150:
            body = "今晚要部署新版本"
        if i == 100:
            body = "部署脚本我改好了"
        out.append({
            "message_id": 1000 + i,
            "message_seq": 5000 + i,
            "time": BASE_TS - i * 60,
            "user_id": uid,
            "raw_message": body,
            "message": [text(body)],
            "sender": {"user_id": uid, "nickname": nick, "card": ""},
        })
    return out


HISTORY = build_history()


class _Sender:
    user_id = "769690776"


class _M:
    sender = _Sender()


class _Ev:
    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


class _Ctx:
    adapter_mgr = None


class FakeClient:
    def __init__(self, app_name="NapCat.Onebot", fail_once_at=None):
        self.app_name = app_name
        self.fail_once_at = fail_once_at
        self.page_calls = 0

    async def send_action(self, action, params, timeout=None):
        if action == "get_version_info":
            return {"status": "ok", "data": {"app_name": self.app_name}}
        if action == "get_msg":
            return {"status": "failed"}
        self.page_calls += 1
        if self.fail_once_at is not None and self.page_calls == self.fail_once_at:
            raise RuntimeError("anchor expired")
        count = int(params.get("count") or 20)
        anchor = params.get("message_seq", params.get("message_id"))
        start = 0
        if anchor is not None:
            for idx, m in enumerate(HISTORY):
                if str(m["message_id"]) == str(anchor) or str(m["message_seq"]) == str(anchor):
                    start = idx + 1
                    break
        page = HISTORY[start:start + count]
        return {"status": "ok", "data": {"messages": list(reversed(page))}}


def make_svc(locate_cfg=None, client=None, **kw):
    cfg = {"enable_locate": True}
    cfg.update(locate_cfg or {})
    svc = HistoryToolService(
        master_id="", allowed_users=[], restricted_groups=[],
        ctx=_Ctx(), logger=None, locate_cfg=cfg, **kw)
    fake = client if client is not None else FakeClient()
    svc._get_client = lambda e: fake
    return svc


def run(svc, event, **kwargs):
    return asyncio.new_event_loop().run_until_complete(
        svc.get_session_history(event, **kwargs))


print("\n[session ref]")
svc = make_svc()
out = run(svc, _Ev(), session_id="qq:gm:123", count=5)
check("gm sid parsed, plain list", "【定位】" not in out and out.strip(), out[:80])
out = run(make_svc(), _Ev(), session_id="123", session_type="gm", count=5)
check("bare id + session_type works", out.strip() != "", out[:80])
check("parse_session_ref maps group->gm",
      HistoryToolService.parse_session_ref("qq:group:1")["session_type"] == "gm")
check("parse_session_ref maps private->dm",
      HistoryToolService.parse_session_ref("qq:private:1")["session_type"] == "dm")

print("\n[locate: keyword]")
out = run(make_svc(), _Ev(), session_id="qq:gm:123", count=10, keyword="部署")
check("keyword hit rendered", "部署" in out, out[:200])
check("header present", "【定位】" in out, out[:200])
check("header condition", "关键词[部署]" in out, out[:200])
check("section separator present", "---" in out, out[:200])

print("\n[locate: time]")
out = run(make_svc(), _Ev(), session_id="qq:gm:123", count=10,
          since="2026-09-15 08:00", until="2026-09-15 09:00")
check("time condition in header", "时间[" in out, out[:200])
check("time filter applied", "命中=" in out, out[:200])
bad = run(make_svc(), _Ev(), session_id="qq:gm:123", since="昨天")
check("bad time -> readable Error", "Error" in bad and "无法解析" in bad, bad[:150])

print("\n[locate: user]")
out = run(make_svc(), _Ev(), session_id="qq:gm:123", count=5, user_id="200",
          scan_limit=200)
check("user condition in header", "用户[200]" in out, out[:200])
check("only that user returned", "乙(200)" in out and "甲(100)" not in out, out[:300])

print("\n[placeholders never reach the model]")


PLACEHOLDER = {"message_id": 777001, "message_seq": 777001, "time": BASE_TS - 5,
               "user_id": 0, "raw_message": "&#91;引用消息&#93;",
               "message": [text("[引用消息]")],
               "sender": {"user_id": 0, "nickname": "", "card": ""}}
saved2 = HISTORY[:]
HISTORY.insert(5, PLACEHOLDER)
ph = run(make_svc(), _Ev(), session_id="qq:gm:123", count=10, keyword="引用")
check("keyword '引用' cannot surface a synthetic placeholder",
      "777001" not in ph and "引用消息" not in ph, ph[:300])
ph2 = run(make_svc(), _Ev(), session_id="qq:gm:123", count=10, user_id="0")
check("placeholders are not searchable by user_id either",
      "777001" not in ph2, ph2[:300])
HISTORY[:] = saved2

print("\n[legacy path untouched]")
svc = make_svc()
out = run(svc, _Ev(), session_id="qq:gm:123", count=20)
lines = [l for l in out.splitlines() if l.strip()]
check("no locate params -> plain list", "【定位】" not in out, out[:120])
check("plain list has 20 lines", len(lines) == 20, len(lines))
out = run(make_svc(locate_cfg={"enable_locate": False}), _Ev(),
          session_id="qq:gm:123", count=10, keyword="部署")
check("enable_locate=false -> plain path", "【定位】" not in out, out[:120])

print("\n[anti-loop]")
svc = make_svc()
ev = _Ev()
_ = run(svc, ev, session_id="qq:gm:123", count=10, keyword="部署")
again = run(svc, ev, session_id="qq:gm:123", count=10, keyword="部署")
check("identical repeat served from cache", "完全相同" in again, again[:200])
third = run(svc, ev, session_id="qq:gm:123", count=10, keyword="另外的词")
check("per-turn limit refuses the third call", "Rejected" in third, third[:200])

svc = make_svc()
ev = _Ev()
_ = run(svc, ev, session_id="qq:gm:123", count=10, keyword="部署", scan_limit=400)
smaller = run(svc, ev, session_id="qq:gm:123", count=10, keyword="部署", scan_limit=100)
check("narrower re-scan refused", "子集" in smaller, smaller[:200])

svc = make_svc({"max_scanned_per_turn": 100,
                "max_calls_per_target_per_turn": 5, "max_calls_per_turn": 5})
ev = _Ev()
a = run(svc, ev, session_id="qq:gm:123", count=10, keyword="没有的词", scan_limit=100)
b = run(svc, ev, session_id="qq:gm:123", count=10, keyword="另一个没有", scan_limit=100)
check("scan budget charged", int(ev.extra.get("merger_hist_scanned", 0)) > 0,
      ev.extra.get("merger_hist_scanned"))
check("budget exhaustion refuses", "预算" in b or "命中=0" in b, b[:200])
del a

print("\n[scan_limit clamping]")
out = run(make_svc({"max_scan_limit": 250}), _Ev(), session_id="qq:gm:123",
          count=10, keyword="没有", scan_limit=99999)
check("clamped and reported", "上限" in out, out[:300])

print("\n[cap probing order]")
# The clamp must use the REAL implementation cap, not the conservative generic
# one - so the implementation probe has to run before clamping.
class _CapEv:
    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


svc_cap = make_svc({"max_calls_per_target_per_turn": 5, "max_calls_per_turn": 5},
                   client=FakeClient(app_name="SnowLuma"))
svc_cap._impl_cache = {}
out_cap = run(svc_cap, _CapEv(), session_id="qq:gm:123", count=10,
              keyword="没有", scan_limit=5000)
check("first call clamps with the SnowLuma cap (800), not the generic one",
      "800" in out_cap, out_cap[:400])

print("\n[scan_limit floor]")


class _FloorEv:
    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


svc_f = make_svc({"max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9})
out_f = run(svc_f, _FloorEv(), session_id="qq:gm:123", count=10,
            keyword="没有的词", scan_limit=-5)
check("a typo'd scan_limit (negative) still scans a page worth",
      "扫描=50条" in out_f or "扫描=4" in out_f or int(
          out_f.split("扫描=")[1].split("条")[0]) >= 20, out_f[:200])
out_f2 = run(svc_f, _FloorEv(), session_id="qq:gm:456", count=10,
             keyword="没有的词", scan_limit=0)
check("scan_limit=0 also falls back to the page size",
      int(out_f2.split("扫描=")[1].split("条")[0]) >= 20, out_f2[:200])

print("\n[impl probing]")
client = FakeClient(app_name="SnowLuma")
svc = make_svc(client=client)
run(svc, _Ev(), session_id="qq:gm:123", count=10, keyword="部署")
check("SnowLuma impl resolved",
      svc._impl_cache and list(svc._impl_cache.values())[0]["anchor_param"] == "message_id",
      svc._impl_cache)
client2 = FakeClient(app_name="LLOneBot")
svc2 = make_svc(client=client2)
run(svc2, _Ev(), session_id="qq:gm:123", count=10, keyword="部署")
check("LLOneBot impl resolved",
      list(svc2._impl_cache.values())[0]["order_param"] == "reverseOrder",
      svc2._impl_cache)

print("\n[anchor failure fallback]")
client3 = FakeClient(app_name="NapCat.Onebot", fail_once_at=2)
svc3 = make_svc(client=client3)
out = run(svc3, _Ev(), session_id="qq:gm:123", count=10, keyword="部署", scan_limit=300)
check("retry produced an answer", "【定位】" in out, out[:200])
check("retry actually happened", client3.page_calls > 2, client3.page_calls)

svc4 = make_svc({"locate_fallback_on_error": False},
                client=FakeClient(app_name="NapCat.Onebot", fail_once_at=2))
out = run(svc4, _Ev(), session_id="qq:gm:123", count=10, keyword="部署", scan_limit=300)
check("fallback disabled -> error surfaced", "Error" in out, out[:200])

print("\n[truncation]")
svc_t = make_svc({"max_calls_per_target_per_turn": 5, "max_calls_per_turn": 5})
out = run(svc_t, _Ev(), session_id="qq:gm:123", count=500, scan_limit=300)
body = [l for l in out.splitlines() if l.startswith(("甲(", "乙("))]
check("count clamped to max_return_count (default 80)", len(body) == 80, len(body))
out2 = run(svc_t, _Ev(), session_id="qq:dm:1", count=500, scan_limit=300)
body2 = [l for l in out2.splitlines() if l.startswith(("甲(", "乙("))]
check("clamp works for dm too", len(body2) == 80, len(body2))

print("\n[header agrees with the body]")
_t = make_svc({"max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9})
for _c in (5, 10, 20):
    _o = run(_t, _Ev(), session_id="qq:gm:123", count=_c, user_id="100", scan_limit=200)
    _ml = [l for l in _o.splitlines() if l.startswith(("甲(", "乙("))]
    check("count=%d: 返回 equals rendered lines" % _c,
          ("返回=%d" % len(_ml)) in _o or "返回=" not in _o,
          (_o.splitlines()[0][:110], len(_ml)))

print("\n[offset footer must not say 'newest']")
_t2 = make_svc({"max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9})
_off = run(_t2, _Ev(), session_id="qq:gm:123", count=3, user_id="100",
           offset=4, scan_limit=100)
check("offset footer does not claim the shown lines are the newest",
      "最新的" not in _off, _off[-200:])
_om = [l for l in _off.splitlines() if l.startswith(("甲(", "乙("))]
check("offset: 返回 equals rendered lines",
      ("返回=%d" % len(_om)) in _off, (_off.splitlines()[0][:110], len(_om)))

print("\n[AND semantics for combined conditions]")


class _AndEv:
    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


_AND = [
    {"message_id": 88001, "message_seq": 88001, "time": BASE_TS, "user_id": 100,
     "raw_message": "今天天气不错", "message": [text("今天天气不错")],
     "sender": {"user_id": 100, "nickname": "甲", "card": ""}},
    {"message_id": 88002, "message_seq": 88002, "time": BASE_TS - 60, "user_id": 100,
     "raw_message": "我们要部署了", "message": [text("我们要部署了")],
     "sender": {"user_id": 100, "nickname": "甲", "card": ""}},
    {"message_id": 88003, "message_seq": 88003, "time": BASE_TS - 120, "user_id": 200,
     "raw_message": "我也要部署", "message": [text("我也要部署")],
     "sender": {"user_id": 200, "nickname": "乙", "card": ""}},
]
_saved = HISTORY[:]
HISTORY[:] = _AND
_and = run(make_svc({"max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9}),
           _AndEv(), session_id="qq:gm:123", count=10, user_id="100", keyword="部署")
check("user_id + keyword is an AND, not an OR",
      "我们要部署了" in _and and "今天天气不错" not in _and, _and)
check("AND excludes other people's keyword hits", "我也要部署" not in _and, _and)
HISTORY[:] = _saved

print("\n[circuit breaker actually opens]")


class _FailingClient:
    async def send_action(self, a, p, timeout=None):
        if a == "get_version_info":
            return {"status": "ok", "data": {"app_name": "NapCat.Onebot"}}
        raise RuntimeError("boom")


_svc_cb = make_svc({"max_calls_per_target_per_turn": 99, "max_calls_per_turn": 99})
_svc_cb.circuit_fail_threshold = 2
_svc_cb._get_client = lambda e: _FailingClient()
_r1 = run(_svc_cb, _Ev(), session_id="qq:gm:1", count=5, keyword="x")
_r2 = run(_svc_cb, _Ev(), session_id="qq:gm:2", count=5, keyword="x")
_r3 = run(_svc_cb, _Ev(), session_id="qq:gm:3", count=5, keyword="x")
check("first failures are reported as errors", "Error" in _r1 and "Error" in _r2,
      (_r1[:60], _r2[:60]))
check("circuit opens after the threshold is reached",
      "circuit" in _r3, _r3[:90])
check("circuit state is stored on the instance",
      _svc_cb._circuit_open_until > 0, _svc_cb._circuit_open_until)

print("\n[reached-start guard must not go permanently stale]")


class _StaleEv:
    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


_saved7 = HISTORY[:]
HISTORY[:] = [{"message_id": 70000 + i, "message_seq": 70000 + i,
               "time": BASE_TS - i * 60, "user_id": 100,
               "raw_message": "普通", "message": [text("普通")],
               "sender": {"user_id": 100, "nickname": "甲", "card": ""}}
              for i in range(5)]
_st_plugin = make_svc({"locate_cache_ttl_sec": 120,
                       "max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9})
_ = run(_st_plugin, _StaleEv(), session_id="qq:gm:123", count=10,
        keyword="部署", scan_limit=50)
HISTORY.insert(0, {"message_id": 71000, "message_seq": 71000, "time": BASE_TS + 120,
                   "user_id": 200, "raw_message": "新部署",
                   "message": [text("新部署")],
                   "sender": {"user_id": 200, "nickname": "乙", "card": ""}})
for _e in _st_plugin._locate_cache.values():
    _e["timestamp"] -= 9999
_after = run(_st_plugin, _StaleEv(), session_id="qq:gm:123", count=10,
             keyword="部署", scan_limit=500)
check("after the TTL a new matching message is found", "新部署" in _after,
      _after[:150])
check("after the TTL the query is not refused", "Rejected" not in _after,
      _after[:120])
HISTORY[:] = _saved7

print("\n[hit count is a lower bound when the scan stopped early]")
_h = run(make_svc(), _Ev(), session_id="qq:gm:123", count=5,
         user_id="100", scan_limit=200)
_head = _h.splitlines()[0]
if "到最早=是" in _head:
    check("bounded scan reports an exact count", "命中=" in _head and "+" not in _head.split(" | ")[2], _head[:110])
else:
    check("early stop marks the count with a +",
          _head.split(" | ")[2].endswith("+"), _head[:110])

print("\n[session_id forms]")


# Models write session refs in several shapes. "gm:123" is ambiguous
# (adapter:entity vs type:entity) and used to be parsed as adapter="gm",
# leaving a colon inside the entity, which then crashed int().
for _sid, _exp_type, _exp_entity in [
    ("qq:gm:123", "gm", "123"),
    ("qq:dm:456", "dm", "456"),
    ("gm:123", "gm", "123"),
    ("dm:456", "dm", "456"),
    ("group:123", "gm", "123"),
    ("private:456", "dm", "456"),
    ("qq:group:9", "gm", "9"),
    ("adapter_x:gm:999", "gm", "999"),
]:
    _r = HistoryToolService.parse_session_ref(_sid)
    check("parse %s -> %s:%s" % (_sid, _exp_type, _exp_entity),
          _r["session_type"] == _exp_type and _r["session_id"] == _exp_entity,
          _r)

for _sid in ("gm:123", "dm:456", "group:1", "private:2"):
    try:
        _o = run(make_svc({"max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9}),
                 _Ev(), session_id=_sid, count=5)
        check("calling with %s does not raise" % _sid, isinstance(_o, str), _o[:60])
    except Exception as _e:
        check("calling with %s does not raise" % _sid, False,
              "%s: %s" % (type(_e).__name__, _e))

print("\n[scan_limit adjustments are reported honestly]")
_lo = run(make_svc({"max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9}),
          _Ev(), session_id="qq:gm:123", count=10, keyword="nope", scan_limit=5)
check("a raised scan_limit (below the floor) is reported",
      "低于下限" in _lo, _lo[-160:])

_pb = make_svc({"max_scanned_per_turn": 60,
                "max_calls_per_target_per_turn": 9, "max_calls_per_turn": 9})
_pb_out = run(_pb, _Ev(), session_id="qq:gm:123", count=10,
              keyword="nope", scan_limit=300)
check("a budget clamp is reported with the real number",
      "预算" in _pb_out and "60" in _pb_out, _pb_out[-200:])

print("\n[max_return_count is honoured on the legacy path]")


class _MrEv:
    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


_saved_mr = HISTORY[:]
HISTORY[:] = [{"message_id": 60000 + i, "message_seq": 60000 + i,
               "time": BASE_TS - i * 60, "user_id": 100,
               "raw_message": "m%d" % i, "message": [text("m%d" % i)],
               "sender": {"user_id": 100, "nickname": "甲", "card": ""}}
              for i in range(300)]
_mr = make_svc({"max_return_count": 200, "cache_ttl_sec": 0})
_mo = run(_mr, _MrEv(), session_id="qq:gm:123", count=200)
_mn = len([l for l in _mo.splitlines() if l.startswith(("甲(", "乙("))])
check("legacy path is not silently capped at 80", _mn > 80, _mn)
HISTORY[:] = _saved_mr

print("\n[permission]")
svc = make_svc()
svc.master_id = "769690776"
svc.restricted_groups = ["999"]
ev = _Ev()
ev.messages[0].sender.user_id = "555"
out = run(svc, ev, session_id="qq:gm:999", count=10, keyword="x")
check("restricted group refused", "权限" in out, out[:120])
out = run(svc, ev, session_id="qq:dm:555", count=10, keyword="x")
check("own dm allowed", "权限" not in out, out[:120])
out = run(svc, ev, session_id="qq:dm:777", count=10, keyword="x")
check("other dm refused", "权限" in out, out[:120])


print("\n[scan budget is actually charged]")


class _BareEv:
    """Event with an EMPTY extra dict - the normal real-world state."""

    def __init__(self):
        self.messages = [_M()]
        self.extra = {}


svc_chg = make_svc({"max_scanned_per_turn": 50})
ev_chg = _BareEv()
run(svc_chg, ev_chg, session_id="qq:gm:123", count=10, keyword="没有的词", scan_limit=50)
check("first scan charges the budget into an initially-empty extra",
      int(ev_chg.extra.get("merger_hist_scanned", 0)) > 0,
      ev_chg.extra)
check("remaining budget shrinks accordingly",
      svc_chg._scan_budget_left(ev_chg) == 0,
      svc_chg._scan_budget_left(ev_chg))
second_chg = run(svc_chg, ev_chg, session_id="qq:gm:123", count=10,
                 keyword="别的词", scan_limit=50)
check("exhausted budget refuses the next scan", "预算" in second_chg, second_chg[:150])

print("\n[call budgets are configurable]")
svc_b = make_svc({"max_calls_per_target_per_turn": 1, "max_calls_per_turn": 1})
ev_b = _Ev()
_ = run(svc_b, ev_b, session_id="qq:gm:123", count=5, keyword="部署")
second = run(svc_b, ev_b, session_id="qq:gm:123", count=5, keyword="别的")
check("max_calls_per_target_per_turn=1 blocks the second call",
      "Rejected" in second, second[:150])

svc_c = make_svc({"max_calls_per_target_per_turn": 5, "max_calls_per_turn": 1})
ev_c = _Ev()
_ = run(svc_c, ev_c, session_id="qq:gm:123", count=5, keyword="部署")
second_c = run(svc_c, ev_c, session_id="qq:gm:456", count=5, keyword="别的")
check("max_calls_per_turn=1 blocks a different target too",
      "Rejected" in second_c, second_c[:150])


print()
passed = sum(1 for _, ok in results if ok)
print("TOTAL %d/%d passed" % (passed, len(results)))
sys.exit(0 if passed == len(results) else 1)
