# -*- coding: utf-8 -*-
"""Regression tests for the CQ-escape placeholder filter.

SnowLuma stores an unresolvable reply target as a synthetic event whose
raw_message is CQ-escaped: cqEscape('[引用消息]') == '&#91;引用消息&#93;'.
The old filter compared the unescaped token, so these fake rows were printed
as ": [引用消息] (msg_id:...)" and crowded real messages out of the window.

Run: python3 tests/test_history_filter.py
"""
import asyncio
import importlib.util
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# --- minimal KiraAI stubs so history_tool.py imports standalone ---
for name in ("core", "core.chat", "core.chat.message_utils"):
    sys.modules[name] = types.ModuleType(name)
sys.modules["core.chat.message_utils"].KiraMessageBatchEvent = type("E", (), {})
hx = types.ModuleType("httpx")
hx.Timeout = lambda **k: None
hx.AsyncClient = object
sys.modules["httpx"] = hx

spec = importlib.util.spec_from_file_location(
    "history_tool", os.path.join(ROOT, "history_tool.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
HistoryToolService = mod.HistoryToolService

TEXT = lambda t: {"type": "text", "data": {"text": t}}


def placeholder(mid):
    return {"message_id": mid, "raw_message": "&#91;引用消息&#93;",
            "message": [TEXT("[引用消息]")], "user_id": 0,
            "time": 1757300100,
            "sender": {"user_id": 0, "nickname": "", "card": ""}}


def real_msg(i):
    return {"message_id": i, "raw_message": "msg %d 看&#91;x&#93;" % i,
            "message": [TEXT("msg %d 看[x]" % i)], "user_id": 100 + i,
            "time": 1757300000 + i,
            "sender": {"user_id": 100 + i, "nickname": "U%d" % i}}


results = []


def check(label, cond, detail=""):
    results.append((label, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + label + ((" — " + str(detail)) if detail else ""))


class _Sender:
    user_id = "769690776"


class _M:
    sender = _Sender()


class _Ev:
    messages = [_M()]
    extra = {}


def make_service(messages):
    svc = HistoryToolService(master_id="", use_ws=False)

    async def fake_fetch(session_type, entity, count):
        return messages[-count:], None

    svc._fetch_http = fake_fetch
    return svc


def call(svc, count):
    ev = _Ev()
    ev.extra = {}
    return asyncio.new_event_loop().run_until_complete(
        svc.get_session_history(ev, session_id="qq:gm:427674145", count=count))


# 1) the exact shape from the 2026-09-08 20:32 log
msgs = [real_msg(i) for i in range(1, 19)] + [placeholder(-i) for i in range(1, 13)]
out = call(make_service(msgs), 10)
lines = [l for l in out.splitlines() if l.strip()]
check("escaped placeholder rows filtered", all("引用消息" not in l for l in lines), lines)
check("over-fetch still returns 10 real messages", len(lines) == 10, len(lines))
check("newest real message kept", lines[-1].startswith("U18:"), lines[-1])
check("CQ entities unescaped for display",
      "看[x]" in out and "&#91;" not in out, out.splitlines()[0])

# 2) direct helpers
svc = HistoryToolService(master_id="", use_ws=False)
check("_is_placeholder catches escaped raw", svc._is_placeholder(placeholder(1)) is True)
check("_is_placeholder keeps a real message", svc._is_placeholder(real_msg(7)) is False)
check("_message_to_text unescapes entities", "看[x]" in svc._message_to_text(real_msg(7)))

# 3) the get_msg refresh result must actually be used
svc = HistoryToolService(master_id="", use_ws=True)
fresh = {"message_id": 1, "raw_message": "refreshed text",
         "message": [TEXT("refreshed text")], "user_id": 101, "time": 1757300001,
         "sender": {"user_id": 101, "nickname": "U1"}}


async def fake_ws(client, session_type, entity, count):
    return [placeholder(1)]


async def fake_get_msg(client, mid):
    return fresh


svc._fetch_ws = fake_ws
svc._get_client = lambda ev: object()
svc._get_msg_ws = fake_get_msg
out2 = call(svc, 5)
check("get_msg refresh now takes effect", "refreshed text" in out2, out2)

# 4) regression: a real forward card / bare reply must NOT be filtered - the
#    bot needs their message_id to re-forward them.
FORWARD = {"message_id": 9001, "raw_message": "[CQ:forward,id=res-abc]",
           "message": [{"type": "forward", "data": {"id": "res-abc"}}],
           "user_id": 222, "time": 1757300200,
           "sender": {"user_id": 222, "nickname": "转发者"}}
BARE_REPLY = {"message_id": 9002, "raw_message": "[CQ:reply,id=123]",
              "message": [{"type": "reply", "data": {"id": "123"}}],
              "user_id": 333, "time": 1757300201,
              "sender": {"user_id": 333, "nickname": "回复者"}}
REAL_TOKEN = {"message_id": 9003, "raw_message": "&#91;引用消息&#93;",
              "message": [TEXT("[引用消息]")], "user_id": 444, "time": 1757300202,
              "sender": {"user_id": 444, "nickname": "真人"}}
svc2 = HistoryToolService(master_id="", use_ws=False)
check("forward card kept (not a placeholder)", svc2._is_placeholder(FORWARD) is False)
check("bare reply kept (not a placeholder)", svc2._is_placeholder(BARE_REPLY) is False)
check("real user text '[引用消息]' kept", svc2._is_placeholder(REAL_TOKEN) is False)
check("synthetic placeholder still filtered", svc2._is_placeholder(placeholder(-1)) is True)
out3 = call(make_service([real_msg(1), FORWARD, placeholder(-2)]), 5)
check("forward message_id present in history output",
      "(msg_id:9001)" in out3 and "转发" in out3, out3)

PLACEHOLDER_REAL_UID = {"message_id": 773116280,
                        "raw_message": "&#91;引用消息&#93;",
                        "message": [TEXT("[引用消息]")], "user_id": 769690776,
                        "time": 1757300300,
                        "sender": {"user_id": 769690776, "nickname": "", "card": "",
                                    "role": "member", "sex": "unknown", "age": 0}}
check("synthetic placeholder with a REAL user_id is filtered",
      svc2._is_placeholder(PLACEHOLDER_REAL_UID) is True)
out4 = call(make_service([real_msg(1), PLACEHOLDER_REAL_UID]), 5)
check("real-uid placeholder absent from history output",
      "773116280" not in out4, out4)


print()
passed = sum(1 for _, ok in results if ok)
print("TOTAL %d/%d passed" % (passed, len(results)))
sys.exit(0 if passed == len(results) else 1)
