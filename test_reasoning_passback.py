#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""reasoning_content 回传链路单测（直接 import server.py，不连网络）。"""
import json
import sys

sys.path.insert(0, ".")

try:
    import server  # noqa
except Exception as exc:  # pragma: no cover
    print("import server 失败（可接受，环境差异）:", exc)
    server = None

if server is not None:
    put = server._reasoning_cache_put
    get = server._reasoning_cache_get
    inject = server.inject_reasoning_passback

    fails = []

    def check(name, cond):
        print(("PASS " if cond else "FAIL ") + name)
        if not cond:
            fails.append(name)

    # 1. put/get 往返
    put("call_test1", "推理文本 ABC")
    check("put/get 往返", get("call_test1") == "推理文本 ABC")

    # 2. 未命中返回空
    check("未命中空串", get("call_nope") == "")

    # 3. inject: 消息缺 reasoning_content → 从缓存补齐，并摘掉 reasoning
    msgs = [
        {"role": "user", "content": "q"},
        {"role": "assistant", "content": "", "tool_calls": [
            {"id": "call_test1", "type": "function",
             "function": {"name": "f", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "call_test1", "content": "ok"},
    ]
    payload = {"messages": msgs}
    n = inject(payload, "deepseek-v4.1-flash-free")
    check("inject 补齐 1 条", n == 1)
    check("注入文本正确", msgs[1].get("reasoning_content") == "推理文本 ABC")
    check("reasoning 字段已移除", "reasoning" not in msgs[1])

    # 4. 已带 reasoning_content 的消息不动
    msgs2 = [{"role": "assistant", "content": "", "tool_calls": [
        {"id": "call_test1", "type": "function",
         "function": {"name": "f", "arguments": "{}"}}],
        "reasoning_content": "客户端自带"}]
    payload2 = {"messages": msgs2}
    n2 = inject(payload2, "m")
    check("已有 reasoning_content 不覆盖", n2 == 0
          and msgs2[0]["reasoning_content"] == "客户端自带")

    # 5. 缓存未命中但客户端带 reasoning 字段 → 字段名转正
    msgs3 = [{"role": "assistant", "content": "", "tool_calls": [
        {"id": "call_unknown_x", "type": "function",
         "function": {"name": "f", "arguments": "{}"}}],
        "reasoning": "openrouter 风格"}]
    payload3 = {"messages": msgs3}
    n3 = inject(payload3, "m")
    check("reasoning 转 reasoning_content", n3 == 1
          and msgs3[0].get("reasoning_content") == "openrouter 风格"
          and "reasoning" not in msgs3[0])

    # 6. 无 tool_calls / 非 assistant 消息不注入
    msgs4 = [{"role": "assistant", "content": "纯文本"},
             {"role": "user", "content": "q"}]
    n4 = inject({"messages": msgs4}, "m")
    check("无 tool_calls 不注入", n4 == 0)

    # 7. 过期失效（复原用捕获的原始值，避免写死默认值）
    _orig_ttl = server.REASONING_PASSBACK_TTL
    server.REASONING_PASSBACK_TTL = 0.0
    check("TTL 过期失效", get("call_test1") == "")
    server.REASONING_PASSBACK_TTL = _orig_ttl

    # 8. 超量淘汰不抛异常
    try:
        for i in range(server.REASONING_PASSBACK_MAX + 50):
            put("call_bulk_%d" % i, "t")
        ok8 = get("call_bulk_0") == ""  # 最早的被淘汰
        check("超量淘汰", ok8)
    except Exception as exc:
        check("超量淘汰异常 %s" % exc, False)

    # 9. _collect_chat_stream_blocking 双字段 + 缓存（用假 response）
    class FakeResp:
        def __init__(self, chunks):
            self._c = [c.encode() for c in chunks]
        def iter_lines(self):
            return iter(self._c)

    chunks = []
    chunks.append(json.dumps({"id": "r1", "created": 1, "choices": [
        {"index": 0, "delta": {"role": "assistant", "reasoning_content": "思考前半"}}]}))
    chunks.append(json.dumps({"id": "r1", "created": 1, "choices": [
        {"index": 0, "delta": {"reasoning_content": "，后半"}}]}))
    chunks.append(json.dumps({"id": "r1", "created": 1, "choices": [
        {"index": 0, "delta": {"tool_calls": [
            {"index": 0, "id": "call_tc9", "type": "function",
             "function": {"name": "f", "arguments": "{\"a\":1}"}}]}}]}))
    chunks.append(json.dumps({"id": "r1", "created": 1, "choices": [
        {"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}))
    chunks.append("data: [DONE]")
    res = server._collect_chat_stream_blocking(FakeResp([("data: " + c) if c != "data: [DONE]" else c for c in chunks]), "deepseek")
    msg = res["choices"][0]["message"]
    check("聚合补 reasoning_content", msg.get("reasoning_content") == "思考前半，后半")
    check("聚合保留 reasoning 兼容", msg.get("reasoning") == "思考前半，后半")
    check("聚合写入缓存", get("call_tc9") == "思考前半，后半")


# ---- 追加用例（residual 修复） ------------------------------------------
if server is not None:
    inject2 = server.inject_reasoning_passback
    build_err = server.build_upstream_stream_error_sse

    put("call_res1", "真实思维文本")
    _p_res = {"model": "deepseek-v4.1-flash-free", "messages": [
        {"role": "assistant", "tool_calls": [{"id": "call_res1", "type": "function",
                                              "function": {"name": "f", "arguments": "{}"}}]},
        {"role": "assistant", "tool_calls": [{"id": "call_missing", "type": "function",
                                              "function": {"name": "f", "arguments": "{}"}}]},
    ]}
    _n = inject2(_p_res, "deepseek-v4.1-flash-free")
    check("vendor 兜底占位符注入", _n == 2)
    check("占位符内容正确", _p_res["messages"][1].get("reasoning_content") == "tool call")
    check("缓存命中不被占位覆盖", _p_res["messages"][0].get("reasoning_content") == "真实思维文本")

    _p_nonvendor = {"model": "gpt-5-nano", "messages": [
        {"role": "assistant", "tool_calls": [{"id": "call_missing2", "type": "function",
                                              "function": {"name": "f", "arguments": "{}"}}]},
    ]}
    _n2 = inject2(_p_nonvendor, "gpt-5-nano")
    check("非 vendor 模型不注入", _n2 == 0 and "reasoning_content" not in _p_nonvendor["messages"][0])

    _p_kimi = {"model": "kimi-k2-free", "messages": [
        {"role": "assistant", "tool_calls": [{"id": "call_x", "type": "function",
                                              "function": {"name": "f", "arguments": "{}"}}]},
    ]}
    check("kimi 命中兜底", inject2(_p_kimi, "kimi-k2-free") == 1)

    check("TTL 默认 24h", abs(server.REASONING_PASSBACK_TTL - 86400) < 1e-6)

    _err = build_err(400, 'Upstream request failed: reasoning_content ...', "deepseek-v4.1-flash-free")
    check("error 模式含结构化 error", '"error"' in _err and "choices" not in _err and '[Upstream 400]' in _err)

    server.STREAM_UPSTREAM_ERROR_MODE = "content"
    _old = build_err(400, "boom", "m")
    server.STREAM_UPSTREAM_ERROR_MODE = "error"
    check("content 模式保持旧行为", '"choices"' in _old and '"finish_reason":"stop"' in _old)


if server is not None:
    print("\n%d failed" % len(fails))
    sys.exit(1 if fails else 0)
else:
    print("server import 失败，跳过（改为服务器容器内测）")
    sys.exit(2)
