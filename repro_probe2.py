#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""对照实验：真实 round1 后，round2a(不回传) vs round2b(带 reasoning_content)
各连发 N 次，统计 [Upstream 错误率，验证注入是否有效。"""
import json
import sys
import urllib.request

GW = "http://172.17.0.1:24513"
MODEL = "deepseek-v4.1-flash-free"
N = 10

TOOLS = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "查询指定城市的天气",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}]

Q = ("13支队伍打单循环赛，一共需要进行多少场比赛？请先在心里完整推理"
     "（写出每一步推导），然后再用 get_weather 工具查询北京的天气。"
     "推理必须放在思考里完成，最后必须调用工具。")


def post(payload, timeout=120):
    req = urllib.request.Request(
        GW + "/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "x-api-key": "public"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}


def round1():
    payload = {"model": MODEL, "stream": True, "tools": TOOLS,
               "messages": [{"role": "user", "content": Q}]}
    req = urllib.request.Request(
        GW + "/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "x-api-key": "public"},
        method="POST")
    reasoning = ""
    content = ""
    tool_calls = {}
    with urllib.request.urlopen(req, timeout=180) as r:
        for raw in r:
            line = raw.decode("utf-8", "ignore").strip()
            if not line.startswith("data:"):
                continue
            d = line[5:].strip()
            if d == "[DONE]" or not d:
                continue
            try:
                j = json.loads(d)
            except Exception:
                continue
            for ch in j.get("choices") or []:
                delta = ch.get("delta") or {}
                if delta.get("reasoning_content"):
                    reasoning += delta["reasoning_content"]
                if delta.get("reasoning"):
                    reasoning += delta["reasoning"]
                if delta.get("content"):
                    content += delta["content"]
                for tc in delta.get("tool_calls") or []:
                    i = tc.get("index", 0)
                    e = tool_calls.setdefault(
                        i, {"id": "", "name": "", "args": ""})
                    if tc.get("id"):
                        e["id"] = tc["id"]
                    fn = tc.get("function") or {}
                    if fn.get("name"):
                        e["name"] = fn["name"]
                    if fn.get("arguments"):
                        e["args"] += fn["arguments"]
    tcs = [tool_calls[k] for k in sorted(tool_calls)]
    return reasoning, content, tcs


print("=== round1 ===")
reasoning, content, tcs = round1()
if not tcs:
    print("!! round1 未产生 tool_call")
    sys.exit(1)
print("reasoning 长度:", len(reasoning))
print("tool_call id:", tcs[0]["id"])


def round2(field=None):
    asst = {"role": "assistant", "content": content or "",
            "tool_calls": [{
                "id": t["id"], "type": "function",
                "function": {"name": t["name"], "arguments": t["args"]},
            } for t in tcs]}
    if field and reasoning:
        asst[field] = reasoning
    payload = {"model": MODEL, "stream": False, "tools": TOOLS,
               "messages": [
                   {"role": "user", "content": Q},
                   asst,
                   {"role": "tool", "tool_call_id": tcs[0]["id"],
                    "content": "北京今天晴，22°C"},
               ]}
    code, body = post(payload)
    c = ""
    try:
        c = body["choices"][0]["message"].get("content", "")
    except Exception:
        c = json.dumps(body, ensure_ascii=False)[:150]
    bad = c.startswith("[Upstream") or "error" in c[:60]
    return code, bad, c


bad_a = []
bad_b = []
shown = 0
for i in range(N):
    code, bad, c = round2(None)
    if bad:
        bad_a.append((i, c[:220]))
        if shown < 2:
            print("A#%d 坏样本: HTTP %s %s" % (i, code, c[:200]))
            shown += 1
shown = 0
for i in range(N):
    code, bad, c = round2("reasoning_content")
    if bad:
        bad_b.append((i, c[:220]))
        if shown < 2:
            print("B#%d 坏样本: HTTP %s %s" % (i, code, c[:200]))
            shown += 1

print("\n===== 结果 =====")
print("A 不回传 reasoning_content: %d/%d 失败" % (len(bad_a), N))
print("B 回传 reasoning_content:   %d/%d 失败" % (len(bad_b), N))
