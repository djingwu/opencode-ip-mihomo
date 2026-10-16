"""opencode 埋点分析脚本：从 request_trace 表出性能/质量报告。

用法（服务器上）：
    python3 /tmp/trace_report.py                 # 最近 1 小时
    python3 /tmp/trace_report.py --hours 24      # 最近 24 小时
    python3 /tmp/trace_report.py --hours 6 --model mimo-v2.6-flash-free
"""
import argparse
import collections
import json
import sqlite3
import sys
import time

DB = "/usr/local/ai/opencode-ip-mihomo/data/metrics.db"
COLS = ["id", "ts", "ts_ms", "model", "endpoint", "client", "is_stream", "client_tools",
        "proxy_ip", "egress_ip", "ttft_ms", "total_ms", "chunks", "out_bytes",
        "status", "attempt", "finish_reason", "downstream_tool_calls",
        "upstream_status", "error_type", "prompt_tokens", "completion_tokens"]


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))]


def secs(ms):
    return (ms or 0) / 1000.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=1)
    ap.add_argument("--model", default="")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--client", default="")
    ap.add_argument("--json", action="store_true",
                    help="输出 JSON 摘要（便于外部告警/看板消费）")
    args = ap.parse_args()

    since_ms = int((time.time() - args.hours * 3600) * 1000)
    con = sqlite3.connect("file:%s?mode=ro" % args.db, uri=True)
    cur = con.cursor()

    where = ["ts_ms >= ?"]
    params = [since_ms]
    if args.model:
        where.append("model = ?")
        params.append(args.model)
    if args.client:
        where.append("client = ?")
        params.append(args.client)
    rows = cur.execute(
        "SELECT %s FROM request_trace WHERE %s ORDER BY id DESC"
        % (",".join(COLS), " AND ".join(where)),
        tuple(params),
    ).fetchall()
    con.close()

    if not rows:
        msg = "窗口内无 trace 记录（检查 TRACE_ENABLED / 网关是否有流量）"
        if args.json:
            print(json.dumps({"ok": False, "reason": msg}, ensure_ascii=False))
        else:
            print(msg)
        return

    R = [dict(zip(COLS, r)) for r in rows]
    n = len(R)
    print("=" * 72)
    print("opencode 埋点报告 | 窗口=最近%d小时 | 记录=%d 条 | %s ~ %s"
          % (args.hours, n, R[-1]["ts"], R[0]["ts"]))
    print("=" * 72)

    # ---------- 1. 总览 ----------
    ok = [r for r in R if r["status"] == "ok"]
    bad = [r for r in R if r["status"] != "ok"]
    stream = [r for r in R if r["is_stream"]]
    print("\n[总览]")
    print("  请求 %d | 成功 %d (%.1f%%) | 失败 %d (%.1f%%)"
          % (n, len(ok), len(ok) * 100.0 / n, len(bad), len(bad) * 100.0 / n))
    print("  流式 %d (%.1f%%) | 非流式 %d"
          % (len(stream), len(stream) * 100.0 / n, n - len(stream)))
    print("  客户端自带工具 %d (%.1f%%) | 网关注入工具 %d"
          % (sum(1 for r in R if r["client_tools"]),
             sum(1 for r in R if r["client_tools"]) * 100.0 / n,
             sum(1 for r in R if not r["client_tools"])))

    # ---------- 2. 错误分布 ----------
    errs = collections.Counter(r["error_type"] or "-" for r in bad)
    if errs:
        print("\n[错误类型分布]")
        for k, v in errs.most_common():
            print("  %-14s %d" % (k, v))
    codes = collections.Counter(r["upstream_status"] for r in R
                                 if r["upstream_status"])
    if codes:
        print("\n[上游状态码]")
        for k, v in sorted(codes.items(), key=lambda x: -x[1]):
            print("  HTTP %-5s %d" % (k, v))

    # ---------- 3. 性能 ----------
    print("\n[性能]")
    f = lambda k: sorted([r[k] for r in R if r[k] is not None])
    for key, name in (("ttft_ms", "TTFT"), ("total_ms", "总耗时")):
        v = f(key)
        if v:
            print("  %-7s n=%-5d min=%6.2fs p50=%6.2fs p90=%6.2fs p99=%7.2fs max=%7.2fs avg=%6.2fs"
                  % (name, len(v), secs(v[0]), secs(pct(v, .5)), secs(pct(v, .9)),
                     secs(pct(v, .99)), secs(v[-1]), sum(v) / len(v) / 1000.0))

    # TTFT 与 total 的差 = "生成段"耗时，用来看 prefill vs 生成谁是大头
    gen = sorted([r["total_ms"] - r["ttft_ms"] for r in R
                  if r["ttft_ms"] and r["total_ms"]])
    if gen:
        print("  生成段   n=%-5d min=%6.2fs p50=%6.2fs p90=%6.2fs max=%7.2fs  (total - ttft)"
              % (len(gen), secs(gen[0]), secs(pct(gen, .5)), secs(pct(gen, .9)), secs(gen[-1])))

    # ---------- 3b. 客户端维度 ----------
    print("\n  按客户端：")
    by_cli = collections.defaultdict(list)
    for r in R:
        by_cli[r["client"] or "unknown"].append(r)
    hdr2 = "  %-12s %5s %7s %9s %9s %9s" % ("client", "n", "ok%", "ttft.p50",
                                             "total.p50", "total.max")
    print(hdr2)
    print("  " + "-" * (len(hdr2) - 2))
    for cl in sorted(by_cli, key=lambda x: -len(by_cli[x])):
        rs = by_cli[cl]
        n_c = len(rs)
        okc = sum(1 for r in rs if r["status"] == "ok")
        tt = sorted([r["ttft_ms"] for r in rs if r["ttft_ms"] is not None])
        to = sorted([r["total_ms"] for r in rs if r["total_ms"] is not None])
        print("  %-12s %5d %6.1f%% %9s %9s %9s"
              % (cl[:12], n_c, okc * 100.0 / n_c,
                 ("%.1fs" % secs(pct(tt, .5))) if tt else "-",
                 ("%.1fs" % secs(pct(to, .5))) if to else "-",
                 ("%.1fs" % secs(to[-1])) if to else "-"))

    print("\n  按模型（仅成功请求）：")
    by_model = collections.defaultdict(list)
    for r in ok:
        by_model[r["model"]].append(r)
    hdr = "  %-34s %5s %8s %8s %8s %8s %8s" % ("model", "n", "ttft.p50", "total.p50", "total.p90", "total.max", "chunks")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for m in sorted(by_model, key=lambda x: -len(by_model[x])):
        rs = by_model[m]
        t = sorted([r["total_ms"] for r in rs])
        tt = sorted([r["ttft_ms"] for r in rs if r["ttft_ms"] is not None])
        ch = sorted([r["chunks"] for r in rs])
        print("  %-34s %5d %8s %8s %8s %8s %8d"
              % (m[:34], len(rs),
                 "%.1fs" % secs(pct(tt, .5)) if tt else "-",
                 "%.1fs" % secs(pct(t, .5)),
                 "%.1fs" % secs(pct(t, .9)),
                 "%.1fs" % secs(t[-1]),
                 pct(ch, .5)))

    # ---------- 4. 质量 ----------
    print("\n[质量]")
    fr = collections.Counter(r["finish_reason"] or "-" for r in R)
    print("  finish_reason 分布:")
    for k, v in fr.most_common():
        print("    %-16s %d" % (k, v))
    # 注意区分：client_tools=1 的下游 tool_calls 是客户端（如 opencode CLI）合法
    # 发起的工作流，不是故障；只有 client_tools=0 却收到 tool_calls 才是
    # 「网关注入的工具被模型误用 → 下游接不住 → 卡死」的质量问题。
    dtc = [r for r in R if r["downstream_tool_calls"]]
    dtc_injected = [r for r in dtc if not r["client_tools"]]
    dtc_client = [r for r in dtc if r["client_tools"]]
    print("  下游收到 tool_calls             : %d / %d" % (len(dtc), n))
    print("    ├─ 客户端自带工具（正常工作流）: %d" % len(dtc_client))
    print("    └─ 工具由网关注入（卡死风险）  : %d / %d (%.2f%%)"
          % (len(dtc_injected), n, len(dtc_injected) * 100.0 / n))
    empty = [r for r in R if r["error_type"] == "empty_stream"]
    if empty:
        print("  空流重试失败（empty_stream）  : %d" % len(empty))
    retried = [r for r in R if r["attempt"] and r["attempt"] > 1]
    print("  发生过重试的请求              : %d / %d (%.1f%%)"
          % (len(retried), n, len(retried) * 100.0 / n))
    ac = collections.Counter(r["attempt"] for r in R)
    print("  attempt 分布: %s" % ", ".join("a%d=%d" % (k, v) for k, v in sorted(ac.items())))
    print("  注：流式请求的 prompt/completion tokens 由单独的 model_usage 表聚合，此处可能为 0。")

    # ---------- 5. 慢请求 Top ----------
    print("\n[最慢的 10 条]")
    for r in sorted(R, key=lambda x: -(x["total_ms"] or 0))[:10]:
        print("  %s %-30s total=%6.1fs ttft=%-6s chunks=%-5d attempt=%d %s %s"
              % (r["ts"], (r["model"] or "")[:30], secs(r["total_ms"]),
                 ("%.1fs" % secs(r["ttft_ms"])) if r["ttft_ms"] else "-",
                 r["chunks"] or 0, r["attempt"] or 1, r["status"],
                 r["error_type"] or ""))

    # ---------- 6. 卡死嫌疑明细（只看工具由网关注入的） ----------
    if dtc_injected:
        print("\n[卡死嫌疑明细]（client_tools=0 却收到 tool_calls：网关注入的工具被模型误用）")
        for r in dtc_injected[:20]:
            print("  %s %-30s finish=%-10s ttft=%-6s total=%.1fs"
                  % (r["ts"], (r["model"] or "")[:30], r["finish_reason"] or "-",
                     ("%.1fs" % secs(r["ttft_ms"])) if r["ttft_ms"] else "-",
                     secs(r["total_ms"])))
    elif dtc_client:
        print("\n[客户端自带工具的工具调用] %d 条，属正常工作流，无需关注"
              % len(dtc_client))

    # ---------- 7. 处理量 ----------
    print("\n[吞吐/体量]")
    span_s = (R[0]["ts_ms"] - R[-1]["ts_ms"]) / 1000.0 or 1
    print("  窗口跨度 %.0f 秒，平均 %.2f req/s" % (span_s, n / span_s))
    tb = sum(r["out_bytes"] or 0 for r in R)
    print("  输出总量 %.2f MB，均值 %.1f KB/请求" % (tb / 1e6, tb / n / 1024.0))
    tp = sum(r["prompt_tokens"] or 0 for r in R)
    tc = sum(r["completion_tokens"] or 0 for r in R)
    if tp or tc:
        print("  prompt tokens %d（均值 %.0f）| completion tokens %d（均值 %.0f）"
              % (tp, tp / n, tc, tc / n))
    print()

    # ---------- 8. JSON 摘要（供告警 / 看板消费）----------
    if args.json:
        _ok = len(ok)
        _ttft = sorted([r["ttft_ms"] for r in R if r["ttft_ms"] is not None])
        _tot = sorted([r["total_ms"] for r in R if r["total_ms"] is not None])
        payload = {
            "ok": True,
            "window_hours": args.hours,
            "requests": n,
            "success": _ok,
            "failed": len(bad),
            "success_rate": round(_ok * 100.0 / n, 2),
            "stream_ratio": round(len(stream) * 100.0 / n, 2),
            "retry_ratio": round(len([r for r in R if (r["attempt"] or 1) > 1])
                               * 100.0 / n, 2),
            "ttft_p50_ms": pct(_ttft, .5) if _ttft else None,
            "ttft_p99_ms": pct(_ttft, .99) if _ttft else None,
            "total_p50_ms": pct(_tot, .5) if _tot else None,
            "total_p99_ms": pct(_tot, .99) if _tot else None,
            "total_max_ms": _tot[-1] if _tot else None,
            "errors": dict(errs),
            "upstream_codes": {str(k): v for k, v in codes.items()},
            "stall_risk_injected_tools": len(dtc_injected),
            "by_client": {
                cl: {
                    "n": len(rs),
                    "ok": sum(1 for r in rs if r["status"] == "ok"),
                    "avg_ms": round(sum(r["total_ms"] or 0 for r in rs) / len(rs), 1),
                } for cl, rs in sorted(by_cli.items())
            },
            "by_model": {
                m: {
                    "n": len(rs),
                    "p50_ms": pct(sorted([r["total_ms"] for r in rs]), .5),
                } for m, rs in sorted(by_model.items())
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except sqlite3.Error as e:
        print("数据库不可用: %s（表可能还没建，先让网关处理几个请求）" % e)
        sys.exit(1)
