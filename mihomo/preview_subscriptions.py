#!/usr/bin/env python3
"""交互式订阅节点预览（Windows bat 的稳定后端）。

用法（双击 preview-subscriptions.bat 或命令行）：
    python preview_subscriptions.py
"""
import sys
import tempfile
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from list_subscription_nodes import (  # noqa: E402
    DEFAULT_EXCLUDE_KEYWORDS,
    DEFAULT_UA,
    fetch_subscription,
    parse_node_names,
    summarize,
)

BANNER = """================================================
  OpenCode IP Rotator - Subscription Preview
  Enter sub links, view full node list
  (X = placeholder node, will be auto-excluded)
================================================"""


def main() -> int:
    print(BANNER)
    print()
    print("Enter subscription URLs (Clash sub links), one per line.")
    print("Press Enter on an empty line to start; type q to quit.")
    print()

    urls: list[str] = []
    count = 0
    while True:
        try:
            raw = input(f"Sub URL {count+1}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not raw:
            break
        if raw.lower() == "q":
            print("Quit.")
            return 0
        # 支持一次粘贴多个链接（空格/逗号分隔），逐条校验
        added = 0
        for one in raw.replace(",", " ").split():
            one = one.strip()
            if one.startswith(("http://", "https://")):
                urls.append(one)
                added += 1
            else:
                print(f"  [跳过] 不是有效的 http(s) 链接: {one}")
        if added:
            count += added
            print(f"  已添加 {count} 个订阅（继续输入下一个，或直接回车结束）")

    if not urls:
        print("[INFO] No URLs entered. Exiting.")
        return 0

    # 先校验 pyyaml（解析 Clash YAML 订阅需要）
    try:
        import yaml  # noqa: F401
    except ImportError:
        print("[INFO] Installing pyyaml ...")
        os.system(f'"{sys.executable}" -m pip install pyyaml -q')
        try:
            import yaml  # noqa: F401
        except ImportError:
            print("[WARN] pyyaml unavailable; YAML subscriptions may not parse.")

    all_names: list[str] = []
    failed_urls: list[str] = []

    for i, url in enumerate(urls, start=1):
        print(f"\n=== Subscription {i}: {url}")
        try:
            content = fetch_subscription(url, DEFAULT_UA)
            names, fmt = parse_node_names(content)
        except Exception as e:
            print(f"  [FAIL] fetch/parse error: {e}")
            failed_urls.append(url)
            continue

        if not names:
            print(f"  [WARN] No nodes parsed (format: {fmt}). Subscription may need login/UA/expired.")
            failed_urls.append(url)
            continue

        print(f"  Format: {fmt} | Nodes: {len(names)}")
        for n in names:
            mark = "X" if any(k in n for k in DEFAULT_EXCLUDE_KEYWORDS) else "OK"
            print(f"    [{mark}]  {n}")
        ph, ok = summarize(names, DEFAULT_EXCLUDE_KEYWORDS)
        print(f"  >> placeholder: {ph} | usable: {ok}")
        all_names.extend(names)

    print("\n================ Summary ================")
    if all_names:
        ph, ok = summarize(all_names, DEFAULT_EXCLUDE_KEYWORDS)
        print(f"Total: {len(all_names)} | placeholder/invalid: {ph} | usable: {ok}")
        print(f"Unique: {len(set(all_names))} ({len(all_names) - len(set(all_names))} duplicates will be deduped by mihomo)")
    if failed_urls:
        print(f"[WARN] {len(failed_urls)} subscription(s) failed:")
        for u in failed_urls:
            print(f"  - {u}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
