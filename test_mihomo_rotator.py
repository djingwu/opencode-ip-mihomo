#!/usr/bin/env python3
"""mihomo 节点切换引擎 — 本地冒烟测试

前提：本机已启动 mihomo（mixed-port 7890 + external-controller 9090，ROTATOR 组）。
用法：
    python test_mihomo_rotator.py            # 只读：列出节点/当前节点/存活状态
    python test_mihomo_rotator.py --switch   # 切到下一个健康节点并验证 IP 变更
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ.setdefault("MIHOMO_API_URL", "http://127.0.0.1:9090")
os.environ.setdefault("MIHOMO_GROUP", "ROTATOR")
os.environ.setdefault("MIHOMO_OUTBOUND_PROXY", "http://127.0.0.1:7890")

from rotator import (  # noqa: E402
    mihomo_alive_nodes,
    mihomo_good_nodes,
    mihomo_current_node,
    mihomo_group_nodes,
    mihomo_switch_next_node,
    is_mihomo_enabled,
    get_public_ip_via_proxy,
)


def main() -> int:
    if not is_mihomo_enabled():
        print("[FAIL] MIHOMO_API_URL 未配置，无法测试")
        return 1

    nodes = mihomo_group_nodes()
    print(f"ROTATOR 组节点数: {len(nodes)}")
    if not nodes:
        print("[FAIL] 组里没有节点。检查 mihomo 配置/机场订阅是否加载成功。")
        return 1

    alive = mihomo_alive_nodes()
    print(f"存活节点数: {len(alive)} (mihomo health-check)")
    good = mihomo_good_nodes()
    print(f"延迟达标节点数: {len(good)} (alive 且 <= MIHOMO_MAX_LATENCY)")
    current = mihomo_current_node()
    print(f"当前节点: {current}")

    ip = get_public_ip_via_proxy({"http": os.environ["MIHOMO_OUTBOUND_PROXY"], "https": os.environ["MIHOMO_OUTBOUND_PROXY"]})
    print(f"当前出口 IP: {ip}")

    if "--switch" in sys.argv:
        print("\n==> 切换到下一个健康节点...")
        for _ in range(3):
            node = mihomo_switch_next_node()
            if not node:
                print("[FAIL] 切换失败")
                return 1
            time.sleep(2)
            new_ip = get_public_ip_via_proxy({"http": os.environ["MIHOMO_OUTBOUND_PROXY"], "https": os.environ["MIHOMO_OUTBOUND_PROXY"]})
            print(f"  已切到: {node} | 出口 IP: {new_ip}")
            if new_ip and new_ip != ip:
                print(f"[OK] IP 变更成功: {ip} -> {new_ip}")
                return 0
            print("  警告: IP 未变化，继续尝试下一个节点...")
        print("[FAIL] 多次切换后 IP 仍未变化")
        return 1

    print("\n[OK] 只读检查完成。加 --switch 参数可实际切换节点。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
