#!/usr/bin/env python3
"""根据机场订阅链接列表生成 mihomo/config.yaml（支持多个订阅，扩大节点池）。

用法：
    python3 generate_mihomo_config.py --secret <控制密码> URL1 URL2 URL3 ...
    python3 generate_mihomo_config.py --group-type load-balance --secret <密码> URL1 URL2 ...
    python3 generate_mihomo_config.py --exclude-filter '(?i)剩余|到期|重置' --secret <密码> URL1 URL2 ...
"""
import argparse
import secrets
import sys
from pathlib import Path

DEFAULT_OUTPUT = Path(__file__).resolve().parent / "config.yaml"

# 默认排除"信息占位节点"：剩余流量/套餐到期/官网/客服 这类能连通但非真实出口的节点
DEFAULT_EXCLUDE_FILTER = r"(?i)剩余流量|剩余|重置|套餐到期|到期|过期|欠费|流量提醒|距离下次|官网|客服|邮箱|联系|公告|通知|群组|订阅更新|刷新订阅|连接不上|无法连接"


def build_config(sub_urls: list[str], group_type: str, secret: str, exclude_filter: str) -> dict:
    providers = {}
    for i, url in enumerate(sub_urls, start=1):
        name = f"subscription{i}"
        providers[name] = {
            "type": "http",
            "url": url,
            "interval": 3600,
            # 部分机场按 User-Agent 区分返回内容（非 Clash 客户端 UA 可能拿不到节点）
            "user-agent": "clash-verge/v2.0.0",
            "path": f"./providers/{name}.yaml",
            "health-check": {
                "enable": True,
                "interval": 300,
                "url": "https://www.gstatic.com/generate_204",
            },
        }

    # mihomo 不允许代理组同时缺少 use/proxies。空部署使用 DIRECT 作内部哨兵，
    # 后续添加订阅或免费 provider 时 panel_manager 会自动替换掉它。
    group = {"name": "ROTATOR"}
    if providers:
        group["use"] = list(providers.keys())
    else:
        group["proxies"] = ["DIRECT"]
    if exclude_filter:
        group["exclude-filter"] = exclude_filter
    if group_type == "load-balance":
        group["type"] = "load-balance"
        group["strategy"] = "round-robin"
        group["disable-udp"] = False
    else:
        group["type"] = "select"

    return {
        "mixed-port": 7890,
        # allow-lan 必须为 true：让 mixed-port 监听 0.0.0.0，
        # 否则只监听容器内 127.0.0.1，rotator/proxy 独立容器将无法访问
        "allow-lan": True,
        "bind-address": "*",
        "mode": "rule",
        "log-level": "info",
        "ipv6": False,
        "unified-delay": True,
        "tcp-concurrent": True,
        "external-controller": "0.0.0.0:9090",
        "secret": secret,
        "proxy-providers": providers,
        "proxy-groups": [group],
        "rules": ["MATCH,ROTATOR"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="生成 mihomo/config.yaml（多订阅）")
    parser.add_argument("urls", nargs="*", help="机场订阅链接（一个或多个）")
    parser.add_argument("--empty", action="store_true", help="生成不含订阅 provider 的空节点池配置")
    parser.add_argument("--secret", default="", help="external-controller 控制密码")
    parser.add_argument(
        "--group-type",
        choices=["select", "load-balance"],
        default="select",
        help="select=429时显式切换节点(默认)；load-balance=每请求自动轮换节点",
    )
    parser.add_argument(
        "--exclude-filter",
        default=DEFAULT_EXCLUDE_FILTER,
        help="按名称正则排除节点（默认排除'剩余流量/套餐到期'等信息占位节点）；设为空字符串不排除",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="输出文件路径")
    args = parser.parse_args()

    if not args.urls and not args.empty:
        parser.error("至少提供一个订阅链接，或使用 --empty 生成空配置")

    if not args.secret:
        args.secret = secrets.token_hex(8)
        print(f"[提示] 未指定 --secret，已自动生成: {args.secret}", file=sys.stderr)

    config = build_config(args.urls, args.group_type, args.secret, args.exclude_filter)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    # 手动序列化以保证可读性（保留字典顺序）
    lines = [
        "# ============================================================",
        "# 由 generate_mihomo_config.py 自动生成 — OpenCode IP Rotator",
        "# 订阅数量: %d | 组类型: %s" % (len(args.urls), args.group_type),
        "# ============================================================",
        "",
        f"mixed-port: {config['mixed-port']}",
        f"allow-lan: {str(config['allow-lan']).lower()}",
        f"bind-address: \"{config['bind-address']}\"",
        f"mode: {config['mode']}",
        f"log-level: {config['log-level']}",
        f"ipv6: {str(config['ipv6']).lower()}",
        "unified-delay: true",
        "tcp-concurrent: true",
        "",
        "external-controller: 0.0.0.0:9090",
        f"secret: \"{config['secret']}\"",
        "",
        "proxy-providers:",
    ]
    if not config["proxy-providers"]:
        lines[-1] = "proxy-providers: {}"
    for name, p in config["proxy-providers"].items():
        lines += [
            f"  {name}:",
            "    type: http",
            f"    url: \"{p['url']}\"",
            f"    interval: {p['interval']}",
            f"    user-agent: \"{p.get('user-agent', '')}\"",
            f"    path: {p['path']}",
            "    health-check:",
            f"      enable: {str(p['health-check']['enable']).lower()}",
            f"      interval: {p['health-check']['interval']}",
            f"      url: {p['health-check']['url']}",
        ]
    lines += ["", "proxy-groups:"]
    g = config["proxy-groups"][0]
    lines += [
        "  - name: ROTATOR",
        f"    type: {g['type']}",
    ]
    if g.get("exclude-filter"):
        lines.append(f"    exclude-filter: '{g['exclude-filter']}'")
    if g.get("strategy"):
        lines.append(f"    strategy: {g['strategy']}")
        lines.append(f"    disable-udp: {str(g['disable-udp']).lower()}")
    if g.get("use"):
        lines.append("    use:")
        for name in g["use"]:
            lines.append(f"      - {name}")
    else:
        lines.append("    proxies:")
        for name in g.get("proxies", ["DIRECT"]):
            lines.append(f"      - {name}")
    lines += ["", "# 全部流量走 ROTATOR 组", "rules:", "  - MATCH,ROTATOR", ""]

    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] 已生成 {out}")
    print(f"     节点池来自 {len(args.urls)} 个订阅，组类型: {g['type']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
