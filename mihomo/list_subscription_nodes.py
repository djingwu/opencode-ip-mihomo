#!/usr/bin/env python3
"""拉取并列出机场订阅中的节点，用于部署前确认。

支持 Clash YAML 订阅和 base64/纯文本 URI 订阅（vmess/vless/trojan/ss/hysteria2/anytls 等）。
占位节点（剩余流量/套餐到期类）会打 ✗ 标记，解析失败的条目也会单独提示。

用法：
    python3 list_subscription_nodes.py URL1 URL2 ...
    python3 list_subscription_nodes.py --user-agent "clash-verge/v2.0.0" URL
    python3 list_subscription_nodes.py --keyword 官方 --keyword 测试 URL
"""
import argparse
import base64
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Optional, Tuple

DEFAULT_UA = "clash-verge/v2.0.0"

# 与 rotator.py 的 MIHOMO_EXCLUDE_KEYWORDS 保持一致
DEFAULT_EXCLUDE_KEYWORDS = [
    "剩余流量", "剩余", "重置", "套餐到期", "到期", "过期", "欠费", "流量提醒", "距离下次",
    "官网", "客服", "邮箱", "联系", "公告", "通知", "群组", "订阅更新", "刷新订阅", "连接不上", "无法连接",
]


def fetch_subscription(url: str, user_agent: str) -> bytes:
    """拉取订阅内容（部分机场校验 UA）。"""
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def try_base64_decode(text: str) -> Optional[bytes]:
    """尝试把文本按 base64 解码（自动补 padding）。"""
    s = "".join(text.strip().split())
    if not s:
        return None
    pad = "=" * (-len(s) % 4)
    try:
        return base64.b64decode(s + pad, validate=True)
    except Exception:
        return None


def extract_uri_name(uri: str) -> Optional[str]:
    """从单个代理 URI 中提取节点名。"""
    uri = uri.strip()
    if not uri:
        return None
    # vmess://base64(json) → ps 字段
    if uri.startswith("vmess://"):
        raw = try_base64_decode(uri[len("vmess://"):])
        if raw:
            try:
                data = json.loads(raw.decode("utf-8", errors="replace"))
                ps = data.get("ps")
                if ps:
                    return str(ps)
            except Exception:
                pass
        return None
    # vless/trojan/ss/hysteria2/anytls/snell 等：URL 片段 # 后是节点名
    if "#" in uri:
        frag = uri.rsplit("#", 1)[1]
        try:
            return urllib.parse.unquote(frag)
        except Exception:
            return frag
    # ss:// 老格式（无 # 但内容 base64 编码了 method:pass@host:port）
    if uri.startswith("ss://"):
        raw = try_base64_decode(uri[len("ss://"):])
        if raw:
            return None  # 无名称，无法提取
    return None


def parse_uri_list(text: str) -> List[str]:
    """解析 URI 列表（兼容换行分隔与空白分隔）。"""
    names: List[str] = []
    for line in text.splitlines():
        for token in line.split():
            name = extract_uri_name(token)
            if name:
                names.append(name)
            elif token.lower().startswith(("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://", "anytls://")):
                names.append("未命名节点")
    return names


def parse_node_names(content: bytes) -> Tuple[List[str], str]:
    """解析订阅内容 → (节点名列表, 格式说明)。"""
    text = content.decode("utf-8", errors="replace")
    stripped = text.strip()

    # 1) Clash YAML / JSON 格式（proxies 数组）
    try:
        import yaml  # noqa: F401
        data = yaml.safe_load(stripped)
        if isinstance(data, dict):
            proxies = data.get("proxies")
            if isinstance(proxies, list):
                names = []
                for p in proxies:
                    if isinstance(p, dict) and p.get("name"):
                        names.append(str(p["name"]))
                if names:
                    return names, "clash-yaml/json"
    except ImportError:
        pass
    except Exception:
        pass

    # 2) base64 编码的 URI 列表（机场老格式）
    decoded = try_base64_decode(stripped)
    if decoded:
        names = parse_uri_list(decoded.decode("utf-8", errors="replace"))
        if names:
            return names, "base64-urilist"

    # 3) 纯文本 URI 列表
    names = parse_uri_list(stripped)
    if names:
        return names, "plain-urilist"

    return [], "unknown"


def summarize(names: List[str], keywords: List[str]) -> Tuple[int, int]:
    """返回 (占位节点数, 正常节点数)。"""
    placeholder = sum(1 for n in names if any(k in n for k in keywords))
    return placeholder, len(names) - placeholder


def main() -> int:
    parser = argparse.ArgumentParser(description="列出机场订阅节点（部署前验货）")
    parser.add_argument("urls", nargs="*", help="机场订阅链接（一个或多个）")
    parser.add_argument(
        "--url-file",
        default="",
        help="从文件读取订阅链接（每行一个，避免命令行特殊字符问题，Windows 推荐）",
    )
    parser.add_argument("--user-agent", default=DEFAULT_UA, help="请求 UA（部分机场校验）")
    parser.add_argument("--keyword", action="append", default=[], help="额外占位关键词，可多次指定")
    args = parser.parse_args()

    # 支持从文件读订阅链接（优先），否则用命令行参数
    urls: List[str] = list(args.urls)
    if args.url_file:
        try:
            p = Path(args.url_file)
            if p.exists():
                file_urls = [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
                urls.extend(file_urls)
            else:
                print(f"[警告] 文件不存在: {args.url_file}")
        except Exception as e:
            print(f"[警告] 读取文件失败: {e}")
    if not urls:
        parser.print_help()
        return 1

    keywords = DEFAULT_EXCLUDE_KEYWORDS + args.keyword
    all_names: List[str] = []
    failed_urls: List[str] = []

    for i, url in enumerate(urls, start=1):
        print(f"\n=== 订阅 {i}: {url}")
        try:
            content = fetch_subscription(url, args.user_agent)
            names, fmt = parse_node_names(content)
        except Exception as e:
            print(f"  [失败] 拉取/解析错误: {e}")
            failed_urls.append(url)
            continue

        if not names:
            print(f"  [警告] 未解析到任何节点 (格式: {fmt})。订阅可能需登录/UA 校验/已过期。")
            failed_urls.append(url)
            continue

        print(f"  格式: {fmt} | 节点数: {len(names)}")
        for n in names:
            mark = "✗ 占位" if any(k in n for k in keywords) else "✓"
            print(f"    {mark}  {n}")
        ph, ok = summarize(names, keywords)
        print(f"  >> 占位节点: {ph} | 正常节点: {ok}")

        all_names.extend(names)

    # 汇总
    print("\n================ 汇总 ================")
    if all_names:
        ph, ok = summarize(all_names, keywords)
        print(f"总节点: {len(all_names)} | 占位/无效: {ph} | 可用: {ok}")
        # 去重统计
        print(f"去重后节点: {len(set(all_names))} (重名 {len(all_names) - len(set(all_names))} 个会被 mihomo 去重)")
    if failed_urls:
        print(f"[警告] {len(failed_urls)} 个订阅解析失败: ")
        for u in failed_urls:
            print(f"  - {u}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
