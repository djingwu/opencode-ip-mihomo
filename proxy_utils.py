"""Shared proxy URL normalization helpers.

SOCKS5 entries are executed as ``socks5h://`` so DNS resolution happens on the
proxy server.  This matches v2rayN/mihomo-style remote DNS behavior and avoids
false failures from containers resolving a hostname locally before the SOCKS
CONNECT request.
"""


def normalize_proxy_url(value: str) -> str:
    """Return a canonical proxy URL suitable for runtime use and storage."""
    addr = str(value or "").strip()
    if addr.lower().startswith("socks5://"):
        return "socks5h://" + addr[len("socks5://"):]
    return addr


# v6 池条目标记：自建 socks 出口机均带公网 IPv6（ATYP=4 CONNECT 探针实测
# 支持），但 socks 对域名目标默认解析 v4；socks URL 又表达不了"入口 v4、
# 目标 v6"。故 v6 条目以同 URL + "#v6" 后缀登记——curl 对代理 URL 的
# fragment 原样忽略（实测 #v6 与 ?f=6 都不影响 v4 通路），server.py 出站层
# 识别该标记后强制走 IPv6（见 _v6_egress_rewrite）。
SOCKS_V6_MARK = "#v6"


def with_v6_mark(url: str) -> str:
    """给池条目追加 v6 标记（幂等）。"""
    addr = str(url or "").strip()
    if not addr or SOCKS_V6_MARK in addr:
        return addr
    return addr + SOCKS_V6_MARK


def strip_v6_mark(url: str) -> str:
    """剥掉 v6 标记，返回可直接交给 curl 的代理 URL。"""
    return str(url or "").replace(SOCKS_V6_MARK, "", 1)
