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
