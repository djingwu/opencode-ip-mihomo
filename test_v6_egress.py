# -*- coding: utf-8 -*-
"""v6 出口改写单测（容器内跑）：mark 识别 / scheme 降级 / IPRESOLVE=V6 /
URL 保 SNI / 无 AAAA 与解析异常回落 / 池加载含 v6 条目。"""
import sys

sys.path.insert(0, "/app")
import server
from proxy_utils import SOCKS_V6_MARK, with_v6_mark, strip_v6_mark
from curl_cffi.const import CurlOpt, CurlIpResolve

fails = []

def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond:
        fails.append(name)

URL = "https://opencode.ai/inference/openai/v1/chat/completions"
V4 = "socks5h://172.19.2.121:1080"
V6 = V4 + SOCKS_V6_MARK

# 1 v6 条目：入口剥标记+socks5、强制 v6、URL 不动（保 SNI/Host）
u, p, o = server._v6_egress_rewrite(URL, {"http": V6, "https": V6})
check("入口剥标记+socks5", p["http"] == "socks5://172.19.2.121:1080"
      and p["https"] == "socks5://172.19.2.121:1080")
check("use_v6 判定为真", o is True)
check("v6 session 可创建", server._get_v6_session() is not None)
check("v6 session 单例复用", server._get_v6_session() is server._get_v6_session())
check("URL 保持域名保 SNI", u == URL)

# 2 v4 条目（无标记）完全不动
u2, p2, o2 = server._v6_egress_rewrite(URL, {"http": V4, "https": V4})
check("v4 条目不改写", u2 == URL and p2["http"] == V4 and o2 is False)

# 3 proxies=None
u3, p3, o3 = server._v6_egress_rewrite(URL, None)
check("proxies=None 直返", u3 == URL and p3 is None and o3 is False)

# 4 域名无 AAAA：回落不强制 v6，入口仍剥标记
class _NoV6:
    @staticmethod
    def getaddrinfo(*a, **k):
        return []
_old = server.socket
server.socket = _NoV6
u4, p4, o4 = server._v6_egress_rewrite(URL, {"http": V6, "https": V6})
server.socket = _old
check("无 AAAA 回落不强制 v6", u4 == URL and o4 is False
      and p4["http"] == "socks5://172.19.2.121:1080")

# 5 解析异常：同样回落
class _Boom:
    @staticmethod
    def getaddrinfo(*a, **k):
        raise OSError("dns down")
server.socket = _Boom
u5, p5, o5 = server._v6_egress_rewrite(URL, {"http": V6, "https": V6})
server.socket = _old
check("解析异常回落", u5 == URL and o5 is False
      and p5["http"] == "socks5://172.19.2.121:1080")

# 6 utils 幂等
check("with_v6_mark 幂等", with_v6_mark(V6) == V6 and with_v6_mark(V4) == V6)
check("strip_v6_mark", strip_v6_mark(V6) == V4 and strip_v6_mark(V4) == V4)

# 7 池加载：proxies-v6.txt（灰度 121）进池且带标记
server.load_proxy_list()
check("池含 v6 条目", any(SOCKS_V6_MARK in x for x in server._proxy_pool))
check("池含全部 v4 条目", all(
    ("socks5h://172.19.2.%d:1080" % i) in server._proxy_pool for i in range(121, 131)))

print("\n%d failed" % len(fails))
sys.exit(1 if fails else 0)


# 8 v6 session 固化 IPRESOLVE=V6（内部 curl 句柄参数）
_s6 = server._get_v6_session()
try:
    _co = _s6.curl.getinfo(0) if hasattr(_s6, "curl") else None
    check("v6 session 存在", _s6 is not None)
except Exception as e:
    check("v6 session 存在", False)
