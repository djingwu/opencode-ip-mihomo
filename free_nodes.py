#!/usr/bin/env python3
"""免费节点抓取器：从 ConfigForge-V2Ray 拉取免费 V2Ray 节点，
转换为 mihomo 可用的 Clash YAML provider 文件。

用法：
  python free_nodes.py [--url URL] [--out FILE] [--max N]
  python free_nodes.py --add        # 拉取并生成 providers/free.yaml
"""
import argparse
import base64
import json
import ipaddress
import os
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlsplit

DEFAULT_URL = "https://raw.githubusercontent.com/ShatakVPN/ConfigForge-V2Ray/main/configs/light.txt"
# 备用镜像
MIRROR_URL = "https://cdn.jsdelivr.net/gh/ShatakVPN/ConfigForge-V2Ray@main/configs/light.txt"
ALL_URL = "https://raw.githubusercontent.com/ShatakVPN/ConfigForge-V2Ray/main/configs/all.txt"
MIRROR_ALL_URL = "https://cdn.jsdelivr.net/gh/ShatakVPN/ConfigForge-V2Ray@main/configs/all.txt"
DEFAULT_OUT = "providers/free.yaml"
MAX_NODES = 50  # 默认最多取 50 个（避免免费节点太多拖慢 health-check）

# ConfigForge-V2Ray 网页中的国家列表。手动模式不再从节点名称猜国家，
# 而是直接读取官方 configs/{country}/all.txt，与其 Web 提取界面一致。
COUNTRY_OPTIONS = [
    {"value": "all", "label": "全部国家/地区"},
    {"value": "at", "label": "🇦🇹 AT Austria"},
    {"value": "au", "label": "🇦🇺 AU Australia"},
    {"value": "bg", "label": "🇧🇬 BG Bulgaria"},
    {"value": "br", "label": "🇧🇷 BR Brazil"},
    {"value": "ca", "label": "🇨🇦 CA Canada"},
    {"value": "ch", "label": "🇨🇭 CH Switzerland"},
    {"value": "cz", "label": "🇨🇿 CZ Czech Republic"},
    {"value": "de", "label": "🇩🇪 DE Germany"},
    {"value": "es", "label": "🇪🇸 ES Spain"},
    {"value": "fi", "label": "🇫🇮 FI Finland"},
    {"value": "fr", "label": "🇫🇷 FR France"},
    {"value": "gb", "label": "🇬🇧 GB United Kingdom"},
    {"value": "hk", "label": "🇭🇰 HK Hong Kong"},
    {"value": "hu", "label": "🇭🇺 HU Hungary"},
    {"value": "id", "label": "🇮🇩 ID Indonesia"},
    {"value": "il", "label": "🇮🇱 IL Israel"},
    {"value": "in", "label": "🇮🇳 IN India"},
    {"value": "ir", "label": "🇮🇷 IR Iran"},
    {"value": "it", "label": "🇮🇹 IT Italy"},
    {"value": "jp", "label": "🇯🇵 JP Japan"},
    {"value": "kr", "label": "🇰🇷 KR South Korea"},
    {"value": "kz", "label": "🇰🇿 KZ Kazakhstan"},
    {"value": "lt", "label": "🇱🇹 LT Lithuania"},
    {"value": "md", "label": "🇲🇩 MD Moldova"},
    {"value": "nl", "label": "🇳🇱 NL Netherlands"},
    {"value": "pl", "label": "🇵🇱 PL Poland"},
    {"value": "pt", "label": "🇵🇹 PT Portugal"},
    {"value": "rs", "label": "🇷🇸 RS Serbia"},
    {"value": "ru", "label": "🇷🇺 RU Russia"},
    {"value": "se", "label": "🇸🇪 SE Sweden"},
    {"value": "sg", "label": "🇸🇬 SG Singapore"},
    {"value": "si", "label": "🇸🇮 SI Slovenia"},
    {"value": "tr", "label": "🇹🇷 TR Turkey"},
    {"value": "ua", "label": "🇺🇦 UA Ukraine"},
    {"value": "us", "label": "🇺🇸 US United States"},
    {"value": "vn", "label": "🇻🇳 VN Vietnam"},
]
REGION_OPTIONS = COUNTRY_OPTIONS
REGION_LABELS = {item["value"]: item["label"] for item in COUNTRY_OPTIONS}
REGION_LABELS["unknown"] = "未识别"
COUNTRY_CODES = {item["value"] for item in COUNTRY_OPTIONS if item["value"] != "all"}
COUNTRY_ALIASES = {
    "hong-kong": "hk",
    "north-america": "us",
    "south-america": "br",
    "europe": "de",
    "asia": "jp",
    "oceania": "au",
    "africa": "all",
    "middle-east": "il",
}
# 自动模式仍用于全局 light.txt 的地域均衡摘要，不参与手动国家选择。
REGION_KEYWORDS = {
    "north-america": ("🇺🇸", "🇨🇦", "🇲🇽", "united states", "usa", "us-", " america", "canada", "mexico", "美国", "加拿大", "墨西哥"),
    "south-america": ("🇧🇷", "🇦🇷", "🇨🇱", "🇨🇴", "brazil", "argentina", "chile", "colombia", "巴西", "阿根廷", "智利", "哥伦比亚"),
    "europe": ("🇬🇧", "🇩🇪", "🇫🇷", "🇳🇱", "🇮🇹", "🇪🇸", "🇸🇪", "🇨🇭", "🇷🇺", "europe", "uk", "london", "germany", "france", "netherlands", "russia", "英国", "德国", "法国", "荷兰", "欧洲", "俄罗斯"),
    "hong-kong": ("🇭🇰", "hong kong", "hongkong", "hong_kong", "hkg", "hk-", "hk ", " hk", "香港"),
    "asia": ("🇨🇳", "🇯🇵", "🇰🇷", "🇸🇬", "🇹🇼", "🇮🇳", "🇮🇩", "🇹🇭", "🇻🇳", "🇲🇾", "asia", "japan", "tokyo", "korea", "singapore", "taiwan", "india", "中国", "日本", "韩国", "新加坡", "台湾", "印度", "亚洲"),
    "oceania": ("🇦🇺", "🇳🇿", "australia", "sydney", "melbourne", "new zealand", "澳大利亚", "澳洲", "新西兰", "大洋洲"),
    "africa": ("🇿🇦", "🇪🇬", "🇳🇬", "africa", "south africa", "egypt", "nigeria", "南非", "埃及", "尼日利亚", "非洲"),
    "middle-east": ("🇦🇪", "🇸🇦", "🇮🇱", "🇹🇷", "middle east", "dubai", "uae", "saudi", "israel", "turkey", "中东", "迪拜", "阿联酋", "沙特", "以色列", "土耳其"),
}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def fetch_text(url: str) -> Optional[str]:
    """拉取免费节点源；优先复用 mihomo 出站代理，失败再直连。"""
    proxy = os.environ.get("FREE_NODES_PROXY", "").strip() or os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()
    try:
        from curl_cffi import requests as cffi_requests
        kwargs: Dict[str, Any] = {
            "headers": {"User-Agent": UA, "Accept": "text/plain,*/*"},
            "timeout": 25,
            "impersonate": "chrome124",
        }
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        response = cffi_requests.get(url, **kwargs)
        if response.status_code == 200 and response.content:
            return response.content.decode("utf-8", errors="ignore")
        print(f"[WARN] fetch {url} -> HTTP {response.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] proxied fetch {url} failed: {e}", file=sys.stderr)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        return urllib.request.urlopen(req, timeout=25).read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[WARN] direct fetch {url} failed: {e}", file=sys.stderr)
        return None


def _query_value(query: Dict[str, List[str]], *names: str, default: str = "") -> str:
    for name in names:
        values = query.get(name)
        if values:
            return unquote(str(values[0]))
    return default


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def is_routable_proxy_server(value: Any) -> bool:
    """Return False for endpoints that cannot be public proxies from the VPS.

    ConfigForge occasionally contains local helper endpoints such as
    ``127.0.0.1:1080``. Those can work only on the source maintainer's machine;
    inside our independent mihomo container they always resolve to mihomo
    itself and cause a misleading ``connection refused`` delay result.
    Hostnames are retained unless they are explicitly local. Literal IPs must
    be globally routable.
    """
    host = str(value or "").strip().strip("[]").rstrip(".").lower()
    if not host or host == "localhost" or host.endswith(".localhost"):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return True


def parse_vless(uri: str) -> Optional[Dict[str, Any]]:
    """解析 vless:// URI，并保留 mihomo 建连所需的传输/TLS/Reality 参数。"""
    try:
        parsed = urlsplit(uri)
        if parsed.scheme.lower() != "vless" or not parsed.hostname or not parsed.port:
            return None
        query = parse_qs(parsed.query, keep_blank_values=True)
        uuid = unquote(parsed.username or "").strip()
        if not uuid:
            return None
        node: Dict[str, Any] = {
            "name": _node_name(unquote(parsed.fragment or "")),
            "type": "vless",
            "server": parsed.hostname,
            "port": parsed.port,
            "uuid": uuid,
        }
        security = _query_value(query, "security", default="none").lower()
        if security in {"tls", "reality"}:
            node["tls"] = True
        servername = _query_value(query, "sni", "servername")
        if servername:
            node["servername"] = servername
        fingerprint = _query_value(query, "fp", "fingerprint")
        if fingerprint:
            node["client-fingerprint"] = fingerprint
        flow = _query_value(query, "flow")
        if flow:
            node["flow"] = flow
        if _truthy(_query_value(query, "insecure", "allowInsecure")):
            node["skip-cert-verify"] = True
        alpn = _query_value(query, "alpn")
        if alpn:
            node["alpn"] = [part.strip() for part in alpn.split(",") if part.strip()]
        packet_encoding = _query_value(query, "packetEncoding", "packet-encoding")
        if packet_encoding and packet_encoding.lower() not in {"none", "false"}:
            node["packet-encoding"] = packet_encoding

        network = _query_value(query, "type", default="tcp").lower()
        if network and network != "tcp":
            node["network"] = network
        if network == "ws":
            path = _query_value(query, "path", default="/") or "/"
            host = _query_value(query, "host", default=servername or parsed.hostname)
            ws_opts: Dict[str, Any] = {"path": path}
            if host:
                ws_opts["headers"] = {"Host": host}
            early_data = _query_value(query, "ed")
            if early_data.isdigit():
                ws_opts["max-early-data"] = int(early_data)
            early_header = _query_value(query, "eh")
            if early_header:
                ws_opts["early-data-header-name"] = early_header
            node["ws-opts"] = ws_opts
        elif network == "grpc":
            service_name = _query_value(query, "serviceName", "service-name")
            if service_name:
                node["grpc-opts"] = {"grpc-service-name": service_name}
        elif network in {"http", "h2"}:
            path = _query_value(query, "path")
            host = _query_value(query, "host")
            http_opts: Dict[str, Any] = {}
            if path:
                http_opts["path"] = [path]
            if host:
                http_opts["headers"] = {"Host": [host]}
            if http_opts:
                node["http-opts"] = http_opts

        if security == "reality":
            public_key = _query_value(query, "pbk", "public-key")
            short_id = _query_value(query, "sid", "short-id")
            reality_opts: Dict[str, Any] = {}
            if public_key:
                reality_opts["public-key"] = public_key
            if short_id:
                reality_opts["short-id"] = short_id
            if reality_opts:
                node["reality-opts"] = reality_opts
        return node
    except (ValueError, TypeError):
        return None


def parse_trojan(uri: str) -> Optional[Dict[str, Any]]:
    """解析 trojan:// URI，正确 URL 解码 WS/gRPC 与 TLS 参数。"""
    try:
        parsed = urlsplit(uri)
        if parsed.scheme.lower() != "trojan" or not parsed.hostname or not parsed.port:
            return None
        query = parse_qs(parsed.query, keep_blank_values=True)
        password = unquote(parsed.username or "").strip()
        if not password:
            return None
        node: Dict[str, Any] = {
            "name": _node_name(unquote(parsed.fragment or "")),
            "type": "trojan",
            "server": parsed.hostname,
            "port": parsed.port,
            "password": password,
        }
        # Trojan 默认就是 TLS；显式 security=none 才不写 tls。
        if _query_value(query, "security", default="tls").lower() != "none":
            node["tls"] = True
        servername = _query_value(query, "sni", "servername")
        if servername:
            node["servername"] = servername
        fingerprint = _query_value(query, "fp", "fingerprint")
        if fingerprint:
            node["client-fingerprint"] = fingerprint
        if _truthy(_query_value(query, "insecure", "allowInsecure")):
            node["skip-cert-verify"] = True
        alpn = _query_value(query, "alpn")
        if alpn:
            node["alpn"] = [part.strip() for part in alpn.split(",") if part.strip()]

        network = _query_value(query, "type", default="tcp").lower()
        if network and network != "tcp":
            node["network"] = network
        if network == "ws":
            path = _query_value(query, "path", default="/") or "/"
            host = _query_value(query, "host", default=servername or parsed.hostname)
            ws_opts: Dict[str, Any] = {"path": path}
            if host:
                ws_opts["headers"] = {"Host": host}
            early_data = _query_value(query, "ed")
            if early_data.isdigit():
                ws_opts["max-early-data"] = int(early_data)
            early_header = _query_value(query, "eh")
            if early_header:
                ws_opts["early-data-header-name"] = early_header
            node["ws-opts"] = ws_opts
        elif network == "grpc":
            service_name = _query_value(query, "serviceName", "service-name")
            if service_name:
                node["grpc-opts"] = {"grpc-service-name": service_name}
        return node
    except (ValueError, TypeError):
        return None


def _decode_b64_text(value: str) -> str:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")


def parse_ss(uri: str) -> Optional[Dict[str, Any]]:
    """解析 SIP002 Shadowsocks URI，兼容 plugin 参数。"""
    try:
        raw = uri[len("ss://"):]
        main, _, fragment = raw.partition("#")
        endpoint, sep, query_text = main.partition("?")
        query = parse_qs(query_text, keep_blank_values=True) if sep else {}
        if "@" in endpoint:
            userinfo, hostport = endpoint.rsplit("@", 1)
            decoded_userinfo = unquote(userinfo)
            if ":" not in decoded_userinfo:
                decoded_userinfo = _decode_b64_text(decoded_userinfo)
        else:
            decoded = _decode_b64_text(endpoint)
            decoded_userinfo, hostport = decoded.rsplit("@", 1)
        method, sep, password = decoded_userinfo.partition(":")
        host, sep2, port_text = hostport.rpartition(":")
        if not sep or not sep2 or not method or not password or not host:
            return None
        node: Dict[str, Any] = {
            "name": _node_name(unquote(fragment or "")),
            "type": "ss",
            "server": host.strip("[]"),
            "port": int(port_text),
            "cipher": method,
            "password": unquote(password),
        }
        plugin_spec = _query_value(query, "plugin")
        if plugin_spec:
            parts = [unquote(part) for part in plugin_spec.split(";") if part]
            plugin_name = parts[0]
            plugin_args: Dict[str, Any] = {}
            flags = set()
            for part in parts[1:]:
                if "=" in part:
                    key, value = part.split("=", 1)
                    plugin_args[key] = value
                else:
                    flags.add(part)
            if plugin_name in {"v2ray-plugin", "obfs-local"}:
                node["plugin"] = plugin_name
                opts: Dict[str, Any] = {}
                if plugin_name == "v2ray-plugin":
                    opts["mode"] = plugin_args.get("mode", "websocket")
                    if plugin_args.get("host"):
                        opts["host"] = plugin_args["host"]
                    if plugin_args.get("path"):
                        opts["path"] = plugin_args["path"]
                    if "tls" in flags:
                        opts["tls"] = True
                else:
                    if plugin_args.get("obfs"):
                        opts["mode"] = plugin_args["obfs"]
                    if plugin_args.get("obfs-host"):
                        opts["host"] = plugin_args["obfs-host"]
                if opts:
                    node["plugin-opts"] = opts
        return node
    except (ValueError, TypeError, UnicodeDecodeError):
        return None


def parse_vmess(uri: str) -> Optional[Dict[str, Any]]:
    """解析 vmess:// base64(JSON) URI → mihomo 代理配置。"""
    try:
        encoded = uri[len("vmess://"):].split("#", 1)[0].strip()
        padded = encoded + "=" * (-len(encoded) % 4)
        payload = json.loads(base64.b64decode(padded).decode("utf-8", errors="ignore"))
        if not isinstance(payload, dict):
            return None
        host = str(payload.get("add") or "").strip()
        port = int(payload.get("port") or 0)
        uuid = str(payload.get("id") or "").strip()
        if not host or not port or not uuid:
            return None
        node: Dict[str, Any] = {
            "name": _node_name(str(payload.get("ps") or "")),
            "type": "vmess",
            "server": host,
            "port": port,
            "uuid": uuid,
            "alterId": int(payload.get("aid") or 0),
            "cipher": str(payload.get("scy") or "auto"),
        }
        network = str(payload.get("net") or "tcp")
        if network and network != "tcp":
            node["network"] = network
        tls = str(payload.get("tls") or "").lower()
        if tls in {"tls", "reality"}:
            node["tls"] = True
        servername = str(payload.get("sni") or payload.get("host") or "").strip()
        if servername and node.get("tls"):
            node["servername"] = servername
        if network == "ws":
            node["ws-opts"] = {"path": str(payload.get("path") or "/"), "headers": {"Host": str(payload.get("host") or host)}}
        elif network == "grpc":
            service_name = str(payload.get("path") or payload.get("serviceName") or "")
            if service_name:
                node["grpc-opts"] = {"grpc-service-name": service_name}
        return node
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _decode_subscription_lines(text: str) -> List[str]:
    """兼容明文 URI 列表和 base64 包裹的订阅内容。"""
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if any(line.lower().startswith(("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://", "anytls://")) for line in lines):
        return lines
    compact = "".join(lines)
    try:
        decoded = base64.b64decode(compact + "=" * (-len(compact) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return lines
    decoded_lines = [line.strip() for line in decoded.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    return decoded_lines or lines


def parse_hysteria2(uri: str) -> Optional[Dict[str, Any]]:
    """解析 hysteria2:// / hy2:// URI。"""
    try:
        parsed = urlsplit(uri)
        if parsed.scheme.lower() not in {"hysteria2", "hy2"} or not parsed.hostname or not parsed.port:
            return None
        query = parse_qs(parsed.query)
        password = unquote(parsed.username or "")
        if not password:
            return None
        node: Dict[str, Any] = {
            "name": _node_name(unquote(parsed.fragment or "")),
            "type": "hysteria2",
            "server": parsed.hostname,
            "port": parsed.port,
            "password": password,
        }
        servername = (query.get("sni") or query.get("servername") or [""])[0]
        if servername:
            node["sni"] = unquote(servername)
        if (query.get("insecure") or [""])[0].lower() in {"1", "true", "yes"}:
            node["skip-cert-verify"] = True
        if (query.get("obfs") or [""])[0]:
            node["obfs"] = (query.get("obfs") or [""])[0]
            if query.get("obfs-password"):
                node["obfs-password"] = query["obfs-password"][0]
        return node
    except (ValueError, TypeError):
        return None


def parse_anytls(uri: str) -> Optional[Dict[str, Any]]:
    """解析 anytls://password@host:port URI。"""
    try:
        parsed = urlsplit(uri)
        if parsed.scheme.lower() != "anytls" or not parsed.hostname or not parsed.port:
            return None
        query = parse_qs(parsed.query)
        password = unquote(parsed.username or "")
        if not password:
            return None
        node: Dict[str, Any] = {
            "name": _node_name(unquote(parsed.fragment or "")),
            "type": "anytls",
            "server": parsed.hostname,
            "port": parsed.port,
            "password": password,
        }
        servername = (query.get("sni") or query.get("servername") or [""])[0]
        if servername:
            node["servername"] = unquote(servername)
        insecure = (query.get("insecure") or [""])[0].lower()
        if insecure in {"1", "true", "yes"}:
            node["skip-cert-verify"] = True
        if (query.get("udp") or [""])[0].lower() in {"1", "true", "yes"}:
            node["udp"] = True
        return node
    except (ValueError, TypeError):
        return None


def parse_uri(uri: str) -> Optional[Dict[str, Any]]:
    if uri.startswith(("hysteria2://", "hy2://")):
        return parse_hysteria2(uri)
    if uri.startswith("anytls://"):
        return parse_anytls(uri)
    if uri.startswith("vless://"):
        return parse_vless(uri)
    if uri.startswith("trojan://"):
        return parse_trojan(uri)
    if uri.startswith("vmess://"):
        return parse_vmess(uri)
    if uri.startswith("ss://"):
        return parse_ss(uri)
    return None


def infer_region(value: str) -> str:
    """从公开节点元数据中推断大区；无法可靠判断时返回 unknown。"""
    text = unquote(str(value or "")).lower()
    # 香港必须先于亚洲判断，避免被泛亚洲规则吞掉。
    hong_kong_keywords = REGION_KEYWORDS.get("hong-kong", ())
    if any(keyword in text for keyword in hong_kong_keywords):
        return "hong-kong"
    for region, keywords in REGION_KEYWORDS.items():
        if region == "hong-kong":
            continue
        if any(keyword in text for keyword in keywords):
            return region
    return "unknown"


def normalize_country_code(region: Optional[str]) -> str:
    value = str(region or "").strip().lower()
    return COUNTRY_ALIASES.get(value, value)


def region_label(region: Optional[str]) -> str:
    normalized = normalize_country_code(region)
    return REGION_LABELS.get(normalized, "自动解析")


def get_region_options() -> List[Dict[str, str]]:
    return [dict(item) for item in REGION_OPTIONS]


def summarize_regions(nodes: List[Dict[str, Any]]) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    for node in nodes:
        region = str(node.get("region") or "unknown")
        summary[region] = summary.get(region, 0) + 1
    return summary


def select_nodes_by_region(nodes: List[Dict[str, Any]], max_nodes: int, region_mode: str = "auto", region: Optional[str] = None) -> List[Dict[str, Any]]:
    """按地域清洗节点列表。

    自动模式会根据公开实例的名称/主机名解析地域，并从已识别地域轮询取样，
    避免单一地域的公共实例挤占节点池。手动模式仅返回指定地域。
    """
    mode = (region_mode or "auto").strip().lower()
    requested = normalize_country_code(region)
    if mode not in {"auto", "manual"}:
        raise ValueError("地域模式必须为 auto 或 manual")
    if mode == "manual" and requested not in REGION_LABELS:
        raise ValueError("请选择有效的国家/地区")

    if mode == "manual":
        filtered = nodes if requested == "all" else [node for node in nodes if node.get("region") == requested]
        return filtered[:max_nodes]

    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for node in nodes:
        buckets.setdefault(str(node.get("region") or "unknown"), []).append(node)
    selected: List[Dict[str, Any]] = []
    known_regions = [region for region in REGION_KEYWORDS if buckets.get(region)]
    while known_regions and len(selected) < max_nodes:
        next_round: List[str] = []
        for bucket_region in known_regions:
            if buckets[bucket_region] and len(selected) < max_nodes:
                selected.append(buckets[bucket_region].pop(0))
            if buckets[bucket_region]:
                next_round.append(bucket_region)
        known_regions = next_round
    # 没有可识别地域、或已识别节点不足时，再以原始顺序补充未识别节点。
    for node in buckets.get("unknown", []):
        if len(selected) >= max_nodes:
            break
        selected.append(node)
    return selected


def _node_name(frag: str) -> str:
    """从 URI fragment 提取节点名（URL 解码），默认用协议前缀。"""
    if frag:
        try:
            name = unquote(frag).strip()
            if name:
                return f"免费-{name}"
        except Exception:
            pass
    return "免费节点"


def to_clash_yaml(nodes: List[Dict[str, Any]]) -> str:
    """使用 PyYAML 完整序列化嵌套传输参数，避免手写 YAML 丢字段。"""
    import yaml

    proxies = [
        {key: value for key, value in node.items() if key != "region"}
        for node in nodes
    ]
    return yaml.safe_dump(
        {"proxies": proxies},
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    ).rstrip()


def _parse_free_node_text(text: str) -> List[Dict[str, Any]]:
    nodes: List[Dict[str, Any]] = []
    for line in _decode_subscription_lines(text):
        node = parse_uri(line)
        if node and is_routable_proxy_server(node.get("server")):
            node["region"] = infer_region(f"{node.get('name', '')} {node.get('server', '')} {line}")
            nodes.append(node)
    seen = set()
    unique_nodes = []
    for node in nodes:
        # 同一 server:port 可能有不同 UUID/密码/SNI；只按端点去重会把
        # ConfigForge 的上千条配置错误压缩成 1 条。使用完整代理身份去重。
        key = json.dumps(
            {k: node.get(k) for k in sorted(node) if k not in {"name", "region"}},
            ensure_ascii=False,
            sort_keys=True,
        )
        if key in seen:
            continue
        seen.add(key)
        unique_nodes.append(node)

    # mihomo 以 name 标识代理；不同配置若恰好同名会互相覆盖或导致组引用歧义。
    name_counts: Dict[str, int] = {}
    for node in unique_nodes:
        base_name = str(node.get("name") or "免费节点")
        name_counts[base_name] = name_counts.get(base_name, 0) + 1
        if name_counts[base_name] > 1:
            node["name"] = f"{base_name} #{name_counts[base_name]}"
    return unique_nodes


def fetch_free_nodes(
    url: str = DEFAULT_URL,
    max_nodes: int = MAX_NODES,
    region_mode: str = "auto",
    region: Optional[str] = None,
) -> List[Dict[str, Any]]:
    mode = (region_mode or "auto").strip().lower()
    requested = normalize_country_code(region)
    if mode not in {"auto", "manual"}:
        raise ValueError("地域模式必须为 auto 或 manual")

    if url != DEFAULT_URL:
        sources = [url]
        direct_country = False
    elif mode == "manual" and requested in COUNTRY_CODES:
        # 与 ConfigForge Web 页面一致：国家选择直接读取 configs/{cc}/all.txt。
        sources = [
            f"https://raw.githubusercontent.com/ShatakVPN/ConfigForge-V2Ray/main/configs/{requested}/all.txt",
            f"https://cdn.jsdelivr.net/gh/ShatakVPN/ConfigForge-V2Ray@main/configs/{requested}/all.txt",
        ]
        direct_country = True
    elif mode == "manual" and requested == "all":
        sources = [ALL_URL, MIRROR_ALL_URL]
        direct_country = True
    elif mode == "manual":
        raise ValueError("请选择有效的国家/地区")
    else:
        sources = [DEFAULT_URL, MIRROR_URL]
        direct_country = False

    for source in sources:
        text = fetch_text(source)
        if not text:
            continue
        nodes = _parse_free_node_text(text)
        if direct_country:
            # 国家目录已由 ConfigForge 按该国家的测速结果排序，不能再次按节点
            # 名称推断/过滤，否则会把 1130 条错误缩减到极少数。
            for node in nodes:
                node["region"] = requested
            return nodes[:max_nodes]
        selected = select_nodes_by_region(nodes, max_nodes, "auto", None)
        if selected:
            return selected
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="抓取免费 V2Ray 节点并生成 mihomo provider")
    parser.add_argument("--url", default=DEFAULT_URL, help="免费节点订阅 URL")
    parser.add_argument("--out", default=DEFAULT_OUT, help="输出 Clash YAML 文件路径")
    parser.add_argument("--max", type=int, default=MAX_NODES, help="最多节点数")
    parser.add_argument("--region-mode", choices=("auto", "manual"), default="auto", help="地域筛选模式")
    parser.add_argument("--region", default=None, help="手动国家代码，如 hk/jp/us/all")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出抓取结果")
    parser.add_argument("--dir", default=".", help="工作目录（输出文件相对此目录）")
    args = parser.parse_args()

    out_path = Path(args.dir) / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        nodes = fetch_free_nodes(args.url, args.max, args.region_mode, args.region)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    if not nodes:
        print("[ERROR] 未获取到任何符合地域筛选条件的可用节点", file=sys.stderr)
        return 1

    out_path.write_text(to_clash_yaml(nodes) + "\n", encoding="utf-8")
    result = {
        "ok": True,
        "node_count": len(nodes),
        "region_mode": args.region_mode,
        "region": normalize_country_code(args.region) if args.region_mode == "manual" else None,
        "region_summary": summarize_regions(nodes),
        "output": str(out_path),
    }
    if args.json:
        import json
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(f"[OK] 已生成 {len(nodes)} 个免费节点 -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
