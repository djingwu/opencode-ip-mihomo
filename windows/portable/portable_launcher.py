"""Windows portable process launcher for OpenCode IP Mihomo."""
from __future__ import annotations

import ctypes
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import webbrowser
from pathlib import Path

CREATE_NO_WINDOW = 0x08000000
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000


def root_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent.parent
    return Path(__file__).resolve().parents[2]


ROOT = root_dir()
APP = ROOT / "app"
BIN = ROOT / "bin"
DATA = ROOT / "data"
LOGS = ROOT / "logs"
MIHOMO = ROOT / "mihomo"
RUNTIME = ROOT / "runtime"


def load_portable_env() -> None:
    env_file = ROOT / "portable.env"
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip().strip('"').strip("'")


load_portable_env()
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "24513"))


def process_image(pid: int) -> Path | None:
    if os.name != "nt":
        return None
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return None
    try:
        size = ctypes.c_ulong(32768)
        buffer = ctypes.create_unicode_buffer(size.value)
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            return None
        return Path(buffer.value).resolve()
    finally:
        kernel32.CloseHandle(handle)


def managed_process(name: str, expected: Path) -> int | None:
    pid_file = RUNTIME / f"{name}.pid"
    try:
        pid = int(pid_file.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return None
    image = process_image(pid)
    if image is None or os.path.normcase(str(image)) != os.path.normcase(str(expected.resolve())):
        return None
    return pid


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def prepare_files() -> None:
    for directory in (DATA, LOGS, MIHOMO / "providers", RUNTIME):
        directory.mkdir(parents=True, exist_ok=True)
    config = MIHOMO / "config.yaml"
    if not config.exists():
        template = MIHOMO / "config.example.yaml"
        if not template.exists():
            raise RuntimeError("Missing mihomo/config.example.yaml; extract the complete ZIP again.")
        content = template.read_text(encoding="utf-8")
        content = content.replace("allow-lan: true", "allow-lan: false")
        content = content.replace('bind-address: "*"', "bind-address: 127.0.0.1")
        content = content.replace("external-controller: 0.0.0.0:9090", "external-controller: 127.0.0.1:9090")
        config.write_text(content, encoding="utf-8")
        print("Created local-only mihomo/config.yaml.")
    (DATA / "proxies.txt").touch(exist_ok=True)


def child_environment() -> dict[str, str]:
    env = os.environ.copy()
    values = {
        "OPENCODE_ZEN_HOST": "127.0.0.1",
        "OPENCODE_ZEN_PORT": str(GATEWAY_PORT),
        "OPENCODE_ZEN_TARGET_BASE": "https://opencode.ai/zen/v1",
        "CUSTOM_OUTBOUND_PROXY": "http://127.0.0.1:7890",
        "WARP_ROTATOR_URL": "http://127.0.0.1:8001",
        "MIHOMO_API_URL": "http://127.0.0.1:9090",
        "MIHOMO_OUTBOUND_PROXY": "http://127.0.0.1:7890",
        "MIHOMO_GROUP": "ROTATOR",
        "MIHOMO_SECRET": "",
        "MIHOMO_CONFIG_DIR": str(MIHOMO),
        "MIHOMO_CONTROLLER_CONFIG_PATH": (MIHOMO / "config.yaml").as_posix(),
        "METRICS_DB_PATH": str(DATA / "metrics.db"),
        "PROXY_LIST_FILE": str(DATA / "proxies.txt"),
        "PROXY_STATE_FILE": str(DATA / "proxy_pool_state.json"),
        "EGRESS_STATE_FILE": str(DATA / "egress_state.json"),
        "FREE_NODES_MAX": "50",
        "DISABLE_ELEVATION": "1",
        "LOG_LEVEL": "INFO",
    }
    env.update(values)
    return env


def start_component(name: str, executable: Path, arguments: list[str], env: dict[str, str]) -> int:
    pid = managed_process(name, executable)
    if pid:
        print(f"{name} is already running (PID {pid}).")
        return pid
    out_handle = (LOGS / f"{name}.out.log").open("ab", buffering=0)
    err_handle = (LOGS / f"{name}.err.log").open("ab", buffering=0)
    try:
        process = subprocess.Popen(
            [str(executable), *arguments],
            cwd=str(ROOT),
            env=env,
            stdout=out_handle,
            stderr=err_handle,
            creationflags=CREATE_NO_WINDOW,
        )
    finally:
        out_handle.close()
        err_handle.close()
    (RUNTIME / f"{name}.pid").write_text(str(process.pid), encoding="ascii")
    print(f"Started {name} (PID {process.pid}).")
    return process.pid


def wait_for_gateway() -> bool:
    for _ in range(40):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{GATEWAY_PORT}/health", timeout=2) as response:
                if response.status == 200:
                    return True
        except Exception:
            time.sleep(0.5)
    return False


def start_all(open_browser: bool = True) -> int:
    prepare_files()
    executables = {
        "mihomo": BIN / "mihomo.exe",
        "rotator": APP / "opencode-rotator.exe",
        "gateway": APP / "opencode-gateway.exe",
    }
    for executable in executables.values():
        if not executable.exists():
            raise RuntimeError(f"Missing runtime file: {executable}")
    owners = {7890: "mihomo", 9090: "mihomo", 8001: "rotator", GATEWAY_PORT: "gateway"}
    for port, name in owners.items():
        if port_open(port) and not managed_process(name, executables[name]):
            raise RuntimeError(f"Port {port} is already used by another program.")
    env = child_environment()
    start_component("mihomo", executables["mihomo"], ["-d", str(MIHOMO)], env)
    time.sleep(1)
    start_component("rotator", executables["rotator"], [], env)
    time.sleep(0.5)
    start_component("gateway", executables["gateway"], [], env)
    if not wait_for_gateway():
        raise RuntimeError("Gateway startup timed out. Review files in the logs folder.")
    dashboard = f"http://127.0.0.1:{GATEWAY_PORT}/dashboard"
    print("OpenCode IP Mihomo is running.")
    print(f"Dashboard: {dashboard}")
    if open_browser:
        webbrowser.open(dashboard)
    return 0


def stop_all() -> int:
    expected = {
        "gateway": APP / "opencode-gateway.exe",
        "rotator": APP / "opencode-rotator.exe",
        "mihomo": BIN / "mihomo.exe",
    }
    for name, executable in expected.items():
        pid = managed_process(name, executable)
        if pid:
            taskkill = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "taskkill.exe"
            result = subprocess.run(
                [str(taskkill), "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                creationflags=CREATE_NO_WINDOW,
                check=False,
            )
            if result.returncode == 0:
                print(f"Stopped {name} process tree (PID {pid}).")
            else:
                detail = (result.stderr or result.stdout).strip()
                print(f"Could not stop {name}: {detail or 'taskkill failed'}")
        else:
            print(f"{name} is not running.")
        try:
            (RUNTIME / f"{name}.pid").unlink()
        except FileNotFoundError:
            pass
    return 0


def status() -> int:
    expected = {
        "mihomo": BIN / "mihomo.exe",
        "rotator": APP / "opencode-rotator.exe",
        "gateway": APP / "opencode-gateway.exe",
    }
    result = {name: managed_process(name, path) for name, path in expected.items()}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(result.values()) else 1


def main() -> int:
    command = sys.argv[1].lower() if len(sys.argv) > 1 else "start"
    try:
        if command == "start":
            return start_all(open_browser="--no-browser" not in sys.argv[2:])
        if command == "stop":
            return stop_all()
        if command == "status":
            return status()
        print("Usage: opencode-launcher.exe [start|stop|status] [--no-browser]")
        return 2
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
