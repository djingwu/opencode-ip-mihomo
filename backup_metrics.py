#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""metrics.db 在线热备 + 保留策略。

为什么不用 cp：
  SQLite 在 WAL 模式下数据分散在 .db / -wal / -shm 三个文件，直接 cp .db
  可能拿到「主库已更新但 WAL 未 checkpoint」的不一致快照。用
  Connection.backup() 会走 SQLite 官方备份 API，保证快照自洽。

为什么不用 sqlite3 CLI：机器上没装（Ubuntu 精简），但 python3 自带 sqlite3。

保留：默认 7 天，按日期命名，清理更早的备份。
"""
import os
import shutil
import sqlite3
import sys
import time

DB = os.environ.get("METRICS_DB", "/usr/local/ai/opencode-ip-mihomo/data/metrics.db")
BACKUP_DIR = os.environ.get("BACKUP_DIR", "/usr/local/ai/opencode-ip-mihomo/backups")
KEEP_DAYS = int(os.environ.get("KEEP_DAYS", "7"))


def log(msg):
    print("[backup] %s" % msg)


def main():
    if not os.path.exists(DB):
        log("ERROR: source db not found: %s" % DB)
        return 1

    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(BACKUP_DIR, "metrics-%s.db" % stamp)

    src_size = os.path.getsize(DB)
    tmp = dest + ".tmp"
    try:
        # read-only 打开源库，避免备份进程干扰正在写的应用
        src = sqlite3.connect("file:%s?mode=ro" % DB, uri=True, timeout=30)
        dst = sqlite3.connect(tmp, timeout=30)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()

        # 校验备份可读且行数合理
        chk = sqlite3.connect(tmp)
        try:
            n = chk.execute(
                "SELECT COUNT(*) FROM request_history").fetchone()[0]
        finally:
            chk.close()

        shutil.move(tmp, dest)
        dst_size = os.path.getsize(dest)
        log("OK  %s  (%.1f KB -> %.1f KB, request_history=%d rows)"
            % (os.path.basename(dest), src_size / 1024.0,
               dst_size / 1024.0, n))
    except Exception as exc:
        log("ERROR: backup failed: %s: %s" % (type(exc).__name__, exc))
        for p in (tmp,):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return 1

    # ── 保留策略 ──
    cutoff = time.time() - KEEP_DAYS * 86400
    removed = 0
    freed = 0
    for name in os.listdir(BACKUP_DIR):
        if not (name.startswith("metrics-") and name.endswith(".db")):
            continue
        p = os.path.join(BACKUP_DIR, name)
        try:
            st = os.stat(p)
            if st.st_mtime < cutoff:
                os.remove(p)
                removed += 1
                freed += st.st_size
        except Exception as exc:
            log("WARN: prune %s failed: %s" % (name, exc))
    if removed:
        log("pruned %d old backup(s), freed %.1f KB (keep %d days)"
            % (removed, freed / 1024.0, KEEP_DAYS))

    # 兜底：备份目录异常膨胀（>2G）时保留最近 5 份
    total = 0
    files = []
    for name in os.listdir(BACKUP_DIR):
        p = os.path.join(BACKUP_DIR, name)
        if os.path.isfile(p):
            try:
                st = os.stat(p)
                total += st.st_size
                files.append((st.st_mtime, p, st.st_size))
            except Exception:
                pass
    if total > 2 * 1024 ** 3 and len(files) > 5:
        files.sort()
        for _mt, p, sz in files[:-5]:
            try:
                os.remove(p)
                log("pruned oversized: %s (%.1f KB)"
                    % (os.path.basename(p), sz / 1024.0))
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())