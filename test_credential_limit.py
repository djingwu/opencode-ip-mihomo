# -*- coding: utf-8 -*-
"""凭据级限流快速失败的单元测试。

验证三件事：
  1. 首次 429（未轮换过）→ 允许轮换重试
  2. 已轮换过仍 429 → 快速失败（返回 False），不再轮换
  3. 轮换预算用尽 → 快速失败
  4. ContextVar 在并发协程间不串味
"""
import asyncio
import sys
import types

# ── 构造最小可测环境：抽出被测逻辑，桩掉外部依赖 ──
import contextvars
import random

MAX_RETRIES_ON_429 = 4
RATE_LIMIT_ROTATE_BACKOFF_SECONDS = 0.0
CREDENTIAL_LIMIT_ROTATION_BUDGET = 1
CREDENTIAL_LIMIT_SKIP_QUOTA = True
LONG_BAN_THRESHOLD_SECONDS = 300
BACKOFF_CAP = 30
UPSTREAM_API_KEY_OVERRIDE = "public"
INITIAL_BACKOFF = 1.0

_rate_limit_rotations = contextvars.ContextVar("rl", default=0)


def _reset_rate_limit_rotations():
    _rate_limit_rotations.set(0)


class _Log:
    def warning(self, m, *a):
        print('    [warn] ' + (m % a if a else m))

    def info(self, m, *a):
        pass


log = _Log()


class _Quota:
    def __init__(self):
        self.records = []

    def record_upstream_quota(self, proxy, model, retry_after, reason):
        self.records.append((proxy, model, retry_after, reason))

    def next_proxy_after(self, cur, model=None):
        return None


class _Pool:
    def __init__(self):
        self.record_upstream_quota = _Quota().record_upstream_quota
        self.quota_calls = []

    def record_upstream_quota(self, p, m, r, reason):
        self.quota_calls.append((p, m, r, reason))

    def next_proxy_after(self, cur, model=None):
        return None


proxy_pool = _Pool()
ROTATE_ON_429 = True

_rotate_calls = []


async def rotate_egress_safely(reason):
    _rotate_calls.append(reason)
    return True, "socks5h://172.19.2.999:1080"


def _reset_request_session(*a, **k):
    pass


def classify_upstream_429(response):
    return "upstream_rate_limit", None, {}


# ── 被测函数（与 server.py 保持一致）──
async def recover_from_rate_limit(response, model_name, endpoint, session,
                                  pooled, egress_key, attempt,
                                  stream_pool_key=None, session_key="",
                                  current_proxy="", rotation_used=False):
    if rotation_used:
        log.warning(
            "429 for '%s' on %s persists AFTER egress rotation -> treating as "
            "credential-level limit (not per-egress); passing through without "
            "further rotation.", model_name, egress_key)
        return False

    category, retry_after, _ = classify_upstream_429(response)
    if category == "quota":
        proxy_pool.record_upstream_quota(current_proxy or None, model_name,
                                        retry_after, "quota")
        return False
    if retry_after is not None and retry_after > LONG_BAN_THRESHOLD_SECONDS:
        return False

    # rotate_now 视为 True（真实里由 _egress_rate_policy 决定）
    if not ROTATE_ON_429 or attempt >= MAX_RETRIES_ON_429:
        return False

    _used = _rate_limit_rotations.get()
    if _used >= CREDENTIAL_LIMIT_ROTATION_BUDGET:
        log.warning(
            "429 for '%s' on %s; egress-rotation budget (%d/%d) already spent -> "
            "likely credential-level limit, passing through.",
            model_name, egress_key, _used, CREDENTIAL_LIMIT_ROTATION_BUDGET)
        return False

    proxy_pool.record_upstream_quota(current_proxy or None, model_name,
                                    retry_after, "rate_limit")
    rotated, new_ip = await rotate_egress_safely(
        "confirmed HTTP 429 x%d (attempt %d/%d)" % (2, attempt, MAX_RETRIES_ON_429))
    if not rotated:
        return False

    _reset_request_session(endpoint, session, pooled=pooled,
                           stream_pool_key=stream_pool_key)
    _rate_limit_rotations.set(_used + 1)
    return True


# ══════════════ 测试 ══════════════
FAILS = []


def check(cond, label):
    print('  %-4s %s' % ('OK' if cond else 'FAIL', label))
    if not cond:
        FAILS.append(label)


async def main():
    print('=== 场景1: 首次 429，未轮换 → 允许轮换 ===')
    _reset_rate_limit_rotations()
    _rotate_calls.clear()
    proxy_pool.quota_calls.clear()
    r = await recover_from_rate_limit(
        object(), 'step-5-preview-free', 'chat', None, True,
        'proxy:x', 1, current_proxy='socks5h://1.1.1.1', rotation_used=False)
    check(r is True, '首次 429 返回 True（会重试）')
    check(len(_rotate_calls) == 1, '确实轮换了一次出口')
    check(_rate_limit_rotations.get() == 1, '轮换计数累加到 1')

    print('\n=== 场景2: 已轮换仍 429 → 凭据级，快速失败 ===')
    _rotate_calls.clear()
    proxy_pool.quota_calls.clear()
    r = await recover_from_rate_limit(
        object(), 'step-5-preview-free', 'chat', None, True,
        'proxy:x', 2, current_proxy='socks5h://2.2.2.2',
        rotation_used=True)
    check(r is False, '轮换后仍 429 返回 False（透传给下游）')
    check(len(_rotate_calls) == 0, '不再轮换出口')
    check(len(proxy_pool.quota_calls) == 0,
          '凭据级 429 不记 per-egress 冷却（不误伤健康出口）')

    print('\n=== 场景3: 轮换预算用尽 → 快速失败 ===')
    _reset_rate_limit_rotations()
    _rate_limit_rotations.set(CREDENTIAL_LIMIT_ROTATION_BUDGET)
    _rotate_calls.clear()
    r = await recover_from_rate_limit(
        object(), 'step-5-preview-free', 'chat', None, True,
        'proxy:x', 1, current_proxy='socks5h://3.3.3.3', rotation_used=False)
    check(r is False, '预算用尽返回 False')
    check(len(_rotate_calls) == 0, '预算用尽不再轮换')

    print('\n=== 场景4: ContextVar 并发隔离（关键）===')
    _rotate_calls.clear()

    async def one_request(tag):
        _reset_rate_limit_rotations()
        await asyncio.sleep(0)          # 让出控制权，制造交错
        r1 = await recover_from_rate_limit(
            object(), 'm-' + tag, 'chat', None, True, 'proxy:x', 1,
            current_proxy='p-' + tag, rotation_used=False)
        await asyncio.sleep(0)
        # 此时 _rate_limit_rotations 应仍只属于本请求
        mine = _rate_limit_rotations.get()
        await asyncio.sleep(0)
        r2 = await recover_from_rate_limit(
            object(), 'm-' + tag, 'chat', None, True, 'proxy:x', 2,
            current_proxy='p-' + tag, rotation_used=(mine > 0))
        return tag, r1, mine, r2

    results = await asyncio.gather(one_request('A'), one_request('B'),
                                   one_request('C'))
    for tag, r1, mine, r2 in results:
        check(r1 is True and mine == 1 and r2 is False,
              '请求 %s：首次轮换成功，计数=1，再次 429 快速失败（未串味）' % tag)
    check(len(_rotate_calls) == 3,
          '三个并发请求各轮换一次（共 %d 次），互不干扰' % len(_rotate_calls))

    print('\n=== 场景5: reset 生效（新请求不继承上一请求的计数）===')
    _reset_rate_limit_rotations()
    check(_rate_limit_rotations.get() == 0, 'reset 后计数归零')


asyncio.run(main())
print('\n%d failure(s)' % len(FAILS))
sys.exit(1 if FAILS else 0)