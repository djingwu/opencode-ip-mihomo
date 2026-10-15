# -*- coding: utf-8 -*-
"""detect_client 单元测试：验证识别规则。

用真实的客户端头组合，检验识别结果是否正确。
"""
import io
import re
import sys

# 从 server.py 抽出 detect_client 相关代码，避免导入整个应用
src = io.open('server.py', encoding='utf-8').read()

start = src.find('CLIENT_PROBE_ENABLED = os.environ.get(')
end = src.find('def get_realistic_headers(')
BLOCK = src[start:end]

NS = {
    'os': __import__('os'),
    'json': __import__('json'),
}
exec(compile(BLOCK, 'block', 'exec'), NS)
detect_client = NS['detect_client']
_fingerprint = NS['_fingerprint_downstream_client']


class FakeHeaders(dict):
    def get(self, k, default=''):
        for key in self:
            if key.lower() == k.lower():
                return self[key]
        return default


class FakeReq:
    def __init__(self, headers):
        self.headers = FakeHeaders(headers)


CASES = [
    # (说明, 请求头, 期望标签)
    ('codex CLI (rust binary UA)',
     {'user-agent': 'codex_cli_rs/0.20.0 (Ubuntu 24.04; x86_64)'}, 'codex'),
    ('codex via openai-node',
     {'user-agent': 'codex_cli_rs/0.21.0', 'x-stainless-lang': 'js'}, 'codex'),
    ('openai-python SDK（普通 OpenAI SDK，不等于 codex）',
     {'user-agent': 'OpenAI/Python 1.55.0', 'x-stainless-lang': 'python',
      'x-stainless-os': 'Linux', 'x-stainless-runtime': 'CPython'}, 'openai-sdk'),
    ('openai-node SDK（普通 OpenAI SDK）',
     {'user-agent': 'OpenAI/Node.js 4.78.0', 'x-stainless-lang': 'js'},
     'openai-sdk'),
    ('opencode CLI (真 UA)',
     {'user-agent': 'opencode/2.0.29/cli'}, 'opencode'),
    ('opencode CLI 带 channel',
     {'user-agent': 'opencode/latest/2.0.29/cli'}, 'opencode'),
    ('opencode ai-sdk 风格',
     {'user-agent': 'opencode/1.18.31 ai-sdk/1.2.3'}, 'opencode'),
    ('WorkBuddy CLI（实测真实客户端 2026-10-15）',
     {'user-agent': 'WorkBuddy/5.7.6 WorkBuddy/5.7.6 CLI/2.156.0'}, 'workbuddy'),
    ('CodeBuddy CLI',
     {'user-agent': 'CodeBuddy/1.2.3 CLI/2.100.0'}, 'codebuddy'),
    ('curl',
     {'user-agent': 'curl/8.5.0'}, 'curl'),
    ('python-requests',
     {'user-agent': 'python-requests/2.32.3'}, 'python-requests'),
    ('httpx',
     {'user-agent': 'python-httpx/0.27.2'}, 'python-httpx'),
    ('claude-code',
     {'user-agent': 'claude-cli/1.0.0 (external, cli)'}, 'claude-code'),
    ('anthropic sdk',
     {'user-agent': 'Anthropic/Python 0.40.0'}, 'claude-code'),
    ('cline',
     {'user-agent': 'cline/3.21.0'}, 'cline'),
    ('浏览器直连',
     {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/155.0.0.0'},
     'browser'),
    ('显式声明 x-opencode-client',
     {'user-agent': 'python-requests/2.32.3', 'x-opencode-client': 'my-tool'},
     'my-tool'),
    ('显式声明 x-client-type',
     {'user-agent': 'curl/8.5.0', 'x-client-type': 'codex-cli'}, 'codex-cli'),
    ('originator 头（codex 会带）',
     {'user-agent': 'codex_cli_rs/0.20.0', 'originator': 'codex_vscode'},
     'codex_vscode'),
    ('完全无 UA',
     {}, 'no-ua'),
    ('UA 为空串',
     {'user-agent': ''}, 'no-ua'),
    ('未知 UA',
     {'user-agent': 'MyCustomAgent/1.0'}, 'other'),
    # 关键回归：网关自己的伪装头绝不能出现在 raw_request 里
    ('伪装头不该出现（防御性：若出现则按声明处理）',
     {'user-agent': 'opencode/2.0.29/cli', 'x-opencode-client': 'cli'},
     'cli'),
]

print('=== detect_client 测试 ===')
fails = 0
for desc, hdrs, expect in CASES:
    got = detect_client(FakeReq(hdrs))
    ok = (got == expect)
    if not ok:
        fails += 1
    print('%-4s %-42s expect=%-14s got=%s' % ('OK' if ok else 'FAIL', desc, expect, got))

# 指纹脱敏测试
print('\n=== 指纹脱敏 ===')
snap = _fingerprint(FakeReq({
    'user-agent': 'opencode/2.0.29/cli',
    'x-opencode-session': 'ses_abcdef123456789xyzXYZ',
    'authorization': 'Bearer SECRET_SHOULD_NOT_APPEAR',
    'x-stainless-os': 'Linux',
}), {'metadata': {'user_agent': 'test/1.0'}})
print(snap)
assert 'SECRET_SHOULD_NOT_APPEAR' not in snap, 'authorization 泄漏！'
assert 'abcdef123456789' not in snap, 'session 未脱敏！'
print('OK: authorization 未采集、session 已脱敏')

print('\n%d failure(s)' % fails)
sys.exit(1 if fails else 0)