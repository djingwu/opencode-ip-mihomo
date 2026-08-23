# Contributing Guidelines

感谢你为 `opencode-ip-mihomo` 提交改进。

## 开发环境

- Python 3.11+
- Docker Engine / Docker Desktop
- Docker Compose v2

## 本地检查

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -p "test_*.py" -q
docker compose -f docker-compose.mihomo.yml config --quiet
docker compose -f docker-compose.mihomo.yml -f docker-compose.build.yml config --quiet
```

## 提交规范

1. 从 `master` 或 `main` 创建功能分支；
2. 保持 proxy-server、rotator、mihomo 的职责边界；
3. 不提交 `.env`、订阅链接、代理账号密码、SQLite 数据库和真实 mihomo 配置；
4. 提交信息使用简洁的 Conventional Commit 风格，例如 `fix: handle provider reload`；
5. 提交 Pull Request 时说明变更内容、验证命令和可能的部署影响。

## 安全提醒

请不要在 Issue、Pull Request 或日志截图中公开机场订阅 URL、代理凭据、mihomo secret、Docker Hub token 或 VPS 凭据。
