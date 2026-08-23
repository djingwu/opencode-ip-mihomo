# OpenCode IP Mihomo — Windows 便携版

该版本已经包含打包后的 Python 服务程序和 `mihomo.exe`，不需要安装 Python、Docker 或 Docker Desktop。

## 使用

1. 将 ZIP 完整解压，例如解压到 `D:\OpenCode-IP-Mihomo`，不要直接在 ZIP 内运行。
2. 双击 `start-windows.bat`。
3. 浏览器访问 `http://127.0.0.1:24513/dashboard`。
4. 停止服务时双击 `stop-windows.bat`。

首次启动会自动创建 `mihomo\config.yaml`、`data`、`logs` 和 `runtime` 目录。

## 数据和更新

运行数据位于：

- `data\`
- `mihomo\config.yaml`
- `mihomo\panel_state.json`
- `mihomo\providers\`

这些文件可能包含订阅链接或代理账号，不要公开分享。更新便携包前先停止服务并备份这些文件。

## 常见问题

- 默认端口：控制台/API `24513`、mihomo `7890`、Controller `9090`、轮换服务 `8001`。如果 `24513` 被占用，将 `portable.env.example` 复制为 `portable.env` 并修改 `GATEWAY_PORT`。
- 启动失败时查看 `logs\mihomo.err.log`、`logs\rotator.err.log` 和 `logs\gateway.err.log`。
- 未签名 EXE 可能触发 Windows SmartScreen；请只从可信发布渠道下载，并先用 Windows Defender 扫描。

## API

基础地址：`http://127.0.0.1:24513`

- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/responses`
- `POST /v1/messages`
