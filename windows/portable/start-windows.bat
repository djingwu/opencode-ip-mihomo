@echo off
setlocal
cd /d "%~dp0"
"%~dp0app\opencode-launcher.exe" start
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo.
  echo Startup failed. Check the message above and files in logs.
  pause
)
exit /b %EC%
