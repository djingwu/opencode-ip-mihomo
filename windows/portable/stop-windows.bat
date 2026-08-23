@echo off
setlocal
cd /d "%~dp0"
"%~dp0app\opencode-launcher.exe" stop
echo.
echo Done.
pause
