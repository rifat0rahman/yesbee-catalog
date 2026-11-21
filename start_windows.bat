@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo The app is not set up yet.
    echo Double-click setup_windows.bat first.
    pause
    exit /b 1
)

powershell -NoProfile -Command ^
  "try { Invoke-WebRequest -Uri 'http://127.0.0.1:8000' -UseBasicParsing -TimeoutSec 2 ^| Out-Null; exit 0 } catch { exit 1 }"

if not errorlevel 1 (
    start "" "http://127.0.0.1:8000"
    exit /b 0
)

start "Yesbee Catalog Server" /min "%~dp0.venv\Scripts\python.exe" "%~dp0main.py"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ready = $false; " ^
  "for ($i = 0; $i -lt 60; $i++) { " ^
  "  try { Invoke-WebRequest -Uri 'http://127.0.0.1:8000' -UseBasicParsing -TimeoutSec 1 ^| Out-Null; $ready = $true; break } " ^
  "  catch { Start-Sleep -Milliseconds 500 } " ^
  "}; " ^
  "if ($ready) { Start-Process 'http://127.0.0.1:8000' }"
