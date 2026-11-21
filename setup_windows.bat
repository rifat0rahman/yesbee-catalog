@echo off
setlocal
cd /d "%~dp0"

echo Checking for Python...
py -3.13 --version >nul 2>&1
if errorlevel 1 (
    echo Python 3.13 is not available. Attempting automatic installation...
    where winget >nul 2>&1
    if errorlevel 1 (
        echo Windows Package Manager was not found.
        echo Install Python 3.13 from https://www.python.org/downloads/windows/
        echo Enable "Add Python to PATH", then run this setup again.
        pause
        exit /b 1
    )
    winget install --id Python.Python.3.13 -e --accept-source-agreements --accept-package-agreements
    if errorlevel 1 (
        echo Could not install Python automatically.
        pause
        exit /b 1
    )
    echo Python was installed. Opening setup again is required.
    echo Close this window and double-click setup_windows.bat again.
    pause
    exit /b 0
)

if not exist ".venv\Scripts\python.exe" (
    echo Creating the local Python environment...
    py -3.13 -m venv .venv
    if errorlevel 1 (
        echo Could not create the Python environment.
        pause
        exit /b 1
    )
)

echo Installing dependencies...
call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo Could not install Python dependencies.
    pause
    exit /b 1
)

echo Installing Chromium for Playwright...
python -m playwright install chromium
if errorlevel 1 (
    echo Could not install Chromium.
    pause
    exit /b 1
)

echo Creating the desktop launcher...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$desktop = [Environment]::GetFolderPath('Desktop'); " ^
  "$shell = New-Object -ComObject WScript.Shell; " ^
  "$shortcut = $shell.CreateShortcut((Join-Path $desktop 'Yesbee Catalog.lnk')); " ^
  "$shortcut.TargetPath = (Resolve-Path '.\start_windows.bat').Path; " ^
  "$shortcut.WorkingDirectory = (Resolve-Path '.').Path; " ^
  "$shortcut.Description = 'Start Yesbee Catalog locally'; " ^
  "$shortcut.Save()"

echo.
echo Setup complete.
echo Double-click "Yesbee Catalog" on the Desktop to start the app.
pause
