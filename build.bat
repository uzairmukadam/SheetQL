@echo off
setlocal EnableExtensions
cd /d "%~dp0" || exit /b 1

where py >nul 2>&1 && (set "PY=py -3") || (set "PY=python")

echo SheetQL build pipeline
echo =======================
echo.
echo [1/2] Installing project + build dependencies ^(.[all,dev]^)...
%PY% -m pip install -q -e ".[all,dev]"
if errorlevel 1 goto :fail

echo.
echo [2/2] Building executable ^(Nuitka, then PyInstaller if needed — may take many minutes^)...
%PY% build.py --backend auto
if errorlevel 1 goto :fail

echo.
echo Done. Run the app from dist — see build log for the path.
echo   Nuitka one-file: dist\sheetql.exe
echo   PyInstaller folder ^(default^): dist\sheetql\sheetql.exe
pause
exit /b 0

:fail
echo.
echo Build failed — see messages above.
pause
exit /b 1
