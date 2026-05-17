@echo off
cd /d "%~dp0"
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0restart_portal.ps1"
