@echo off
chcp 65001 >nul
setlocal

set "ROOT=%~dp0"

echo ========================================
echo 短线打板盯盘系统启动脚本
echo ========================================
echo.

if not exist "%ROOT%backend\app\main.py" (
  echo 未找到后端入口文件：%ROOT%backend\app\main.py
  pause
  exit /b 1
)

if not exist "%ROOT%frontend\package.json" (
  echo 未找到前端 package.json：%ROOT%frontend\package.json
  pause
  exit /b 1
)

echo 启动后端服务：http://127.0.0.1:8000
start "Watchtower Backend" cmd /k "cd /d "%ROOT%backend" && python -m uvicorn app.main:app --reload"

echo 启动前端网站：http://127.0.0.1:5173/
start "Watchtower Frontend" cmd /k "cd /d "%ROOT%frontend" && if not exist node_modules npm install && npm run dev -- --host 127.0.0.1"

echo.
echo 已发起启动，请稍等服务完成初始化。
echo 前端访问地址：http://127.0.0.1:5173/
echo 后端接口地址：http://127.0.0.1:8000
echo.
pause
