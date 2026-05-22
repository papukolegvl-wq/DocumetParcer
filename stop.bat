@echo off
chcp 65001 >nul 2>&1
title Document Processing Pipeline — Остановка

echo ============================================================
echo   Остановка всех контейнеров...
echo ============================================================

docker compose down 2>nul || docker-compose down

echo.
echo   Все контейнеры остановлены.
echo.
pause
