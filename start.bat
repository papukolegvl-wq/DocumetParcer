@echo off
chcp 65001 >nul 2>&1
title Document Processing Pipeline — Запуск

echo ============================================================
echo   Document Processing Pipeline — Автозапуск
echo ============================================================
echo.

:: ─── Проверяем Docker ───
echo [1/3] Проверка Docker...
docker info >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ОШИБКА] Docker не запущен!
    echo Запустите Docker Desktop и дождитесь полной загрузки,
    echo затем запустите этот скрипт повторно.
    echo.
    pause
    exit /b 1
)
echo       Docker работает.

:: ─── Проверяем docker-compose ───
echo [2/3] Проверка Docker Compose...
docker compose version >nul 2>&1
if errorlevel 1 (
    docker-compose --version >nul 2>&1
    if errorlevel 1 (
        echo.
        echo [ОШИБКА] Docker Compose не найден!
        echo Обновите Docker Desktop до последней версии.
        echo.
        pause
        exit /b 1
    )
    set COMPOSE_CMD=docker-compose
) else (
    set COMPOSE_CMD=docker compose
)
echo       Docker Compose доступен.

:: ─── Запуск контейнеров ───
echo [3/3] Сборка и запуск контейнеров (это может занять 5-10 мин при первом запуске)...
echo.

%COMPOSE_CMD% up --build -d

if errorlevel 1 (
    echo.
    echo [ОШИБКА] Не удалось запустить контейнеры. Проверьте вывод выше.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   Все сервисы успешно запущены!
echo ============================================================
echo.
echo   Airflow UI:           http://localhost:8080   (admin / admin)
echo   MinIO Console:        http://localhost:9001   (minioadmin / minioadmin)
echo   Elasticsearch:        http://localhost:9200
echo   Search Backend (UI):  http://localhost:8000
echo.
echo   Для остановки выполните:  %COMPOSE_CMD% down
echo ============================================================
echo.
pause
