#!/usr/bin/env bash
set -e

echo "============================================================"
echo "  Document Processing Pipeline — Автозапуск"
echo "============================================================"
echo

# ─── Проверяем Docker ───
echo "[1/3] Проверка Docker..."
if ! docker info >/dev/null 2>&1; then
    echo
    echo "[ОШИБКА] Docker не запущен!"
    echo "Запустите Docker (Docker Desktop или systemctl start docker)"
    echo "и повторите попытку."
    exit 1
fi
echo "      Docker работает."

# ─── Определяем команду compose ───
echo "[2/3] Проверка Docker Compose..."
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif docker-compose --version >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "[ОШИБКА] Docker Compose не найден!"
    exit 1
fi
echo "      Docker Compose доступен."

# ─── Запуск ───
echo "[3/3] Сборка и запуск контейнеров..."
echo

cd "$(dirname "$0")"
$COMPOSE_CMD up --build -d

echo
echo "============================================================"
echo "  Все сервисы успешно запущены!"
echo "============================================================"
echo
echo "  Airflow UI:           http://localhost:8080   (admin / admin)"
echo "  MinIO Console:        http://localhost:9001   (minioadmin / minioadmin)"
echo "  Elasticsearch:        http://localhost:9200"
echo "  Search Backend (UI):  http://localhost:8000"
echo
echo "  Для остановки:  $COMPOSE_CMD down"
echo "============================================================"
