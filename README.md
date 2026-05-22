# 📦 Document Processing Pipeline — Портативный пакет

Полностью автономный конвейер обработки документов. Упакован в Docker — работает на любой машине с Docker Desktop «из коробки».

---

## Архитектура системы

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│  ┌──────────┐   ┌──────────────────────────────┐   ┌─────────┐ │
│  │  MinIO   │──▶│       Apache Airflow          │──▶│  Elast- │ │
│  │  S3 API  │   │                               │   │  icsear │ │
│  │ :9000    │   │  ensure_infra ──▶ scan_s3     │   │  ch     │ │
│  │ :9001    │   │                  /        \    │   │ :9200   │ │
│  │ (console)│   │       python_extract   ocr    │   │         │ │
│  │          │   │                 \       /     │   │         │ │
│  │          │   │           embed + index ──────│──▶│         │ │
│  │          │   │                               │   │         │ │
│  │          │   │   Webserver :8080              │   │         │ │
│  │          │   │   Scheduler (фоновый)          │   │         │ │
│  └──────────┘   └──────────────────────────────┘   └─────────┘ │
│                                                                 │
│  ┌──────────┐   ┌────────────┐                                  │
│  │ Postgres │   │  FastAPI   │                                  │
│  │ :5432    │   │  Backend   │                                  │
│  │(метаданн)│   │  :8000     │                                  │
│  └──────────┘   └────────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Компоненты

| Сервис | Порт | Логин / Пароль | Назначение |
|--------|------|----------------|------------|
| **MinIO Console** | `localhost:9001` | `minioadmin` / `minioadmin` | Загрузка документов в S3 |
| **MinIO S3 API** | `localhost:9000` | — | S3-совместимый API |
| **Airflow UI** | `localhost:8080` | `admin` / `admin` | Мониторинг DAG-конвейера |
| **Elasticsearch** | `localhost:9200` | — | Векторный индекс + полнотекст |
| **Search Backend** | `localhost:8000` | — | FastAPI UI + REST API поиска |
| **PostgreSQL** | `localhost:5432` | `airflow` / `airflow_password` | Метаданные Airflow |

### Airflow DAG — Граф обработки

DAG `s3_document_processing_pipeline` состоит из 5 связанных задач:

1. **`ensure_infrastructure_task`** — проверка/создание бакета S3 и индекса ES
2. **`scan_s3_documents_task`** — сканирование бакета, определение типов файлов, фильтрация уже обработанных
3. **`extract_text_via_python_task`** — извлечение текста из PDF (pdfplumber) и текстовых файлов. Некачественные PDF (скан-копии) перенаправляются на OCR
4. **`extract_text_via_ocr_task`** — OCR-распознавание изображений (PNG/JPG) и PDF-фоллбэков через Tesseract
5. **`generate_embeddings_and_index_task`** — нарезка на чанки, генерация 384-мерных векторов (all-MiniLM-L6-v2), индексация в Elasticsearch

---

## 🚀 Быстрый старт (перенос на другой ноутбук)

### Предварительные требования

На целевом ноутбуке должен быть установлен только:

- **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** (Windows/macOS) или **Docker + Docker Compose** (Linux)
- **Минимум 8 ГБ ОЗУ** выделено для Docker (рекомендуется 10+ ГБ)

> ⚠️ **Ничего больше не нужно** — ни Python, ни Node.js, ни Elasticsearch, ни Tesseract. Всё внутри Docker.

### Шаг 1. Скопировать папку

Скопируйте **всю папку `real-pipeline/`** на другой ноутбук (через USB-флешку, архив, сеть — как удобно):

```
real-pipeline/
├── airflow/dags/document_processing_dag.py   ← DAG-конвейер
├── backend/
│   ├── index.html                            ← веб-интерфейс поиска
│   ├── main.py                               ← FastAPI роуты
│   └── search_service.py                     ← поисковый сервис
├── docker-compose.yml                        ← оркестрация всех сервисов
├── Dockerfile.airflow                        ← образ Airflow + Tesseract OCR
├── Dockerfile.backend                        ← образ FastAPI Backend
├── requirements.txt                          ← Python-зависимости
├── .dockerignore                             ← исключения из Docker сборки
├── start.bat                                 ← ⭐ Запуск (Windows)
├── start.sh                                  ← ⭐ Запуск (Linux/macOS)
├── stop.bat                                  ← Остановка (Windows)
└── README.md                                 ← Этот файл
```

### Шаг 2. Запустить

#### Windows
1. Убедитесь, что **Docker Desktop запущен** (иконка кита в трее)
2. Дважды кликните по файлу **`start.bat`**
3. Дождитесь завершения сборки (первый раз ~5-10 минут, далее ~30 секунд)

#### Linux / macOS
```bash
cd real-pipeline
chmod +x start.sh
./start.sh
```

### Шаг 3. Проверить

После запуска откройте в браузере:

| Что | URL |
|-----|-----|
| 🔍 **Поиск документов** | http://localhost:8000 |
| 📊 **Airflow (DAG граф)** | http://localhost:8080 |
| 📁 **MinIO (загрузка файлов)** | http://localhost:9001 |

---

## 📋 Тестирование конвейера (шаг за шагом)

### 1. Загрузите документ в MinIO

1. Откройте http://localhost:9001 (логин `minioadmin` / `minioadmin`)
2. Перейдите в **Object Browser** → бакет `incoming-documents`
3. Нажмите **Create new path** → введите код ЕГРПОУ, например `38472910`
4. Загрузите PDF или изображение внутрь папки

### 2. Наблюдайте обработку в Airflow

1. Откройте http://localhost:8080 (логин `admin` / `admin`)
2. Найдите DAG **`s3_document_processing_pipeline`** → включите тумблер
3. DAG запускается каждую минуту автоматически (или нажмите **Trigger DAG**)
4. Перейдите во вкладку **Graph** — увидите 5 этапов конвейера
5. Кликните на любую задачу → **Logs** чтобы видеть ход обработки

### 3. Выполните поиск

1. Откройте http://localhost:8000
2. Введите ЕГРПОУ `38472910` → нажмите **Найти**
3. Попробуйте **семантический поиск**: введите запрос по смыслу (например «поставка оборудования»)
4. Используйте **ИИ-Ассистент Gemini** для анализа найденных фрагментов

---

## 🛑 Остановка

#### Windows
Дважды кликните **`stop.bat`** или выполните:
```cmd
docker compose down
```

#### Linux / macOS
```bash
docker compose down
```

Данные (MinIO, Elasticsearch, PostgreSQL) сохраняются в Docker volumes и переживают перезапуски.

Чтобы **полностью удалить всё** (включая данные):
```bash
docker compose down -v
```

---

## 🔧 Диагностика проблем

| Проблема | Решение |
|----------|---------|
| Контейнеры не запускаются | Убедитесь что Docker Desktop запущен и выделено ≥8 ГБ ОЗУ |
| Airflow показывает Import Error | Перезапустите: `docker compose restart airflow-scheduler airflow-webserver` |
| Elasticsearch падает с ошибкой памяти | Увеличьте лимит ОЗУ в Docker Desktop → Settings → Resources |
| OCR не распознаёт текст | В контейнере установлены пакеты Tesseract для `ukr`, `rus`, `eng` |
| Порт занят | Остановите конфликтующий сервис или измените порты в `docker-compose.yml` |
