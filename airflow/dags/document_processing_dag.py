from datetime import datetime, timedelta
import io
import os
import re
import traceback
import json
import zipfile
import xml.etree.ElementTree as ET
from PIL import Image

from airflow import DAG
from airflow.operators.python import PythonOperator

import boto3
import pdfplumber
import pytesseract
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

# ──────────────────────────────────────────────────────────────────────────────
# Конфигурационные константы
# ──────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"
BUCKET_NAME       = "incoming-documents"

ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST", "http://pipeline-elasticsearch:9200")
ES_INDEX_NAME      = "client_documents"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────────

def get_s3_client():
    """Создает клиент подключения к MinIO/S3"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def get_es_client():
    """Создает клиент подключения к Elasticsearch"""
    return Elasticsearch(ELASTICSEARCH_HOST)


def _cyrillic_ratio(text: str) -> float:
    """Возвращает долю кириллических символов среди всех букв"""
    if not text.strip():
        return 0.0
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return 0.0
    cyrillic = sum(1 for c in letters if "\u0400" <= c <= "\u04FF")
    return cyrillic / len(letters)


def _extract_text_from_docx(file_bytes: bytes) -> str:
    """Извлекает текст из файлов DOCX (Office Open XML) без сторонних зависимостей"""
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            xml_content = z.read("word/document.xml")
            root = ET.fromstring(xml_content)
            
            paragraphs = []
            for elem in root.iter():
                # Проверяем тег параграфа
                if elem.tag.endswith('}p') or elem.tag == 'p':
                    p_text = []
                    # Находим все текстовые элементы внутри параграфа
                    for child in elem.iter():
                        if child.tag.endswith('}t') or child.tag == 't':
                            if child.text:
                                p_text.append(child.text)
                    paragraphs.append("".join(p_text))
            return "\n".join(paragraphs)
    except Exception as e:
        print(f"[!] Ошибка извлечения текста из DOCX: {e}")
        return ""


# ──────────────────────────────────────────────────────────────────────────────
# ШАГ 1: Проверка и инициализация инфраструктуры
# ──────────────────────────────────────────────────────────────────────────────

def ensure_infrastructure(**context):
    """
    Проверяет и инициализирует:
      - бакет incoming-documents в MinIO
      - индекс client_documents в Elasticsearch (с маппингом dense_vector)
    """
    # 1. Проверяем S3 Бакет
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"[✓] MinIO бакет '{BUCKET_NAME}' уже существует.")
    except Exception:
        print(f"[+] Создаём бакет '{BUCKET_NAME}' в MinIO...")
        s3.create_bucket(Bucket=BUCKET_NAME)

    # 2. Проверяем Elasticsearch Индекс
    es = get_es_client()
    if not es.indices.exists(index=ES_INDEX_NAME):
        mapping = {
            "mappings": {
                "properties": {
                    "egrpou":       {"type": "keyword"},
                    "filename":     {"type": "keyword"},
                    "text":         {"type": "text"},
                    "vector": {
                        "type":       "dense_vector",
                        "dims":       384,
                        "index":      True,
                        "similarity": "cosine",
                    },
                    "processed_at": {"type": "date"},
                }
            }
        }
        es.indices.create(index=ES_INDEX_NAME, body=mapping)
        print(f"[+] Индекс Elasticsearch '{ES_INDEX_NAME}' успешно создан.")
    else:
        print(f"[✓] Индекс Elasticsearch '{ES_INDEX_NAME}' уже существует.")


# ──────────────────────────────────────────────────────────────────────────────
# ШАГ 2: Сканирование S3 — поиск новых файлов
# ──────────────────────────────────────────────────────────────────────────────

def scan_s3_documents(**context):
    """
    Сканирует S3 бакет, определяет типы файлов (pdf, image, text).
    Исключает файлы, уже проиндексированные в Elasticsearch.

    Публикует в XCom список файлов:
      [{"key": "...", "egrpou": "...", "filename": "...", "ext_type": "pdf"|"image"|"text"}, ...]
    """
    s3 = get_s3_client()
    es = get_es_client()

    # Получаем ВСЕ объекты из бакета (с пагинацией)
    all_objects = []
    continuation_token = None
    try:
        while True:
            list_kwargs = {"Bucket": BUCKET_NAME}
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token
            response = s3.list_objects_v2(**list_kwargs)
            all_objects.extend(response.get("Contents", []))
            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break
    except Exception as e:
        print(f"[!] Ошибка получения списка файлов из S3: {e}")
        context["ti"].xcom_push(key="files_to_process", value=[])
        return

    if not all_objects:
        print("[i] Бакет S3 пуст. Новых файлов для обработки нет.")
        context["ti"].xcom_push(key="files_to_process", value=[])
        return

    IMAGE_EXTS = {".png", ".jpg", ".jpeg"}
    PDF_EXTS   = {".pdf"}
    DOCX_EXTS  = {".docx"}

    files_to_process = []

    for obj in all_objects:
        file_key  = obj["Key"]
        file_size = obj["Size"]

        # Пропускаем папки и пустые объекты
        if file_size == 0 or file_key.endswith("/"):
            continue

        # Разбираем путь S3: ожидаем структуру '{egrpou}/{filename}'
        path_parts = file_key.strip("/").split("/")
        if len(path_parts) >= 2:
            egrpou   = path_parts[0]
            filename = "/".join(path_parts[1:])
        else:
            egrpou   = "00000000"
            filename = file_key
            print(f"[!] Не удалось извлечь ЕГРПОУ из пути '{file_key}'. Присвоен дефолтный.")

        # Проверяем, был ли файл уже проиндексирован (по ID первого чанка)
        file_id = f"{egrpou}-{filename.replace('/', '_').replace('.', '_')}-0"
        try:
            if es.exists(index=ES_INDEX_NAME, id=file_id):
                print(f"[~] Файл '{filename}' (ЄДРПОУ {egrpou}) уже проіндексований (ID: {file_id}). Пропускаємо.")
                continue
        except Exception as e:
            print(f"[!] Ошибка проверки индекса ES для '{filename}': {e}")

        # Определяем тип файла
        ext = os.path.splitext(filename.lower())[1]
        if ext in PDF_EXTS:
            ext_type = "pdf"
        elif ext in IMAGE_EXTS:
            ext_type = "image"
        elif ext in DOCX_EXTS:
            ext_type = "docx"
        else:
            ext_type = "text"

        files_to_process.append({
            "key":      file_key,
            "egrpou":   egrpou,
            "filename": filename,
            "ext_type": ext_type,
        })
        print(f"[+] Найден новый файл для обработки: {file_key} (тип: {ext_type})")

    print(f"\n[i] Итого новых файлов для обработки: {len(files_to_process)}")
    context["ti"].xcom_push(key="files_to_process", value=files_to_process)


# ──────────────────────────────────────────────────────────────────────────────
# ШАГ 3: Извлечение текста через Python (pdfplumber / raw text)
# ──────────────────────────────────────────────────────────────────────────────

def extract_text_via_python(**context):
    """
    Обрабатывает файлы типа 'pdf' и 'text' из scan_s3_documents_task:
      - PDF: извлекает текст через pdfplumber; если кириллицы < 30% — файл
             направляется в список ocr_callbacks для OCR-фоллбэка.
      - text: декодирует байты как UTF-8.

    Публикует в XCom:
      successful_extractions — список {"key", "egrpou", "filename", "text"}
      ocr_callbacks          — список {"key", "egrpou", "filename"} для OCR-фоллбека
    """
    ti = context["ti"]
    files_to_process = ti.xcom_pull(task_ids="scan_s3_documents_task", key="files_to_process") or []

    # Отбираем только pdf, docx и text
    target_files = [f for f in files_to_process if f["ext_type"] in ("pdf", "docx", "text")]
    print(f"[i] Файлов для Python-парсинга: {len(target_files)}")

    s3 = get_s3_client()
    successful_extractions = []
    ocr_callbacks          = []

    for file_meta in target_files:
        file_key  = file_meta["key"]
        egrpou    = file_meta["egrpou"]
        filename  = file_meta["filename"]
        ext_type  = file_meta["ext_type"]

        print(f"\n--- Python-парсинг: {file_key} ---")

        # Скачиваем файл из S3 в оперативную память
        try:
            file_obj = io.BytesIO()
            s3.download_fileobj(BUCKET_NAME, file_key, file_obj)
            file_obj.seek(0)
            file_data = file_obj.read()
            print(f"[✓] Файл скачан ({len(file_data)} байт).")
        except Exception as e:
            print(f"[!] Ошибка скачивания '{file_key}': {e}")
            continue

        raw_text = ""

        if ext_type == "pdf":
            try:
                with pdfplumber.open(io.BytesIO(file_data)) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            pages_text.append(text)
                    raw_text = "\n".join(pages_text)
                print(f"[✓] pdfplumber извлёк {len(raw_text)} символов.")

                ratio = _cyrillic_ratio(raw_text)
                print(f"[i] Доля кириллицы: {ratio:.2%}")

                if ratio < 0.3 or not raw_text.strip():
                    print(f"[~] Качество текста низкое — направляем '{filename}' на OCR-фоллбэк.")
                    ocr_callbacks.append({"key": file_key, "egrpou": egrpou, "filename": filename})
                    continue
                else:
                    print(f"[✓] PDF успешно распарсен через pdfplumber.")
            except Exception as e:
                print(f"[!] Ошибка pdfplumber для '{filename}': {e}")
                traceback.print_exc()
                ocr_callbacks.append({"key": file_key, "egrpou": egrpou, "filename": filename})
                continue

        elif ext_type == "docx":
            try:
                raw_text = _extract_text_from_docx(file_data)
                print(f"[✓] _extract_text_from_docx извлёк {len(raw_text)} символов.")
            except Exception as e:
                print(f"[!] Ошибка парсинга DOCX для '{filename}': {e}")
                traceback.print_exc()
                continue

        else:  # ext_type == "text"
            raw_text = file_data.decode("utf-8", errors="ignore")
            print(f"[✓] Текстовый файл декодирован ({len(raw_text)} символов).")

        if raw_text.strip():
            successful_extractions.append({
                "key":      file_key,
                "egrpou":   egrpou,
                "filename": filename,
                "text":     raw_text,
            })

    print(f"\n[i] Python-парсинг завершён. Успешно: {len(successful_extractions)}, OCR-фоллбэк: {len(ocr_callbacks)}")
    ti.xcom_push(key="successful_extractions", value=successful_extractions)
    ti.xcom_push(key="ocr_callbacks",          value=ocr_callbacks)


# ──────────────────────────────────────────────────────────────────────────────
# ШАГ 4: Извлечение текста через OCR (Tesseract)
# ──────────────────────────────────────────────────────────────────────────────

def extract_text_via_ocr(**context):
    """
    Обрабатывает два источника файлов для OCR:
      1. Файлы типа 'image' из scan_s3_documents_task.
      2. OCR-фоллбэки из extract_text_via_python_task (PDF с плохим текстовым слоем).

    Для PDF: конвертирует в изображения через pdf2image, затем Tesseract.
    Для изображений: Tesseract напрямую.

    Публикует в XCom:
      ocr_extractions — список {"key", "egrpou", "filename", "text"}
    """
    ti = context["ti"]
    files_to_process = ti.xcom_pull(task_ids="scan_s3_documents_task", key="files_to_process") or []
    ocr_callbacks    = ti.xcom_pull(task_ids="extract_text_via_python_task", key="ocr_callbacks") or []

    # Отбираем изображения из S3-сканирования
    image_files = [f for f in files_to_process if f["ext_type"] == "image"]

    # Объединяем источники
    all_ocr_jobs = []
    for f in image_files:
        all_ocr_jobs.append({"key": f["key"], "egrpou": f["egrpou"], "filename": f["filename"], "source_type": "image"})
    for f in ocr_callbacks:
        all_ocr_jobs.append({"key": f["key"], "egrpou": f["egrpou"], "filename": f["filename"], "source_type": "pdf_fallback"})

    print(f"[i] Файлов для OCR-обработки: {len(all_ocr_jobs)} (изображений: {len(image_files)}, PDF-фоллбэков: {len(ocr_callbacks)})")

    s3 = get_s3_client()
    ocr_extractions = []

    for job in all_ocr_jobs:
        file_key    = job["key"]
        egrpou      = job["egrpou"]
        filename    = job["filename"]
        source_type = job["source_type"]

        print(f"\n--- OCR обработка ({source_type}): {file_key} ---")

        # Скачиваем файл из S3
        try:
            file_obj = io.BytesIO()
            s3.download_fileobj(BUCKET_NAME, file_key, file_obj)
            file_obj.seek(0)
            file_data = file_obj.read()
            print(f"[✓] Файл скачан ({len(file_data)} байт).")
        except Exception as e:
            print(f"[!] Ошибка скачивания '{file_key}': {e}")
            continue

        raw_text = ""

        try:
            if source_type == "pdf_fallback":
                from pdf2image import convert_from_bytes
                print(f"[i] Конвертирование PDF в изображения (DPI=300)...")
                images = convert_from_bytes(file_data, dpi=300)
                ocr_pages = []
                for i, img in enumerate(images):
                    page_text = pytesseract.image_to_string(img, lang="ukr+rus+eng")
                    if page_text.strip():
                        ocr_pages.append(page_text)
                    print(f"  [OCR] Страница {i+1}/{len(images)}: {len(page_text)} символов")
                raw_text = "\n".join(ocr_pages)
                print(f"[✓] OCR PDF завершён. Всего символов: {len(raw_text)}")

            else:  # image
                print(f"[i] OCR изображения через Tesseract...")
                img = Image.open(io.BytesIO(file_data))
                raw_text = pytesseract.image_to_string(img, lang="ukr+rus+eng")
                print(f"[✓] OCR изображения завершён. Символов: {len(raw_text)}")

        except Exception as e:
            print(f"[!] Ошибка OCR для '{filename}': {e}")
            traceback.print_exc()
            continue

        if raw_text.strip():
            ocr_extractions.append({
                "key":      file_key,
                "egrpou":   egrpou,
                "filename": filename,
                "text":     raw_text,
            })
        else:
            print(f"[!] OCR не извлёк текст из '{filename}'. Файл будет пропущен.")

    print(f"\n[i] OCR завершён. Успешно распознано файлов: {len(ocr_extractions)}")
    ti.xcom_push(key="ocr_extractions", value=ocr_extractions)


# ──────────────────────────────────────────────────────────────────────────────
# ШАГ 5: Генерация эмбеддингов и индексация в Elasticsearch
# ──────────────────────────────────────────────────────────────────────────────

def generate_embeddings_and_index(**context):
    """
    Объединяет результаты Python-парсера и OCR.
    Для каждого документа:
      1. Нарезает текст на перекрывающиеся чанки (~150 слов, overlap 30 слов).
      2. Генерирует эмбеддинги через SentenceTransformer("all-MiniLM-L6-v2").
      3. Записывает чанки с векторами в индекс Elasticsearch.
    """
    ti = context["ti"]
    python_extractions = ti.xcom_pull(task_ids="extract_text_via_python_task", key="successful_extractions") or []
    ocr_extractions    = ti.xcom_pull(task_ids="extract_text_via_ocr_task",    key="ocr_extractions")        or []

    all_documents = python_extractions + ocr_extractions
    print(f"[i] Всего документов для индексации: {len(all_documents)} "
          f"(Python-парсер: {len(python_extractions)}, OCR: {len(ocr_extractions)})")

    if not all_documents:
        print("[i] Нет документов для индексации. Завершаем.")
        return

    # Загружаем векторную модель
    print("\n[i] Загрузка векторной модели sentence-transformers (all-MiniLM-L6-v2)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print("[✓] Модель успешно загружена.")

    es = get_es_client()
    processed_at = datetime.utcnow().isoformat()

    CHUNK_SIZE = 150   # слов в чанке
    OVERLAP    = 30    # перекрытие в словах

    total_chunks_indexed = 0

    for doc in all_documents:
        egrpou   = doc["egrpou"]
        filename = doc["filename"]
        raw_text = doc["text"]

        print(f"\n--- Индексация: {filename} (ЄДРПОУ {egrpou}) ---")

        # Нарезка на чанки
        words  = raw_text.split()
        chunks = []
        i      = 0
        while i < len(words):
            chunk_words = words[i : i + CHUNK_SIZE]
            chunks.append(" ".join(chunk_words))
            if i + CHUNK_SIZE >= len(words):
                break
            i += (CHUNK_SIZE - OVERLAP)

        print(f"[i] Нарезано {len(chunks)} чанков для индексации.")

        for idx, chunk_text in enumerate(chunks):
            chunk_id = f"{egrpou}-{filename.replace('/', '_').replace('.', '_')}-{idx}"
            try:
                vector = model.encode(chunk_text).tolist()
                doc_body = {
                    "egrpou":       egrpou,
                    "filename":     filename,
                    "text":         chunk_text,
                    "vector":       vector,
                    "processed_at": processed_at,
                }
                es.index(index=ES_INDEX_NAME, id=chunk_id, body=doc_body)
                print(f"  [✓] Чанк #{idx+1}/{len(chunks)} проіндексовано (ID: {chunk_id})")
                total_chunks_indexed += 1
            except Exception as e:
                print(f"  [!] Ошибка индексации чанка #{idx+1}: {e}")

        print(f"[✓] Документ '{filename}' полностью проіндексовано.")

    print(f"\n{'='*60}")
    print(f"[✓] КОНВЕЙЕР ЗАВЕРШЁН. Всього проіндексовано чанків: {total_chunks_indexed}")
    print(f"{'='*60}")


# ──────────────────────────────────────────────────────────────────────────────
# Определение Airflow DAG
# ──────────────────────────────────────────────────────────────────────────────

with DAG(
    "s3_document_processing_pipeline",
    default_args=default_args,
    description="Конвейер обработки документів: S3 → Python/OCR парсинг → Elasticsearch",
    schedule_interval="*/1 * * * *",  # Запуск каждую минуту
    catchup=False,
    max_active_runs=1,
    tags=["documents", "ocr", "elasticsearch", "nlp"],
) as dag:

    # Шаг 1: Проверка инфраструктуры
    task_ensure_infra = PythonOperator(
        task_id="ensure_infrastructure_task",
        python_callable=ensure_infrastructure,
    )

    # Шаг 2: Сканирование S3
    task_scan_s3 = PythonOperator(
        task_id="scan_s3_documents_task",
        python_callable=scan_s3_documents,
    )

    # Шаг 3: Извлечение текста через Python (pdfplumber)
    task_python_extract = PythonOperator(
        task_id="extract_text_via_python_task",
        python_callable=extract_text_via_python,
    )

    # Шаг 4: Извлечение текста через OCR (Tesseract)
    task_ocr_extract = PythonOperator(
        task_id="extract_text_via_ocr_task",
        python_callable=extract_text_via_ocr,
    )

    # Шаг 5: Генерация эмбеддингов и индексация в ES
    task_index_es = PythonOperator(
        task_id="generate_embeddings_and_index_task",
        python_callable=generate_embeddings_and_index,
    )

    # ──────────────────────────────────────────────────
    # Граф зависимостей:
    #
    #   ensure_infrastructure_task
    #           │
    #   scan_s3_documents_task
    #         /         \
    # extract_text_    extract_text_
    # via_python_task  via_ocr_task
    #         \         /
    #   generate_embeddings_and_index_task
    # ──────────────────────────────────────────────────

    task_ensure_infra >> task_scan_s3 >> [task_python_extract, task_ocr_extract]
    task_python_extract >> task_ocr_extract          # OCR нужны fallback-данные от Python-парсера
    [task_python_extract, task_ocr_extract] >> task_index_es
