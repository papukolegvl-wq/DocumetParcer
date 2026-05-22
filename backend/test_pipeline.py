import os
import time
import boto3
from elasticsearch import Elasticsearch
import urllib.request
import urllib.parse
import json

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "incoming-documents"

ELASTICSEARCH_HOST = "http://elasticsearch:9200"
ES_INDEX_NAME = "client_documents"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version="s3v4")
    )

def test_pipeline():
    print("--- ЗАПУСК ТЕСТА КОНВЕЙЕРА ОБРАБОТКИ ---")
    s3 = get_s3_client()
    es = Elasticsearch(ELASTICSEARCH_HOST)

    # 1. Создаем тестовое содержимое
    egrpou = "12345678"
    filename = "contract_vector.txt"
    s3_key = f"{egrpou}/{filename}"
    
    document_content = (
        "Договор аренды офисного помещения №402 in городе Киев. "
        "Арендатор: Общество с ограниченной ответственностью 'Вектор' с кодом ЕГРПОУ 12345678. "
        "Предмет договора: аренда нежилого помещения площадью 150 квадратных метров на улице Крещатик. "
        "Ежемесячная оплата составляет пятьдесят тысяч гривен. Контактное лицо: Иванов Иван Иванович."
    )

    print(f"Загружаем тестовый документ в MinIO: {BUCKET_NAME}/{s3_key}...")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=document_content.encode('utf-8')
    )
    print("Документ успешно загружен в MinIO.")

    # 2. Ждем выполнения DAG в Airflow
    wait_time = 75
    print(f"Ждем {wait_time} секунд для обработки документа планировщиком Airflow (DAG запускается каждую минуту)...")
    for i in range(wait_time, 0, -10):
        print(f"Осталось ждать: {i} сек...")
        time.sleep(10) if i >= 10 else time.sleep(i)

    # 3. Проверяем Elasticsearch
    print("\nПроверяем наличие проиндексированного документа в Elasticsearch...")
    query = {
        "query": {
            "term": {"egrpou": egrpou}
        }
    }
    
    try:
        res = es.search(index=ES_INDEX_NAME, body=query)
        hits = res["hits"]["hits"]
        print(f"Найдено документов в индексе для ЕГРПОУ {egrpou}: {len(hits)}")
        for idx, hit in enumerate(hits):
            print(f"Чанк #{idx+1} [ID: {hit['_id']}]:")
            print(f"  Файл: {hit['_source']['filename']}")
            print(f"  Текст: {hit['_source']['text']}")
            print(f"  Вектор (размерность): {len(hit['_source']['vector'])}")
            print(f"  Скор: {hit['_score']}")
    except Exception as e:
        print(f"Ошибка поиска в ES: {e}")
        return

    # 4. Проверяем FastAPI Backend Поиск через API
    print("\nТестируем API поиска бэкенда (http://localhost:8000)...")
    
    # Тест 1: Keyword поиск
    print("\n1. Тест полнотекстового поиска (keyword)...")
    query_params_kw = urllib.parse.urlencode({
        "egrpou": egrpou,
        "query": "аренда офисного помещения Крещатик",
        "mode": "keyword"
    })
    search_url_kw = f"http://localhost:8000/search?{query_params_kw}"
    try:
        req = urllib.request.Request(search_url_kw)
        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode('utf-8')
            res_json = json.loads(res_body)
            print("Keyword поиск успешен! Результаты:")
            for item in res_json:
                print(f"  - Файл: {item['filename']}, Скор: {item['score']}, Текст: {item['text']}")
    except Exception as e:
        print(f"Ошибка отправки запроса Keyword: {e}")

    # Тест 2: Семантический векторный поиск
    print("\n2. Тест семантического (векторного) поиска (semantic)...")
    query_params_sem = urllib.parse.urlencode({
        "egrpou": egrpou,
        "query": "где находится арендованный офис и какая стоимость",
        "mode": "semantic"
    })
    search_url_sem = f"http://localhost:8000/search?{query_params_sem}"
    try:
        req = urllib.request.Request(search_url_sem)
        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode('utf-8')
            res_json = json.loads(res_body)
            print("Семантический поиск успешен! Результаты:")
            for item in res_json:
                print(f"  - Файл: {item['filename']}, Скор: {item['score']}, Текст: {item['text']}")
    except Exception as e:
        print(f"Ошибка отправки запроса Semantic: {e}")

    print("\n--- ТЕСТ КОНВЕЙЕРА ЗАВЕРШЕН ---")

if __name__ == "__main__":
    test_pipeline()
