from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch

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

dag = DAG(
    "clear_elasticsearch_pipeline",
    default_args=default_args,
    description="Ручное удаление индекса client_documents в Elasticsearch",
    schedule_interval=None,  # Только ручной запуск (on-demand)
    catchup=False,
    tags=["elasticsearch", "maintenance"],
)

def clear_es_index(**context):
    es = Elasticsearch(ELASTICSEARCH_HOST)
    try:
        if es.indices.exists(index=ES_INDEX_NAME):
            es.indices.delete(index=ES_INDEX_NAME)
            print(f"[✓] Индекс '{ES_INDEX_NAME}' успешно удален.")
        else:
            print(f"[i] Индекс '{ES_INDEX_NAME}' не существует, удаление не требуется.")
    except Exception as e:
        print(f"[!] Ошибка при удалении индекса: {e}")
        raise e

clear_task = PythonOperator(
    task_id="clear_es_index_task",
    python_callable=clear_es_index,
    dag=dag,
)
