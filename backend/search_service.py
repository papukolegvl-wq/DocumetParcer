import os
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

class SearchService:
    def __init__(self):
        # Подключение к Elasticsearch
        es_host = os.environ.get("ELASTICSEARCH_HOST", "http://localhost:9200")
        self.es = Elasticsearch(es_host)
        self.index_name = "client_documents"
        
        # Модель для генерации эмбеддингов (384-размерные векторы)
        # При первом запуске она скачается автоматически во внутренний кэш
        print("Инициализация модели sentence-transformers...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        print("Модель готова.")
        
        # Автоматическая инициализация индекса в ES
        self.initialize_index()

    def initialize_index(self):
        """Создает индекс в Elasticsearch, если он не существует"""
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "egrpou": {"type": "keyword"},
                        "filename": {"type": "keyword"},
                        "text": {"type": "text"},
                        "vector": {
                            "type": "dense_vector",
                            "dims": 384,  # Модель all-MiniLM-L6-v2 возвращает 384 измерения
                            "index": True,
                            "similarity": "cosine"
                        },
                        "processed_at": {"type": "date"}
                    }
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)
            print(f"Индекс '{self.index_name}' успешно создан.")
        else:
            print(f"Индекс '{self.index_name}' уже существует.")

    def get_embedding(self, text: str):
        """Генерирует векторный эмбеддинг для текста"""
        vector = self.model.encode(text)
        return vector.tolist()

    def index_chunk(self, chunk_id: str, egrpou: str, filename: str, text: str, processed_at: str):
        """Индексирует один текстовый чанк с вектором в Elasticsearch"""
        vector = self.get_embedding(text)
        doc = {
            "egrpou": egrpou,
            "filename": filename,
            "text": text,
            "vector": vector,
            "processed_at": processed_at
        }
        res = self.es.index(index=self.index_name, id=chunk_id, body=doc)
        return res

    def search(self, egrpou: str, query: str, mode: str = "keyword", limit: int = 10):
        """
        Ищет документы клиента с фильтрацией по ЕГРПОУ.
        mode может быть 'keyword' или 'semantic'
        """
        # Если поискового текстового запроса нет, возвращаем все документы данного клиента
        if not query.strip():
            body = {
                "query": {
                    "term": {"egrpou": egrpou}
                },
                "size": limit
            }
            res = self.es.search(index=self.index_name, body=body)
            return self._format_results(res)

        if mode == "keyword":
            # Классический текстовый поиск с фильтром по ЕГРПОУ
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"text": query}}
                        ],
                        "filter": [
                            {"term": {"egrpou": egrpou}}
                        ]
                    }
                },
                "size": limit
            }
            res = self.es.search(index=self.index_name, body=body)
            return self._format_results(res)
            
        elif mode == "semantic":
            # Векторный (k-NN) поиск с фильтром по ЕГРПОУ
            query_vector = self.get_embedding(query)
            body = {
                "knn": {
                    "field": "vector",
                    "query_vector": query_vector,
                    "k": limit,
                    "num_candidates": limit * 5,
                    "filter": {
                        "term": {"egrpou": egrpou}
                    }
                }
            }
            res = self.es.search(index=self.index_name, body=body)
            return self._format_results(res, is_knn=True)
        else:
            raise ValueError(f"Неизвестный режим поиска: {mode}")

    def _format_results(self, raw_res, is_knn=False):
        """Вспомогательный метод для красивого форматирования выдачи результатов"""
        hits = raw_res.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            source = hit["_source"]
            # В случае k-NN возвращается скор близости _score, нормируем его для отображения
            score = hit["_score"]
            results.append({
                "id": hit["_id"],
                "text": source["text"],
                "filename": source["filename"],
                "egrpou": source["egrpou"],
                "processed_at": source.get("processed_at"),
                "score": round(score, 4)
            })
        return results

    def get_unique_files(self, egrpou: str) -> list:
        """Возвращает список уникальных имен файлов для данного ЕГРПОУ"""
        body = {
            "size": 0,
            "query": {
                "term": {"egrpou": egrpou}
            },
            "aggs": {
                "unique_files": {
                    "terms": {
                        "field": "filename",
                        "size": 1000
                    }
                }
            }
        }
        res = self.es.search(index=self.index_name, body=body)
        buckets = res.get("aggregations", {}).get("unique_files", {}).get("buckets", [])
        return [b["key"] for b in buckets]

