from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os
from search_service import SearchService

# Инициализируем FastAPI
app = FastAPI(
    title="Document Search API Service",
    description="Реальный API поиска документов клиента с поддержкой Keyword и Vector/Semantic поиска",
    version="1.0.0"
)

# Настройка CORS, чтобы фронтенд мог свободно общаться с бэкендом
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ленивая инициализация поискового сервиса
search_service = None

@app.on_event("startup")
def startup_event():
    global search_service
    # Создаем экземпляр сервиса поиска при запуске FastAPI
    search_service = SearchService()

@app.get("/", response_class=HTMLResponse)
def read_root():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h3>Search Frontend UI (index.html not found)</h3>"

class SearchResult(BaseModel):
    id: str
    text: str
    filename: str
    egrpou: str
    processed_at: Optional[str]
    score: float

@app.get("/search", response_model=List[SearchResult])
def search_documents(
    egrpou: str = Query(..., description="Код ЕГРПОУ/ОКПО клиента"),
    query: Optional[str] = Query("", description="Текстовый запрос для поиска внутри документов"),
    mode: Optional[str] = Query("keyword", description="Режим поиска: 'keyword' (по точным словам) или 'semantic' (векторный по смыслу)"),
    limit: Optional[int] = Query(10, description="Лимит выводимых чанков")
):
    """
    Выполняет поиск документов конкретного клиента (по ЕГРПОУ) в Elasticsearch.
    Поддерживает полнотекстовый (keyword) и семантический (semantic) режимы.
    """
    if not egrpou:
        raise HTTPException(status_code=400, detail="Параметр 'egrpou' является обязательным.")
    
    if mode not in ["keyword", "semantic"]:
        raise HTTPException(status_code=400, detail="Режим 'mode' должен быть либо 'keyword', либо 'semantic'.")

    try:
        results = search_service.search(egrpou=egrpou, query=query, mode=mode, limit=limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка поиска в Elasticsearch: {str(e)}")


class DocumentPayload(BaseModel):
    id: str
    egrpou: str
    filename: str
    text: str

@app.post("/index")
def index_document_chunk(payload: DocumentPayload):
    """
    Позволяет вручную проиндексировать документ/чанк.
    Используется для прямого тестирования Elasticsearch бэкенда в обход Airflow.
    """
    try:
        processed_at = datetime.now().isoformat()
        res = search_service.index_chunk(
            chunk_id=payload.id,
            egrpou=payload.egrpou,
            filename=payload.filename,
            text=payload.text,
            processed_at=processed_at
        )
        return {
            "status": "success",
            "message": f"Чанк '{payload.id}' успешно проиндексирован",
            "elasticsearch_response": dict(res)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось проиндексировать чанк: {str(e)}")

@app.get("/unique_files")
def get_unique_files(egrpou: str = Query(..., description="Код ЕГРПОУ/ОКПО клиента")):
    """
    Возвращает список всех уникальных файлов клиента с данным ЕГРПОУ
    """
    if not egrpou:
        raise HTTPException(status_code=400, detail="Параметр 'egrpou' является обязательным.")
    try:
        files = search_service.get_unique_files(egrpou)
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось получить список файлов: {str(e)}")

