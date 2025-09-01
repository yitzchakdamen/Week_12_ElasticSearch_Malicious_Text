
from fastapi import FastAPI
import logging
import management_runner
from config import config
from management.services import Services


logger = logging.getLogger(__name__)
app = FastAPI()


@app.get("/api/antisemitic-documents")
async def get_antisemitic_documents():
    return management_runner.dal.get(index_name=config.INDEX_NAME,  query=Services.query_to_get_antisemitic_documents) if management_runner.status["done"] else "processing"

        
@app.get("/api/weapons")
async def get_weapons():
    return management_runner.dal.get(index_name = config.INDEX_NAME, query=Services.query_to_get_weapons_documents) if management_runner.status["done"] else "processing"

        
