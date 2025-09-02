
from fastapi import FastAPI
import logging
import management_runner
from config import config
from management.services import Services


logger = logging.getLogger(__name__)
app = FastAPI()


@app.get("/api/antisemitic-documents")
async def get_antisemitic_documents():
    if  management_runner.status["done"]:
        return management_runner.dal.scan(index_name=config.INDEX_NAME,  query=Services.query_to_get_antisemitic_documents)
    return f"processing ... done {management_runner.status['sum']} process"
        
@app.get("/api/weapons")
async def get_weapons():
    if management_runner.status["done"]: 
        return management_runner.dal.scan(index_name = config.INDEX_NAME, query=Services.query_to_get_weapons_documents)
    return f"processing ... done {management_runner.status['sum']} process"
        
