from elasticsearch import Elasticsearch, helpers
import logging

logger = logging.getLogger(__name__)

class ElasticSearchDal:

    def __init__(self, elasticsearch_host: str) -> None:
        self.es:Elasticsearch = Elasticsearch(elasticsearch_host)
        logger.info("Elasticsearch cluster is up!" if self.es.ping() else "Elasticsearch cluster is down!")

    def build_mappings(self, mapping:dict) -> dict:
        return {'mappings': {'properties': {field: {"type": field_type} for field, field_type in mapping.items()}}}
    
    def create_index(self, index_name: str, mappings: dict):
        if self.es.indices.exists(index=index_name):
            logger.warning(f"Index '{index_name}' already exists.")
            return False
        
        response = self.es.indices.create(index=index_name, body=mappings)
        logger.info(f"Index '{index_name}' created with mappings: {mappings}")
        return response

    def index_document(self, index_name: str, document: dict, id=None):
        if not self.es.indices.exists(index=index_name):
            logger.warning(f"Index '{index_name}' does not exist.")
            return False
        
        check = self.es.index(index=index_name, document=document, id=id)
        logger.info(f"Document indexed in '{check.body['_index']}'  id: {check.body['_id']}, -> {check.body['result']}")
        return check
    
    def update_document(self, index_name:str, id:str, document:dict):
        if not self.es.indices.exists(index=index_name):
            logger.warning(f"Index '{index_name}' does not exist.")
            return False
        
        check = self.es.update(index=index_name,id=id,doc=document)
        return check
    
    def index_many(self, index_name: str, documents:list[dict]):
        helpers.bulk(client=self.es, actions=[{"_index": index_name, "_source": doc} for doc in documents], refresh="wait_for")

    def update_many(self, documents:list[dict]):
        helpers.bulk(client=self.es, actions=[{"_op_type": "update", "_index": doc['_index'],"_id": doc["_id"], "doc": doc['_source']} for doc in documents], refresh="wait_for")

    def scan(self, index_name: str, query):
        return list(helpers.scan(self.es, index=index_name, query={"query": query}))
    
    def delete_document(self, index_name:str, id=None, query=None):
        if query:
            return self.es.delete_by_query(index=index_name, body={"query": query})
        elif id is not None:
            return self.es.delete(index=index_name , id=id)
            
