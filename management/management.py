from dal.elasticsearch_dal import ElasticSearchDal
from management.file_manager import FileManager
from management.services import Services
from management.analysis import Analysis

class Management:
    
    def __init__(self, dal:ElasticSearchDal,  index_name:str) -> None:
        self.dal = dal
        self.index_name = index_name
        self.dal.create_index(index_name=index_name, mappings=self.dal.build_mappings(Services.format_mapping))
    
    def index_documents(self, file_url:str, index_name:str):
        documents:list[dict] = FileManager.uploading_content(file_url=file_url)  # type: ignore
        format_documents = Services.format_document(documents)
        self.dal.index_many(documents=format_documents, index_name=index_name)

    def delete_not_antisemitic_document(self):
        return self.dal.delete_document(index_name=self.index_name, query=Services.query_to_delete)
        
    def analysis_weapons_detected(self, weapons_file_url:str):
        weapons = FileManager.uploading_content(weapons_file_url).split()  # type: ignore
        documents:list[dict] = self.dal.scan(self.index_name, query={"match": {"text": weapons}})
        
        for doc in documents:
            doc['_source']['weapons_detected'] = Analysis.weapons_detected(text=doc['_source']['text'], weapons=weapons)
        self.dal.update_many(documents=documents)
        
    def analysis_sentiment(self):
        documents:list[dict] = self.dal.scan(self.index_name, query={"match_all": {}})
        
        for doc in documents:
            doc['_source']['sentiment'] = Analysis.sentiment_category(Analysis.analyze_sentiment(text=doc['_source']['text']))
        self.dal.update_many(documents=documents)