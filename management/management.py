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

    def analysis_document(self, document: dict ,weapons:list):
        document['weapons_detected'] = Analysis.weapons_detected(text=document['text'],weapons=weapons)
        document['sentiment'] = Analysis.sentiment_category(Analysis.analyze_sentiment(text=document['text']))
        return document
    
    def delete_document(self):
        return self.dal.delete_document(index_name=self.index_name, query=Services.query_to_delete)
        
    def analysis_all_document(self, weapons_file_url:str):
        weapons:list = FileManager.uploading_content(weapons_file_url).split() # type: ignore
        documents:list[dict] = self.dal.get(self.index_name, query={"match_all": {}})
        for doc in documents:
            doc['_source'] = self.analysis_document(document=doc['_source'], weapons=weapons)
        self.dal.update_many(documents=documents)
        

        
        
        
        
# {
#   "query": {
#     "bool": {
#       "must": [
#         { "match": { "Antisemitic": "0" }},
#         { "terms": { "Sentiment": ["neutral", "positive"] }},
#         {
#           "bool": {
#             "should": [
#               {
#                 "script_score": {
#                   "query": {
#                     "exists": {
#                       "field": "Weapons"
#                     }
#                   },
#                   "script": {
#                     "source": "if (doc['Weapons'].size() > 0) { return doc['Weapons'].size() } else { return 0 }"
#                   }
#                 }
#               }
#             ]
#           }
#         }
#       ],
#       "must_not": []
#     }
#   }
# }
