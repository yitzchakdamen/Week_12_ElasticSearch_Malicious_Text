from dal.elasticsearch_dal import ElasticSearchDal
from config import config
import time
import logging

logger = logging.getLogger(__name__)


dal = ElasticSearchDal(config.ELASTICSEARCH_HOST)
status = {"done": False}

i = 0 

while not dal.es.ping() and i < 10: 
    print(f"Waiting for Elasticsearch... attempt {i+1}")
    time.sleep(10)
    i += 1

if i == 10:
    logger.warning("Elasticsearch not available after 10 attempts")
else:
    logger.info("Elasticsearch is up!")
