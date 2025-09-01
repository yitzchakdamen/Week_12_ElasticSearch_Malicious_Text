import os
import logging


logger = logging.getLogger(__name__)

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200/")
APP_PORT:int = int(os.getenv("APP_PORT", '8000'))
APP_HOST = os.getenv("APP_HOST", "localhost")
INDEX_NAME = os.getenv("INDEX_NAME", 'tweets')
DATA_URL = os.getenv("DATA_URL", r"data\tweets_injected 3.csv")
WEAPONS_FILE_URL = os.getenv("WEAPONS_FILE_URL", r"data\weapon_list.txt")


logger.info(f"""
            Configuration Loaded:
            ELASTICSEARCH_HOST: {ELASTICSEARCH_HOST}
            data_url: {DATA_URL}
            """)