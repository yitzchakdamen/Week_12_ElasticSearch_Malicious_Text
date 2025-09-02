import logging, threading, uvicorn, management_runner
from config import config
from management.management import Management
from app.app import app



logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logging.getLogger('kafka').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def run_management():
    
    management = Management(dal=management_runner.dal, index_name=config.INDEX_NAME)
    management_runner.status['sum'] = 1
    management.index_documents(file_url=config.DATA_URL, index_name=config.INDEX_NAME)
    management_runner.status['sum'] = 2
    management.analysis_weapons_detected(weapons_file_url=config.WEAPONS_FILE_URL)
    management_runner.status['sum'] = 3
    management.analysis_sentiment()
    management_runner.status['sum'] = 4
    management.delete_not_antisemitic_document()
    management_runner.status["done"] = True


if __name__ == "__main__":
    threading.Thread(target=run_management, daemon=True).start()
    uvicorn.run(app, host=config.APP_HOST, port=config.APP_PORT)
