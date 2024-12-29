from src.supply_chain_optimization.logger import logging
from src.supply_chain_optimization.exception import CustomException
import sys
from src.supply_chain_optimization.components.data_ingestion import DataIngestion
from src.supply_chain_optimization.components.data_ingestion import DataIngestionConfig
from src.supply_chain_optimization.pipelines.transform_pipeline import transformer_Pipeline

if __name__=="__main__":
    logging.info("The execution started")

    try:
        #data_ingestion_config=DataIngestionConfig()
        #data_ingestion=DataIngestion()
        #data_ingestion.initiate_data_ingestion()
            elt = transformer_Pipeline()
            elt.execute_pipelines()

    except Exception as e:
        logging.info("Custom Exception")
        raise CustomException(e,sys)