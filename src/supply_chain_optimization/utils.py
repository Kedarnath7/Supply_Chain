import os
import sys
from src.supply_chain_optimization.exception import CustomException
from src.supply_chain_optimization.logger import logging
import pandas as pd
from dotenv import load_dotenv
import pymysql

load_dotenv()

host=os.getenv("host")
user=os.getenv("user")
password=os.getenv("password")
db=os.getenv("db")


def read_sql_data():
    logging.info("reading sqldb started")
    try:
        mydb=pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db
        )
        logging.info("Connection established")
        df=pd.read_sql_query('Select * from supplychain',mydb)
        print(df.head())

        return df
        
    except Exception as ex:
        raise CustomException(ex,sys)
