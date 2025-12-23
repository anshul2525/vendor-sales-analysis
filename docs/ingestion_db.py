import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# -------------------- DB PATH (SAME EVERYWHERE) --------------------
DB_PATH = r"C:\Users\Anshul\data\inventory.db"

# -------------------- LOGGING --------------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# -------------------- DB ENGINE --------------------
engine = create_engine(f"sqlite:///{DB_PATH}")

# -------------------- FUNCTIONS --------------------
def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(
        table_name,
        con=engine,
        if_exists='replace',
        index=False
    )


def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''

    start = time.time()
    folder_path = r"E:\ETE - 1\data"

    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)

            logging.info(f'Ingesting {file} into database')
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time_taken = (end - start) / 60

    logging.info('---------- Ingestion complete ----------')
    logging.info(f'Total Time Taken: {total_time_taken} minutes')


# -------------------- MAIN --------------------
if __name__ == '__main__':
    load_raw_data()
