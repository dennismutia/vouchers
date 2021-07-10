import gzip
import json
import os
import glob
import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine

# path to folder with data
dir = os.path.join(os.getcwd(), 'data/vouchers/')

# database connection
engine = create_engine('postgresql://postgres@localhost:5432/vouchers')

# data when to start writing files to db
max_date = datetime.strptime('2018-11-23', '%Y-%m-%d') # change to fetch from db

def read_file_to_db(dir):

    with gzip.open(dir, "rb") as f:
        data = json.loads(f.read())

    key = list(data)[0]
    df = pd.DataFrame.from_dict(data[key])
    df['date'] = datetime.strptime(key, '%Y-%m-%d')
    df = df.reset_index()
    df.to_sql('stg_vouchers', engine, if_exists='replace')
    
    return df

def get_files(dir, max_date):    
    
    files_to_db = []

    for file in glob.glob(os.path.join(dir, "*.gz")):
        file_date = datetime.strptime(file[50:60], '%Y-%m-%d')
        if file_date > max_date:
            files_to_db.append(file)

    
    for file in files_to_db:
        try:
            read_file_to_db(file)
            print('{} written successfully to db'.format(file))
        except Exception as e:
            print(e)


if __name__ == "__main__":
    get_files(dir, max_date)