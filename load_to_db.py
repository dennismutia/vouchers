import gzip
import json
import os
import glob
import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine

def get_max_date(engine):
    """
    This function gets the latest date in the database. This is the date when data was last loaded.

    Arguments:
        engine: the database connection string

    Returns:
        max_date: date when data was last loaded into the db. Used to filter files after this date for loading to the database.
    """
    try:
        max_date = pd.read_sql_query("select max(date)::timestamp from stg_vouchers;", con=engine).iloc[0].item()
    except:
        max_date = datetime.strptime('2000-01-01', '%Y-%m-%d') 
    return max_date


def read_file_to_db(file, engine):
    """
    This function reads, cleans and writes .json files to a staging table in a database.

    Arguments:
        file: the path of json the file to be written to db
        engine: the database connection string

    Returns:
        None
    """
    with gzip.open(file, "rb") as f:
        data = json.loads(f.read())

    key = list(data)[0]
    df = pd.DataFrame.from_dict(data[key])
    df['date'] = datetime.strptime(key, '%Y-%m-%d')
    df = df.reset_index(drop=True)
    df.to_sql('stg_vouchers', engine, if_exists='append', index=False)
    

def get_files(dir, max_date):    
    """
    This function loops through .json files and identifies the ones not already staged to the db. 
    It then calls the read_file_to_db() function to write these files to the staging db.

    Arguments:
        dir: path to the directory containing the json files
        max_date: the latest date date in the staging table. Used to filter files to the staged to the database.

    Returns:
        None
    """
    
    files_to_db = []

    for file in glob.glob(os.path.join(dir, "*.gz")):
        file_date = datetime.strptime(file[50:60], '%Y-%m-%d')
        if file_date > max_date:
            files_to_db.append(file)

    
    for file in files_to_db:
        try:
            read_file_to_db(file, engine)
            print('{} written successfully to db'.format(file))
        except Exception as e:
            print(e)

if __name__ == "__main__":
    # path to folder with data
    dir = os.path.join(os.getcwd(), 'data/vouchers/')

    # database connection
    engine = create_engine('postgresql://postgres@localhost:5432/vouchers')

    # date when to start writing files to db
    max_date = get_max_date(engine)

    # write data to db
    get_files(dir, max_date)