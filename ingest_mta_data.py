import urllib.request
import pandas as pd
import datetime
from sqlalchemy  import *
import os
import sys
import logging 
 
logging.basicConfig(filename='run.log', level=logging.INFO, filemode='w', format='%(name)s - %(levelname)s - %(message)s')

## SET FUNCTIONS
def get_turnstile_data(date):

    ### create file URL 
    file_url='http://web.mta.info/developers/data/nyct/turnstile/turnstile_{0}.txt'.format(date)
    
    ### download file and save as temp name
    urllib.request.urlretrieve(file_url, '{0}.txt'.format(date))
    created_file='{0}.txt'.format(date)

    return created_file

def write_to_database(connection,destination_table_name,summary_data_df):

    ### write data to AWS RDS 
    summary_data_df.to_sql(destination_table_name, connection, if_exists='append', index=False, index_label=None, chunksize=None, dtype=None, method=None)

def iterate(connection,begin_date,end_date,destination):

    ### set time ranges 
    print(begin_date,end_date)
    dt = pd.date_range(begin_date, end_date, freq='W-SAT')

    ### match MTA date file format
    date_list=dt.format(formatter=lambda x: x.strftime('%y%m%d'))

    logging.info('**BEGIN PULL**')

    for i in date_list:

        logging.info('downloading file {0}'.format(i))
        file=get_turnstile_data(i)
        data = pd.read_csv(file, sep=",")

        #something in raw file causing this
        raw_data=data.rename(columns={"C/A": "CA", "EXITS                                                               ": "EXITS"})
        summary_data=raw_data

        # write data to RDS 
        write_to_database(connection,destination,summary_data)

        #remove file so we keep space on EC2
        os.remove(file)

if __name__ == "__main__": 

    ### get database info
    host=os.getenv('host_url')
    database=os.getenv('default_database')
    username=os.getenv('db_username')
    pwd=os.getenv('db_pwd')

    ### create connection - assumes mysql
    engine = create_engine('mysql://{0}:{1}@{2}/{3}'.format(username,pwd,host,database))
    connection = engine.connect()

    ### get arguments and begin
    arguments=sys.argv
    iterate(connection,arguments[1],arguments[2],arguments[3])
    


