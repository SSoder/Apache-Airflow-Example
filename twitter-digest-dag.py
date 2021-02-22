# -*- coding: utf-8 -*-
"""
Created on Sat Feb 20 2021
Last updated: 2021-2-22

@author: Stefan Soder

"""
#***************************************************************************************
#   ->  Dag definition file import statements. The import statements commented out are 
#       associated with the ETL functions being called by the Airflow Python Operators.
#       They are not completed yet, but will be uploaded and this file will be updated
#       as those get finished.
#
#***************************************************************************************

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# import MongoExtract
# import TweetTempLoad
# import TweetCleanup
# import TweetMainLoad


#***************************************************************************************
#   -> Default Argument definitions, copied from Airflow tutorial
#
#***************************************************************************************

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : 'False,
    'email' : ['stefan.soder2013@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
    }

#***************************************************************************************
#   -> Main DAG definition. I chose this method as opposed to the "with (DAG as dag)..."
#   method simply by personal preference.
#
#***************************************************************************************
 
dag = DAG(
    'tweet_digest',
    default_args=default_args,
    description='A simple dag to extract data from mongodb and insert into postgresql.',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(2)
)

#***************************************************************************************
#   -> Operator definitions. At this point I am still working on finishing the operator 
#   functions the dag calls, but those should be done by CoB today, no later than lunch
#   tomorrow. I wanted to prioritize publishing the dag definition file.
#
# **************************************************************************************


mongoExtract_1 = PythonOperator(
    task_id='MongoExtractor',
    python_callable=mongoExtract,
    dag=dag
)

postgreLoad_2 = pythonOperator(
    task_id='PostgreLoader',
    python_callable=tweetTempLoad,
    dag=dag
)

localCleanup_3 = pythonOperator(
    task_id='LocalDataCleanup',
    python_callable=tweetCleanup,
    dag=dag
)

postgreTemptoMain_4 = pythonOperator(
    task_id='PostgreMainDataLoad',
    python_callable=tweetMainLoad,
    dag=dag
)

#***************************************************************************************
#   -> Here we establish task dependancy. In terms of graph structure, this is a very 
#   straightforward dag, moving in linear fashion from one task to the next. Due to the 
#   very simple ETL pipeline defined, there is no need for branching tasks or complex
#   dependencies. To write in plain english, the dag is traversed as follows:
#       1. MongoExtractor - Reads document data from MongoDB, writes to local JSON file
#
#       2. LoadPostgre -    Reads the json file(s) created in step 1, and inserts into a 
#                           PostgreSQL holding table.
#
#       3. LocalCleanup -   This function reads the ID strings in the holding table,
#                           removes matching documents from MongoDB, and deletes the 
#                           matching JSON files from local harddrive.
#
#       4. PostgreTransfer- Once the MongoDB and JSON entries have been deleted, this 
#                           function transfers the data from the holding table to the main
#                           postgresql data table. The reason for the holding table is 
#                           to provide a convenient holding place for the SQL data while 
#                           removing the associated MongoDB docs and JSON files to prevent
#                           unnecessary and excessive duplication/triplication of data. 
#
#***************************************************************************************

mongoExtract_1 >> postgreLoad_2 >> localCleanup_3 >> postgreTemptoMain_4
