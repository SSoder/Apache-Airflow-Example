# UPDATE: 2021-2-23

## Data Handling Code
The files MongoExtract.py and TweetTempLoad.py have been uploaded to the repository. Currently they perform as expected when run independently from the command line. They have not been connected to the pipeline to be run independently by Airflow.

*MongoExtract* performs the following actions:
1. Parse configuration data for MongoDB login
2. Connects to MongoDB
3. Queries the MongoDB collection for the oldest 100 tweet documents
4. Writes each tweet document to a local json file.

*TweetTempLoad* performs the following actions:
1. Read local JSON files containing tweet data into a list of dictionaries
2. Parse list of dictionaries to extract the desired information to be inserted into PostgreSQL database
3. Stores parsed information in a list of tuples to be passed to the SQL execution function
4. Obtains database connection information for PostgreSQL
5. Connects to PostgreSQL database
6. Iterares through the list of tuples, performing the data insertion operation for each tuple of values

*Next files to be uploaded: TweetCleanup.py, TweetMainLoad.py*

# Apache Airflow Example

## Introduction
The purpose of this repository is to showcase an example of an Apache Airflow ETL Pipeline. It is a structurally simple pipeline, moving in linear fashion between 4 tasks. 
The data source is a MongoDB collection that houses tweets captured by a tweepy-based, Twitter-streaming application that grabs a subset of tweets based on a simple filter of account handles and hashtags. The application then performs a basic sentiment and subjectivity analysis on the main text of the tweet (performed by the TextBlob library) prior to storing it in a MongoDB document collection. 

## Pipeline Outline
This Apache Airflow pipeline performs the following steps:
1. Reads document(s) from MongoDB, writes the document to a JSON file on local harddrive
2. Reads the JSON file, inserts field data into a holding table in a PostgreSQL database
3. Reads primary key index values of all rows in the holding table, and deletes matching records from MongoDB and local JSON files
4. Moves the data from the holding table to a main data table in the PostgreSQL database

## Code
Presented below are the main sections of the dag definition file. The description of each file matches the commenting in the raw code. 

### Import Statements
Dag definition file import statements. There are ETL functions imported that will be called by the Airflow PythonOperators. The primary Airflow libraries are also imported.

```
from airflow import DAG
from airflow.operators.python import PythonOperator
# import MongoExtract
# import TweetTempLoad
# import TweetCleanup
# import TweetMainLoad
```


### DAG Definition
Main DAG definition. I chose this method as opposed to the "with (DAG as dag)..." method simply by personal preference. (There is a section above this in the actual code that defines the default arguments. It is copied with minimal changes from the Apache Airflow Tutorial.)

```
dag = DAG(
    'tweet_digest',
    default_args=default_args,
    description='A simple dag to extract data from mongodb and insert into postgresql.',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(2)
)
```


### Operator Definitions
These operators are all Python Operators referencing the files imported above. Import statements commented out functions that are currently WIP.

```
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
```

### Task Dependency
Here we establish task dependancy. In terms of graph structure, this is a very straightforward dag, moving in linear fashion from one task to the next. Due to the very simple ETL pipeline defined, there is no need for branching tasks or complex dependencies. To write in plain english, the dag is traversed as follows:
1. MongoExtractor - Reads document data from MongoDB, writes to local JSON file
2. LoadPostgre -    Reads the json file(s) created in step 1, and inserts into a PostgreSQL holding table.
3. LocalCleanup -   This function reads the ID strings in the holding table, removes matching documents from MongoDB, and deletes the matching JSON files from local harddrive.
4. PostgreTransfer- Once the MongoDB and JSON entries have been deleted, this function transfers the data from the holding table to the main postgresql data table. The reason for the holding table is to provide a convenient holding place for the SQL data while removing the associated MongoDB docs and JSON files to prevent unnecessary duplication/triplication of data.

`mongoExtract_1 >> postgreLoad_2 >> localCleanup_3 >> postgreTemptoMain_4`




Author: Stefan Soder
Date: 2021-2-22
