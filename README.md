# Project 5: Data Pipelines with Airflow


## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project
```
data-pipelines-airflow-sparkify
│   create_tables.sql            # Create tables in redshift
│   pics                         # Images for projects
│   README.md                    # Project description
|   example-dag.png                      # Pipeline DAG image
│   
└───airflow                      # Airflow home
|   |               
│   └───dags                     
│   |   │ udac_example_dag.py    # DAG definition
|   |   |
|   └───plugins
│       │  
|       └───helpers
|       |   | sql_queries.py     # All sql queries needed
|       |
|       └───operators
|       |   | data_quality.py    # DataQualityOperator
|       |   | load_dimension.py  # LoadDimensionOperator
|       |   | load_fact.py       # LoadFactOperator
|       |   | stage_redshift.py  # StageToRedshiftOperator
```

## Add Airflow Connections
Use Airflow's UI to configure your AWS credentials and connection to Redshift.

1. To go to the Airflow UI:
- You can use the Project Workspace here and click on the blue Access Airflow button in the bottom right.
- If you'd prefer to run Airflow locally, open http://localhost:8080 in Google Chrome (other browsers occasionally have issues rendering the Airflow UI).

2. Click on the Admin tab and select Connections.

![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/admin-connections.png)

3. Under Connections, select Create.
![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/create-connection.png)
4. On the create connection page, enter the following values:

- Conn Id: Enter aws_credentials.
- Conn Type: Enter Amazon Web Services.
- Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
- Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

Once you've entered these values, select Save and Add Another.
![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/connection-aws-credentials.png)


5. On the next create connection page, enter the following values:

- Conn Id: Enter redshift.
- Conn Type: Enter Postgres.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
- Schema: Enter dev. This is the Redshift database you want to connect to.
- Login: Enter awsuser.
- Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter 5439.
Once you've entered these values, select Save.
![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/cluster-details.png)
![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/connection-redshift.png)

## Datasets
Here are the s3 links for two datasets
```
Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

```
## Building the operators
Build four different operators that will stage the data, transform the data, and run checks on data quality.

### Stage Operator
The stage operator is to load any JSON formatted files from S3 to Amazon Redshift.  The operator creates and runs a SQL COPY statement based on the parameters provided. 

### Fact and Dimension Operators
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. 
Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. 


Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

### Data Quality Operator
The data quality operator is used to run checks on the data itself.
The operator receives one or more SQL based test cases along with the expected results and execute the tests. 
For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

## Start the DAG
1. Start the DAG by switching it state from OFF to ON.

2. Refresh the page and click on the uda_example_dag to view the current state.

3. The whole pipeline should take around 10 minutes to complete.
![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/result-airflow-tree.png)
![image](https://github.com/weinanlee/data-pipelines-airflow-sparkify/blob/master/pics/result-airflow-graph.png)
