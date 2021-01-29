The purpose of this database in the context of Sparkify is to move their processes and data onto the cloud.
Their data currently resides in S3, in a directory of JSON user activity logs, therefore an ETL pipeline has been built to
stage this data in Redshift and transform the data into a set of dimensional tables which can then be used by the analytics team.

As the data resides in S3, in a directory of JSON user activity logs, I have developed an ETL pipeline to stage the data in Redshift, 
where I have implemented staging tables for both the event_data and song_data and then migrated this data into a star schema of relational tables.
The relational tables can now be used by the analytics team to gather further insight into the Sparkify dataset.
   
File Descriptions:
 - dwh.cfg: Contains config information for the data warehouse.
 - sql_queries.py: Contains strings to delete, create and insert tables and data.
 - create_tables.py: Contains code to run drop and create table statements.
 - etl.py: Contains code to run etl process through insert statements.

Run Instructions:

1. Launch a new Python3 launcher (console)
2. run create_tables.py
3. run etl.py
