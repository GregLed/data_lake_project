## Project summary
The purpose of this project is to upgrade Sparkify data analytical capabilities by moving from a data warehouse (AWS Redshift) to a data lake. The raw data resides on two AWS S3 buckets:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

## Project steps
1. Load the raw data from from S3 buckets
2. Process the data into analytics tables using Spark
3. Load them back into S3 as a set of dimensional tables

## ETL pipeline steps
1. Load song and log datasets from S3 buckets into AWS ERM cluster
2. Transform the raw data into analytical tables optimized for queries (partitioning)
3. Store transformed data in parquet format on S3 bucket

## Database schema
| Table | Description |
| ---- | ---- |
| songplays | fact table for played songs (played by who, when on which devise etc.) | 
| users | dimensional table for users (names, gender and level) | 
| songs | dimensional table for songs (artist, title, year and duration) | 
| artists | dimensional table for artists (name and location info) | 
| time | timestamps breakdown | 

## Example queries
1. SELECT artist_id, count(*) as cnt FROM songplays GROUP BY artist_id ORDER BY cnt DESC LIMIT 10;