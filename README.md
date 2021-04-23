This script convert parquet files to csv stored in s3. Run the script in AWS Glue as ETL Job.

It uses pendulum library to break timestamps into date range so that we can loop through s3 folders strcuture like year/month/day/hour
