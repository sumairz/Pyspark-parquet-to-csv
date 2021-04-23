import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark
import pendulum
from pendulum import Interval,Pendulum

s3ReadFolder = {S3_PARQUET_SOURCE_FOLDER}
s3WriteFolder = {S3_CSV_TARGET_FOLDER}

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

start = pendulum.utcfromtimestamp({START_TIMESTAMP}) 
end = pendulum.utcfromtimestamp({END_TIMESTAMP})

# getDateList return list date range in between start and end
def getDateList(start, end):
    dif = int((end - start).total_seconds() / 3600)
    date_list = [(start + Interval(hours=x)) for x in range(dif)]
    return date_list

# readParquetFile read parquet files
def readParquetFile(date):
    return spark.read.parquet(s3ReadFolder + format(date.year, "02") + "/" + format(date.month, "02") + "/" + format(date.day, "02") + "/" + format(date.hour, "02") + "/*.parquet")

# writeCSV write the data to csv file in folder 
def writeCSV(df, date):
    df.coalesce(1).write.option("header", "true").mode("append").csv(s3WriteFolder + format(date.year, "02") + "/" + format(date.month,"02") + "/" + format(date.day, "02") + "/" + format(date.hour, "02"))

date_list = getDateList(start,end)

for date in date_list:
    try:
        df = readParquetFile(date)
        writeCSV(df, date)
    except Exception as e:
        print(e)
        
print("csv saved")