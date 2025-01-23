import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1737647737386 = glueContext.create_data_frame.from_options(connection_type="kinesis",connection_options={"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:ap-south-1:920372998244:stream/Test", "classification": "json", "startingPosition": "earliest", "inferSchema": "true"}, transformation_ctx="dataframe_AmazonKinesis_node1737647737386")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        AmazonKinesis_node1737647737386 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1737647742855_path = "s3://mybucket4etl/output-data" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour))  + "/"
        AmazonS3_node1737647742855 = glueContext.write_dynamic_frame.from_options(frame=AmazonKinesis_node1737647737386, connection_type="s3", format="json", connection_options={"path": AmazonS3_node1737647742855_path, "partitionKeys": []}, transformation_ctx="AmazonS3_node1737647742855")

glueContext.forEachBatch(frame = dataframe_AmazonKinesis_node1737647737386, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
job.commit()