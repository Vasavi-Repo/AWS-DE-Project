import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum  # Added missing imports
from pyspark.sql.types import StringType

# Define S3 input and output paths
input_s3_path = "s3://mydatalakee/raw-data/transactional-data/"
output_s3_path = "s3://mydatalakee/processed-data/"

# Read Raw Data from S3 into DynamicFrame
dynamic_frame_raw = glueContext.create_dynamic_frame.from_catalog(
    database="retaildb",  # Fixed the quotation marks
    table_name="retail_transactional_data",  # Fixed the quotation marks
    bookmark_options={"job_bookmark_option": "job-bookmark-enable"},
    transformation_ctx="dynamic_frame_raw"
)

# Convert to DataFrame for transformation
data_frame_raw = dynamic_frame_raw.toDF()

# Example: Filtering transactions with amount less than or equal to 0
data_frame_filtered = data_frame_raw.filter(
    (col("Units_Sold") >= 0) & (col("Unit_Price") >= 0) & (col("Total_Sales") >= 0)  # Fixed 'or' to '|'
)

# Example: Adding a column for the day of the week
data_frame_transformed = data_frame_filtered.withColumn(
    "day_of_week", date_format(col("Date"), "EEEE")  # Fixed the date format logic
)

# Example: Aggregating sales data by day and region
sales_aggregated = data_frame_transformed.groupBy(
    "Date", "Store_Location"
).agg(
    sum("Total_Sales").alias("total_sales")  # Fixed column name casing
)

# Convert back to DynamicFrame for writing to S3
dynamic_frame_processed = DynamicFrame.fromDF(sales_aggregated, glueContext, "convert")

# Write the transformed data back to S3 in Parquet format, partitioned by date
glueContext.write_dynamic_frame.from_options(
    dynamic_frame_processed,
    connection_type="s3",
    connection_options={"path": output_s3_path, "partitionKeys": ["Date"]},
    format="parquet",
    transformation_ctx="write_parquet"
)

# Commit the Glue job
job.commit()

