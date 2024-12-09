import boto3
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import *
import QlikReportingUtils
from QlikReportingUtils import *

from functools import reduce as python_reduce, partial
from builtins import max as python_max
from datetime import datetime, timedelta
import re

args = getResolvedOptions(
    sys.argv,
    [
        "BucketName",
        "ClusterIdentifier",
        "DbUser",
        "DbName",
        "RedshiftTmpDir",
        "DriverJdbc",
        "Host",
        "Port",
        "Region",
        "JobRole"
    ],
)

BUCKET = args["BucketName"]
cluster_identifier = args["ClusterIdentifier"]
db_user = args["DbUser"]
db_name = args["DbName"]
redshift_tmp_dir = args["RedshiftTmpDir"]
driver_jdbc = args["DriverJdbc"]
host = args["Host"]
port = args["Port"]
region = args["Region"]
job_role = args["JobRole"]


# create spark context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


redshift_client = boto3.client("redshift", region_name=region)

response_redshift = redshift_client.get_cluster_credentials(
    ClusterIdentifier=cluster_identifier,
    DbUser=db_user,
    DbName=db_name,
    DurationSeconds=3600,
)

QlikReportingUtils.URL = f"{driver_jdbc}://{host}:{port}/{db_name}"
QlikReportingUtils.USER = f"{response_redshift['DbUser']}"
QlikReportingUtils.PASSWORD = f"{response_redshift['DbPassword']}"
QlikReportingUtils.TMP_DIR = redshift_tmp_dir
QlikReportingUtils.SPARK_OBJECT = spark
QlikReportingUtils.IAM_ROLE = job_role
QlikReportingUtils.GLUE_CONTEXT = glueContext

#s3_path  = f"s3://{bucket_name}/{target_flow}"
#s3_path += "/{table_name}"

################ set attr to dataframe class
setattr(DataFrame, "do_mapping", do_mapping)
setattr(DataFrame, "drop_duplicates_ordered", drop_duplicates_ordered)
setattr(DataFrame, "assert_no_duplicates", assert_no_duplicates)
setattr(DataFrame, "save_to_s3_parquet", save_to_s3_parquet)
setattr(DataFrame, "save_data_to_redshift_table", save_data_to_redshift_table)
setattr(DataFrame, "columns_to_lower", columns_to_lower)
setattr(DataFrame, "cast_decimal_to_double", cast_decimal_to_double)


import boto3
from botocore.exceptions import ClientError




data = read_data_from_s3_csv(f"s3://{BUCKET}/man/mapping_area_resp.csv")

data.show()

data.save_data_to_redshift_table("man.qlk_sls_mapping_area_resp")

print("finished!!!")