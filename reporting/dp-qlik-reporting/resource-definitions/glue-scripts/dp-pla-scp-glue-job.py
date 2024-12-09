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
import uuid

from functools import reduce as python_reduce, partial
from builtins import max as python_max
from datetime import datetime, timedelta
import re

args = getResolvedOptions(
    sys.argv,
    [
        "BucketName",
        "TargetFlow",
        "ClusterIdentifier",
        "DbUser",
        "DbName",
        "RedshiftTmpDir",
        "DriverJdbc",
        "Host",
        "Port",
        "Region",
        "JobRole",
        "IngestionDate"
    ],
)

BUCKET = args["BucketName"]
target_flow  = args["TargetFlow"]
cluster_identifier = args["ClusterIdentifier"]
db_user = args["DbUser"]
db_name = args["DbName"]
redshift_tmp_dir = args["RedshiftTmpDir"]
driver_jdbc = args["DriverJdbc"]
host = args["Host"]
port = args["Port"]
region = args["Region"]
job_role = args["JobRole"]
INGESTION_DATE = args["IngestionDate"]
if INGESTION_DATE == "YESTERDAY": INGESTION_DATE = (datetime.today() - timedelta(days=1)).strftime('%Y/%m/%d')


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


ingestion_id = str(uuid.uuid1())
print("ingestion_id:", ingestion_id)

begin_datetime = datetime.utcnow()
print("begin_datetime:", begin_datetime)



#################
### FULL DATA ###
#################


# get lastest available date for full ingestion

print("getting lastest available date for bulk ingestions...")

lastest_date = (
    read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=bulk/frequency=daily/", sep=";")
    .select("year", "month", "day")
    
    .withColumn("year", lpad(col("year"), 4, "0"))
    .withColumn("month", lpad(col("month"), 2, "0"))
    .withColumn("day", lpad(col("day"), 2, "0"))
    
    .orderBy(col("year").desc(), col("month").desc(), col("day").desc())
    .limit(1)
)

year, month, day = lastest_date.collect()[0]
print(f"BULK: loading lastest_date={year}/{month}/{day}")

################
### CALENDAR ###
################

print("LOADING CALENDAR (FULL)...")

calendar = (
    read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=bulk/frequency=daily/year={year}/month={month}/day={day}/CALENDAR*", sep=";")
    
    .withColumn("START_DATE", to_date(col("START_DATE"), 'yyyy/MM/dd HH:mm:ss'))
    .withColumn("END_DATE", to_date(col("END_DATE"), 'yyyy/MM/dd HH:mm:ss'))
    
    .withColumn("input_file_name", input_file_name())
    .withColumn("ingestion_timestamp", lit(begin_datetime))
    .withColumn("ingestion_id", lit(ingestion_id))
).save_data_to_redshift_table("dwa.scp_sls_ft_forecast_calendar", truncate=True)

calendar.show()


#################
### MACROITEM ###
#################


print("LOADING MACROITEM (FULL)...")

macroitem = (
    read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=bulk/frequency=daily/year={year}/month={month}/day={day}/MACROITEM*", sep=";")

    .withColumn("AVERAGE_VAL", col("AVERAGE_VAL").cast("double"))
    .withColumn("CURRENCY", col("CURRENCY").cast("int"))
    
    .withColumn("input_file_name", input_file_name())
    .withColumn("ingestion_timestamp", lit(begin_datetime))
    .withColumn("ingestion_id", lit(ingestion_id))
).save_data_to_redshift_table("dwa.scp_sls_ft_forecast_macroitem", truncate=True)

macroitem.show()


##############
### WEEKLY ###
##############


print("LOADING WEEKLY (delta)...")

already_loaded_dates = (
    read_redshift_or_empty("dwa.scp_sls_ft_forecast_weekly_raw", schema="year string, month string, day string")
    .select("year", "month", "day").distinct()
)

print("already_loaded_dates:")
already_loaded_dates.show()

new_dates = (
    read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=delta/frequency=weekly/", sep=";")
    
    .withColumn("year", lpad(col("year"), 4, "0"))
    .withColumn("month", lpad(col("month"), 2, "0"))
    .withColumn("day", lpad(col("day"), 2, "0"))
    
    .select("year", "month", "day").distinct()
    
    .join(already_loaded_dates, ["year", "month", "day"], "leftanti")
)

print("new_dates:")
new_dates.show()

if new_dates.count() > 0:
    weekly = (
        read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=delta/frequency=weekly/", sep=";")
        
        .withColumn("year", lpad(col("year"), 4, "0"))
        .withColumn("month", lpad(col("month"), 2, "0"))
        .withColumn("day", lpad(col("day"), 2, "0"))
        .withColumn("forecastdate", to_date(col("forecastdate"), 'yyyy-MM-dd'))
        .withColumn("forecastmonth", to_date(col("forecastmonth"), 'yyyy-MM-dd'))
        .withColumn("forecastqty", col("forecastqty").cast("double"))
        .withColumn("matematico", col("matematico").cast("double"))
        
        .withColumn("input_file_name", input_file_name())
        .withColumn("ingestion_timestamp", lit(begin_datetime))
        .withColumn("ingestion_id", lit(ingestion_id))
        
        .join(new_dates, ["year", "month", "day"], "leftsemi")
        
        .withColumnRenamed("forecastdate", "forecast_date")
        .withColumnRenamed("forecastmonth", "forecast_month")
        .withColumnRenamed("forecastqty", "forecast_qty")
        .withColumnRenamed("forecasttype", "forecast_type")
        .withColumnRenamed("forecastvalue", "forecast_value")
        .withColumnRenamed("idcanale", "id_canale")
        .withColumnRenamed("idcliente", "id_cliente")
        .withColumnRenamed("logisticarea", "logistic_area")        
        .withColumnRenamed("networkcode", "network_code")
        .withColumnRenamed("SKU", "sku")
        .withColumnRenamed("PROCESSFK", "process_fk")
    ).localCheckpoint()
    
    weekly_aggr = (
        weekly
        
        .groupBy("year", "month", "day", "forecast_date", "logistic_area", "sku")
        .agg(
            sum("forecast_qty").alias("forecast_qty"),
            sum("matematico").alias("matematico"),
        )
        
        .withColumn("version", dense_rank().over(Window.partitionBy("year", "month").orderBy(col("day").asc())))
        .withColumn("version", concat(lit("W."), col("year"), lit("-"), col("month"), lit("."), col("version")))
        
        .withColumn("ingestion_date", to_date(concat_ws("-", col("year"), col("month"), col("day")), 'yyyy-MM-dd'))
        .drop("year", "month", "day")
    ).localCheckpoint()
    
    weekly.save_data_to_redshift_table("dwa.scp_sls_ft_forecast_weekly_raw", truncate=False)
    weekly_aggr.save_data_to_redshift_table("dwa.scp_sls_ft_forecast_weekly", truncate=False)

    print("weekly:")
    weekly.show()
    print("weekly_aggr:")
    weekly_aggr.show()
else:
    print("no new dates: skipping weekly!")


###############
### MONTHLY ###
###############


print("LOADING MONTHLY (delta)...")

already_loaded_dates = (
    read_redshift_or_empty("dwa.scp_sls_ft_forecast_monthly_raw", schema="year string, month string, day string")
    .select("year", "month", "day").distinct()
)

print("already_loaded_dates:")
already_loaded_dates.show()

new_dates = (
    read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=delta/frequency=monthly/", sep=";")
    
    .withColumn("year", lpad(col("year"), 4, "0"))
    .withColumn("month", lpad(col("month"), 2, "0"))
    .withColumn("day", lpad(col("day"), 2, "0"))
    
    .select("year", "month", "day").distinct()
    
    .join(already_loaded_dates, ["year", "month", "day"], "leftanti")
)

print("new_dates:")
new_dates.show()

if new_dates.count() > 0:
    monthly = (
        read_data_from_s3_csv(f"s3://{BUCKET}/scp/forecast/ing/mode=delta/frequency=monthly/", sep=";")
        
        .withColumn("year", lpad(col("year"), 4, "0"))
        .withColumn("month", lpad(col("month"), 2, "0"))
        .withColumn("day", lpad(col("day"), 2, "0"))
        .withColumn("forecastdate", to_date(col("forecastdate"), 'yyyy-MM-dd'))
        .withColumn("forecastmonth", to_date(col("forecastmonth"), 'yyyy-MM-dd'))
        .withColumn("forecastqty", col("forecastqty").cast("double"))
        .withColumn("matematico", col("matematico").cast("double"))
        
        .withColumn("input_file_name", input_file_name())
        .withColumn("ingestion_timestamp", lit(begin_datetime))
        .withColumn("ingestion_id", lit(ingestion_id))

        .join(new_dates, ["year", "month", "day"], "leftsemi")
        
        .withColumnRenamed("forecastdate", "forecast_date")
        .withColumnRenamed("forecastmonth", "forecast_month")
        .withColumnRenamed("forecastqty", "forecast_qty")
        .withColumnRenamed("forecasttype", "forecast_type")
        .withColumnRenamed("forecastvalue", "forecast_value")
        .withColumnRenamed("idcanale", "id_canale")
        .withColumnRenamed("idcliente", "id_cliente")
        .withColumnRenamed("logisticarea", "logistic_area")        
        .withColumnRenamed("networkcode", "network_code")
        .withColumnRenamed("SKU", "sku")
        .withColumnRenamed("PROCESSFK", "process_fk")
    ).localCheckpoint()
    
    monthly_aggr = (
        monthly
        
        .groupBy("year", "month", "day", "forecast_date", "logistic_area", "sku")
        .agg(
            sum("forecast_qty").alias("forecast_qty"),
            sum("matematico").alias("matematico"),
        )
        
        .withColumn("version", dense_rank().over(Window.partitionBy("year", "month").orderBy(col("day").asc())))
        .withColumn("version", concat(lit("M."), col("year"), lit("-"), col("month"), lit("."), col("version")))
        
        .withColumn("ingestion_date", to_date(concat_ws("-", col("year"), col("month"), col("day")), 'yyyy-MM-dd'))
        .drop("year", "month", "day")
    ).localCheckpoint()
    
    monthly.save_data_to_redshift_table("dwa.scp_sls_ft_forecast_monthly_raw", truncate=False)
    monthly_aggr.save_data_to_redshift_table("dwa.scp_sls_ft_forecast_monthly", truncate=False)
    
    print("monthly:")
    monthly.show()
    print("monthly_aggr:")
    monthly_aggr.show()
else:
    print("no new dates: skipping weekly!")


################
### FORECAST ###
################

print("LOADING FORECAST (full, from redshift)...")


weekly_aggr_redshift = read_data_from_redshift_table("dwa.scp_sls_ft_forecast_weekly").localCheckpoint()
monthly_aggr_redshift = read_data_from_redshift_table("dwa.scp_sls_ft_forecast_monthly").localCheckpoint()

weekly_aggr_redshift_lastest = (
    weekly_aggr_redshift
    .join(weekly_aggr_redshift.agg(max("ingestion_date").alias("ingestion_date")), ["ingestion_date"], "leftsemi")
    .withColumn("forecast_type", lit("WEEKLY"))
)

monthly_aggr_redshift_lastest = (
    monthly_aggr_redshift
    .join(monthly_aggr_redshift.agg(max("ingestion_date").alias("ingestion_date")), ["ingestion_date"], "leftsemi")
    .withColumn("forecast_type", lit("MONTHLY"))
)

forecast = (
    weekly_aggr_redshift_lastest.unionByName(monthly_aggr_redshift_lastest)
    
    .withColumn("lastest_forecast", dense_rank().over(Window.partitionBy().orderBy(col("ingestion_date").desc(), col("forecast_type").asc())))
    .withColumn("lastest_forecast", when(col("lastest_forecast") == 1, lit(True)).otherwise(lit(False)))
).save_data_to_redshift_table("dwa.scp_sls_ft_forecast", truncate=True)


forecast.show()
















    