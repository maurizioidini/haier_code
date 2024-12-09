import boto3
import os
import sys
from datetime import datetime, timezone
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from builtins import max as python_max
from functools import reduce as python_reduce
import uuid


# get parameters from conf
args = getResolvedOptions(sys.argv,
                           [
                            'SourceBucketName',
                            'TargetSchemaName',
                            'TargetTableName',
                            'ClusterIdentifier',
                            'DbUser',
                            'DbName',
                            'RedshiftTmpDir',
                            'DriverJdbc',
                            'Host',
                            'Port',
                            'Region',
                            'Mode',
                            'JobRole',
                            'Pkey',
                            'IncrementalField'
                           ])




source_bucket_name   = args['SourceBucketName']
target_schema_name = args['TargetSchemaName']
target_table_name  = args['TargetTableName']
cluster_identifier = args['ClusterIdentifier']
db_user            = args['DbUser']
db_name            = args['DbName']
redshift_tmp_dir   = args['RedshiftTmpDir']
driver_jdbc        = args['DriverJdbc']
host               = args['Host']
port               = args['Port']
region             = args['Region']
mode               = args['Mode']
job_role           = args['JobRole']
p_key              = args['Pkey'].split(",")
incremental_field  = [x for x in args['IncrementalField'].split(",") if x != "<EMPTY>"]

target_table = f"{target_schema_name}.{target_table_name}"
print("target_table:", target_table)

ingestion_id = str(uuid.uuid1())
print("ingestion_id:", ingestion_id)


# create spark context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


redshift_client = boto3.client('redshift', region_name=region)
s3_client = boto3.client('s3')

response_redshift = redshift_client.get_cluster_credentials(
    ClusterIdentifier=cluster_identifier,
    DbUser=db_user,
    DbName=db_name,
    DurationSeconds=3600
)

my_conn_options = {
    "url": f"{driver_jdbc}://{host}:{port}/{db_name}",
    "user": response_redshift['DbUser'],
    "password": response_redshift['DbPassword'],
    "redshiftTmpDir": redshift_tmp_dir,
}


###### DEFINE USEFUL FUNCTIONS

def drop_duplicates_ordered(self, keyCols, orderCols):
    print(f"dropping duplicates on {keyCols} using ordering {orderCols}...")
    if len(orderCols) == 0:
        print(f"NO orderCols: falling back to dropDuplicates without ordering")
        return self.dropDuplicates(keyCols)

    win = Window.partitionBy(keyCols).orderBy(orderCols)

    return (
        self.withColumn("drop_duplicates_ordered___rn", row_number().over(win))
        .where(col("drop_duplicates_ordered___rn") == 1)
        .drop("drop_duplicates_ordered___rn")
    )

setattr(DataFrame, "drop_duplicates_ordered", drop_duplicates_ordered)


def assert_no_duplicates(self, *cols):
    cols = list(cols)

    df = self.groupBy(cols).count().where(col("count") > 1)

    if df.count() == 0:
        return self

    print("ERROR: DUPLICATES FOUND!")
    df.limit(10).show(truncate=False)
    raise RuntimeError(f"ERROR: DUPLICATES FOUND ON TABLE!")

setattr(DataFrame, "assert_no_duplicates", assert_no_duplicates)


def read_redshift(table):
    query = f"select * from {table}"
    print(f"query: {query}")

    data = (
        spark.read.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", my_conn_options["url"])
        .option("query", query)
        .option("user", response_redshift['DbUser'])
        .option("password", response_redshift['DbPassword'])
        .option("tempdir", my_conn_options["redshiftTmpDir"])
        .option("forward_spark_s3_credentials", "true")
        .option("datetimeRebaseMode", "LEGACY")
        .load()
    )

    return data


def read_redshift_or_empty(table):
    try:
        return read_redshift(table)
    except Exception as ex:
        print(str(ex))

        if not ("relation" in str(ex) and "does not exist" in str(ex)):
            raise ex   # do not catch other exceptions than "table xxx does not exist"

        print("read_redshift_or_empty: returning empty table!")
        return spark.createDataFrame([], schema=StructType([]))


def get_last_insertion_timestamp(table_name) -> datetime:

    try:
        query = f"select max(ingestion_timestamp) from {table_name}"
        data = (
            spark.read.format("io.github.spark_redshift_community.spark.redshift")
            .option("url", my_conn_options["url"])
            .option("query", query)
            .option("user", response_redshift['DbUser'])
            .option("password", response_redshift['DbPassword'])
            .option("tempdir", my_conn_options["redshiftTmpDir"])
            .option("forward_spark_s3_credentials", "true")
            .option("datetimeRebaseMode", "LEGACY")
            .load()
        )
        last_insertion_timestamp = data.select(data.columns[0]).first()[0]
    except:
        last_insertion_timestamp = None

    if last_insertion_timestamp is None:
        last_insertion_timestamp = datetime(1970,1,1,0,0)
    return last_insertion_timestamp.replace(tzinfo=timezone.utc)


def save_redshift(self, table_name, truncate=True):

    print(f"Saving data to redshift (using AWS DynamicFrame): {table_name}...")

    preaction = f"truncate table {table_name}"

    data = DynamicFrame.fromDF(self, glueContext, table_name)

    if truncate:
        print(f"truncating table {truncate}")
        connection_options = my_conn_options | {"dbtable": table_name, "preactions": preaction}
    else:
        connection_options = my_conn_options | {"dbtable": table_name}

    glueContext.write_dynamic_frame.from_options(
        frame=data,
        connection_type="redshift",
        connection_options = connection_options
    )


    return self

setattr(DataFrame, "save_redshift", save_redshift)




####### START PROCESSING

begin_datetime = datetime.utcnow()
print("begin_datetime:", begin_datetime)

source_bucket = source_bucket_name.split("/")[0]
sub_path = "/".join(source_bucket_name.split("/")[1:]) if "/" in source_bucket_name else ''

print("source_bucket:", source_bucket)
print("sub_path:", sub_path)

s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket=f"{source_bucket}", Prefix=sub_path)


if "Contents" not in response:
    print("'contents' not in response, exiting... (check if the bucket is empty!!!)")
    job.commit() # Only necessary if you are loading data from s3 and you have job bookmarks enabled.
    os._exit(0)


##### GET S3 PATHS TO LOAD
if mode == 'Full':
    print("perfoming Full...")
    # get last inserted files
    last_insertion_timestamp = None
    list_files = [content for content in response['Contents'] ]
    last_insert = python_max(list_files, key=lambda x: x['LastModified'])
    sub_path = "/".join(last_insert['Key'].split("/")[:-2])
    source_files = {f"s3://{source_bucket}/{sub_path}/*"}
    print(f"source_files: {source_files}")

## mode == 'Delta'
else:
    print("perfoming Delta...")
    # read last insertion date
    last_insertion_timestamp = get_last_insertion_timestamp(f"{target_schema_name}.{target_table_name}")
    print(f"last_insertion_timestamp: {last_insertion_timestamp}")
    sub_paths = ["/".join(content['Key'].split("/")[:-2]) for content in response['Contents'] if content['LastModified'].replace(tzinfo=timezone.utc) > last_insertion_timestamp ]
    source_files = {f"s3://{source_bucket}/{sub_path}/*" for sub_path in sub_paths}
    print(f"source_files: {source_files}")


if not source_files:

    end_datetime = datetime.utcnow()
    print("end_datetime:", end_datetime)

    log_entry = {
        "redshift_output_table_name": target_table,
        "ingestion_id": ingestion_id,
        "mode": mode,
        "incremental_field": " ; ".join(incremental_field) or "",
        "begin_datetime": begin_datetime,
        "end_datetime": end_datetime,
        "p_key": ", ".join(p_key),
        "message": "no data detected after last_insertion_timestamp: ingestion skipped"
    }
    if last_insertion_timestamp is not None: log_entry["last_insertion_timestamp"] = last_insertion_timestamp

    print("log_entry:", log_entry)

    spark.createDataFrame([log_entry]).save_redshift("cfg.dto_cdsview_load_log", truncate=False)    # todo set correct log table

    print("No files detected, exiting :)")
    os._exit(0)


########## READ INCOMING DATA

# read last data
new_data = (
    spark.read
    .option("datetimeRebaseMode", "LEGACY")
    .option("recursiveFileLookup", True)
    .parquet(*source_files)

    .withColumn("input_file_name", input_file_name())
    .withColumn("ingestion_timestamp", lit(begin_datetime))
    .withColumn("ingestion_id", lit(ingestion_id))
).localCheckpoint()

#print(new_data.limit(1).collect())  # may give problems with invalid timestamps

loaded_parquet_paths = [x[0] for x in new_data.select("input_file_name").distinct().orderBy("input_file_name").collect()]
count_loaded_rows_from_parquet = new_data.count()

########## DETECT & CLEAN ERRORS

performed_cleaning_steps = []

get_pkey = [concat(lit(f"{k}='"), col(k), lit("'")) for k in p_key]     # helper spark function to construct a representation of p_key ("key1='value1' and key2='value2' and ...")
get_pkey = python_reduce(lambda a, b: concat(a, lit(" AND "), b), get_pkey)
print("get_pkey:", get_pkey)


date_timestamp_columns = [field.name for field in new_data.schema.fields if isinstance(field.dataType, (DateType, TimestampType))]   # all date and timestamp columns
print("date_timestamp_columns:", date_timestamp_columns)

string_columns = [field.name for field in new_data.schema.fields if isinstance(field.dataType, (StringType))]   # all date and timestamp columns
print("string_columns:", string_columns)

all_errors = []  # list of dataframes containing errors


#### check on pkey duplicates for FULL ingestions

if mode == "Full":
    duplicates = (
        new_data
        .withColumn("__count", count("*").over(Window.partitionBy(get_pkey)))
        .where(col("__count") > 1)
        .select(
            get_pkey.alias("pkey_value"),
            concat(lit(f"duplicated primary key on Full extraction!! there are "), col("__count"), lit(" duplicates for this key...")).alias("error_message"),
            lit(None).cast("string").alias("error_column"),
            lit(None).cast("string").alias("error_value"),
            col("input_file_name").alias("s3_input_file_name"),
        )
    ).localCheckpoint()

    all_errors.append(duplicates)
    performed_cleaning_steps.append("duplicates_on_pkey")

    # note: deduplication on p_key is already performed later...


#### check on very old dated

if date_timestamp_columns:
    DATE_ERROR_LIMIT = to_date(lit('1970-01-01'))
    ERROR_DATE = to_date(lit("1900-01-01"))

    cond_all = python_reduce(lambda a, b: a | b, [col(c) < DATE_ERROR_LIMIT for c in date_timestamp_columns])
    new_data_filtered = new_data.where(cond_all).localCheckpoint()
    print(f"pre-filtering new_data_filtered on {cond_all}...")

    if new_data_filtered.count() > 0:
        for c in date_timestamp_columns:
            cond = col(c) < DATE_ERROR_LIMIT
            print(f"checking on {cond}...")

            errors = (
                new_data_filtered
                .where(cond)
                .select(
                    get_pkey.alias("pkey_value"),
                    concat(lit(f"date/time column {c} too old: '"), col(c), lit("' -> setting '"), ERROR_DATE, lit("'")).alias("error_message"),
                    lit(c).alias("error_column"),
                    col(c).cast("string").alias("error_value"),
                    col("input_file_name").alias("s3_input_file_name"),
                )
            ).localCheckpoint()

            new_data = new_data.withColumn(c, when(cond, ERROR_DATE).otherwise(col(c)))

            all_errors.append(errors)
    else:
        print("new_data_filtered.count() == 0, skipping check!")

    performed_cleaning_steps.append("fix_date_before_limit")
else:
    print("no date_timestamp_columns, skipping check!")


#### check on columns containing value "null" as a string  -> convert to None

if string_columns:
    cond_all = python_reduce(lambda a, b: a | b, [lower(col(c)) == "null" for c in string_columns])
    new_data_filtered = new_data.where(cond_all).localCheckpoint()
    print(f"pre-filtering new_data_filtered on {cond_all}...")


    if new_data_filtered.count() > 0:
        for c in string_columns:
            cond = lower(col(c)) == "null"
            print(f"checking on {cond}...")

            errors = (
                new_data_filtered
                .where(cond)
                .select(
                    get_pkey.alias("pkey_value"),
                    concat(lit(f"column {c} has value: '"), col(c), lit("' -> setting NULL")).alias("error_message"),
                    lit(c).alias("error_column"),
                    col(c).cast("string").alias("error_value"),
                    col("input_file_name").alias("s3_input_file_name"),
                )
            ).localCheckpoint()

            #new_data = new_data.withColumn(c, when(cond, lit(None)).otherwise(col(c)))

            all_errors.append(errors)
    else:
        print("new_data_filtered.count() == 0, skipping check!")

    performed_cleaning_steps.append("log_null_as_string")
    print("performed_cleaning_steps:", performed_cleaning_steps)
else:
    print("no string_columns, skipping check!")

#### end checks ####


########## UNION ALL ERRORS AND APPEND TO REDSHIFT LOG TABLE

if all_errors:
    all_errors = (
        python_reduce(DataFrame.unionByName, all_errors)
        .withColumn("redshift_output_table_name", lit(target_table))
        .withColumn("ingestion_timestamp", lit(begin_datetime))
        .withColumn("ingestion_id", lit(ingestion_id))
    ).localCheckpoint()

    all_errors.show(truncate=False)

    all_errors.save_redshift("cfg.dto_cdsview_load_errors", truncate=False)    # todo set correct log table

    errors_count = all_errors.count()
else:
    print("no errors... skipping!")
    errors_count = 0

########## PERFORM DEDUPLICATION & SCHEMA MERGE BETWEEN NEW AND REDSHIFT DATA

if mode == "Delta":
    redshift_data = read_redshift_or_empty(target_table)

    print("schema new_data:", new_data)
    print("schema redshift_data:", redshift_data)

    count_before_update = redshift_data.count()

    new_data = new_data.unionByName(redshift_data, allowMissingColumns=True)  # automatic schema evolution

    print("evolved_schema:", new_data)
else:
    count_before_update = None

updated_data = new_data.drop_duplicates_ordered(p_key, [col(x).desc() for x in incremental_field + ["ingestion_timestamp"]]).localCheckpoint()   # dedup on incremental_field(s) and then ingestion_timestamp

########## SAVE RESULTS TO REDSHIFT

count_after_update = updated_data.count()
print("count_after_update:", count_after_update)

begin_copy_datetime = datetime.utcnow()
print("begin_copy_datetime:", begin_copy_datetime)

updated_data.save_redshift(target_table)


######### SAVE LOG TO REDSHIFT


end_datetime = datetime.utcnow()
print("end_datetime:", end_datetime)

log_entry = {
    "redshift_output_table_name": target_table,
    "ingestion_id": ingestion_id,
    "mode": mode,
    "incremental_field": " ; ".join(incremental_field) or "",
    "begin_datetime": begin_datetime,
    "begin_copy_datetime": begin_copy_datetime,
    "end_datetime": end_datetime,
    "count_loaded_rows_from_parquet": count_loaded_rows_from_parquet,
    "errors_count": errors_count,
    "count_after_update": count_after_update,
    "performed_cleaning_steps": " ; ".join(performed_cleaning_steps) or "",
    "loaded_parquet_paths": " ; ".join(loaded_parquet_paths) or "",
    "p_key": ", ".join(p_key)
}
if last_insertion_timestamp is not None: log_entry["last_insertion_timestamp"] = last_insertion_timestamp
if count_before_update is not None: log_entry["count_before_update"] = count_before_update
print("log_entry:", log_entry)

spark.createDataFrame([log_entry]).save_redshift("cfg.dto_cdsview_load_log", truncate=False)    # todo set correct log table









