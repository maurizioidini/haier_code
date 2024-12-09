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

ingestion_year, ingestion_month, ingestion_day = INGESTION_DATE.split("/")


RELATIVE_PATH = f'plm/model/ing/mode=delta/entity=?source_type/year={ingestion_year}/month={ingestion_month}/day={ingestion_day}/'
BULK_RELATIVE_PATH = f'plm/model/ing/mode=bulk/entity=?source_type/year={ingestion_year}/month={ingestion_month}/day={ingestion_day}/'

BASE_DIR = f's3://{BUCKET}/{RELATIVE_PATH}'
BULK_BASE_DIR = f's3://{BUCKET}/{BULK_RELATIVE_PATH}'

#BUCKET_PROCESSING = BUCKET.replace("-ing-", "-pro-").replace("-raw-", "-rel-")
#OUTPUT_DIR = f's3://{BUCKET_PROCESSING}/model/'
OUTPUT_DIR = f's3://{BUCKET}/plm/model/pro/'

print("BUCKET", BUCKET)

print("RELATIVE_PATH", RELATIVE_PATH)
print("BULK_RELATIVE_PATH", BULK_RELATIVE_PATH)

print("INGESTION_DATE", INGESTION_DATE)

print("BASE_DIR", BASE_DIR)
print("BULK_BASE_DIR", BULK_BASE_DIR)

print("OUTPUT_DIR", OUTPUT_DIR)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

DISPLAY_LOG = False


#######################


def remove_brackets(text):
    return re.sub(r'\[.*?\]', '', text).strip()

def standardize_col_name(col_name, keep_brackets=False):
    global STANDARDIZATION_OUTPUT

    output = col_name

    output = output.replace(".", "")

    if keep_brackets: return output

    # Remove square brackets (and text inside)
    output = remove_brackets(col_name)


    # Replace all non-alphanumeric characters with underscores (keep open square bracket if keep_brackets)
    output = re.sub(r'[^\w]+', ' ', output).strip().replace(" ", "_")

    # Collapse multiple underscores (open square bracket still there if keep_brackets)
    output = re.sub(r'_+', '_', output)

    if DISPLAY_LOG: print(f">> standardize_col_name {col_name} -> {output}")

    return output


# remap input columns to columns suitable for writing in output
def standardizeColumns(self, keep_brackets=False):
    columns_mapping = {f"`{x}`": f"{standardize_col_name(x, keep_brackets=keep_brackets)}" for x in self.columns}

    return (
        self
        .select([col(k).alias(v) for k, v in columns_mapping.items()])
    )

setattr(DataFrame, "standardizeColumns", standardizeColumns)


def extract_model_class_from_path(path):
    out = path.split("_")[-1].split(".")[0]
    if DISPLAY_LOG: print(f"extract_model_class_from_path = {out}")

    return out


def assertNoDuplicates(self, *cols):
    cols = list(cols)

    df = (
        self
        .withColumn("__cnt", count("*").over(Window.partitionBy(cols)))
        #.groupBy("LevelCode", "MetricName", "Dim1Name", "Dim2Name", "Dim1Value", "Dim2Value")
        .where(col("__cnt") > 1)
        .orderBy(cols)
    )

    if df.count() == 0: return self

    print("ERROR: DUPLICATES FOUND!")
    df.show(10, truncate=False)
    raise RuntimeError("ERROR: DUPLICATES FOUND!")

setattr(DataFrame, "assertNoDuplicates", assertNoDuplicates)


def assertNoNull(self, *cols):
    cols = list(cols)
    cond = python_reduce(lambda x, y: x | y, [col(c).isNull() for c in cols])

    df = (
        self
        .where(cond)
    )

    if df.count() == 0: return self

    if DISPLAY_LOG: print("ERROR: NULL VALUES FOUND!")
    if DISPLAY_LOG: print("\n".join([str(x) for x in df.limit(10).collect()]))
    raise RuntimeError("ERROR: NULL VALUES FOUND!")

setattr(DataFrame, "assertNoNull", assertNoNull)


def assertNoNullOrEmpty(self, *cols):
    cols = list(cols)
    cond = python_reduce(lambda x, y: x | y, [(col(c).isNull() | (trim(col(c)) == "")) for c in cols])

    df = (
        self
        .where(cond)
    )

    if df.count() == 0: return self

    if DISPLAY_LOG: print("ERROR: NULL VALUES FOUND!")
    if DISPLAY_LOG: print("\n".join([str(x) for x in df.limit(10).collect()]))
    raise RuntimeError("ERROR: NULL VALUES FOUND!")

setattr(DataFrame, "assertNoNullOrEmpty", assertNoNullOrEmpty)


def dropNull(self, *cols):
    cols = list(cols)
    cond = python_reduce(lambda x, y: x | y, [col(c).isNull() for c in cols])

    df = (
        self
        .where(cond)
    )

    if df.count() == 0: return self

    if DISPLAY_LOG: print(f"DROPPED {df.count()} ROWS WITH NULL VALUES:")
    if DISPLAY_LOG: print("\n".join([str(x) for x in df.limit(10).collect()]))

    return self.dropna(subset=cols)

setattr(DataFrame, "dropNull", dropNull)


def dropNullOrEmpty(self, *cols):
    cols = list(cols)
    cond = python_reduce(lambda x, y: x | y, [(col(c).isNull() | (trim(col(c)) == "")) for c in cols])

    df = (
        self
        .where(cond)
    )

    if df.count() == 0: return self

    if DISPLAY_LOG: print(f"DROPPED {df.count()} ROWS WITH NULL VALUES:")
    if DISPLAY_LOG: print("\n".join([str(x) for x in df.limit(10).collect()]))

    return self.dropna(subset=cols)

setattr(DataFrame, "dropNullOrEmpty", dropNullOrEmpty)


def drop_duplicates_ordered(self, keyCols, orderCols):
    win = Window.partitionBy(keyCols).orderBy(orderCols)

    return (
        self
        .withColumn("drop_duplicates_ordered___rn", row_number().over(win))
        .where(col("drop_duplicates_ordered___rn") == 1)
        .drop("drop_duplicates_ordered___rn")
    )

setattr(DataFrame, "drop_duplicates_ordered", drop_duplicates_ordered)


def merge_list(self, others):
    res = self
    for df in others:
        res = res.unionByName(df, allowMissingColumns=True)

    return res

setattr(DataFrame, "merge_list", merge_list)


def merge_no_duplicates(self, others, keyCols):
    win = Window.partitionBy(keyCols).orderBy("merge_no_duplicates___priority")

    if type(others) != list:
        others = [others]

    res = self.withColumn("merge_no_duplicates___priority", lit(0))
    for idx, df in enumerate(others):
        res = res.unionByName(df.withColumn("merge_no_duplicates___priority", lit(idx+1)), allowMissingColumns=True)

    return (
        res
        .drop_duplicates_ordered(keyCols, ["merge_no_duplicates___priority"])
        .drop("merge_no_duplicates___priority")
    )

setattr(DataFrame, "merge_no_duplicates", merge_no_duplicates)



def prettyPrint(self):
    if not DISPLAY_LOG: return self

    data = self.limit(10).collect()

    for x in data:
        x = x.asDict()

        for k, v in x.items():
            print(f"{k}: {v}")

        print("\n")

    return self

setattr(DataFrame, "prettyPrint", prettyPrint)




def printCount(self, message="Count"):
    cnt = self.count()

    print(f"{message}: {cnt}")

    return self

setattr(DataFrame, "printCount", printCount)



def list_s3_directory(bucket_name, prefix):
    s3_client = boto3.client('s3')

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    yield f"s3://{bucket_name}/{obj['Key']}"

            else:
                print(f"No objects found in {bucket_name}/{prefix}")

    except ClientError as e:
        print(f"An error occurred: {e}")
        if e.response['Error']['Code'] == 'AllAccessDisabled':
            print("All access to this object has been disabled.")
        elif e.response['Error']['Code'] == 'AccessDenied':
            print("Access denied. Check your bucket policy and IAM permissions.")
        else:
            print("An unexpected error occurred.")


def removeLastEmptyColumn(self):
    cols_to_remove = {x for x in self.columns if x.startswith("_c")}
    for c in cols_to_remove:
        if DISPLAY_LOG: print(">>> REMOVING COL", c)
        self = self.drop(c)
    return self

setattr(DataFrame, "removeLastEmptyColumn", removeLastEmptyColumn)




source_type_mapping = {
    "model": "Model",
    "accessory": "Accessory",
    "programmedaccessory": "ProgrammedAccessory"
}



############################ FUNZIONE LETTURA FILE OVERVIEW

def read_model_overview_file(path):
    print("read_model_overview", path)

    model_overview_item = (
        spark.read.csv(path, header=True, sep="||")    # should read only one file (model overview)

        .removeLastEmptyColumn()

        .withColumnRenamed("SKU [item_id]", "SKU")
        .withColumnRenamed("Date Last Modify [last_mod_date]", "Date_Last_Modify")

        .standardizeColumns(keep_brackets=True)
    )

    if "Date_Last_Modify" in model_overview_item.columns:
        print("drop_duplicates on SKU using Date_Last_Modify DESC")
        model_overview_item = model_overview_item.drop_duplicates_ordered(
            keyCols=["SKU"],
            orderCols=[col("Date_Last_Modify").desc()]    # se la colonna c'è, usala per ordinare a parità di SKU
        )
    else:
        print("drop_duplicates on SKU (Date_Last_Modify not present, keeping a random row...)")
        model_overview_item = model_overview_item.dropDuplicates( ["SKU"] )    # altrimenti rip prendi una riga sola a caso

    columns_to_extract = ["SKU", "MKT Tree", "R&D Tree", "EPREL Tree", "Environmental Tree"]
    data_columns = set(model_overview_item.columns) - {"SKU"}

    model_overview_item = (
        model_overview_item
        .select(columns_to_extract + [to_json(struct(data_columns)).alias("OverviewData")])

        .withColumn("Model_Overview_File", lit(path))
        .withColumn("Ingestion_Date", lit(INGESTION_DATE))

        .assertNoNullOrEmpty("SKU")
        .assertNoDuplicates("SKU")
    )

    #model_class_item.printSchema()
    if DISPLAY_LOG: print(model_overview_item.columns)

    return model_overview_item


################### CARICAMENTO OVERVIEW - FLUSSO DELTA (INGESTION_DATE)


#for INGESTION_DATE in {"/".join(x.split("/")[5:8]) for x in list_s3_directory(BUCKET, "TEST_ON_PROD") if "2024" in x and "06/14" not in x and "06/17" not in x}:
#    RELATIVE_PATH = f'TEST_ON_PROD/?source_type/{INGESTION_DATE}/'

all_overview_df = []

for source_type in ["model", "accessory", "programmedaccessory"]:
    i = 0

    CURRENT_RELATIVE_PATH = RELATIVE_PATH.replace("?source_type", source_type)

    end_file = [x for x in list_s3_directory(BUCKET, CURRENT_RELATIVE_PATH) if "EndFile" in x]
    assert len(end_file) == 1, f"NO END FILE DETECTED!!! DATA IS NOT READY!!! (path is {BUCKET} -> {CURRENT_RELATIVE_PATH})"

    for path in list_s3_directory(BUCKET, CURRENT_RELATIVE_PATH):
        if "_Overview" not in path: continue   # skip model class file
        print(path)

        assert i == 0, "MULTIPLE OVERVIEW FILES DETECTED!!"

        model_overview_item = (
            read_model_overview_file(path)
            .withColumn("Source_Type", lit(source_type_mapping[source_type]))
        )

        all_overview_df.append(model_overview_item)

        i += 1

    print(f"loaded {i} files!!")

if len(all_overview_df) > 0:
    all_overview_df_united = python_reduce(DataFrame.unionByName, all_overview_df)

    (
        all_overview_df_united
        .coalesce(1)
        .write.format("parquet")
        .partitionBy("Ingestion_Date")    # overwrite just the partition!
        .parquet(f"{OUTPUT_DIR}pro_model_overview_history", mode="overwrite")
    )


################# CARICAMENTO OVERVIEW - FLUSSO BULK (INGESTION_DATE)

all_overview_df = []

for source_type in ["model", "accessory", "programmedaccessory"]:
    i = 0

    CURRENT_RELATIVE_PATH = BULK_RELATIVE_PATH.replace("?source_type", source_type)

    for path in list_s3_directory(BUCKET, CURRENT_RELATIVE_PATH):
        if "_Overview" not in path: continue   # skip model class file
        print(path)

        assert i == 0, "MULTIPLE OVERVIEW FILES DETECTED!!"

        model_overview_item = (
            read_model_overview_file(path)
            .withColumn("Source_Type", lit(source_type_mapping[source_type]))
        )

        all_overview_df.append(model_overview_item)

        i += 1

    print(f"loaded {i} files!!")

if len(all_overview_df) > 0:
    all_overview_df_united = python_reduce(DataFrame.unionByName, all_overview_df)

    (
        all_overview_df_united
        .coalesce(1)
        .write.format("parquet")
        .partitionBy("Ingestion_Date")    # overwrite just the partition!
        .parquet(f"{OUTPUT_DIR}pro_model_bulk_overview_history", mode="overwrite")
    )

################# CARICAMENTO HISTORY OVERVIEW (DELTA & BULK SEPARATI)


pro_model_overview_history = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(f"{OUTPUT_DIR}pro_model_overview_history")
    .withColumn('ingestion_type', lit('standard'))
    .drop("Revision")  # not needed anymore - todo delete
)

pro_model_overview_history.show()
print(pro_model_overview_history.columns)

pro_model_bulk_overview_history = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(f"{OUTPUT_DIR}pro_model_bulk_overview_history")
    .withColumn('ingestion_type', lit('bulk'))
    .drop("Revision")  # not needed anymore - todo delete
)

pro_model_bulk_overview_history.show()
print(pro_model_bulk_overview_history.columns)


#################### MODEL OVERVIEW (DELTA) CURRENT (ULTIMA VERSIONE PER OGNI SKU, FORMATO LUNGO)


pro_model_overview_current = (
    pro_model_overview_history

    .drop_duplicates_ordered( keyCols = ["SKU"], orderCols = [col("Ingestion_Date").desc()])   # keep last Ingestion_Date for each SKU (delta sends the entire item when modified)

    .withColumn("OverviewData", from_json(col("OverviewData"), MapType(StringType(), StringType())))
    .withColumn("Key", explode(map_keys(col("OverviewData"))))
    .withColumn("Value", col("OverviewData")[col("Key")])

    .select("SKU", "Key", "Value", "Ingestion_Date", "Ingestion_Type")

    .save_to_s3_parquet(f"{OUTPUT_DIR}pro_model_overview_current")
).localCheckpoint()

pro_model_overview_current.show()


################ MODEL OVERVIEW BULK CURRENT (ULTIMA VERSIONE PER OGNI SKU, FORMATO LUNGO)

_filter = (
    pro_model_overview_current
    .groupBy("SKU").agg(max("Ingestion_Date").alias("Ingestion_Date"))
)

pro_model_bulk_overview_current = (
    pro_model_bulk_overview_history.alias("bulk")

    .join(_filter.alias("delta"), (col("delta.SKU") == col("bulk.SKU")) & (col("bulk.Ingestion_Date") <= col("delta.Ingestion_Date")), "leftanti")   # discard bulk rows before last delta of the SKU (delta overwrites bulk)

    .withColumn("OverviewData", from_json(col("OverviewData"), MapType(StringType(), StringType())))
    .withColumn("Key", explode(map_keys(col("OverviewData"))))
    .withColumn("Value", col("OverviewData")[col("Key")])

    .select("SKU", "Key", "Value", "Ingestion_Date", "Ingestion_Type")

    .save_to_s3_parquet(f"{OUTPUT_DIR}pro_model_bulk_overview_current")
).localCheckpoint()

pro_model_bulk_overview_current.show(truncate=False)


################


###
# CALCULATE MAPPING mapping_ClassId_Tree: ClassId -> Tree
###

MKT_Tree = (
    pro_model_overview_history.unionByName(pro_model_bulk_overview_history)
    .select(col("MKT Tree").alias("ClassId"), lit("MKT Tree").alias("Tree"))
    .withColumn("ClassId", explode(split("ClassId", "~")))
    .distinct()
)

EPREL_Tree = (
    pro_model_overview_history.unionByName(pro_model_bulk_overview_history)
    .select(col("EPREL Tree").alias("ClassId"), lit("EPREL Tree").alias("Tree"))
    .withColumn("ClassId", explode(split("ClassId", "~")))
    .distinct()
)

rd_tree = (
    pro_model_overview_history.unionByName(pro_model_bulk_overview_history)
    .select(col("R&D Tree").alias("ClassId"), lit("R&D Tree").alias("Tree"))
    .withColumn("ClassId", explode(split("ClassId", "~")))
    .distinct()
)


env_tree = (
    pro_model_overview_history.unionByName(pro_model_bulk_overview_history)
    .select(col("Environmental Tree").alias("ClassId"), lit("Environmental Tree").alias("Tree"))
    .withColumn("ClassId", explode(split("ClassId", "~")))
    .distinct()
)

id_tree = (
    python_reduce(DataFrame.union, [MKT_Tree, EPREL_Tree, rd_tree, env_tree])

    .distinct()

    .assertNoDuplicates("ClassId")
)

#assert id_tree.groupBy("ClassId").agg(countDistinct("Tree").alias("cnt")).where(col("cnt") > 1).count() == 0

mapping_ClassId_Tree = {x["ClassId"]: x["Tree"] for x in id_tree.collect()}

if DISPLAY_LOG: print(mapping_ClassId_Tree)


###################### FUNZIONE LETTURA FILE CLASS


def read_model_class_file(path):
    print("read_model_class", path)

    model_class = extract_model_class_from_path(path)

    tree = mapping_ClassId_Tree[model_class]
    print(f">>> extracted tree = {tree}")

    model_class_item = (
        spark.read.option("recursiveFileLookup", "true")
        .csv(path, header=True, sep="||")    # should read only one file (model overview)

        .removeLastEmptyColumn()

        .standardizeColumns(keep_brackets=True)

        .dropDuplicates(["SKU", "ClassID"])
    )

    columns_to_extract = ["SKU", "ClassID"]
    data_columns = set(model_class_item.columns) - set(columns_to_extract)

    model_class_item = (
        model_class_item
        .select(columns_to_extract + [to_json(struct(data_columns)).alias("ClassData")])

        .withColumn("Model_Class_File", lit(path))
        .withColumn("Tree_Name", lit(tree))
        .withColumn("Ingestion_Date", lit(INGESTION_DATE))

        .assertNoNullOrEmpty("SKU", "ClassID")
        .assertNoDuplicates("SKU")   # there is one ClassID for each file
    )

    #model_class_item.printSchema()
    #if DISPLAY_LOG: print(model_class_item.columns)

    return model_class_item


################## INGESTION FILE CLASS - FLUSSO DELTA (INGESTION_DATE)


all_class_df = []

#for INGESTION_DATE in {"/".join(x.split("/")[5:8]) for x in list_s3_directory(BUCKET, "TEST_ON_PROD") if "2024" in x and "06/14" not in x and "06/17" not in x}:
#    RELATIVE_PATH = f'TEST_ON_PROD/?source_type/{INGESTION_DATE}/'
for source_type in ["model", "accessory", "programmedaccessory"]:
    i = 0

    CURRENT_RELATIVE_PATH = RELATIVE_PATH.replace("?source_type", source_type)

    for path in list_s3_directory(BUCKET, CURRENT_RELATIVE_PATH):
        if "_Class" not in path: continue   # skip model class file
        print(path)

        model_class_item = (
            read_model_class_file(path)
            .withColumn("Source_Type", lit(source_type_mapping[source_type]))
        )

        if DISPLAY_LOG: print(model_class_item)

        all_class_df.append(model_class_item)

        #if i == 10: break     # speedup testing
        i += 1

    print(f"loaded {i} files!!")


if len(all_class_df) > 0:
    all_class_df_united = python_reduce(DataFrame.unionByName, all_class_df)

    (
        all_class_df_united
        .coalesce(1)
        .write.format("parquet")
        .partitionBy("Ingestion_Date")    # overwrite just the partition!
        .parquet(f"{OUTPUT_DIR}pro_model_class_history", mode="overwrite")
    )


################## INGESTION FILE CLASS - FLUSSO BULK (INGESTION_DATE)


all_class_df = []

#for INGESTION_DATE in {"/".join(x.split("/")[5:8]) for x in list_s3_directory(BUCKET, "TEST_ON_PROD") if "2024" in x and "06/14" not in x and "06/17" not in x}:
#    RELATIVE_PATH = f'TEST_ON_PROD/?source_type/{INGESTION_DATE}/'
for source_type in ["model", "accessory", "programmedaccessory"]:
    i = 0

    CURRENT_RELATIVE_PATH = BULK_RELATIVE_PATH.replace("?source_type", source_type)

    for path in list_s3_directory(BUCKET, CURRENT_RELATIVE_PATH):
        if "_Class" not in path: continue   # skip model class file
        print(path)

        model_class_item = (
            read_model_class_file(path)
            .withColumn("Source_Type", lit(source_type_mapping[source_type]))
        )

        if DISPLAY_LOG: print(model_class_item)

        all_class_df.append(model_class_item)

        #if i == 10: break     # speedup testing
        i += 1

    print(f"loaded {i} files!!")


if len(all_class_df) > 0:
    all_class_df_united = python_reduce(DataFrame.unionByName, all_class_df)

    (
        all_class_df_united
        .coalesce(1)
        .write.format("parquet")
        .partitionBy("Ingestion_Date")    # overwrite just the partition!
        .parquet(f"{OUTPUT_DIR}pro_model_bulk_class_history", mode="overwrite")
    )


########################## CARICAMENTO INTERO STORICO CLASS (DELTA & BULK SEPARATI)



pro_model_class_history = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(f"{OUTPUT_DIR}pro_model_class_history")
    .withColumn('ingestion_type', lit('standard'))
    .drop("Revision")   # not needed anymore - todo delete
).localCheckpoint()

pro_model_class_history.show()


pro_model_bulk_class_history = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(f"{OUTPUT_DIR}pro_model_bulk_class_history")
    .withColumn('ingestion_type', lit('bulk'))
    .drop("Revision")   # not needed anymore - todo delete
).localCheckpoint()

pro_model_bulk_class_history.show()


########################## MODEL CLASS CURRENT (ULTIMA VERSIONE PER OGNI SKU, FORMATO LUNGO)



pro_model_class_current = (
    pro_model_class_history

    .drop_duplicates_ordered( keyCols = ["SKU", "ClassId"], orderCols = [col("Ingestion_Date").desc()])   # for each SKU+ClassID keep last Ingestion_Date

    .withColumn("ClassData", from_json(col("ClassData"), MapType(StringType(), StringType())))
    .withColumn("Key", explode(map_keys(col("ClassData"))))
    .withColumn("Value", col("ClassData")[col("Key")])

    .select("SKU", "ClassId", "Tree_Name", "Key", "Value", "Ingestion_Date", "Ingestion_Type")

    .save_to_s3_parquet(f"{OUTPUT_DIR}pro_model_class_current")
)

pro_model_class_current.show()


############################## MODEL CLASS BULK CURRENT (ULTIMA VERSIONE PER OGNI SKU, FORMATO LUNGO)


_filter = (
    pro_model_class_current
    .groupBy("SKU", "ClassId").agg(max("Ingestion_Date").alias("Ingestion_Date"))
)

pro_model_bulk_class_current = (
    pro_model_bulk_class_history.alias("bulk")

    .join(_filter.alias("delta"), (col("delta.SKU") == col("bulk.SKU")) & (col("delta.ClassId") == col("bulk.ClassId")) & (col("bulk.Ingestion_Date") <= col("delta.Ingestion_Date")), "leftanti")   # discard bulk rows before last delta of the SKU+ClassId (delta overwrites bulk)

    .withColumn("ClassData", from_json(col("ClassData"), MapType(StringType(), StringType())))
    .withColumn("Key", explode(map_keys(col("ClassData"))))
    .withColumn("Value", col("ClassData")[col("Key")])

    .select("SKU", "ClassId", "Tree_Name", "Key", "Value", "Ingestion_Date", "Ingestion_Type")

    .save_to_s3_parquet(f"{OUTPUT_DIR}pro_model_bulk_class_current")
)

pro_model_bulk_class_current.show()


##############################


array_cols = {x[0].strip() for x in spark.read.csv(f"{OUTPUT_DIR}/CONFIG/array_cols.txt", schema="Key string").collect()}
array_cols = {standardize_col_name(i, keep_brackets=False) for i in array_cols} - {""}

number_cols = {x[0].strip() for x in spark.read.csv(f"{OUTPUT_DIR}/CONFIG/number_cols.txt", schema="Key string").collect()}
number_cols = {standardize_col_name(i, keep_brackets=False) for i in number_cols} - {""}

date_cols = {x[0].strip() for x in spark.read.csv(f"{OUTPUT_DIR}/CONFIG/date_cols.txt", schema="Key string").collect()}
date_cols = {standardize_col_name(i, keep_brackets=False) for i in date_cols} - {""}



col_type_mapping = (
    spark.createDataFrame([{"Type": "Number", "Std_Key": x} for x in number_cols])
    .unionByName(
        spark.createDataFrame([{"Type": "Date", "Std_Key": x} for x in date_cols])
    )
    .unionByName(
        spark.createDataFrame([{"Type": "Array", "Std_Key": x} for x in array_cols])
    )
    .distinct()
)


col_type_mapping.show()


#################


pro_model_data_current = (
    pro_model_overview_current
    .unionByName(pro_model_class_current, allowMissingColumns=True)
    .unionByName(pro_model_bulk_overview_current, allowMissingColumns=True)
    .unionByName(pro_model_bulk_class_current, allowMissingColumns=True)
)

key_standardization_mapping = spark.createDataFrame([
    { "Key": k[0], "Std_Key": standardize_col_name(k[0], keep_brackets=False) }
    for k in
    pro_model_data_current.select("Key").distinct().collect()
])

pro_model_data_current = (
    pro_model_data_current

    .withColumn("Key",      # fix puntuali campi con nome sbagliato
        when(col("Key") == "Natural Oli Qty (g) [26529]", lit("Natural Oil Qty (g) [26529]"))
        .otherwise(col("Key"))
    )

    .join(key_standardization_mapping, ["Key"], "left")
    .join(col_type_mapping, ["Std_Key"], "left")

    .drop_duplicates_ordered(["Key", "SKU", "ClassId"], [col("Ingestion_Date").desc(), col("Ingestion_Type").desc()])    # [col("Ingestion_Type").desc(), col("Ingestion_Date").desc()]    to give priority to delta

    .withColumn("Value",
        when(col("Type") == "Number", col("Value").cast("double"))
        .when(col("Type") == "Date", to_date(col("Value")))
        .when(col("Type") == "Array", regexp_replace(col("Value"), "~", "; "))
        .otherwise(col("Value"))
    )

    .filter(~col("SKU").isin('AP-000050', 'A-000238')) #remove wrong products

    .save_to_s3_parquet(f"{OUTPUT_DIR}pro_model_data_current")
    .assertNoDuplicates("Key", "SKU", "ClassId")
).localCheckpoint()

pro_model_data_current.where(col("SKU") == "03850301").where(col("Std_Key") == "Brand_Code").show(truncate=False)
pro_model_data_current.show()



########################



sie_mat_business_status_mapping = (
    spark.read.csv(f"s3://{BUCKET}/plm/model/man/Business_Status_Mapping/business_status_mapping.csv", header=True)

    .select(
        col("PLM Status").alias("Release_Status"),
        #col("Status Code").alias("Release_Status_SAP"),
        col("Status Description").alias("Business_Status"),
    )

    .withColumn("Release_Status",
        when(col("Release_Status") == "Out of Production", lit("Out Of Production"))
        .otherwise(col("Release_Status"))
    )

    .distinct()

    .assertNoDuplicates("Release_Status")
    .save_data_to_redshift_table("man.sie_mat_business_status_mapping")
)

sie_mat_business_status_mapping.show()



#######################



pro_model_base_sku_current = (
    pro_model_data_current

    .where(col("Std_Key") == "Related_Derivate_Model")

    .withColumn("Derivative_SKU", explode(split("Value", "; ")))
    .select(col("Derivative_SKU").alias("SKU"), col("SKU").alias("Base_SKU"))
    .distinct()

    .assertNoDuplicates("SKU")

    .save_to_s3_parquet(f"{OUTPUT_DIR}pro_model_base_sku_current")
)

pro_model_base_sku_current.show()


#########################


cabn = (
    read_data_from_redshift_table("mtd.sap_dm_zww_ptp_cabn_cabn")

    .drop_duplicates_ordered(["CharcInternalID"], [col("TimeIntervalNumber").desc()])   # keep last "version" for each characteristic_id

    .select(
        col("charcinternalid").alias("characteristic_id"),
        col("characteristic").alias("characteristic_descr"),
    )

    .where(col("characteristic_descr").isin(['ZPROD_COMCODEEXT', 'ZPREF_ORIG', 'ZPROD_INWCODE']))

    .withColumn("characteristic_descr",
        when(col("characteristic_descr") == "ZPROD_COMCODEEXT", lit("Commodity_Code_Extension"))
        .when(col("characteristic_descr") == "ZPREF_ORIG", lit("Material_Preferential_Origin"))
        .when(col("characteristic_descr") == "ZPROD_INWCODE", lit("Inward_Processing_Code"))
    )

    .assertNoDuplicates("characteristic_id")
)

ausp = (
    read_data_from_redshift_table("mtd.sap_dm_zww_ptp_ausp_ausp")

    .drop_duplicates_ordered(["clfnobjectid", "CharcInternalID"], [col("TimeIntervalNumber").desc()])   # keep last "version" for each product_code+characteristic_id

    .select(
        col("clfnobjectid").alias("product_code"),
        col("charcinternalid").alias("characteristic_id"),
        col("charcvalue").alias("characteristic_value"),
    )

    .join(cabn, ["characteristic_id"], "inner")

    #.where(col("characteristic_id").isin(['0000000086', '0000000022', '0000000085']))

    .assertNoDuplicates("product_code", "characteristic_id")

    .groupBy("product_code")
    .pivot("characteristic_descr")
    .agg(min("characteristic_value"))
).localCheckpoint()
print("ausp:")
ausp.show(5)


#########################



mat_wf_source = (
    read_data_from_redshift_table("dwe.qlk_mat_ft_workflow")
)

mat_wf_release_date = (
    mat_wf_source
    .withColumn("root_target_attachments_item_id_split", explode(split(col("root_target_attachments_item_id"), "~")))
    .filter(col("task_name") == "CHG - Model Marketing Released")

    .select(col("root_target_attachments_item_id_split").alias("product_code"), col("creation_8_digit_date").alias("date_released_workflow"), "last_mod_date")

    .drop_duplicates_ordered(["product_code"], [col("last_mod_date").desc(), col("date_released_workflow").asc()])
    .drop("last_mod_date")
)

mat_wf_smd_imput_date = (
    mat_wf_source
    .withColumn("root_target_attachments_item_id_split", explode(split(col("root_target_attachments_item_id"), "~")))
    .filter(col("task_name") == "CHG - Model Ready For Production")

    .select(col("root_target_attachments_item_id_split").alias("product_code"),col("creation_smd_input_date").alias("SMD_Input_date"), "last_mod_date")

    .drop_duplicates_ordered(["product_code"], [col("last_mod_date").desc(), col("SMD_Input_date").asc()])
    .drop("last_mod_date")
)

zww_ptp_aws_i_materplant_cust_marc=(
  read_data_from_redshift_table("public.zww_ptp_aws_i_materplant_cust")
   .select(col("material").alias("product_code"),col("HERKL").alias("country_of_origin"))
   .where(col("plant")=="POC1")
   .distinct()
   .assert_no_duplicates("product_code")
)

sap_dm_i_product_trd_classfctn=(
  read_data_from_redshift_table("mtd.sap_dm_i_product_trd_classfctn")
   .select(col("product").alias("product_code"),col("trd_classfctn_nmbr").alias("commodity"), col("last_update_datetime"))
   .where(col("validity_end_date")>INGESTION_DATE)
   .drop_duplicates_ordered(["product_code"], [col("last_update_datetime").desc()])
)

sap_dm_i_product_mara=(
  read_data_from_redshift_table("mtd.sap_dm_i_product_mara")
   .select(col("product").alias("product_code"),col("productgroup").alias("product_group"))
   .assert_no_duplicates("product_code")
)

rea_sie_dm_product_overview = (
    pro_model_data_current

    .where(col("Tree_Name").isNull())

    .groupBy("SKU")
    .pivot("Std_Key")
    .agg(min("Value"))

    .join(sie_mat_business_status_mapping, ["Release_Status"], "left")
    .join(pro_model_base_sku_current, ["SKU"], "left")

    .withColumnRenamed("SKU", "Product_Code")
    .withColumnRenamed("Base_Sku", "Base_Product_Code")

    .join(mat_wf_release_date, ["product_code"], "left")
    .join(mat_wf_smd_imput_date, ["product_code"], "left")
    .join(ausp, ["product_code"], "left")
    .join(zww_ptp_aws_i_materplant_cust_marc, ["product_code"], "left")
    .join(sap_dm_i_product_trd_classfctn, ["product_code"], "left")
    .join(sap_dm_i_product_mara, ["product_code"], "left")

    # mapping names
    .withColumnRenamed("name", "product_descr")
    .withColumnRenamed("third_parties_code", "customer_brand_code")
    .withColumnRenamed("third_parties_brand", "customer_brand")
    .withColumnRenamed("tech_type", "type")
    .withColumnRenamed("first_committed_manufacturing_date", "cmd_first")
    .withColumnRenamed("1_make_2_buy", "make_buy")
    .withColumnRenamed("active_service", "active")
    .withColumnRenamed("base_product_code", "related_base")
    .withColumnRenamed("related_derivate_model", "relate_derivate")
    .withColumnRenamed("related_manual", "related_user_manuals")
    .withColumnRenamed("related_primary_images", "related_images_primary")
    .withColumnRenamed("related_secondary_images", "related_images_secondary")
    .withColumnRenamed("related_triple", "related_triples")
    .withColumnRenamed("related_triple_familily", "related_triple_families")
    .withColumnRenamed("supplier_code_cas", "supplier_code_plm")
    .withColumnRenamed("websites_country", "country_www")

    .withColumnRenamed("date_released", "release_date_plm")
    .withColumnRenamed("date_released_workflow", "release_date_workflow")

    .withColumn("product_type_code", split("object_type", "-")[0])
    .withColumn("product_type_descr", split("object_type", "-")[1])

    .save_data_to_redshift_table("mtd.sie_dm_product_overview")
)

rea_sie_dm_product_overview.show(1)



###########################


rea_sie_dm_product_detail_mkt = (
    pro_model_data_current

    .where(col("Tree_Name") == "MKT Tree")

    .groupBy("SKU")
    .pivot("Std_Key")
    .agg(min("Value"))

    .withColumnRenamed("SKU", "Product_Code")
    .withColumnRenamed("Base_Sku", "Base_Product_Code")

    .save_data_to_redshift_table("mtd.sie_dm_product_detail_mkt")
)

rea_sie_dm_product_detail_mkt.show(1)


#############################


rea_sie_dm_product_detail_rd = (
    pro_model_data_current

    .where(col("Tree_Name") == "R&D Tree")

    .groupBy("SKU")
    .pivot("Std_Key")
    .agg(min("Value"))

    .withColumnRenamed("SKU", "Product_Code")
    .withColumnRenamed("Base_Sku", "Base_Product_Code")

    # mapping names
    .withColumnRenamed("product_net_weight_kg", "net_weight_kg")
    .withColumnRenamed("product_gross_weight_kg", "gross_weight_kg")
    .withColumnRenamed("packed_product_height_mm", "height_of_the_packed_product_mm")
    .withColumnRenamed("packed_product_width_mm", "width_of_the_packed_product_mm")
    .withColumnRenamed("packed_product_depth_mm", "depth_of_the_packed_product_mm")
    .withColumnRenamed("material_packaging_paper_kg", "packaging_material_carbon_kg")
    .withColumnRenamed("material_packaging_plastic_kg", "packaging_material_plastic_kg")
    .withColumnRenamed("material_packaging_wood_kg", "packaging_material_wood_kg")
    .withColumnRenamed("material_packaging_metal_kg", "packaging_material_metal_kg")

    .save_data_to_redshift_table("mtd.sie_dm_product_detail_rd")
)

rea_sie_dm_product_detail_rd.show(1)



###############################


rea_sie_dm_product_detail_eprel = (
    pro_model_data_current

    .where(col("Tree_Name") == "EPREL Tree")

    .groupBy("SKU")
    .pivot("Std_Key")
    .agg(min("Value"))

    .withColumnRenamed("SKU", "Product_Code")
    .withColumnRenamed("Base_Sku", "Base_Product_Code")

    .save_data_to_redshift_table("mtd.sie_dm_product_detail_eprel")
)

rea_sie_dm_product_detail_eprel.show(1)



##################



rea_sie_dm_product_detail_env = (
    pro_model_data_current

    .where(col("Tree_Name") == "Environmental Tree")

    .groupBy("SKU")
    .pivot("Std_Key")
    .agg(min("Value"))

    .withColumnRenamed("SKU", "Product_Code")
    .withColumnRenamed("Base_Sku", "Base_Product_Code")

    .save_data_to_redshift_table("mtd.sie_dm_product_detail_env")
)

rea_sie_dm_product_detail_env.show(1)



####################


# from: original col name -> to: renamed col
mapping_names = {
    "related_derivate_model": "relate_derivate",
    "related_manual": "related_user_manuals",
    "related_primary_images": "related_images_primary",
    "related_secondary_images": "related_images_secondary",
    "related_triple": "related_triples",
    "related_triple_familily": "related_triple_families",
    "websites_country": "country_www",
}


for array_col in array_cols:
    dest_name = array_col.lower()
    if dest_name in mapping_names: dest_name = mapping_names[dest_name]   # rename only if present in mapping_names

    print(f"array_col={array_col} -> dest_name={dest_name}")

    (
        pro_model_data_current

        .where(col("Std_Key") == lit(array_col))

        .select("SKU", "Value")
        .withColumn(dest_name, explode(split(col("Value"), "; ")))
        .drop("Value")

        .where(col(dest_name).isNotNull())

        .withColumnRenamed("SKU", "Product_Code")
        .withColumnRenamed("Base_Sku", "Base_Product_Code")

        .save_data_to_redshift_table(f"mtd.sie_dm_product_array_{dest_name}")

    ).show(5)



###################


rea_model_category_current = (
    pro_model_data_current

    .select("SKU", "ClassId", "Tree_Name")
    .distinct()

    .assertNoDuplicates("SKU", "ClassId")

    .withColumnRenamed("SKU", "Product_Code")

    .save_data_to_redshift_table("mtd.sie_dm_product_category")
)

rea_model_category_current.show()



####################


tree_names = (
    pro_model_data_current

    .select("SKU", "Tree_Name")
    .distinct()

    .groupBy("SKU")
    .agg(
        concat_ws(", ", collect_set(col("Tree_Name"))).alias("tree_names")
    )
)

model_data = (
    pro_model_data_current

    .groupBy("SKU")
    .pivot("Std_Key")
    .agg(min("Value"))
).localCheckpoint()


base_model_data = (
    model_data

    .select(
        col("Sku").alias("Base_Sku"),
        col("committed_manufacturing_date").alias("comm_manuf_date_base_model"),
        col("start_manufacturing_date").alias("start_prod_date_base_model"),
    )
)

rea_sie_dm_product = (
    model_data

    .withColumn("product_code", col("sku"))

    .join(sie_mat_business_status_mapping, ["Release_Status"], "left")
    .join(pro_model_base_sku_current, ["SKU"], "left")   # add base_sku
    .join(tree_names, ["SKU"], "left")
    .join(base_model_data, ["Base_Sku"], "left")

    .join(mat_wf_release_date, ["product_code"], "left")
    .join(mat_wf_smd_imput_date, ["product_code"], "left")
    .join(ausp, ["product_code"], "left")
    .join(zww_ptp_aws_i_materplant_cust_marc, ["product_code"], "left")
    .join(sap_dm_i_product_trd_classfctn, ["product_code"], "left")
    .join(sap_dm_i_product_mara, ["product_code"], "left")

    .withColumn("producer_code", split("factory_cas", "-")[0])
    .withColumn("producer_descr", col("factory_cas"))
    .withColumn("supplier_code", split("supplier_code_sap", "-")[0])
    .withColumn("supplier_descr", col("supplier_code_sap"))

    .withColumn("product_type_code", split("object_type", "-")[0])
    .withColumn("product_type_descr", split("object_type", "-")[1])

    .select(
        col("product_code"),
        col("name").alias("product_descr"),
        col("name").alias("product_short_descr"),
        col("product_type_code"),
        col("product_type_descr"),
        col("product_line_code"),   #.alias("ind_division_code"),
        col("product_line"),   #.alias("ind_division_descr"),
        col("brand_code"),
        col("brand"),   #.alias("brand_descr"),
        col("product_category_code"),   #.alias("line_code"),
        col("product_category"),   #.alias("line_descr"),
        col("industrial_structure_code"),   #.alias("ind_structure_code"),
        col("industrial_structure"),   #.alias("ind_structure_descr"),
        col("industrial_family_code"),   #.alias("ind_family_code"),
        col("industrial_family"),   #.alias("ind_family_descr"),
        col("producer_code"),
        col("producer_descr"),
        col("release_status_sap"),   #.alias("production_code"),
        col("release_status"),   #.alias("production_descr"),
        col("start_manufacturing_date"),   #.alias("start_prod_date"),
        col("stop_production_date"),   #.alias("end_prod_date"),
        col("supplier_code"),
        col("supplier_descr"),
        col("third_parties_code").alias("customer_brand_code"),
        col("third_parties_brand").alias("customer_brand"),
        col("ean_code"),
        col("product_net_weight_kg").alias("net_weight_kg"),
        col("product_gross_weight_kg").alias("gross_weight_kg"),
        col("packed_product_height_mm").alias("height_of_the_packed_product_mm"),
        col("packed_product_width_mm").alias("width_of_the_packed_product_mm"),
        col("packed_product_depth_mm").alias("depth_of_the_packed_product_mm"),
        (col("packed_product_height_mm") / 1000 * col("packed_product_width_mm") / 1000 * col("packed_product_depth_mm") / 1000).alias("volume_m3"),
        #lit("").alias("creation_date"),
        col("date_last_modify"),   #.alias("update_date"),
        col("deletion_date"),   #.alias("delete_date"),
        col("logistic_area"),   #.alias("active_areas"),
        col("business_line"),   #.alias("fob"),
        col("family_category"),   #.alias("sector"),
        col("material_packaging_paper_kg").alias("packaging_material_carbon_kg"),
        col("material_packaging_plastic_kg").alias("packaging_material_plastic_kg"),
        col("material_packaging_wood_kg").alias("packaging_material_wood_kg"),
        #col("name"),   #.alias("product_d16"),
        col("number_of_batteries"),   #.alias("batteries_number"),
        col("battery_pack_weight_kg"),   #.alias("total_battery_weights"),
        col("material_packaging_metal_kg").alias("packaging_material_metal_kg"),
        #lit("").alias("months_life"),
        col("business_sector"),   #.alias("business_sector_descr"),
        col("type_of_batteries"),   #.alias("battery_type"),
        #col("type_of_batteries").alias("battery_type_descr"),
        #lit("").alias("country_code"),
        #lit("").alias("country_descr"),
        col("tech_type").alias("type"),
        col("first_production_date"),   #.alias("first_prod_date"),
        col("connectivity"),
        col("base_derived"),   #.alias("model_type"),
        #col("base_derived").alias("model_type_descr"),
        col("Base_SKU").alias("related_base"),
        col("related_derivate_model"),
        col("committed_manufacturing_date"),   #.alias("comm_manuf_date"),
        col("first_committed_manufacturing_date").alias("cmd_first"),
        col("sap_haier_code"),   #.alias("haier_code"),
        col("haier_comm_prod_desc"),   #.alias("haier_cust_mod"),
        col("haier_customer_model"),   #.alias("haier_comm_mod"),
        col("model_owner"),   #.alias("owner"),
        col("plm_category"),
        col("color"),
        #lit("").alias("voltage"),
        #lit("").alias("frequency"),
        #lit("").alias("power"),
        col("eed_cotton_capacity_wm_wd_w_kg"),   #.alias("cap_cotton"),
        #col("eed_cotton_capacity_wm_wd_w_kg"),   #.alias("cap_cotton_wash"),
        col("eed_cotton_capacity_wd_d_kg"),   #.alias("cap_cotton_dry"),
        col("eed_energy_class_wd_w_2019"),   #.alias("energy_class"),
        col("electronics"),   #.alias("elettronica"),
        col("aestetics"),   #.alias("estetica"),
        col("motor"),
        col("comm_manuf_date_base_model"),   #.alias("comm_manuf_date_base_model"),
        col("start_prod_date_base_model"),   #.alias("start_prod_date_base_model"),
        col("related_rem"),   #.alias("nr_rem"),
        #lit("").alias("date_from_5_to_1"),
        #lit("").alias("insertion_md"),
        #lit("").alias("flag_kit"),
        #lit("").alias("new_energy_label"),
        col("interface"),
        col("wine_coolers_category"),   #.alias("product_category"),
        col("number_of_temperature_zones"),   #.alias("num_temp_zones"),
        col("075l_Capacity").alias("075l_capacity"),
        #lit("").alias("ui"),
        col("1_make_2_buy").alias("make_buy"),
        col("active_service").alias("active"),
        col("business_line_code"),
        col("certifications"),
        #col("date_released").alias("release_date"),
        col("eprel_delegated_act"),
        col("eprel_master_model"),
        col("eprel_registration_number"),
        col("eprel_relevant"),
        col("factory_sap"),
        col("family_category_code"),
        #col("logistic_area"),
        col("manufactured_by"),
        col("mkt_pl"),
        col("object_type"),
        col("product_hierarchy_code"),
        col("related_manual"),
        col("related_primary_images"),
        col("related_secondary_images"),
        col("related_triple"),
        col("related_triple_familily"),
        col("repair_index"),
        col("revision"),
        col("sap_haier_russia_code"),
        col("supplier_code_cas"),
        col("websites_country").alias("country_www"),
        col("business_status"),

        col("date_released").alias("release_date_plm"),
        col("date_released_workflow").alias("release_date_workflow"),
        col("smd_input_date"),
        col("Inward_Processing_Code"),
        col("Material_Preferential_Origin"),
        col("Commodity_Code_Extension"),
        col("country_of_origin"),
        col("commodity"),
        col("product_group")
    )

    .save_data_to_redshift_table("mtd.sie_dm_product")
)

rea_sie_dm_product.where(col("Base_Sku").isNotNull()).show(1)


########################


model_class_with_missing_model_overview_current = (
    pro_model_class_current
    .select("SKU", "ClassId", "Ingestion_Date").distinct()
    .join(pro_model_overview_current, ["SKU"], "leftanti")
    .drop_duplicates_ordered( keyCols = ["SKU", "ClassId"], orderCols = [col("Ingestion_Date").desc()])

    .save_to_s3_parquet(f"{OUTPUT_DIR}model_class_with_missing_model_overview_current")

    .prettyPrint()
)

model_class_with_missing_model_overview_current.show(truncate=False)
print(f"model_class_with_missing_model_overview_current: {model_class_with_missing_model_overview_current.count()} (total model_class= {pro_model_class_current.count()})")

#########################


model_overview_with_missing_model_class_current = (
    pro_model_overview_current
    .select("SKU", "Ingestion_Date").distinct()
    .join(pro_model_class_current, ["SKU"], "leftanti")
    .drop_duplicates_ordered( keyCols = ["SKU"], orderCols = [col("Ingestion_Date").desc()]) # todo modified

    .save_to_s3_parquet(f"{OUTPUT_DIR}model_overview_with_missing_model_class_current")

    .prettyPrint()
)

model_overview_with_missing_model_class_current.show(truncate=False)
print(f"model_overview_with_missing_model_class_current: {model_overview_with_missing_model_class_current.count()} (total model_overview= {pro_model_overview_current.count()})")

#########################


check_model_overview_history_inverted_lastmodify = (
    pro_model_overview_history

    .withColumn("OverviewData", from_json("OverviewData", MapType(StringType(), StringType())))

    .select("SKU", "OverviewData.Date_Last_Modify", "Ingestion_Date") # todo modified
    .distinct()

    .assertNoDuplicates("SKU", "Ingestion_Date")     # since data reloaded in the same day is overwritten, should be unique!

    .withColumn("Prev_Date_Last_Modify", lag("Date_Last_Modify").over(Window.partitionBy("SKU").orderBy("Ingestion_Date")))
    .where(col("Date_Last_Modify") < col("Prev_Date_Last_Modify"))

    .select("SKU", "Ingestion_Date", "Date_Last_Modify", "Prev_Date_Last_Modify")
    .drop_duplicates_ordered( keyCols = ["SKU"], orderCols = [col("Ingestion_Date").desc()])

    .save_to_s3_parquet(f"{OUTPUT_DIR}check_model_overview_history_inverted_lastmodify")
)

check_model_overview_history_inverted_lastmodify.show(truncate=False)


######################


check_model_overview_history = (
    pro_model_overview_history

    .withColumn("OverviewData", from_json("OverviewData", MapType(StringType(), StringType())))

    .select("SKU", "OverviewData.Date_Last_Modify", "Ingestion_Date")
    .groupBy("SKU")
    .agg(
        countDistinct("Ingestion_Date").alias("cnt"),
        to_json(collect_set(struct("Ingestion_Date", "Date_Last_Modify"))).alias("dates")
    )
    .orderBy(col("cnt").desc())
    .prettyPrint()

    .save_to_s3_parquet(f"{OUTPUT_DIR}check_model_overview_history")
)

check_model_overview_history.show()


#######################


check_multiple_class_same_sku_tree = (
    rea_model_category_current
    .groupBy("product_code", "Tree_Name")
    .agg(
        collect_set("ClassID").alias("ClassId")
    )
    .where(size("ClassId") > 1)

    .save_to_s3_parquet(f"{OUTPUT_DIR}check_multiple_class_same_sku_tree")
)

check_multiple_class_same_sku_tree.show(truncate=False)


#########################

check_release_statuses_obsolete_rejected_cancelled = (
    rea_sie_dm_product_overview
    .select("Product_Code", "Release_Status")
    .withColumn("Release_Status", trim(upper(col("Release_Status"))))
    .where(col("Release_Status").contains("OBSOLETE") | col("Release_Status").contains("REJECT") | col("Release_Status").contains("CANCEL"))
    .distinct()

    .save_to_s3_parquet(f"{OUTPUT_DIR}check_release_statuses_obsolete_rejected_cancelled")
)

check_release_statuses_obsolete_rejected_cancelled.show()