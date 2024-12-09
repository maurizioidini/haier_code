import boto3
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import *
from pyspark.sql.types import *
import QlikReportingUtils
from QlikReportingUtils import *

args = getResolvedOptions(
    sys.argv,
    [
        "BucketName",
        "SourceFlow",
        "TargetFlow",
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

bucket_name = args["BucketName"]
source_flow  = args["SourceFlow"]
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
QlikReportingUtils.GLUE_CONTEXT = glueContext
QlikReportingUtils.IAM_ROLE = job_role
QlikReportingUtils.DEBUG_MODE = False


s3_bronze_path = f"s3://{bucket_name}/bronze"
s3_bronze_path += "/{table_name}"
s3_mapping_path = f"s3://{bucket_name}/mapping"
s3_mapping_path += "/{table_name}"
s3_target_path = f"s3://{bucket_name}/{target_flow}/"
s3_target_path += "/{table_name}"


################ set attr to dataframe class
setattr(DataFrame, "do_mapping", do_mapping)
setattr(DataFrame, "drop_duplicates_ordered", drop_duplicates_ordered)
setattr(DataFrame, "assert_no_duplicates", assert_no_duplicates)
setattr(DataFrame, "save_to_s3_parquet", save_to_s3_parquet)
setattr(DataFrame, "save_data_to_redshift_table", save_data_to_redshift_table)
setattr(DataFrame, "columns_to_lower", columns_to_lower)
setattr(DataFrame, "cast_decimal_to_double", cast_decimal_to_double)



spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout", "3600")  # Increase broadcast timeout (in seconds)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1g")  # Set a higher limit




################ START PROCESSING BRONZE - MAPPING 2 ORD ####################

# calc: DATA_CONSEGNA_PRESENTE

'''
DATA_CONSEGA_PRESENTE:
Mapping LOAD Distinct
    VBELN as CONSEGNA,
    1
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/LIKP.qvd]
(qvd) Where Len(Trim(WADAT_IST))>0 ;
'''

LIKP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "LIKP_INPUT"
    )
)
# VBELN
DATA_CONSEGNA_PRESENTE = (    # (VBELN) -> DATA CONSEGNA
    LIKP_INPUT
    .where(col("WADAT_IST").isNotNull())    # tieni solo data consegna valorizzata
    .select("VBELN")
    .distinct()
    .repartition(1000, "VBELN")   # todo check repartition
)
DATA_CONSEGNA_PRESENTE = DATA_CONSEGNA_PRESENTE.localCheckpoint()
print(f"DATA_CONSEGNA_PRESENTE OK: {DATA_CONSEGNA_PRESENTE.count()} rows")

# calc: OPEN_ORDER

'''
VBAP:
LOAD
    VBELN as ORDINE ,
    POSNR as RIGA_ORDINE,
    KWMENG as "ORD QTY"
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBAP.qvd](qvd) ;
'''

# was: VBAP
# ORDER_NUMBER, ORDER_LINE -> ORDERED QTY
VBAP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "VBAP_INPUT"
    )
)
_ORDERED_QTY = (
    VBAP_INPUT
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("KWMENG").alias("ORD QTY")
    )
    #.assert_no_duplicates("ORDER_NUMBER", "ORDER_LINE")
)


'''
Left Join(VBAP)

LIPS:
LOAD
    VGBEL   as ORDINE,
    VGPOS   as RIGA_ORDINE,
    sum(LFIMG)  as "DEL QTY"
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/LIPS.qvd]
(qvd)
where Applymap('DATA_CONSEGA_PRESENTE',VBELN,0)=1  // prendo le Qty con la data consegna popolata
Group by VGBEL,VGPOS;
'''
LIPS_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "LIPS_INPUT"
    )
)
# was: LIPS
# ORDER_NUMBER, ORDER_LINE -> DELIVERED QTY
_DELIVERED_QTY = (
    LIPS_INPUT
    .join(
        DATA_CONSEGNA_PRESENTE,   # tieni solo righe con DATA CONSEGNA (VBELN) presente
        ["VBELN"], "leftsemi"
    )
    .select(
        col("VGBEL").alias("ORDER_NUMBER"),
        col("VGPOS").alias("ORDER_LINE"),
        col("LFIMG").alias("DEL QTY")
    )
    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(sum("DEL QTY").alias("DEL QTY"))
    #.assert_no_duplicates("ORDER_NUMBER", "ORDER_LINE")
)


'''
OPEN_ORDER:
Mapping Load
k_OPENORDER,
"ORD QTY"
Where "ORD QTY">0;
LOAD
ORDINE&'|'&RIGA_ORDINE as k_OPENORDER,
"ORD QTY"-Alt("DEL QTY",0) as "ORD QTY"
Resident VBAP;
'''

# ORDERED QUANTITY of ORDERS not fully delivered
# ORDER_NUMBER, ORDER_LINE -> ORD QTY  ( SOLD - DELIVERED )
OPEN_ORDER = (
    _ORDERED_QTY   # ORDER_NUMBER, ORDER_LINE -> ORDERED QTY
    .join(_DELIVERED_QTY, ["ORDER_NUMBER", "ORDER_LINE"], "left")    # add DELIVERED QTY
    .withColumn("DEL QTY", coalesce(col("DEL QTY"), lit(0)))   # DELIVERED QTY = 0 if there is no delivery for the order

    .select(
        "ORDER_NUMBER", "ORDER_LINE",
        (col("ORD QTY") - col("DEL QTY")).alias("ORD QTY")
    )

    .where(col("ORD QTY") > 0)    # keep only not fully delivered orders

    #.assert_no_duplicates("ORDER_NUMBER", "ORDER_LINE")
)
OPEN_ORDER = OPEN_ORDER.localCheckpoint()
print(f"OPEN_ORDER OK: {OPEN_ORDER.count()} rows")

# calc: BLOCK

'''
BLOCK:
mapping LOAD
    VBELN as ORDER_NUMBER, //k
    if(len(trim(LIFSK))>0 or CMGST='C' or CMGST='B',1,0) as "BLOCKING FLAG"
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBAK.qvd](qvd);
'''
VBAK_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "VBAK_INPUT"
    )
)
# ORDER_NUMBER -> BLOCKING FLAG
BLOCK = (
    VBAK_INPUT
    .withColumn("BLOCKING FLAG",
        when(col("LIFSK").isNotNull(), True)
        .when(col("CMGST").isin(["C", "B"]), True)
        .otherwise(False)
    )
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("BLOCKING FLAG")
    )
)
BLOCK = BLOCK.localCheckpoint()
print(f"BLOCK OK: {BLOCK.count()} rows")

# calc: ORDER_VALID

'''
ORDER_VALID:
mapping LOAD
    VBELN as ORDER_NUMBER, //k
    1 as FLAG_ORDER_VALID
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBAK.qvd]
(qvd)  where (Applymap('TVAK',AUART,0)<>0 or AUART='Z23')
and Match(AUART,'ZKL','ZFOC','YFOC','ZWBF','ZWPC','ZWPR')=0  ;  //CONSIGNEMENT NON INCLUSO
'''
TVAK_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVAK_MAPPING"
    )
)
_TVAK_EXTENDED = (
    TVAK_MAPPING.select(col("ORDER_TYPE_CODE"))    #
    .unionByName(spark.createDataFrame([{"ORDER_TYPE_CODE": "Z23"}]))   # ensure to include also this code
)
print("TVAK LOADED OK")


# list of valid ORDER_NUMBER
ORDER_VALID = (
    VBAK_INPUT
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("AUART").alias("ORDER_TYPE_CODE")
    )
    .join(_TVAK_EXTENDED, ["ORDER_TYPE_CODE"], "leftsemi")

    # CONSIGNEMENT NON INCLUSO
    .where(~col("ORDER_TYPE_CODE").isin(['ZKL','ZFOC','YFOC','ZWBF','ZWPC','ZWPR']))
    .select("ORDER_NUMBER")
)
ORDER_VALID = ORDER_VALID.localCheckpoint()
print(f"ORDER_VALID OK: {ORDER_VALID.count()} rows")

# calc: VBAK (ORD QTY ORIG, ORDER_DELIVERY_WAREHOUSE, ORD QTY)

'''

VBAK:
LOAD
    VBELN as ORDER_NUMBER, //k
    POSNR as ORDER_LINE, //k
    KWMENG as "ORD QTY ORIG",
    LGORT as ORDER_DELIVERY_WAREHOUSE,
    Applymap('OPEN_ORDER',VBELN&'|'&POSNR,0) as "ORD QTY"
 FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBAP.qvd]
(qvd)
 where   Applymap('OPEN_ORDER',VBELN&'|'&POSNR,-1)>0 and Applymap('ORDER_VALID',VBELN,0)=1 and len(trim(ABGRU))=0 ;

'''

# ORDER_NUMBER, ORDER_LINE -> ORD QTY ORIG, ORDER_DELIVERY_WAREHOUSE, ORD QTY
VBAK = (
    VBAP_INPUT
    .where(col("ABGRU").isNull())
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("KWMENG").alias("ORD QTY ORIG"),      # total ORDERED (before any delivery)
        col("LGORT").alias("ORDER_DELIVERY_WAREHOUSE")
    )
    # add ORD QTY  (= ORDERED - DELIVERED), keep only open orders
    .join(OPEN_ORDER, ["ORDER_NUMBER", "ORDER_LINE"], "inner")
    .join(ORDER_VALID, ["ORDER_NUMBER"], "leftsemi")    # keep only valid orders
)
VBAK = VBAK.localCheckpoint()
print(f"VBAK OK: {VBAK.count()} rows")

##  new

'''
LIPS:
LOAD
    VGBEL   as ORDER_NUMBER,
    VGPOS   as ORDER_LINE,
    VBELN as DESPATCHING_NOTE,
    (if(Applymap('DATA_CONSEGA_PRESENTE',VBELN,0)=0,LFIMG,0))  as "PICKING QTY",
    (if(Applymap('DATA_CONSEGA_PRESENTE',VBELN,0)=1,LFIMG,0))  as "DEL QTY"
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/LIPS.qvd](qvd);

Left Join(LIPS)

LIKP:
LOAD
VBELN as DESPATCHING_NOTE, //k
Date(ZZDATE_SLOT) as BOOKING_SLOT_LIKP
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/LIKP.qvd]
(qvd);

left Join(VBAK)

Load ORDER_NUMBER,
ORDER_LINE,
sum("PICKING QTY") as "PICKING QTY",
sum("DEL QTY") as "DEL QTY",
// Date(Max(APPOINTMENT)) as APPOINTMENT,
Date(Max(BOOKING_SLOT_LIKP)) as BOOKING_SLOT_LIKP
Resident LIPS
Group By ORDER_NUMBER,ORDER_LINE;

Drop Tables LIPS;

'''

# was: LIPS
# ORDER_NUMBER, ORDER_LINE -> sum(PICKING QTY), sum(DEL QTY), max(BOOKING_SLOT_LIKP)
# PICKING QTY: non consegnata
# DEL QTY: consegnata
_ORDER_PICKING_DEL_QUANTITY = (
    # (PHASE 1) LOAD LIPS
    LIPS_INPUT
    .repartition("VBELN")   # todo check repartition

    .join(DATA_CONSEGNA_PRESENTE.withColumn("__present", lit(True)).repartition(1000, "VBELN"), ["VBELN"], "left")
    .withColumn("PICKING QTY", when(~col("__present"), col("LFIMG")))
    .withColumn("DEL QTY", when(col("__present"), col("LFIMG")))

    .select(
        col("VGBEL").alias("ORDER_NUMBER"),
        col("VGPOS").alias("ORDER_LINE"),
        col("VBELN").alias("DESPATCHING_NOTE"),
        coalesce(col("PICKING QTY"), lit(0)).alias("PICKING QTY"),
        coalesce(col("DEL QTY"), lit(0)).alias("DEL QTY"),
    )

    .repartition(1000, "DESPATCHING_NOTE")   # todo check repartition

    # (PHASE 2) LOAD LIKP -> LIPS
    .join(     # add BOOKING_SLOT_LIKP
        LIKP_INPUT.repartition(1000, "VBELN").select(   # todo check repartition
            col("VBELN").alias("DESPATCHING_NOTE"),
            to_date(col("ZZDATE_SLOT")).alias("BOOKING_SLOT_LIKP")   # todo verify that date is parsed correctly
        ),
        ["DESPATCHING_NOTE"], "left"
    )

    # (PHASE 3) GROUP + SUM LIPS
    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(
        sum("PICKING QTY").alias("PICKING QTY"),
        sum("DEL QTY").alias("DEL QTY"),
        max("BOOKING_SLOT_LIKP").alias("BOOKING_SLOT_LIKP")
    )

    #.assert_no_duplicates("ORDER_NUMBER", "ORDER_LINE")
).localCheckpoint()
print(f"_ORDER_PICKING_DEL_QUANTITY OK: {_ORDER_PICKING_DEL_QUANTITY.count()} rows")


# ORDER_NUMBER, ORDER_LINE ->
#       ORD QTY ORIG, ORDER_DELIVERY_WAREHOUSE, ORD QTY         (FROM VBAP)
#       , PICKING QTY, DEL QTY, BOOKING_SLOT_LIKP               (FROM _ORDER_PICKING_DEL_QUANTITY)
VBAK = (
    VBAK

    .join(_ORDER_PICKING_DEL_QUANTITY, ["ORDER_NUMBER", "ORDER_LINE"], "left")  # add PICKING QTY, DEL QTY, BOOKING_SLOT_LIKP
)


VBAK = VBAK.localCheckpoint()
print(f"VBAK (join _ORDER_PICKING_DEL_QUANTITY) OK: {VBAK.count()} rows")

'''
left keep(VBAK)

VBBE:
LOAD Distinct
    VBELN as ORDER_NUMBER, //k
    POSNR as ORDER_LINE, //k
    ETENR as SCHEDULE_LINE,
    LGORT as "ALLOCATION WAREHOUSE",
    sum(VMENG) as QTY_CONFIRMED
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBBE.qvd]
(qvd)  where VMENG>0
Group by VBELN,POSNR,ETENR,LGORT;
'''


VBBE_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "VBBE_INPUT"
    )
)

# ORDER_NUMBER, ORDER_LINE ->
#       SCHEDULE_LINE, ALLOCATION WAREHOUSE, sum(QTY_CONFIRMED)     (FROM VBBE)
#       "ORD QTY ORIG", "ORDER_DELIVERY_WAREHOUSE"    (from VBAK)
VBBE = (
    VBBE_INPUT
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("ETENR").alias("SCHEDULE_LINE"),
        col("LGORT").alias("ALLOCATION WAREHOUSE"),
        col("VMENG").alias("QTY_CONFIRMED")
    )

    .where(col("QTY_CONFIRMED") > 0)   # sum only positive quantities (it is a "where" not an "having")

    .groupBy("ORDER_NUMBER", "ORDER_LINE", "SCHEDULE_LINE", "ALLOCATION WAREHOUSE")
    .agg(sum("QTY_CONFIRMED").alias("QTY_CONFIRMED"))

    .join(
        VBAK.select(
            "ORDER_NUMBER", "ORDER_LINE",
            "ORD QTY ORIG", "ORDER_DELIVERY_WAREHOUSE"
        ),
        ["ORDER_NUMBER", "ORDER_LINE"],
        "inner"
    )

    #.assert_no_duplicates("ORDER_NUMBER", "ORDER_LINE")
)

VBBE = VBBE.localCheckpoint()
print(f"VBBE OK: {VBBE.count()} rows")

# calc: VBAK (QTY_CONFIRMED)

'''
left Join(VBAK)

LOAD Distinct
    VBELN as ORDER_NUMBER, //k
    POSNR as ORDER_LINE, //k
    sum(VMENG) as QTY_CONFIRMED
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBBE.qvd]
(qvd)  where VMENG>0
Group by VBELN,POSNR;
'''

# ORDER_NUMBER, ORDER_LINE -> sum(QTY_CONFIRMED)
_QTY_CONFIRMED_BY_ORDER_NUMBER_LINE = (
    VBBE_INPUT
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("VMENG").alias("QTY_CONFIRMED")
    )

    .where(col("QTY_CONFIRMED") > 0)   # sum only positive quantities (it is a "where" not an "having")

    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(sum("QTY_CONFIRMED").alias("QTY_CONFIRMED"))
)


# ORDER_NUMBER, ORDER_LINE ->
#       ORD QTY ORIG, ORDER_DELIVERY_WAREHOUSE, ORD QTY         (FROM VBAP)
#       , PICKING QTY, DEL QTY, BOOKING_SLOT_LIKP               (FROM _ORDER_PICKING_DEL_QUANTITY)
#       , QTY_CONFIRMED                                         (FROM _QTY_CONFIRMED_BY_ORDER_NUMBER_LINE)
VBAK = (
    VBAK

    .join(_QTY_CONFIRMED_BY_ORDER_NUMBER_LINE, ["ORDER_NUMBER", "ORDER_LINE"], "left")  # add QTY_CONFIRMED
)

VBAK = VBAK.localCheckpoint()
print(f"VBAK (join _QTY_CONFIRMED_BY_ORDER_NUMBER_LINE) OK: {VBAK.count()} rows")

# calc: VBBE ()

'''
left join(VBBE)

VBEP:
LOAD Distinct
    VBELN as ORDER_NUMBER, //k
    POSNR as ORDER_LINE, //k
    ETENR as SCHEDULE_LINE,
    EDATU as "CONFIRMED DATE",
    EZEIT as "CONFIRMED TIME",
    TDDAT as PLANNED_DISPATCH_DATE
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBEP.qvd](qvd);
'''

VBEP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "VBEP_INPUT"
    )
)
# was: VBEP
# ORDER_NUMBER, ORDER_LINE, SCHEDULE_LINE -> CONFIRMED DATE, CONFIRMED TIME, PLANNED_DISPATCH_DATE
_ORDER_DATES = (
    VBEP_INPUT
    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("ETENR").alias("SCHEDULE_LINE"),
        col("EDATU").alias("CONFIRMED DATE"),
        col("EZEIT").alias("CONFIRMED TIME"),
        col("TDDAT").alias("PLANNED_DISPATCH_DATE")
    )
    .distinct()

    #.assert_no_duplicates("ORDER_NUMBER", "ORDER_LINE", "SCHEDULE_LINE")
)



# ORDER_NUMBER, ORDER_LINE ->
#       SCHEDULE_LINE, ALLOCATION WAREHOUSE, sum(QTY_CONFIRMED)     (FROM VBBE)
#       ORD QTY ORIG, ORDER_DELIVERY_WAREHOUSE                      (FROM VBAK)
#       CONFIRMED DATE, CONFIRMED TIME, PLANNED_DISPATCH_DATE       (FROM ORDER_DATES)
VBBE = (
    VBBE

    # add CONFIRMED DATE, CONFIRMED TIME, PLANNED_DISPATCH_DATE
    .join(_ORDER_DATES, ["ORDER_NUMBER", "ORDER_LINE", "SCHEDULE_LINE"], "left")
)


VBBE = VBBE.localCheckpoint()
print(f"VBBE (join _ORDER_DATES) OK: {VBBE.count()} rows")

#calc: ORDER_STATUS
'''
ORDER_STATUS:
Load Distinct
ORDER_NUMBER,
ORDER_LINE,
"ORD QTY" as "ORD QTY FABS",
"ORD QTY ORIG",
"ORD QTY"/"ORD QTY ORIG" as "% ORD QTY FABS",
'Blocked' as "ORDER STATUS"
Resident VBAK where Applymap('BLOCK',ORDER_NUMBER,0)=1
and "ORD QTY"<>0;

Concatenate(ORDER_STATUS)

Load Distinct
ORDER_NUMBER,
ORDER_LINE,
"PICKING QTY" as "ORD QTY FABS",
"PICKING QTY"/"ORD QTY ORIG" as "% ORD QTY FABS",
"ORD QTY ORIG",
'In Picking' as "ORDER STATUS",
BOOKING_SLOT_LIKP
Resident VBAK where Applymap('BLOCK',ORDER_NUMBER,0)=0
and "PICKING QTY">0;

Concatenate(ORDER_STATUS)

Load Distinct *,
"ORD QTY FABS"/"ORD QTY ORIG" as "% ORD QTY FABS"
where "ORD QTY FABS"<>0;
Load Distinct
ORDER_NUMBER,
ORDER_LINE,
"CONFIRMED DATE",
"CONFIRMED TIME",
PLANNED_DISPATCH_DATE,
"ORD QTY ORIG",
"ALLOCATION WAREHOUSE",
if(Alt(QTY_CONFIRMED,0)>0,Alt(QTY_CONFIRMED,0),0) as "ORD QTY FABS",
'Confirmed SAP' as "ORDER STATUS"
Resident VBBE where Applymap('BLOCK',ORDER_NUMBER,0)=0 ;

Concatenate(ORDER_STATUS)

Load Distinct *,
"ORD QTY FABS"/"ORD QTY ORIG" as "% ORD QTY FABS"
where "ORD QTY FABS"<>0;
Load
ORDER_NUMBER,
ORDER_LINE,
"ORD QTY ORIG",
if("ORD QTY"-Alt("PICKING QTY",0)>(Alt(QTY_CONFIRMED,0)),"ORD QTY"-Alt("PICKING QTY",0)-Alt(QTY_CONFIRMED,0),0) as "ORD QTY FABS",
'Not Deliverables' as "ORDER STATUS"
Resident VBAK where Applymap('BLOCK',ORDER_NUMBER,0)=0
and "ORD QTY"<>0;

drop table VBAK;
drop table VBBE;
'''


_ORDER_STATUS_VBAK_BLOCK = (
    VBAK

    .join(BLOCK, ["ORDER_NUMBER"], "leftsemi")    # keep only rows in "BLOCK"

    .withColumn("ORD QTY FABS", col("ORD QTY"))

    .select(
        col("ORDER_NUMBER"),
        col("ORDER_LINE"),

        col("ORD QTY ORIG"),
        col("ORD QTY FABS"),

        lit("Blocked").alias("ORDER STATUS")
    )
)

_ORDER_STATUS_VBAK_NOBLOCK = (
    VBAK

    .join(BLOCK, ["ORDER_NUMBER"], "leftanti")    # keep only rows NOT in "BLOCK"

    .withColumn("ORD QTY FABS", col("PICKING QTY"))

    .select(
        col("ORDER_NUMBER"),
        col("ORDER_LINE"),

        col("ORD QTY ORIG"),
        col("ORD QTY FABS"),

        lit("In Picking").alias("ORDER STATUS"),

        col("BOOKING_SLOT_LIKP")
    )
)


_ORDER_STATUS_VBBE_NOBLOCK = (
    VBBE

    .join(BLOCK, ["ORDER_NUMBER"], "leftanti")    # keep only rows NOT in "BLOCK"

    .withColumn("ORD QTY FABS", col("QTY_CONFIRMED"))

    .select(
        col("ORDER_NUMBER"),
        col("ORDER_LINE"),

        col("ORD QTY ORIG"),
        col("ORD QTY FABS"),

        lit("Confirmed SAP").alias("ORDER STATUS"),

        col("CONFIRMED DATE"),
        col("CONFIRMED TIME"),
        col("PLANNED_DISPATCH_DATE"),
        col("ALLOCATION WAREHOUSE")
    )
)



_ORDER_STATUS_VBAK_NOBLOCK_NOT_DELIVERABLES = (
    VBAK

    .join(BLOCK, ["ORDER_NUMBER"], "leftanti")    # keep only rows NOT in "BLOCK"

    .withColumn("PICKING QTY", coalesce(col("PICKING QTY"), lit(0)))
    .withColumn("QTY_CONFIRMED", coalesce(col("QTY_CONFIRMED"), lit(0)))

    .withColumn("ORD QTY FABS", col("ORD QTY") - col("PICKING QTY") - col("QTY_CONFIRMED"))

    .select(
        col("ORDER_NUMBER"),
        col("ORDER_LINE"),

        col("ORD QTY ORIG"),
        col("ORD QTY FABS"),

        lit("Not Deliverables").alias("ORDER STATUS")
    )
)


ORDER_STATUS = (
    _ORDER_STATUS_VBAK_BLOCK
    .unionByName(_ORDER_STATUS_VBAK_NOBLOCK, allowMissingColumns=True)
    .unionByName(_ORDER_STATUS_VBBE_NOBLOCK, allowMissingColumns=True)
    .unionByName(_ORDER_STATUS_VBAK_NOBLOCK_NOT_DELIVERABLES, allowMissingColumns=True)

    # as-is: convert negative values to zero, then filter != 0
    # to-be: keep values > 0
    # even easier: keep rows with some % of FABS
    .withColumn("% ORD QTY FABS", col("ORD QTY FABS") / col("ORD QTY ORIG"))
    .where(col("ORD QTY FABS") > 0)

    .distinct()
)

ORDER_STATUS = ORDER_STATUS.localCheckpoint()
print(f"ORDER_STATUS OK: {ORDER_STATUS.count()} rows")

# add VBAK columns to ORDER_STATUS
'''
left Join(ORDER_STATUS)

VBAK:
LOAD
    2 as DATA_TYPE,
//     'SAP' as ORDER_SOURCE,
//  'SAP' as ORDER_SOURCE_DESCR,
    VBELN as ORDER_NUMBER, //k
    AUART as ORDER_TYPE_CODE,
    Today() as DOCUMENT_DATE_ORIGINAL,
    NUM(APPLYMAP('COMMERCIAL_MAP_DATE',TODAY(),TODAY())) AS TIME_ID,
    NUM(APPLYMAP('COMMERCIAL_MAP_DATE',TODAY(),TODAY())) AS TIME_ID_FACT,
    DATE(APPLYMAP('COMMERCIAL_MAP_DATE',TODAY(),TODAY())) AS DOCUMENT_DATE,
    Applymap('TVAKT',AUART) as ORDER_TYPE_DESCR,
    if(AUART='ZBGR',0,1) as FLAG_BGRADE,
    if(AUART='ZBGR','B','A') as "A/B_GRADE",
    Applymap('CAS_CAUSES_RETURN_REASON',if(ApplyMap('ORD_TYPE',VBTYP)=7 or Match(AUART,'ZREB','ZREG')>0 ,AUGRU,NUll()),'Not Defined') as RETURN_REASON,
    Applymap('CAS_RETURN_REASON_DESCR',if(ApplyMap('ORD_TYPE',VBTYP)=7 or Match(AUART,'ZREB','ZREG')>0,AUGRU,Null()),'Not Defined') as RETURN_REASON_DESCR,
    BSTNK as ORDER_CUSTOMER_REFERENCE_1,
    AUDAT as ORDER_DATE,
    AUDAT as ORDER_CUSTOMER_DATE,
    ERNAM as ORDER_CREATION_USER,
    VDATU as ORDER_REQUESTED_DELIVERY_DATE,
    DATE(DAYSTART(VDATU)) as ORD_REQUESTED_DELIVERY_DATE,
    Monthname(DAYSTART(VDATU)) as ORDER_REQUESTED_DELIVERY_MONTH_YEAR,
    LIFSK as ORDER_BLOCKING_CODE,
    LIFSK & ' ' & FAKSK as ORDER_BLOCKING_DESCR,
    VTWEG as DISTR_CHANNEL,
    VKORG as SALES_ORG,
    SPART as DIVISION,
    LIFSK as SAP_ORDER_BLOCK_DELIVERY,
    CMGST as SAP_ORDER_BLOCK_CREDIT,
    Applymap('TVLST',LIFSK) as SAP_ORDER_BLOCK_DELIVERY_TXT,
    if(len(trim(LIFSK))>0,'Delivery Block',
    if(FAKSK='C','Billing Block',
    if(CMGST='C' or CMGST='B','Credit Block'))) as "BLOCKING TYPE",
    ApplyMap('ORD_TYPE',VBTYP) as DOC_TYPE_CODE,
    Applymap('DD07T',VBTYP) as DOC_TYPE_DESCR,
    KNUMV,
    VKBUR as SALES_OFFICE, //k
    ApplyMap('ORD_SIGN',VBTYP) as MOLTIPLICATORE_RESO,
    VBTYP  as CATEGORY,
    VSBED as WAREHOUSE_CLASSIFICATION,
    BSARK as ORDER_SOURCE,
    if(match(AUART,'ZKA','ZKB','ZKE','ZKR'),'No','Yes') as "OLD PERIMETER QLIK",
    Applymap('T176T',BSARK) as ORDER_SOURCE_DESCR
    FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBAK.qvd] (qvd);
'''



COMMERCIAL_MAP_DATE = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "COMMERCIAL_MAP_DATE"
    )
)


TVAKT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVAKT_MAPPING"
    )
)

ORD_TYPE = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "ORD_TYPE"
    )
)

CAS_CAUSES_RETURN_REASON_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "CAS_CAUSES_RETURN_REASON_MAPPING"
    )
)
CAS_RETURN_REASON_DESCR_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "CAS_RETURN_REASON_DESCR_MAPPING"
    )
)

DD07T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "DD07T_MAPPING"
    )
)
ORD_SIGN = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "ORD_SIGN"
    )
)
T176T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "T176T_MAPPING"
    )
)
TVLST_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVLST_MAPPING"
    )
)


# was: VBAK
# ORDER_NUMBER -> <lot of data>
_ORDER_STATUS_VBAK = (
    VBAK_INPUT

    #NUM(APPLYMAP('COMMERCIAL_MAP_DATE',TODAY(),TODAY())) AS TIME_ID,
    #NUM(APPLYMAP('COMMERCIAL_MAP_DATE',TODAY(),TODAY())) AS TIME_ID_FACT,
    #DATE(APPLYMAP('COMMERCIAL_MAP_DATE',TODAY(),TODAY())) AS DOCUMENT_DATE,
    .do_mapping(COMMERCIAL_MAP_DATE, current_date(), "TIME_ID", default_value=lit(current_date()), mapping_name="COMMERCIAL_MAP_DATE")
    .withColumn("TIME_ID_FACT", col("TIME_ID"))
    .withColumn("DOCUMENT_DATE", col("TIME_ID"))

    #Applymap('TVAKT',AUART) as ORDER_TYPE_DESCR,
    .do_mapping(TVAKT_MAPPING, col("AUART"), "ORDER_TYPE_DESCR", mapping_name="TVAKT")

    .do_mapping(ORD_TYPE, col("VBTYP"), "_ord_type_map", mapping_name="ORD_TYPE")
    .withColumn("_cas_return_reason_key",
        when(col("_ord_type_map") == "7", col("AUGRU"))
        .when(col("AUART").isin(['ZREB','ZREG']), col("AUGRU"))
        .otherwise(lit(None))
    )
#Applymap('CAS_CAUSES_RETURN_REASON',if(ApplyMap('ORD_TYPE',VBTYP)=7 or Match(AUART,'ZREB','ZREG')>0 ,AUGRU,NUll()),'Not Defined') as RETURN_REASON,
    .do_mapping(CAS_CAUSES_RETURN_REASON_MAPPING, col("_cas_return_reason_key"), "RETURN_REASON", default_value=lit("Not Defined"), mapping_name="CAS_CAUSES_RETURN_REASON_MAPPING")

#Applymap('CAS_RETURN_REASON_DESCR',if(ApplyMap('ORD_TYPE',VBTYP)=7 or Match(AUART,'ZREB','ZREG')>0,AUGRU,Null()),'Not Defined') as RETURN_REASON_DESCR,
    .do_mapping(CAS_RETURN_REASON_DESCR_MAPPING, col("_cas_return_reason_key"), "RETURN_REASON_DESCR", default_value=lit("Not Defined"), mapping_name="CAS_RETURN_REASON_DESCR")

#Applymap('TVLST',LIFSK) as SAP_ORDER_BLOCK_DELIVERY_TXT,
    .do_mapping(TVLST_MAPPING, col("LIFSK"), "SAP_ORDER_BLOCK_DELIVERY_TXT", mapping_name="TVLST")
#ApplyMap('ORD_TYPE',VBTYP) as DOC_TYPE_CODE,
    .do_mapping(ORD_TYPE, col("VBTYP"), "DOC_TYPE_CODE", mapping_name="ORD_TYPE")
#Applymap('DD07T',VBTYP) as DOC_TYPE_DESCR,
    .do_mapping(DD07T_MAPPING, col("VBTYP"), "DOC_TYPE_DESCR", mapping_name="DD07T")
#ApplyMap('ORD_SIGN',VBTYP) as MOLTIPLICATORE_RESO,
    .do_mapping(ORD_SIGN, col("VBTYP"), "MOLTIPLICATORE_RESO", mapping_name="ORD_SIGN")
#Applymap('T176T',BSARK) as ORDER_SOURCE_DESCR
    .do_mapping(T176T_MAPPING, col("BSARK"), "ORDER_SOURCE_DESCR", mapping_name="T176T")

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("AUART").alias("ORDER_TYPE_CODE"),
        current_date().alias("DOCUMENT_DATE_ORIGINAL"),
        "TIME_ID",
        "TIME_ID_FACT",
        "DOCUMENT_DATE",
        "ORDER_TYPE_DESCR",
        "RETURN_REASON",
        "RETURN_REASON_DESCR",
        col("BSTNK").alias("ORDER_CUSTOMER_REFERENCE_1"),
        col("AUDAT").alias("ORDER_DATE"),
        col("AUDAT").alias("ORDER_CUSTOMER_DATE"),
        col("ERNAM").alias("ORDER_CREATION_USER"),
        col("VDATU").alias("ORDER_REQUESTED_DELIVERY_DATE"),
        to_date(col("VDATU")).alias("ORD_REQUESTED_DELIVERY_DATE"),   # watch out little difference with prev col
        date_format(to_date(col("VDATU")), 'MMM').alias("ORDER_REQUESTED_DELIVERY_MONTH_YEAR"),   # todo check
        col("LIFSK").alias("ORDER_BLOCKING_CODE"),
        concat(col("LIFSK"), lit(" "), col("FAKSK")).alias("ORDER_BLOCKING_DESCR"),
        col("VTWEG").alias("DISTR_CHANNEL"),
        col("VKORG").alias("SALES_ORGANIZATION"),
        col("SPART").alias("DIVISION"),
        col("LIFSK").alias("SAP_ORDER_BLOCK_DELIVERY"),
        col("FAKSK").alias("__FAKSK"),    # temp (for calculating BLOCKING TYPE
        col("CMGST").alias("SAP_ORDER_BLOCK_CREDIT"),
        "SAP_ORDER_BLOCK_DELIVERY_TXT",
        "DOC_TYPE_CODE",
        "DOC_TYPE_DESCR",
        col("KNUMV"),
        col("VKBUR").alias("SALES_OFFICE"),
        "MOLTIPLICATORE_RESO",
        col("VBTYP").alias("CATEGORY"),
        col("VSBED").alias("WAREHOUSE_CLASSIFICATION"),
        col("BSARK").alias("ORDER_SOURCE"),
        "ORDER_SOURCE_DESCR",
        #col("VKORG").alias("company"),
        #col("VKBUR").alias("network"),
    )

    .withColumn("DATA_TYPE", lit(2))

    .withColumn("FLAG_BGRADE", when(col("ORDER_TYPE_CODE") == 'ZBGR', lit(0)).otherwise(lit(1)))
    .withColumn("A/B_GRADE", when(col("ORDER_TYPE_CODE") == 'ZBGR', lit("B")).otherwise(lit("A")))

    .withColumn("BLOCKING TYPE",
        when(trim(col("SAP_ORDER_BLOCK_DELIVERY")) != "", lit("Delivery Block"))
        .when(col("__FAKSK") == "C", lit("Billing Block"))
        .when(col("SAP_ORDER_BLOCK_CREDIT").isin(["C", "B"]), lit("Credit Block"))
        .otherwise(lit(None))
    )

    .withColumn("OLD PERIMETER QLIK", ~col("ORDER_TYPE_CODE").isin('ZKA','ZKB','ZKE','ZKR'))   #
    .withColumn("OLD PERIMETER QLIK", when(col("OLD PERIMETER QLIK"), lit("Yes")).otherwise(lit("No")))

)


ORDER_STATUS = (
    ORDER_STATUS
    .join(_ORDER_STATUS_VBAK, ["ORDER_NUMBER"], "left")
)


ORDER_STATUS = ORDER_STATUS.localCheckpoint()
print(f"ORDER_STATUS (join _ORDER_STATUS_VBAK) OK: {ORDER_STATUS.count()} rows")

# add VBAP columns to ORDER_STATUS
'''
Left Join(ORDER_STATUS)

VBAP:
LOAD
    VBELN as ORDER_NUMBER, //k
    POSNR as ORDER_LINE, //k
    Applymap('PRODUCT',num#(MATNR)) as PRODUCT_ID,
    num#(MATNR) as PRODUCT_CODE,
    num#(ZZLAREA) as AREA_CODE,
    Applymap('ZWW_OTC_LOGAREA',ZZLAREA) as AREA_DESCR,

    LGORT as ORDER_DELIVERY_WAREHOUSE,
    WERKS as PLANT,
    VSTEL as "SHIPPING POINT",
    ROUTE as ROUTING,
    Applymap('TVROT',ROUTE) as ROUTING_DESCR,
    PSTYV,
    date(Coalesce(ZZSO_BOOKDATE,ZZSO_PBSDATE )) as BOOKING_SLOT_VBAP
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBAP.qvd](qvd)
where Applymap('OPEN_ORDER',VBELN&'|'&POSNR,-1)>0; //solo open orders
'''


ZWW_OTC_LOGAREA_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "ZWW_OTC_LOGAREA_MAPPING"
    )
)
TVROT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVROT_MAPPING"
    )
)

# was: VBAP
# ORDER_NUMBER, ORDER_LINE -> <lot of data>
_ORDER_STATUS_VBAP = (
    VBAP_INPUT

    #Applymap('ZWW_OTC_LOGAREA',ZZLAREA) as AREA_DESCR,
    .do_mapping(ZWW_OTC_LOGAREA_MAPPING, col("ZZLAREA"), "AREA_DESCR", mapping_name="ZWW_OTC_LOGAREA")

    #Applymap('TVROT',ROUTE) as ROUTING_DESCR,
    .do_mapping(TVROT_MAPPING, col("ROUTE"), "ROUTING_DESCR", mapping_name="TVROT")

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("MATNR").alias("PRODUCT_CODE"),
        col("ZZLAREA").alias("AREA_CODE"),
        "AREA_DESCR",
        col("LGORT").alias("ORDER_DELIVERY_WAREHOUSE"),
        col("WERKS").alias("PLANT"),
        col("VSTEL").alias("SHIPPING_POINT"),
        col("ROUTE").alias("ROUTING"),
        "ROUTING_DESCR",
        col("PSTYV").alias("PSTYV"),
        coalesce(col("ZZSO_BOOKDATE"), col("ZZSO_PBSDATE")).alias("BOOKING_SLOT_VBAP"),
    )
)


ORDER_STATUS = (
    ORDER_STATUS
    .join(_ORDER_STATUS_VBAP, ["ORDER_NUMBER", "ORDER_LINE"], "left")

    .where(col("DIVISION") != "SE")
)


ORDER_STATUS = ORDER_STATUS.localCheckpoint()
print(f"ORDER_STATUS (join _ORDER_STATUS_VBAP) OK: {ORDER_STATUS.count()} rows")
# calc: ORDER_STATUS_SALES_OFFICE


LINE_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "LINE_MAPPING"
    )
)

mapping_company_network = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier_uk")

    .withColumnRenamed("SALES_ORGANIZATION", "_SALES_ORGANIZATION_MAPPING")
    .withColumnRenamed("SALES_OFFICE", "_SALES_OFFICE_MAPPING")
    .withColumnRenamed("LINE", "_LINE_MAPPING")

    .assert_no_duplicates("_SALES_ORGANIZATION_MAPPING", "_SALES_OFFICE_MAPPING", "_LINE_MAPPING")    # dato che mappiamo sap->plm, verifica duplicati su chiave sap
)


'''
ORDER_STATUS_SALES_OFFICE:
Load *,
if(ApplyMap('Mapping_SalesOffice_UK',SALES_OFFICE,0)=1,SALES_OFFICE&'|'&ApplyMap('LINE',PRODUCT_CODE,999),SALES_OFFICE) as SALES_OFFICE_KEY
Resident ORDER_STATUS
where DIVISION<>'SE';
'''


ORDER_STATUS_SALES_OFFICE = (
    ORDER_STATUS

    #.where(col("DIVISION") != "SE")   # moved to previous step

    .do_mapping(LINE_MAPPING, col("PRODUCT_CODE"), "__line_code", default_value=lit("999"), mapping_name="LINE_MAPPING")

    .join(
        mapping_company_network,
        (col("SALES_ORGANIZATION") == col("_SALES_ORGANIZATION_MAPPING")) &
        (col("SALES_OFFICE") == col("_SALES_OFFICE_MAPPING")) &
        ((col("__line_code") == col("_LINE_MAPPING")) | (col("_LINE_MAPPING").isNull())),
        "left"
    )

    .drop("_SALES_ORGANIZATION_MAPPING", "_SALES_OFFICE_MAPPING", "_LINE_MAPPING")

    #.join(Mapping_SalesOffice_UK.withColumn("__presence_salesoffice_uk", lit(True)), ["SALES_OFFICE"], "left")

    #.do_mapping(LINE_MAPPING, col("PRODUCT_CODE"), "__line_code", default_value=lit(None), mapping_name="LINE_MAPPING")

    #.withColumn("SALES_OFFICE_KEY",
    #    when(col("__presence_salesoffice_uk"), concat(col("SALES_OFFICE"), lit("|"), col("__line_code")))
    #    .otherwise(col("SALES_OFFICE"))
    #)
    #.drop("__presence_salesoffice_uk", "__line_code")
)

ORDER_STATUS_SALES_OFFICE = ORDER_STATUS_SALES_OFFICE.localCheckpoint()
print(f"ORDER_STATUS_SALES_OFFICE OK: {ORDER_STATUS_SALES_OFFICE.count()} rows")

# add VBEP columns to ORDER_STATUS_SALES_OFFICE

'''
left join(ORDER_STATUS_SALES_OFFICE)

VBEP:
LOAD Distinct
    VBELN as ORDER_NUMBER, //k
    POSNR as ORDER_LINE, //k
    date(min(EDATU)) as FIRST_REQ_DEL_DATE
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/VBEP.qvd](qvd)
Group By VBELN,POSNR;
'''


_SALE_NETWORKS_VBEP = (
    VBEP_INPUT

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        to_date(col("EDATU")).alias("FIRST_REQ_DEL_DATE")
    )

    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(min("FIRST_REQ_DEL_DATE").alias("FIRST_REQ_DEL_DATE"))
)

ORDER_STATUS_SALES_OFFICE = (
    ORDER_STATUS_SALES_OFFICE

    .join(_SALE_NETWORKS_VBEP, ["ORDER_NUMBER", "ORDER_LINE"], "left")
)

ORDER_STATUS_SALES_OFFICE = ORDER_STATUS_SALES_OFFICE.localCheckpoint()
print(f"ORDER_STATUS_SALES_OFFICE (join _SALE_NETWORKS_VBEP) OK: {ORDER_STATUS_SALES_OFFICE.count()} rows")

# calc: ORD_COST

'''
ORD_COST:
Load Distinct
US_COMPANY,
US_NETWORK,
PRODUCT_CODE as PRODUCT,
date(makedate(YEAR(DOCUMENT_DATE_ORIGINAL))) as DOCUMENT_DATE,
Year(DOCUMENT_DATE_ORIGINAL) as YEAR
Resident ORDER_STATUS_SALES_OFFICE
where len(COMPANY_ID)>0 and len(US_NETWORK)>0 and len(num(PRODUCT_CODE))>0 and len(DOCUMENT_DATE_ORIGINAL)>0;

'''
'''
ORD_COST = (
    ORDER_STATUS_SALES_OFFICE

    .where(col("COMPANY_ID") != "")
    .where(col("US_NETWORK") != "")
    .where(col("PRODUCT_CODE") != "")
    .where(col("DOCUMENT_DATE_ORIGINAL") != "")

    .select(
        col("US_COMPANY").alias("US_COMPANY"),
        col("US_NETWORK").alias("US_NETWORK"),
        col("PRODUCT_CODE").alias("PRODUCT"),
        to_date(concat(year(col("DOCUMENT_DATE_ORIGINAL")).cast("string"), lit("-01-01"))).alias("DOCUMENT_DATE"),
        year(col("DOCUMENT_DATE_ORIGINAL")).alias("YEAR"),
    )

    .distinct()
)

print("ORD_COST", ORD_COST)'''
'''ORD_COST = ORD_COST.localCheckpoint()
ORD_COST.count()'''
'''
Left Join(ORDER_STATUS_SALES_OFFICE)

LOAD Distinct
    Company&'-@-'&Network as NETWORK_ID,
    "Cluster Code" as RESPONSIBLE_CODE,
    "Cluster Descr" as RESPONSIBLE_DESCR,
    "Region Code" as RESPONSIBLE_CODE1,
    "Region Descr" as RESPONSIBLE_DESCR1,
    "Subcluster Code" as RESPONSIBLE_CODE3,
    "Subcluster Descr" as RESPONSIBLE_DESCR3
FROM [lib://Prj_Anagrafiche/QVD02/RESPONSIBLES.QVD]
(qvd);
'''
'''
RESPONSIBLES_INPUT = read_data_from_redshift_table("mtd.sap_dm_otc_mkt_hier")    #  HELP PAOLO

_RESPONSIBLES = (
    RESPONSIBLES_INPUT
    .select(
        col("VKORG").alias("SALES_ORGANIZATION"),
        col("VKBUR").alias("SALES_OFFICE"),
        col("ccod").alias("RESPONSIBLE_CODE"),    # Cluster
        col("cdes").alias("RESPONSIBLE_DESCR"),
        col("rcod").alias("RESPONSIBLE_CODE1"),    # Region
        col("rdes").alias("RESPONSIBLE_DESCR1"),
        col("scod").alias("RESPONSIBLE_CODE3"),    # Subcluster
        col("sdes").alias("RESPONSIBLE_DESCR3")
    )

    .distinct()     # TODO VERIFICA DUPLICATI
    .drop_duplicates_ordered(["SALES_ORGANIZATION", "SALES_OFFICE"], ["RESPONSIBLE_CODE", "RESPONSIBLE_DESCR", "RESPONSIBLE_CODE1", "RESPONSIBLE_DESCR1", "RESPONSIBLE_CODE3", "RESPONSIBLE_DESCR3"])
    .assert_no_duplicates("SALES_ORGANIZATION", "SALES_OFFICE")
)

ORDER_STATUS_SALES_OFFICE = (
    ORDER_STATUS_SALES_OFFICE

    .join(_RESPONSIBLES, ["SALES_ORGANIZATION", "SALES_OFFICE"], "left")
)


ORDER_STATUS_SALES_OFFICE = ORDER_STATUS_SALES_OFFICE.localCheckpoint()
print(f"ORDER_STATUS_SALES_OFFICE (join _RESPONSIBLES) OK: {ORDER_STATUS_SALES_OFFICE.count()} rows")
'''
'''
Left Join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
  VBELN as ORDER_NUMBER,
  POSNR as ORDER_LINE,
  KUNNR as k_SHIP_POS // Ship to posizione
Resident VBPA
where PARVW='WE';

Left Join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
  VBELN as ORDER_NUMBER,
  KUNNR as k_SHIP_HEADER // Ship to header
Resident VBPA
where PARVW='WE' and POSNR='000000';

Left Join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_SOLD_POS //sold to posizione
Resident VBPA
where PARVW='RE';

Left Join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_SOLD_HEADER //sold to header
Resident VBPA
where PARVW='RE' and POSNR='000000';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_PURCHASE_POS //purchase group posizione
Resident VBPA
where PARVW='ZC';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_PURCHASE_HEADER //purchase group to header
Resident VBPA
where PARVW='ZC' and POSNR='000000';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_MAIN_POS //main group to posizione
Resident VBPA
where PARVW='ZD';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_MAIN_HEADER //main group  to header
Resident VBPA
where PARVW='ZD' and POSNR='000000';


Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_SUPER_POS //super group posizione
Resident VBPA
where PARVW='ZE';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_SUPER_HEADER //super group header
Resident VBPA
where PARVW='ZE' and POSNR='000000';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_SALE_ZONE_POS //sales zone posizione
Resident VBPA where PARVW='YB';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_SALE_ZONE_HEADER //sales zone header
Resident VBPA where PARVW='YB' and POSNR='000000';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_SALE_MAN_POS //sales man posizione
Resident VBPA where PARVW='YD';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_SALE_MAN_HEADER //sales man header
Resident VBPA where PARVW='YD' and POSNR='000000';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_SALE_DIRECTOR_POS //sales director posizione
Resident VBPA where PARVW='YA';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
  KUNNR as k_SALE_DIRECTOR_HEADER //sales director header
Resident VBPA where PARVW='YA' and POSNR='000000';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   POSNR as ORDER_LINE,
  KUNNR as k_RSM_POS //   RSM
Resident VBPA where PARVW='YC';

Left join(ORDER_STATUS_SALES_OFFICE)

VBPA:
Load
   VBELN as ORDER_NUMBER,
   KUNNR as k_RSM_HEADER //   RSM
Resident VBPA where PARVW='YC' and POSNR='000000';
'''

VBPA_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(
        table_name  = "VBPA_INPUT"
    )
)

def extract_vbpa_using_number_line(self, target_name, parw_value):
    _vbpa = (
        VBPA_INPUT
        .where(col("PARVW") == lit(parw_value))
        .select(
            col("VBELN").alias("ORDER_NUMBER"),
            col("POSNR").alias("ORDER_LINE"),
            col("KUNNR").alias(target_name),
        )
    )

    return self.join(_vbpa, ["ORDER_NUMBER", "ORDER_LINE"], "left")

setattr(DataFrame, "extract_vbpa_using_number_line", extract_vbpa_using_number_line)

def extract_vbpa_using_number(self, target_name, parw_value):
    _vbpa = (
        VBPA_INPUT
        .where(col("PARVW") == lit(parw_value))
        .where(col("POSNR") == lit('000000'))
        .select(
            col("VBELN").alias("ORDER_NUMBER"),
            col("KUNNR").alias(target_name),
        )
    )

    return self.join(_vbpa, ["ORDER_NUMBER"], "left")

setattr(DataFrame, "extract_vbpa_using_number", extract_vbpa_using_number)


ORDER_STATUS_SALES_OFFICE = (
    ORDER_STATUS_SALES_OFFICE
    .extract_vbpa_using_number_line(target_name = "k_SHIP_POS", parw_value = "SP") #ex WE, SH
    .extract_vbpa_using_number(target_name = "k_SHIP_HEADER", parw_value = "SP")

    .extract_vbpa_using_number_line(target_name = "k_SOLD_POS", parw_value = "BP") #ex RE, SP
    .extract_vbpa_using_number(target_name = "k_SOLD_HEADER", parw_value = "BP")

    .extract_vbpa_using_number_line(target_name = "k_PURCHASE_POS", parw_value = "ZC")
    .extract_vbpa_using_number(target_name = "k_PURCHASE_HEADER", parw_value = "ZC")

    .extract_vbpa_using_number_line(target_name = "k_MAIN_POS", parw_value = "ZD")
    .extract_vbpa_using_number(target_name = "k_MAIN_HEADER", parw_value = "ZD")

    .extract_vbpa_using_number_line(target_name = "k_SUPER_POS", parw_value = "ZE")
    .extract_vbpa_using_number(target_name = "k_SUPER_HEADER", parw_value = "ZE")

    .extract_vbpa_using_number_line(target_name = "k_SALE_ZONE_POS", parw_value = "YB")
    .extract_vbpa_using_number(target_name = "k_SALE_ZONE_HEADER", parw_value = "YB")

    .extract_vbpa_using_number_line(target_name = "k_SALE_MAN_POS", parw_value = "YD")
    .extract_vbpa_using_number(target_name = "k_SALE_MAN_HEADER", parw_value = "YD")

    .extract_vbpa_using_number_line(target_name = "k_SALE_DIRECTOR_POS", parw_value = "YA")
    .extract_vbpa_using_number(target_name = "k_SALE_DIRECTOR_HEADER", parw_value = "YA")

    .extract_vbpa_using_number_line(target_name = "k_RSM_POS", parw_value = "YC")
    .extract_vbpa_using_number(target_name = "k_RSM_HEADER", parw_value = "YC")

)

ORDER_STATUS_SALES_OFFICE = ORDER_STATUS_SALES_OFFICE.localCheckpoint()
print(f"ORDER_STATUS_SALES_OFFICE (extract vbpa) OK: {ORDER_STATUS_SALES_OFFICE.count()} rows")
'''

ORD:
Load *,
COMPANY_ID&'-'&if(COMPANY_ID=69 and US_NETWORK=82,81,US_NETWORK)&'-'&PRODUCT_CODE&'-'&Year(DOCUMENT_DATE_ORIGINAL) as k_COST,
// COMPANY_ID&'-'&US_NETWORK&'-'&PRODUCT_CODE&'-'&Year(DOCUMENT_DATE_ORIGINAL) as k_COST,
if("ORDER STATUS"='Blocked','Blocked',
if("ORDER STATUS"='In Picking','In Picking',
'Not Blocked')) as "POTENTIAL STATUS",
"% ORD QTY FABS" as "% ORD QTY",

if(Match(DOC_TYPE_CODE,5,6) or Match(ORDER_TYPE_CODE,'ZKA','ZKB'),0,
if(DOC_TYPE_CODE=4 and Match(ORDER_TYPE_CODE,'ZREB','ZREG')>0 and CATEGORY='C',"ORD QTY FABS"*-1, // forzatura per correggere errore config di SAP
"ORD QTY FABS"*MOLTIPLICATORE_RESO)) as "ORD QTY",
if(Match(ORDER_TYPE_CODE,'ZKA','ZKB')>0,"ORD QTY FABS"*MOLTIPLICATORE_RESO,0) as "ORD QTY CONSIGNMENT",

IF(DATA_TYPE=2,ORDER_REQUESTED_DELIVERY_DATE,IF(DATA_TYPE=4,DOCUMENT_DATE_ORIGINAL,MAKEDATE(1900,1,1))) as MRDD,
if(Match(ORDER_TYPE_CODE,'ZKA','ZKB')=0,KNUMV&'|'&ORDER_LINE,Null()) as k_PRICE_CONDITION,
ORDER_BLOCKING_DESCR as "ORDER BLOCKING",

Coalesce(k_SHIP_POS,k_SHIP_HEADER) as DEST_ID,

if(IsNull(k_SOLD_POS) or len(k_SOLD_POS)=0,k_SOLD_HEADER,k_SOLD_POS) as INV_ID,
if(IsNull(k_PURCHASE_POS) or len(k_PURCHASE_POS)=0,k_PURCHASE_HEADER,k_PURCHASE_POS) as PURCHASE_GROUP_CODE,
if(IsNull(k_MAIN_POS) or len(k_MAIN_POS)=0,k_MAIN_HEADER,k_MAIN_POS) as MAIN_GROUP_CODE,
if(IsNull(k_SUPER_POS) or len(k_SUPER_POS)=0,k_SUPER_HEADER,k_SUPER_POS) as SUPER_GROUP_CODE,
Coalesce(k_SALE_ZONE_POS,k_SALE_ZONE_HEADER) as SALE_ZONE_CODE,
if(IsNull(k_SALE_MAN_POS) or len(k_SALE_MAN_POS)=0,k_SALE_MAN_HEADER,k_SALE_MAN_POS) as SALESMAN_CODE,
Coalesce(k_SALE_DIRECTOR_POS,k_SALE_DIRECTOR_HEADER) as SALE_DIRECTOR_CODE,
Coalesce(k_RSM_POS,k_RSM_HEADER) as RSM_CODE,
Applymap('Currency_Map',Applymap('Company_Currency',COMPANY_ID,20)&'|'&Year(DOCUMENT_DATE_ORIGINAL),1) as "EXCHANGE",
Applymap('Company_Currency',COMPANY_ID,20) as CURRENCY_CODE
Resident ORDER_STATUS_SALES_OFFICE
where Applymap('PosServizio',ORDER_TYPE_CODE&'|'&PSTYV,0)=0; //elimino posizioni di articoli di servizio

Drop table ORDER_STATUS_SALES_OFFICE;

'''

Company_Currency_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "Company_Currency_MAPPING"
    )
)
Currency_Map = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "Currency_Map"
    )
)
Currency_Map_Local=Currency_Map.withColumn("exchange_to_local", when(col("exchange_value")==1 , 1).otherwise(1/col("exchange_value")))
Currency_Map_Local=Currency_Map_Local.select(col("key_currency"),col("exchange_to_local")).drop(col("exchange_value"))

PosServizio = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "PosServizio"
    )
)

ORD = (
    ORDER_STATUS_SALES_OFFICE

    #.withColumn(
    #    "k_COST",
    #    concat_ws(
    #        "-",
    #        col("company"),
    #        when((col("company") == 69) & (col("network") == 82), lit(81)).otherwise(col("network")),
    #        col("PRODUCT_CODE"),
    #        year(col("DOCUMENT_DATE"))
    #    )
    #)

    .withColumn(
        "POTENTIAL STATUS",
        when(col("ORDER STATUS") == "Blocked", "Blocked")
        .when(col("ORDER STATUS") == "In Picking", "In Picking")
        .otherwise("Not Blocked")
    )
    .withColumn(
        "% ORD QTY",
        col("% ORD QTY FABS")
    )
    .withColumn(
        "ORD QTY",
        when((col("DOC_TYPE_CODE").isin([5, 6])) | (col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"])), lit(0))
        .when(
            (col("DOC_TYPE_CODE") == 4) & (col("ORDER_TYPE_CODE").isin(["ZREB", "ZREG"])) & (col("CATEGORY") == "C"),
            col("ORD QTY FABS") * -1
        )
        .otherwise(col("ORD QTY FABS") * col("MOLTIPLICATORE_RESO"))
    )
    .withColumn(
        "ORD QTY CONSIGNMENT",
        when(col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"]), col("ORD QTY FABS") * col("MOLTIPLICATORE_RESO")).otherwise(lit(0))
    )
    .withColumn(
        "MRDD",
        when(col("DATA_TYPE") == 2, col("ORDER_REQUESTED_DELIVERY_DATE"))
        .when(col("DATA_TYPE") == 4, col("DOCUMENT_DATE_ORIGINAL"))
        .otherwise(lit("1900-01-01"))
    )
    .withColumn(
        "k_PRICE_CONDITION",
        when(~col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"]), concat_ws("|", col("KNUMV"), col("ORDER_LINE"))).otherwise(lit(None))
    )
    .withColumn(
        "ORDER BLOCKING",
        col("ORDER_BLOCKING_DESCR")
    )
    .withColumn(
        "DEST_ID",
        coalesce(col("k_SHIP_POS"), col("k_SHIP_HEADER"))
    )
    .withColumn(
        "INV_ID",
        when(col("k_SOLD_POS").isNull() | (col("k_SOLD_POS") == ""), col("k_SOLD_HEADER")).otherwise(col("k_SOLD_POS"))
    )
    .withColumn(
        "PURCHASE_GROUP_CODE",
        when(col("k_PURCHASE_POS").isNull() | (col("k_PURCHASE_POS") == ""), col("k_PURCHASE_HEADER")).otherwise(col("k_PURCHASE_POS"))
    )
    .withColumn(
        "MAIN_GROUP_CODE",
        when(col("k_MAIN_POS").isNull() | (col("k_MAIN_POS") == ""), col("k_MAIN_HEADER")).otherwise(col("k_MAIN_POS"))
    )
    .withColumn(
        "SUPER_GROUP_CODE",
        when(col("k_SUPER_POS").isNull() | (col("k_SUPER_POS") == ""), col("k_SUPER_HEADER")).otherwise(col("k_SUPER_POS"))
    )
    .withColumn(
        "SALE_ZONE_CODE",
        coalesce(col("k_SALE_ZONE_POS"), col("k_SALE_ZONE_HEADER"))
    )
    .withColumn(
        "SALESMAN_CODE",
        when(col("k_SALE_MAN_POS").isNull() | (col("k_SALE_MAN_POS") == ""), col("k_SALE_MAN_HEADER")).otherwise(col("k_SALE_MAN_POS"))
    )
    .withColumn(
        "SALE_DIRECTOR_CODE",
        coalesce(col("k_SALE_DIRECTOR_POS"), col("k_SALE_DIRECTOR_HEADER"))
    )
    .withColumn(
        "RSM_CODE",
        coalesce(col("k_RSM_POS"), col("k_RSM_HEADER"))
    )

    #Applymap('Company_Currency',COMPANY_ID,20) as CURRENCY_CODE
    .do_mapping(Company_Currency_MAPPING, col("SALES_ORGANIZATION"), "CURRENCY_CODE", default_value=lit("EUR"), mapping_name="Company_Currency_MAPPING")

    #Applymap('Currency_Map',Applymap('Company_Currency',COMPANY_ID,20)&'|'&Year(DOCUMENT_DATE_ORIGINAL),1) as "EXCHANGE",
    .do_mapping(Currency_Map, concat(col("CURRENCY_CODE"), lit("|"), year(col("DOCUMENT_DATE_ORIGINAL"))), "EXCHANGE", default_value=lit(1), mapping_name="Currency_Map")
    .do_mapping(Currency_Map_Local, concat(col("CURRENCY_CODE"), lit("|"), year(col("DOCUMENT_DATE_ORIGINAL"))), "EXCHANGE_TO_LOCAL", default_value=lit(1), mapping_name="Currency_Map_To_Local")

    #where Applymap('PosServizio',ORDER_TYPE_CODE&'|'&PSTYV,0)=0
    .do_mapping(PosServizio, concat(col("ORDER_TYPE_CODE"), lit("|"), year(col("PSTYV"))), "_presence_PosServizio", default_value=lit(0), mapping_name="PosServizio")
    .where(col("_presence_PosServizio") == 0).drop("_presence_PosServizio")
)

ORD = ORD.localCheckpoint()
print(f"ORD OK: {ORD.count()} rows")

'''

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    KBETR as VAT_PERCENTAGE_1,
    KWERT as VAT_AMOUNT_1_TOT,
    MWSK1 as VAT_REASON_CODE
Resident PRCD_ELEMENTS
where KSCHL='MWST';

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    (KWERT*if(KKURS<0,1/fabs(KKURS),KKURS)) as "ORD NSV LC TOT",
    if(KKURS<0,1/fabs(KKURS),KKURS) as EXCHANGE_RATE_SAP
Resident PRCD_ELEMENTS
where Match(KSCHL,'ZNSV','ZIV1','ZIV2');

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    KWERT as "ORD IC DISCOUNT/SURCHARGE"
Resident PRCD_ELEMENTS
where Match(KSCHL,'ZCDS');

Left Join(ORD)

PRCD_ELEMENTS:
LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    (KWERT) as "ORD RAEE LC"
Resident PRCD_ELEMENTS
 where KSCHL='ZWEE';

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    (KWERT*if(KKURS<0,1/fabs(KKURS),KKURS)) as "ORD GSV LC TOT" //già moltiplicato per qta
Resident PRCD_ELEMENTS
where Match(KSCHL,'ZTTV','ZIV1','ZIV2');

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    KWERT*if(KKURS<0,1/fabs(KKURS),KKURS) as "LIST_PRICE_TOT"
Resident PRCD_ELEMENTS
where KSCHL='ZNET';

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    if(KWERT>0,1,0) as PRICE_FORCED,
    KWERT*if(KKURS<0,1/fabs(KKURS),KKURS) as FORCED_PRICE_TOT
Resident PRCD_ELEMENTS
where KSCHL='PB00';

Left Join(ORD)

PRCD_ELEMENTS:
 LOAD
    KNUMV&'|'&KPOSN as k_PRICE_CONDITION,
    KWERT*if(KKURS<0,1/fabs(KKURS),KKURS) as "REMOVABLE_CHARGES_TOT"
Resident PRCD_ELEMENTS
where KSCHL='ZFEE';

'''

PRCD_ELEMENTS = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "PRCD_ELEMENTS"
    )
)


PRCD_ELEMENTS = (
    PRCD_ELEMENTS
    .withColumn("k_PRICE_CONDITION", concat_ws("|", col("KNUMV"), col("KPOSN")))
    .withColumn("EXCHANGE_RATE_SAP", when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS")))
    .withColumn("KWERT_EXCHANGE_RATE_SAP", col("KWERT") * col("EXCHANGE_RATE_SAP"))
)

PRCD_ELEMENTS_BASE = (
    PRCD_ELEMENTS
    .select("k_PRICE_CONDITION").distinct()
).localCheckpoint()
print(f"PRCD_ELEMENTS_BASE OK: {PRCD_ELEMENTS_BASE.count()} rows")

# VAT_PERCENTAGE_1, VAT_AMOUNT_1_TOT, VAT_REASON_CODE
_VAT = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'MWST')
    .select(
        "k_PRICE_CONDITION",
        col("KBETR").alias("VAT_PERCENTAGE_1"),
        col("KWERT").alias("VAT_AMOUNT_1_TOT"),
        col("MWSK1").alias("VAT_REASON_CODE")
    )
)

# ORD NSV LC TOT, EXCHANGE_RATE_SAP
_NSV = (
    PRCD_ELEMENTS
    .where(col("KSCHL").isin('ZNSV', 'ZIV1', 'ZIV2'))
    .select(
        "k_PRICE_CONDITION",
        (col("KWERT") * when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS"))).alias("ORD NSV LC TOT"),
        when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS")).alias("EXCHANGE_RATE_SAP")
    )
)

# ORD IC DISCOUNT/SURCHARGE
_DISCOUNT_SURCHARGE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZCDS')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT").alias("ORD IC DISCOUNT/SURCHARGE")
    )
)

# ORD RAEE LC
_RAEE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZWEE')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT").alias("ORD RAEE LC")
    )
)

# ORD GSV LC TOT
_GSV = (
    PRCD_ELEMENTS
    .where(col("KSCHL").isin('ZTTV', 'ZIV1', 'ZIV2'))
    .select(
        "k_PRICE_CONDITION",
        (col("KWERT") * when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS"))).alias("ORD GSV LC TOT")
    )
)

# LIST_PRICE_TOT
_LIST_PRICE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZNET')
    .select(
        "k_PRICE_CONDITION",
        (col("KWERT") * when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS"))).alias("LIST_PRICE_TOT")
    )
)

# FORCED_PRICE_TOT, PRICE_FORCED
_FORCED_PRICE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'PB00')
    .select(
        "k_PRICE_CONDITION",
        when(col("KWERT") > 0, lit(1)).otherwise(lit(0)).alias("PRICE_FORCED"),
        (col("KWERT") * when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS"))).alias("FORCED_PRICE_TOT")
    )
)

# removable charges tot
_REMOVABLE_CHARGES = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZFEE')
    .select(
        "k_PRICE_CONDITION",
        (col("KWERT") * when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS"))).alias("removable charges tot")
    )
)

# Join all DataFrames to ORD DataFrame
PRCD_ELEMENTS_ALL = (
    PRCD_ELEMENTS_BASE
    .join(_VAT, ["k_PRICE_CONDITION"], "left")
    .join(_NSV, ["k_PRICE_CONDITION"], "left")
    .join(_DISCOUNT_SURCHARGE, ["k_PRICE_CONDITION"], "left")
    .join(_RAEE, ["k_PRICE_CONDITION"], "left")
    .join(_GSV, ["k_PRICE_CONDITION"], "left")
    .join(_LIST_PRICE, ["k_PRICE_CONDITION"], "left")
    .join(_FORCED_PRICE, ["k_PRICE_CONDITION"], "left")
    .join(_REMOVABLE_CHARGES, ["k_PRICE_CONDITION"], "left")
).assert_no_duplicates("k_PRICE_CONDITION").localCheckpoint()

print(f"PRCD_ELEMENTS_ALL OK: {PRCD_ELEMENTS_ALL.count()} rows")

ORD = (
    ORD

    .join(PRCD_ELEMENTS_ALL, ["k_PRICE_CONDITION"], "left")
)

ORD = ORD.localCheckpoint()
print(f"ORD (join PRCD_ELEMENTS_ALL) OK: {ORD.count()} rows")
'''
ORD = (
    ORD

    .join(COST_EXISTS, ["k_COST"], "left")
)


print()
print("ORD", ORD)'''
'''ORD = ORD.localCheckpoint()
ORD.count()'''

CDHDR_CDPOS_BLOCK = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "CDHDR_CDPOS_BLOCK"
    )
)

CDHDR_CDPOS_UNBLOCK = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "CDHDR_CDPOS_UNBLOCK"
    )
)


ORD = (
    ORD
    .join(CDHDR_CDPOS_BLOCK, ["ORDER_NUMBER"], "left")
    .join(CDHDR_CDPOS_UNBLOCK, ["ORDER_NUMBER"], "left")
)

print("CDHDR_CDPOS_BLOCK", CDHDR_CDPOS_BLOCK)
print("CDHDR_CDPOS_UNBLOCK", CDHDR_CDPOS_UNBLOCK)
print()
print("ORD", ORD)
ORD = ORD.localCheckpoint()
print(f"ORD (join CDHDR_CDPOS_BLOCK / UNBLOCK) OK: {ORD.count()} rows")



###### calcolo costi

qlik_cst_pbcs_transfer_price_costs = read_data_from_redshift_table("dwa.pbcs_cst_ft_transfer_price_costs")
qlik_cst_pbcs_freight_in = read_data_from_redshift_table("dwa.pbcs_cst_ft_freight_in")
qlik_cst_pbcs_std_costs_commerciale = read_data_from_redshift_table("dwa.pbcs_cst_ft_std_costs_commerciale")
qlik_cst_pbcs_stdindef_tca_costs = read_data_from_redshift_table("dwa.pbcs_cst_ft_stdindef_tca_costs")



needed_keys_code_network_product_year = (
    ORD
    .select(
        col("company").alias("company"),
        col("network").alias("network"),
        "product_code",
        year("DOCUMENT_DATE").alias("year")
    )
    .where(col("company").isNotNull())
    .where(col("network").isNotNull())
    .where(col("product_code").isNotNull())
    .where(col("year").isNotNull())
    .distinct()
    .assert_no_duplicates("company", "network", "product_code", "year")
)

tp_commerciale = (
    qlik_cst_pbcs_transfer_price_costs
    .where(col("currency") == "EUR")

    .select("company", "network", "product_code", "year", "tp_commerciale")
    .where(col("company").isNotNull())
    .where(col("network").isNotNull())
    .where(col("product_code").isNotNull())
    .where(col("year").isNotNull())
    .where(col("tp_commerciale").isNotNull())
    .distinct()

    .join(needed_keys_code_network_product_year, ["company", "network", "product_code", "year"], "full")
    .withColumn("tp_commerciale", last("tp_commerciale", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year"))))   # fill going forward
    .withColumn("tp_commerciale", last("tp_commerciale", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year").desc())))   # fill going backward

    .withColumn("key_cost_transfer_price", concat_ws("###", col("company"), col("network"), col("product_code"), col("year")))

    .assert_no_duplicates("key_cost_transfer_price")
    .select(
        "key_cost_transfer_price",
        col("tp_commerciale").alias("COMM_COST_OF_SALES_UNIT"),
    )
).localCheckpoint()
print(f"tp_commerciale OK: {tp_commerciale.count()} rows")

#tp_commerciale.show()

tp_itcy = (
    qlik_cst_pbcs_transfer_price_costs
    .where(col("currency") == "EUR")

    .select("company", "network", "product_code", "year", "tp_itcy")
    .where(col("company").isNotNull())
    .where(col("network").isNotNull())
    .where(col("product_code").isNotNull())
    .where(col("year").isNotNull())
    .where(col("tp_itcy").isNotNull())
    .distinct()

    .join(needed_keys_code_network_product_year, ["company", "network", "product_code", "year"], "full")
    .withColumn("tp_itcy", last("tp_itcy", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year"))))   # fill going forward
    .withColumn("tp_itcy", last("tp_itcy", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year").desc())))   # fill going backward

    .withColumn("key_cost_transfer_price", concat_ws("###", col("company"), col("network"), col("product_code"), col("year")))

    .assert_no_duplicates("key_cost_transfer_price")
    .select(
        "key_cost_transfer_price",
        col("tp_itcy").alias("TP_ITCY_UNIT"),
    )
).localCheckpoint()
print(f"tp_itcy OK: {tp_itcy.count()} rows")

#tp_itcy.show()

freight_in = (
    qlik_cst_pbcs_freight_in

    .select("company", "network", "product_code", "year", "freight_in_eur", "transport_cost_infra_eur")
    .where(col("company").isNotNull())
    .where(col("network").isNotNull())
    .where(col("product_code").isNotNull())
    .where(col("year").isNotNull())
    .distinct()

    #.join(needed_keys_code_network_product_year, ["company", "network", "product_code", "year"], "full")
    .withColumn("freight_in_eur", last("freight_in_eur", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year"))))   # fill going forward
    .withColumn("freight_in_eur", last("freight_in_eur", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year").desc())))   # fill going backward
    .withColumn("transport_cost_infra_eur", last("transport_cost_infra_eur", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year"))))   # fill going forward
    .withColumn("transport_cost_infra_eur", last("transport_cost_infra_eur", ignorenulls=True).over(Window.partitionBy("company", "network", "product_code").orderBy(col("year").desc())))   # fill going backward

    .withColumn("key_cost_freight_in", concat_ws("###", col("company"), col("network"), col("product_code"), col("year")))

    .drop_duplicates(["key_cost_freight_in"])
    .assert_no_duplicates("key_cost_freight_in")

    .select(
        "key_cost_freight_in",
        col("freight_in_eur").alias("TRANSPORT_COST_UNIT"),   # TODO RENAME!
        col("transport_cost_infra_eur").alias("TRANSPORT_COST_INFRA_UNIT"),   # TODO RENAME!
    )
).localCheckpoint()
print(f"freight_in OK: {freight_in.count()} rows")

#freight_in.show()

needed_keys_product_quarter = (
    ORD
    .withColumn("__yq", concat(year("DOCUMENT_DATE"), lit("Q"), date_format("DOCUMENT_DATE", "q")))
    .select("product_code", "__yq")
    .where(col("product_code").isNotNull())
    .where(col("__yq").isNotNull())
    .distinct()
    .assert_no_duplicates("product_code", "__yq")
)

cost_commerciale = (
    qlik_cst_pbcs_std_costs_commerciale
    .where(col("Currency") == "EUR")

    .withColumn("__yq", concat(col("YEAR"), col("QUARTER")))

    .join(needed_keys_product_quarter, ["product_code", "__yq"], "full")
    .withColumn("std_prod_cost_commercial", last("std_prod_cost_commercial", ignorenulls=True).over(Window.partitionBy("product_code").orderBy(col("__yq"))))   # fill going forward
    .withColumn("std_prod_cost_commercial", last("std_prod_cost_commercial", ignorenulls=True).over(Window.partitionBy("product_code").orderBy(col("__yq").desc())))   # fill going backward

    .withColumn("key_cost_commercial", concat_ws("###", col("product_code"), col("__yq")))

    .assert_no_duplicates("key_cost_commercial")
    .select(
        "key_cost_commercial",
        col("std_prod_cost_commercial").alias("STD_PROD_COST_COMMERCIAL_UNIT")
    )
).localCheckpoint()
print(f"cost_commerciale OK: {cost_commerciale.count()} rows")

#cost_commerciale.show()
needed_keys_product_year = (
    ORD
    .select(
        "product_code",
        year("DOCUMENT_DATE").alias("year")
    )
    .where(col("product_code").isNotNull())
    .where(col("year").isNotNull())
    .distinct()
    .assert_no_duplicates("product_code", "year")
)

stdindef_tca = (
    qlik_cst_pbcs_stdindef_tca_costs
    .where(col("Currency") == "EUR")

    .join(needed_keys_product_year, ["product_code", "year"], "full")

    .withColumn("std_prod_cost_no_tca", last("std_prod_cost_no_tca", ignorenulls=True).over(Window.partitionBy("product_code").orderBy(col("year"))))   # fill going forward
    .withColumn("std_prod_cost_no_tca", last("std_prod_cost_no_tca", ignorenulls=True).over(Window.partitionBy("product_code").orderBy(col("year").desc())))   # fill going backward
    .withColumn("tca", last("tca", ignorenulls=True).over(Window.partitionBy("product_code").orderBy(col("year"))))   # fill going forward
    .withColumn("tca", last("tca", ignorenulls=True).over(Window.partitionBy("product_code").orderBy(col("year").desc())))   # fill going backward

    .withColumn("key_cost_stdindef_tca", concat_ws("###", col("product_code"), col("year")))

    .assert_no_duplicates("key_cost_stdindef_tca")
    .select(
        "key_cost_stdindef_tca",
        col("std_prod_cost_no_tca").alias("CONS_COST_OF_SALES_UNIT"),
        col("tca").alias("TCA_UNIT"),
    )
).localCheckpoint()
print(f"stdindef_tca OK: {stdindef_tca.count()} rows")

#stdindef_tca.show()
ORD_WITH_COST = (
    ORD#.select("company_haier", "network_haier", "product_code", "DOCUMENT_DATE")

    .withColumn("__yq", concat(year("DOCUMENT_DATE"), lit("Q"), date_format("DOCUMENT_DATE", "q")))
    .withColumn("key_cost_commercial", concat_ws("###", col("product_code"), col("__yq")))
    .withColumn("key_cost_freight_in", concat_ws("###", col("company"), col("network"), col("product_code"), year("DOCUMENT_DATE")))
    .withColumn("key_cost_transfer_price", concat_ws("###", col("company"), col("network"), col("product_code"), year("DOCUMENT_DATE")))
    .withColumn("key_cost_stdindef_tca", concat_ws("###", col("product_code"), year("DOCUMENT_DATE")))

    .join(cost_commerciale, ["key_cost_commercial"], "left")
    .join(freight_in, ["key_cost_freight_in"], "left")
    .join(tp_commerciale, ["key_cost_transfer_price"], "left")
    .join(tp_itcy, ["key_cost_transfer_price"], "left")
    .join(stdindef_tca, ["key_cost_stdindef_tca"], "left")

)

ORD_WITH_COST.localCheckpoint()
print(f"ORD_WITH_COST OK: {ORD_WITH_COST.count()} rows")
'''
ORD_CALCOLI:
Load *,
ORDER_NUMBER as "ORDER NUMBER",
"ORD QTY" as "POT QTY",
"ORD NSV €" as "POT NSV €",
"ORD NSV LC" as "POT NSV LC",
"ORD GSV €" as "POT GSV €",
"ORD GSV LC" as "POT GSV LC",

"ORD GM TP €" as "POT GM TP €",
"ORD GM TP LC" as "POT GM TP LC",
"ORD GM CP €" as "POT GM CP €",
"ORD GM CP LC" as "POT GM CP LC",

if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD QTY",0) as "ORD BO QTY",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD NSV LC",0) as "ORD BO NSV LC",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD NSV €",0) as "ORD BO NSV €",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD GSV LC",0) as "ORD BO GSV LC",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD GSV €",0) as "ORD BO GSV €",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD GM TP LC",0) as "ORD BO GM TP LC",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD GM TP €",0) as "ORD BO GM TP €",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD GM CP LC",0) as "ORD BO GM CP LC",
if(ORD_REQUESTED_DELIVERY_DATE<MonthStart(Today()),"ORD GM CP €",0) as "ORD BO GM CP €",


if("ORDER STATUS"='Blocked',"ORD QTY",0) as "BLK QTY",
if("ORDER STATUS"='Blocked',"ORD NSV €",0) as "BLK NSV €",
if("ORDER STATUS"='Blocked',"ORD GSV €",0) as "BLK GSV €",
if("ORDER STATUS"='Blocked',"ORD GM TP €",0) as "BLK GM TP €",
if("ORDER STATUS"='Blocked',"ORD GM CP €",0) as "BLK GM CP €",
if("ORDER STATUS"='Blocked',"ORD NSV LC",0) as "BLK NSV LC",
if("ORDER STATUS"='Blocked',"ORD GSV LC",0) as "BLK GSV LC",
if("ORDER STATUS"='Blocked',"ORD GM TP LC",0) as "BLK GM TP LC",
if("ORDER STATUS"='Blocked',"ORD GM CP LC",0) as "BLK GM CP LC",

if("ORDER STATUS"='In Picking',"ORD QTY",0) as "PCK QTY",
if("ORDER STATUS"='In Picking',"ORD NSV €",0) as "PCK NSV €",
if("ORDER STATUS"='In Picking',"ORD GSV €",0) as "PCK GSV €",
if("ORDER STATUS"='In Picking',"ORD GM TP €",0) as "PCK GM TP €",
if("ORDER STATUS"='In Picking',"ORD GM CP €",0) as "PCK GM CP €",
if("ORDER STATUS"='In Picking',"ORD NSV LC",0) as "PCK NSV LC",
if("ORDER STATUS"='In Picking',"ORD GSV LC",0) as "PCK GSV LC",
if("ORDER STATUS"='In Picking',"ORD GM TP LC",0) as "PCK GM TP LC",
if("ORDER STATUS"='In Picking',"ORD GM CP LC",0) as "PCK GM CP LC",

if("ORDER STATUS"='Not Deliverables',"ORD QTY",0) as "OND QTY",
if("ORDER STATUS"='Not Deliverables',"ORD NSV €",0) as "OND NSV €",
if("ORDER STATUS"='Not Deliverables',"ORD GSV €",0) as "OND GSV €",
if("ORDER STATUS"='Not Deliverables',"ORD GM TP €",0) as "OND GM TP €",
if("ORDER STATUS"='Not Deliverables',"ORD GM CP €",0) as "OND GM CP €",
if("ORDER STATUS"='Not Deliverables',"ORD NSV LC",0) as "OND NSV LC",
if("ORDER STATUS"='Not Deliverables',"ORD GSV LC",0) as "OND GSV LC",
if("ORDER STATUS"='Not Deliverables',"ORD GM TP LC",0) as "OND GM TP LC",
if("ORDER STATUS"='Not Deliverables',"ORD GM CP LC",0) as "OND GM CP LC";
Load *,
if(Alt("ORD NSV €",0)=0,0,"ORD NSV €"-(Alt("ORD TP €",0))) as "ORD GM TP €",
if(Alt("ORD NSV LC",0)=0,0,"ORD NSV LC"-(Alt("ORD TP LC",0))) as "ORD GM TP LC",
if(Alt("ORD NSV €",0)=0,0,"ORD NSV €"-(Alt("ORD CP €",0))) as "ORD GM CP €",
if(Alt("ORD NSV LC",0)=0,0,"ORD NSV LC"-(Alt("ORD CP LC",0))) as "ORD GM CP LC",
BOOKING_SLOT as APPOINTMENT;
Load *,
Applymap('TVV3T',CUSTOMER_TYPE_CODE_LEV3,'Not Defined')&'- '&Applymap('TVV5T',CUSTOMER_TYPE_CODE,'Not Defined') as CUSTOMER_TYPE_DESCR,
Applymap('TVV3T',DEST_CUSTOMER_TYPE_CODE_LEV3,'Not Defined')&'- '&Applymap('TVV5T',DEST_CUSTOMER_TYPE_CODE,'Not Defined') as DEST_CUSTOMER_TYPE_DESCR,
Alt(FORCED_PRICE,0)+Alt(LIST_PRICE,0) as NET_PRICE,
"ORD NSV LC"/"EXCHANGE" as "ORD NSV €",
"ORD GSV LC"/"EXCHANGE" as "ORD GSV €",
"ORD TP LC"/"EXCHANGE" as "ORD TP €",
"ORD CP LC"/"EXCHANGE" as "ORD CP €";
Load *,
Applymap('KVGR5',INV_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as CUSTOMER_TYPE_CODE ,
Applymap('KVGR5',DEST_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as DEST_CUSTOMER_TYPE_CODE,
Applymap('KVGR3',INV_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as CUSTOMER_TYPE_CODE_LEV3 ,
Applymap('KVGR3',DEST_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as DEST_CUSTOMER_TYPE_CODE_LEV3,

Applymap('TVGRT',Applymap('VKGRP',DEST_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined'),'Not Defined') as "SALES GROUP",


("ORD NSV LC TOT"+Alt("ORD IC DISCOUNT/SURCHARGE"*Alt(EXCHANGE_RATE_SAP,1)*MOLTIPLICATORE_RESO,0))*"% ORD QTY"*"MOLTIPLICATORE_RESO" as "ORD NSV LC",
("ORD GSV LC TOT"+Alt("ORD IC DISCOUNT/SURCHARGE"*Alt(EXCHANGE_RATE_SAP,1)*MOLTIPLICATORE_RESO,0))*"% ORD QTY"*"MOLTIPLICATORE_RESO" as "ORD GSV LC",
LIST_PRICE_TOT*"% ORD QTY"/"ORD QTY" as LIST_PRICE,
FORCED_PRICE_TOT*"% ORD QTY"/"ORD QTY" as FORCED_PRICE,
TRANSPORT_COST_INFRA_UNIT as TRANSPORT_COST_INFRA,
TRANSPORT_COST_UNIT as TRANSPORT_COST,
"REMOVABLE_CHARGES_TOT"*"% ORD QTY" as "REMOVABLE_CHARGES",
(CONS_COST_OF_SALES_UNIT+TCA_UNIT+TRANSPORT_COST_UNIT+TRANSPORT_COST_INFRA_UNIT)*"ORD QTY" as "ORD CP LC",
(COMM_COST_OF_SALES_UNIT+TRANSPORT_COST_UNIT)*"ORD QTY" as "ORD TP LC",
CONS_COST_OF_SALES_UNIT as CONS_COST_OF_SALES,
COMM_COST_OF_SALES_UNIT as COMM_COST_OF_SALES,
TCA_UNIT as TCA,
"EXCHANGE" as CURRENCY_EURO_EXCH,
Applymap('KNA1',PURCHASE_GROUP_CODE) as PURCHASE_GROUP_DESCR,
Applymap('KNA1',MAIN_GROUP_CODE) as MAIN_GROUP_DESCR,
Applymap('KNA1',SUPER_GROUP_CODE) as SUPER_GROUP_DESCR,
Applymap('KNA1',SALE_ZONE_CODE) as SALE_ZONE_DESCR,
Applymap('KNA1',SALESMAN_CODE) as SALESMAN_NAME,
Applymap('KNA1',SALE_DIRECTOR_CODE) as  SALE_DIRECTOR,
Applymap('KNA1',RSM_CODE) as  RSM,
Coalesce(BOOKING_SLOT_LIKP,BOOKING_SLOT_VBAP) as BOOKING_SLOT,
1 as "FLAG SAP"
Resident ORD;

'''


current_month_start = date_trunc('month', lit(current_date()))



KVGR5_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KVGR5_MAPPING"
    )
)

KVGR3_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KVGR3_MAPPING"
    )
)

TVGRT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVGRT_MAPPING"
    )
)

KNA1_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KNA1_MAPPING"
    )
)

TVV3T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVV3T_MAPPING"
    )
)

TVV5T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVV5T_MAPPING"
    )
)

KNA1_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KNA1_MAPPING"
    )
)


ORD_CALCOLI = (
    ORD_WITH_COST

    #Applymap('KVGR5',INV_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as CUSTOMER_TYPE_CODE ,
    #Applymap('KVGR5',DEST_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as DEST_CUSTOMER_TYPE_CODE,
    #Applymap('KVGR3',INV_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as CUSTOMER_TYPE_CODE_LEV3 ,
    #Applymap('KVGR3',DEST_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined') as DEST_CUSTOMER_TYPE_CODE_LEV3,
    #Applymap('TVGRT',Applymap('VKGRP',DEST_ID&'|'&SALES_ORG&'|'&DISTR_CHANNEL&'|'& DIVISION,'Not Defined'),'Not Defined') as "SALES GROUP",
    .withColumn("INV_CUSTOMER_KEY", concat_ws("|", col("INV_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .withColumn("DEST_CUSTOMER_KEY", concat_ws("|", col("DEST_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .withColumn("SALES_GROUP_KEY", concat_ws("|", col("DEST_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .do_mapping(KVGR5_MAPPING, col("INV_CUSTOMER_KEY"), "CUSTOMER_TYPE_CODE", mapping_name="KVGR5_MAPPING")
    .do_mapping(KVGR5_MAPPING, col("DEST_CUSTOMER_KEY"), "DEST_CUSTOMER_TYPE_CODE", mapping_name="KVGR5_MAPPING")
    .do_mapping(KVGR3_MAPPING, col("INV_CUSTOMER_KEY"), "CUSTOMER_TYPE_CODE_LEV3", mapping_name="KVGR3_MAPPING")
    .do_mapping(KVGR3_MAPPING, col("DEST_CUSTOMER_KEY"), "DEST_CUSTOMER_TYPE_CODE_LEV3", mapping_name="KVGR3_MAPPING")
    .do_mapping(TVGRT_MAPPING, col("SALES_GROUP_KEY"), "SALES GROUP", mapping_name="TVGRT_MAPPING")

    .withColumn("ORD NSV LC", (col("ORD NSV LC TOT") + coalesce(col("ORD IC DISCOUNT/SURCHARGE") * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)) * col("MOLTIPLICATORE_RESO"), lit(0))) * col("% ORD QTY") * col("MOLTIPLICATORE_RESO"))
    .withColumn("ORD GSV LC", (col("ORD GSV LC TOT") + coalesce(col("ORD IC DISCOUNT/SURCHARGE") * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)) * col("MOLTIPLICATORE_RESO"), lit(0))) * col("% ORD QTY") * col("MOLTIPLICATORE_RESO"))
    .withColumn("LIST_PRICE", col("LIST_PRICE_TOT") * col("% ORD QTY") / col("ORD QTY"))
    .withColumn("FORCED_PRICE", col("FORCED_PRICE_TOT") * col("% ORD QTY") / col("ORD QTY"))
    .withColumn("TRANSPORT_COST_INFRA", col("TRANSPORT_COST_INFRA_UNIT")*col("EXCHANGE_TO_LOCAL"))
    .withColumn("TRANSPORT_COST", col("TRANSPORT_COST_UNIT")*col("EXCHANGE_TO_LOCAL"))
    .withColumn("REMOVABLE_CHARGES", col("removable charges tot") * col("% ORD QTY"))
    .withColumn("ORD CP LC", (col("CONS_COST_OF_SALES_UNIT") + col("TCA_UNIT") + col("TRANSPORT_COST_UNIT") + col("TRANSPORT_COST_INFRA_UNIT"))*col("EXCHANGE_TO_LOCAL") * col("ORD QTY"))
    .withColumn("ORD TP LC", (col("COMM_COST_OF_SALES_UNIT") + col("TRANSPORT_COST_UNIT"))*col("EXCHANGE_TO_LOCAL") * col("ORD QTY"))
    .withColumn("CONS_COST_OF_SALES", col("CONS_COST_OF_SALES_UNIT")*col("EXCHANGE_TO_LOCAL"))
    .withColumn("COMM_COST_OF_SALES", col("COMM_COST_OF_SALES_UNIT")*col("EXCHANGE_TO_LOCAL"))
    .withColumn("TCA", col("TCA_UNIT")*col("EXCHANGE_TO_LOCAL"))
    .withColumn("CURRENCY_EURO_EXCH", col("EXCHANGE"))

    #Applymap('KNA1',PURCHASE_GROUP_CODE) as PURCHASE_GROUP_DESCR,
    #Applymap('KNA1',MAIN_GROUP_CODE) as MAIN_GROUP_DESCR,
    #Applymap('KNA1',SUPER_GROUP_CODE) as SUPER_GROUP_DESCR,
    #Applymap('KNA1',SALE_ZONE_CODE) as SALE_ZONE_DESCR,
    #Applymap('KNA1',SALESMAN_CODE) as SALESMAN_NAME,
    #Applymap('KNA1',SALE_DIRECTOR_CODE) as  SALE_DIRECTOR,
    #Applymap('KNA1',RSM_CODE) as  RSM,
    .do_mapping(KNA1_MAPPING, col("PURCHASE_GROUP_CODE"), "PURCHASE_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("MAIN_GROUP_CODE"), "MAIN_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SUPER_GROUP_CODE"), "SUPER_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALE_ZONE_CODE"), "SALE_ZONE_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALESMAN_CODE"), "SALESMAN_NAME", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALE_DIRECTOR_CODE"), "SALE_DIRECTOR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("RSM_CODE"), "RSM", mapping_name="KNA1")

    .withColumn("BOOKING_SLOT", coalesce(col("BOOKING_SLOT_LIKP"),col("BOOKING_SLOT_VBAP")))
    .withColumn("FLAG SAP", lit(1))


    #Applymap('TVV3T',CUSTOMER_TYPE_CODE_LEV3,'Not Defined')&'- '&Applymap('TVV5T',CUSTOMER_TYPE_CODE,'Not Defined') as CUSTOMER_TYPE_DESCR,
    .do_mapping(TVV3T_MAPPING, col("CUSTOMER_TYPE_CODE_LEV3"), "_TVV3T_CUSTOMER_TYPE_CODE", default_value=lit("Not Defined"), mapping_name="TVV3T_MAPPING (temp)")
    .do_mapping(TVV5T_MAPPING, col("CUSTOMER_TYPE_CODE"), "_TVV5T_CUSTOMER_TYPE_CODE", default_value=lit("Not Defined"), mapping_name="TVV5T_MAPPING (temp)")
    .withColumn("CUSTOMER_TYPE_DESCR", concat(col("_TVV3T_CUSTOMER_TYPE_CODE"), lit("-"), col("_TVV5T_CUSTOMER_TYPE_CODE")))
    .drop("_TVV3T_CUSTOMER_TYPE_CODE", "_TVV5T_CUSTOMER_TYPE_CODE")

    #Applymap('TVV3T',DEST_CUSTOMER_TYPE_CODE_LEV3,'Not Defined')&'- '&Applymap('TVV5T',DEST_CUSTOMER_TYPE_CODE,'Not Defined') as DEST_CUSTOMER_TYPE_DESCR,
    .do_mapping(TVV3T_MAPPING, col("DEST_CUSTOMER_TYPE_CODE_LEV3"), "_TVV3T_DEST_CUSTOMER_TYPE_CODE", default_value=lit("Not Defined"), mapping_name="TVV3T_MAPPING (temp)")
    .do_mapping(TVV5T_MAPPING, col("DEST_CUSTOMER_TYPE_CODE"), "_TVV5T_DEST_CUSTOMER_TYPE_CODE", default_value=lit("Not Defined"), mapping_name="TVV5T_MAPPING (temp)")
    .withColumn("DEST_CUSTOMER_TYPE_DESCR", concat(col("_TVV3T_DEST_CUSTOMER_TYPE_CODE"), lit("-"), col("_TVV5T_DEST_CUSTOMER_TYPE_CODE")))
    .drop("_TVV3T_DEST_CUSTOMER_TYPE_CODE", "_TVV5T_DEST_CUSTOMER_TYPE_CODE")

    .withColumn("NET_PRICE", coalesce(col("FORCED_PRICE"), lit(0)) + coalesce(col("LIST_PRICE"), lit(0)))
    .withColumn("ORD NSV €", col("ORD NSV LC") * col("EXCHANGE"))
    .withColumn("ORD GSV €", col("ORD GSV LC") * col("EXCHANGE"))
    .withColumn("ORD TP €", col("ORD TP LC") * col("EXCHANGE"))
    .withColumn("ORD CP €", col("ORD CP LC") * col("EXCHANGE"))


    .withColumn("ORD GM TP €", when(coalesce(col("ORD NSV €"), lit(0)) != 0, col("ORD NSV €") - coalesce(col("ORD TP €"), lit(0))).otherwise(lit(0)))
    .withColumn("ORD GM TP LC", when(coalesce(col("ORD NSV LC"), lit(0)) != 0, col("ORD NSV LC") - coalesce(col("ORD TP LC"), lit(0))).otherwise(lit(0)))
    .withColumn("ORD GM CP €", when(coalesce(col("ORD NSV €"), lit(0)) != 0, col("ORD NSV €") - coalesce(col("ORD CP €"), lit(0))).otherwise(lit(0)))
    .withColumn("ORD GM CP LC", when(coalesce(col("ORD NSV LC"), lit(0)) != 0, col("ORD NSV LC") - coalesce(col("ORD CP LC"), lit(0))).otherwise(lit(0)))
    .withColumn("APPOINTMENT", col("BOOKING_SLOT"))

    .withColumn("POT QTY", col("ORD QTY"))
    .withColumn("POT NSV €", col("ORD NSV €"))
    .withColumn("POT NSV LC", col("ORD NSV LC"))
    .withColumn("POT GSV €", col("ORD GSV €"))
    .withColumn("POT GSV LC", col("ORD GSV LC"))
    .withColumn("POT GM TP €", col("ORD GM TP €"))
    .withColumn("POT GM TP LC", col("ORD GM TP LC"))
    .withColumn("POT GM CP €", col("ORD GM CP €"))
    .withColumn("POT GM CP LC", col("ORD GM CP LC"))
    .withColumn("ORD BO QTY", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD QTY")).otherwise(lit(0)))
    .withColumn("ORD BO NSV LC", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD NSV LC")).otherwise(lit(0)))
    .withColumn("ORD BO NSV €", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD NSV €")).otherwise(lit(0)))
    .withColumn("ORD BO GSV LC", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD GSV LC")).otherwise(lit(0)))
    .withColumn("ORD BO GSV €", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD GSV €")).otherwise(lit(0)))
    .withColumn("ORD BO GM TP LC", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD GM TP LC")).otherwise(lit(0)))
    .withColumn("ORD BO GM TP €", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD GM TP €")).otherwise(lit(0)))
    .withColumn("ORD BO GM CP LC", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD GM CP LC")).otherwise(lit(0)))
    .withColumn("ORD BO GM CP €", when(col("ORD_REQUESTED_DELIVERY_DATE") < current_month_start, col("ORD GM CP €")).otherwise(lit(0)))
    .withColumn("BLK QTY", when(col("ORDER STATUS") == "Blocked", col("ORD QTY")).otherwise(lit(0)))
    .withColumn("BLK NSV €", when(col("ORDER STATUS") == "Blocked", col("ORD NSV €")).otherwise(lit(0)))
    .withColumn("BLK GSV €", when(col("ORDER STATUS") == "Blocked", col("ORD GSV €")).otherwise(lit(0)))
    .withColumn("BLK GM TP €", when(col("ORDER STATUS") == "Blocked", col("ORD GM TP €")).otherwise(lit(0)))
    .withColumn("BLK GM CP €", when(col("ORDER STATUS") == "Blocked", col("ORD GM CP €")).otherwise(lit(0)))
    .withColumn("BLK NSV LC", when(col("ORDER STATUS") == "Blocked", col("ORD NSV LC")).otherwise(lit(0)))
    .withColumn("BLK GSV LC", when(col("ORDER STATUS") == "Blocked", col("ORD GSV LC")).otherwise(lit(0)))
    .withColumn("BLK GM TP LC", when(col("ORDER STATUS") == "Blocked", col("ORD GM TP LC")).otherwise(lit(0)))
    .withColumn("BLK GM CP LC", when(col("ORDER STATUS") == "Blocked", col("ORD GM CP LC")).otherwise(lit(0)))
    .withColumn("PCK QTY", when(col("ORDER STATUS") == "In Picking", col("ORD QTY")).otherwise(lit(0)))
    .withColumn("PCK NSV €", when(col("ORDER STATUS") == "In Picking", col("ORD NSV €")).otherwise(lit(0)))
    .withColumn("PCK GSV €", when(col("ORDER STATUS") == "In Picking", col("ORD GSV €")).otherwise(lit(0)))
    .withColumn("PCK GM TP €", when(col("ORDER STATUS") == "In Picking", col("ORD GM TP €")).otherwise(lit(0)))
    .withColumn("PCK GM CP €", when(col("ORDER STATUS") == "In Picking", col("ORD GM CP €")).otherwise(lit(0)))
    .withColumn("PCK NSV LC", when(col("ORDER STATUS") == "In Picking", col("ORD NSV LC")).otherwise(lit(0)))
    .withColumn("PCK GSV LC", when(col("ORDER STATUS") == "In Picking", col("ORD GSV LC")).otherwise(lit(0)))
    .withColumn("PCK GM TP LC", when(col("ORDER STATUS") == "In Picking", col("ORD GM TP LC")).otherwise(lit(0)))
    .withColumn("PCK GM CP LC", when(col("ORDER STATUS") == "In Picking", col("ORD GM CP LC")).otherwise(lit(0)))
    .withColumn("OND QTY", when(col("ORDER STATUS") == "Not Deliverables", col("ORD QTY")).otherwise(lit(0)))
    .withColumn("OND NSV €", when(col("ORDER STATUS") == "Not Deliverables", col("ORD NSV €")).otherwise(lit(0)))
    .withColumn("OND GSV €", when(col("ORDER STATUS") == "Not Deliverables", col("ORD GSV €")).otherwise(lit(0)))
    .withColumn("OND GM TP €", when(col("ORDER STATUS") == "Not Deliverables", col("ORD GM TP €")).otherwise(lit(0)))
    .withColumn("OND GM CP €", when(col("ORDER STATUS") == "Not Deliverables", col("ORD GM CP €")).otherwise(lit(0)))
    .withColumn("OND NSV LC", when(col("ORDER STATUS") == "Not Deliverables", col("ORD NSV LC")).otherwise(lit(0)))
    .withColumn("OND GSV LC", when(col("ORDER STATUS") == "Not Deliverables", col("ORD GSV LC")).otherwise(lit(0)))
    .withColumn("OND GM TP LC", when(col("ORDER STATUS") == "Not Deliverables", col("ORD GM TP LC")).otherwise(lit(0)))
    .withColumn("OND GM CP LC", when(col("ORDER STATUS") == "Not Deliverables", col("ORD GM CP LC")).otherwise(lit(0)))

    #.withColumn("COMPANY_CODE", col("COMPANY"))
    #.withColumn("NETWORK_CODE", col("NETWORK"))
    .withColumn("SHIPPING POINT", col("SHIPPING_POINT"))

    .withColumnRenamed("COMM_COST_OF_SALES", "TP_COMMERCIALE")\
    .withColumnRenamed("CONS_COST_OF_SALES", "STD_PROD_COST_NO_TCA")\
    .withColumnRenamed("TRANSPORT_COST", "FREIGHT_IN")
    #.withColumnRenamed("RESPONSIBLE_CODE", "Cluster Code")\
    #.withColumnRenamed("RESPONSIBLE_CODE1", "Region Code")\
    #.withColumnRenamed("RESPONSIBLE_CODE3", "Subcluster Code")

    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    .distinct()

    .withColumn("key_fact", concat_ws("###", lit("ORDER"), col("order_number"), col("order_line")))
    .assert_no_duplicates("key_fact")
)

ORD_CALCOLI = ORD_CALCOLI.columns_to_lower().localCheckpoint()
print(f"ORD_CALCOLI OK: {ORD_CALCOLI.count()} rows")


##### Save Golden Sources
list_sap_cols_ord = ["area_code", "category", "company", "confirmed date", "confirmed time", "currency_code", "data_type", "dest_id", "distr_channel", "division", "doc_type_code", "document_date", "exchange", "exchange_rate_sap", "first_req_del_date", "inv_id", "key_cost_commercial", "key_cost_freight_in", "key_cost_stdindef_tca", "key_cost_transfer_price", "key_fact", "knumv", "main_group_code", "moltiplicatore_reso", "network", "ord gsv €", "ord gsv lc", "ord ic discount/surcharge", "ord nsv €", "ord nsv lc", "ord qty", "ord qty consignment", "ord qty fabs", "ord qty orig", "ord raee lc", "ord_requested_delivery_date", "order status", "order_blocking_code", "order_blocking_date", "order_blocking_user", "order_creation_user", "order_customer_date", "order_customer_reference_1", "order_date", "order_delivery_warehouse", "order_line", "order_number", "order_requested_delivery_date", "order_requested_delivery_month_year", "order_source", "order_type_code", "order_unblocking_date", "order_unblocking_user", "plant", "product_code", "pstyv", "purchase_group_code", "removable charges tot", "rsm_code", "sale_director_code", "sale_zone_code", "sales_office", "sales_organization", "salesman_code", "sap_order_block_credit", "sap_order_block_delivery", "super_group_code", "time_id"]
ord_gs = (
    ORD_CALCOLI
    .select(list_sap_cols_ord)
    .save_data_to_redshift_table("dwa.sap_sls_ft_order")
)

##### Save remaining cols
# ORD_CALCOLI.columns \ list_sap_cols_ord U {key_fact}
list_dims_cols_ord = list(set(ORD_CALCOLI.columns).difference(list_sap_cols_ord))+['key_fact']

ord_dim = (
    ORD_CALCOLI
    .select(list_dims_cols_ord)
    .save_data_to_redshift_table("dwa.sap_sls_dim_order")
)

