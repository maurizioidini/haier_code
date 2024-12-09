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
QlikReportingUtils.IAM_ROLE = job_role
QlikReportingUtils.GLUE_CONTEXT = glueContext
QlikReportingUtils.DEBUG_MODE = False

s3_source_path = f"s3://{bucket_name}/{source_flow}"
s3_target_path = f"s3://{bucket_name}/{target_flow}/"

s3_source_path += "/{table_name}"
s3_target_path += "/{table_name}"


################ set attr to dataframe class
setattr(DataFrame, "do_mapping", do_mapping)
setattr(DataFrame, "drop_duplicates_ordered", drop_duplicates_ordered)
setattr(DataFrame, "assert_no_duplicates", assert_no_duplicates)
setattr(DataFrame, "save_to_s3_parquet", save_to_s3_parquet)
setattr(DataFrame, "columns_to_lower", columns_to_lower)
setattr(DataFrame, "cast_decimal_to_double", cast_decimal_to_double)



spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


################ START MAPPING BRONZE 2 MAPPING ####################

###### TVAKT
TVAKT_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVAKT_INPUT"
    )
)
TVAKT_MAPPING = (
    TVAKT_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("AUART").alias("ORDER_TYPE_CODE"),
        col("BEZEI").alias("ORDER_TYPE_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVAKT_MAPPING")
).assert_no_duplicates("ORDER_TYPE_CODE")

###### T176T
T176T_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "T176T_INPUT"
    )
)
T176T_MAPPING = (
    T176T_INPUT
    .where(col("SPRAS") == "EN")
    .select("BSARK", "VTEXT")
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="T176T_MAPPING")
).assert_no_duplicates("BSARK")

###### T052U
T052U_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "T052U_INPUT"
    )
)
T052U_MAPPING = (
    T052U_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("ZTERM").alias("PAYMENT_EXPIRING_CODE"),
        col("TEXT1").alias("PAYMENT_EXPIRING_DESCR")
    )
    .distinct()
    .drop_duplicates_ordered(["PAYMENT_EXPIRING_CODE"], ["PAYMENT_EXPIRING_DESCR"])
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="T052U_MAPPING")
).assert_no_duplicates("PAYMENT_EXPIRING_CODE")

###### TVV3T
TVV3T_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVV3T_INPUT"
    )
)
TVV3T_MAPPING = (
    TVV3T_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("KVGR3"),
        col("BEZEI").alias("CUSTOMER_TYPE_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVV3T_MAPPING")
).assert_no_duplicates("KVGR3")

###### TVLKT
TVLKT_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVLKT_INPUT"
    )
)
TVLKT_MAPPING = (
    TVLKT_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("LFART").alias("SHIPMENT_CODE"),
        col("VTEXT").alias("SHIPMENT_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVLKT_MAPPING")
).assert_no_duplicates("SHIPMENT_CODE")

###### DD07T
DD07T_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "DD07T_INPUT"
    )
)
DD07T_MAPPING = (
    DD07T_INPUT
    .where((col("DOMNAME") == 'VBTYPL') & (col("DDLANGUAGE") == 'EN'))
    .select(
        col("DOMVALUE_L").alias("DOC_TYPE"),
        col("DDTEXT").alias("DOC_TYPE_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="DD07T_MAPPING")
).assert_no_duplicates("DOC_TYPE")

###### TVROT
TVROT_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVROT_INPUT"
    )
)
TVROT_MAPPING = (
    TVROT_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("ROUTE").alias("ROUTING"),
        col("BEZEI").alias("ROUTING_DESCR")
    )

).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVROT_MAPPING")
).assert_no_duplicates("ROUTING")

###### TVLST
TVLST_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVLST_INPUT"
    )
)
TVLST_MAPPING = (
    TVLST_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("LIFSP").alias("SAP_ORDER_BLOCK_DELIVERY"),
        col("VTEXT").alias("SAP_ORDER_BLOCK_DELIVERY_TXT")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVLST_MAPPING")
).assert_no_duplicates("SAP_ORDER_BLOCK_DELIVERY")


###### ZWW_OTC_LOGAREA
ZWW_OTC_LOGAREA_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "ZWW_OTC_LOGAREA_INPUT"
    )
)
ZWW_OTC_LOGAREA_MAPPING = (
    ZWW_OTC_LOGAREA_INPUT
    .select(
        col("ZZLAREA").alias("AREA_CODE"),
        col("ZZLAREAT").alias("AREA_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="ZWW_OTC_LOGAREA_MAPPING")
).assert_no_duplicates("AREA_CODE")

###### Currency_Map
Currency_INPUT = read_data_from_s3_parquet(   # TODO
    path = s3_source_path.format(
        table_name  = "Currency_INPUT"
    )
)

Currency_Map = (
    read_data_from_redshift_table("dwa.pbcs_cst_ft_budget_fx")
    
    .select(
        concat_ws('|', col("currency"), col("year")).alias("key_Currency"),
        col("beg_balance").alias("EXCHANGE_VALUE")
    )
    
    .withColumn("EXCHANGE_VALUE", 1 / col("EXCHANGE_VALUE"))

    .assert_no_duplicates("key_Currency")
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="Currency_Map")
)

###### COMM_COMPANIES
COMM_COMPANIES_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "COMM_COMPANIES_INPUT"
    )
)
Company_Currency_MAPPING = (
    COMM_COMPANIES_INPUT
    .select(
        col("COMPANY_ID"),
        col("CURRENCY")
    )
    
    .withColumn("CURRENCY",
        when(col("COMPANY_ID") == "6790", lit("EUR"))
        .otherwise(col("CURRENCY"))        
    )
    
    .withColumn("CURRENCY",
        when(col("CURRENCY") == "20", lit("EUR"))
        .otherwise(col("CURRENCY"))        
    )

    .distinct()
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="Company_Currency_MAPPING")
).assert_no_duplicates("COMPANY_ID")

###### LFA1
LFA1_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "LFA1_INPUT"
    )
)
LFA1_MAPPING = (
    LFA1_INPUT
    .select(
        col("LIFNR").alias("PICKING_CARRIER"),
        col("NAME1").alias("PICKING_CARRIER_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="LFA1_MAPPING")
).assert_no_duplicates("PICKING_CARRIER")

###### TVAK
TVAK_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVAK_INPUT"
    )
)
TVAK_MAPPING = (
    TVAK_INPUT
    .where(col("VBTYP").isin(['C', 'K', 'L', 'H']))
    .select(
        col("AUART").alias("ORDER_TYPE_CODE"),
        col("VBTYP").alias("FLAG_ORDER_TYPE_CATEGORY")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVAK_MAPPING")
).assert_no_duplicates("ORDER_TYPE_CODE")

###### LINE
LINE_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "LINE_INPUT"
    )
)
LINE_MAPPING = (
    LINE_INPUT
    .select(
        col("PRODUCT_CODE"),
        col("LINE_CODE")
    )
    .drop_duplicates_ordered(["PRODUCT_CODE"], ["LINE_CODE"])
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="LINE_MAPPING")
).assert_no_duplicates("PRODUCT_CODE")

###### KNA1
KNA1_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "KNA1_INPUT"
    )
)
KNA1_MAPPING = (
    KNA1_INPUT
    .select(
        col("KUNNR"),
        col("NAME1")
    )
    .distinct()
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="KNA1_MAPPING")
).assert_no_duplicates("KUNNR")

###### TVV5T
TVV5T_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVV5T_INPUT"
    )
)
TVV5T_MAPPING = (
    TVV5T_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("KVGR5"),
        col("BEZEI").alias("CUSTOMER_TYPE_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVV5T_MAPPING")
).assert_no_duplicates("KVGR5")

###### KVGR3 (FROM KNVV)
KNVV_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "KNVV_INPUT"
    )
)
KVGR3_MAPPING = (
    KNVV_INPUT
    .select(
        concat_ws('|', col("KUNNR"), col("VKORG"), col("VTWEG"), col("SPART")).alias("key_KNVV"),
        col("KVGR3")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="KVGR3_MAPPING")
).assert_no_duplicates("key_KNVV")

###### KNVV
KNVV_MAPPING = (
    KNVV_INPUT
    .select(
        concat_ws('|', col("KUNNR"), col("VKORG"), col("VTWEG"), col("SPART")).alias("key_KNVV"),
        col("VKBUR").alias("SALES_OFFICE_ANAGRAFICO")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="KNVV_MAPPING")
).assert_no_duplicates("key_KNVV")

###### KVGR5 (FROM KNVV)
KVGR5_MAPPING = (
    KNVV_INPUT
    .select(
        concat_ws('|', col("KUNNR"), col("VKORG"), col("VTWEG"), col("SPART")).alias("key_KNVV"),
        col("KVGR5")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="KVGR5_MAPPING")
).assert_no_duplicates("key_KNVV")

###### VKGRP (FROM KNVV)
VKGRP_MAPPING = (
    KNVV_INPUT
    .select(
        concat_ws('|', col("KUNNR"), col("VKORG"), col("VTWEG"), col("SPART")).alias("key_KNVV"),
        col("VKGRP")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="VKGRP_MAPPING")
).assert_no_duplicates("key_KNVV")

###### TVGRT
TVGRT_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVGRT_INPUT"
    )
)
TVGRT_MAPPING = (
    TVGRT_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("VKGRP"),
        col("BEZEI").alias("CUSTOMER_GROUP_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVGRT_MAPPING")
).assert_no_duplicates("VKGRP")

###### VSART_DESCR (FROM T173T)
T173T_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "T173T_INPUT"
    )
)
VSART_DESCR_MAPPING = (
    T173T_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("VSART"),
        col("BEZEI")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="VSART_DESCR_MAPPING")
).assert_no_duplicates("VSART")

###### CAS_CAUSES_RETURN_REASON (FROM MAPPING_RETURN_REASONS_INPUT)
MAPPING_RETURN_REASONS_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "MAPPING_RETURN_REASONS_INPUT"
    )
)
CAS_CAUSES_RETURN_REASON_MAPPING = (
    MAPPING_RETURN_REASONS_INPUT
    .select(
        col("SAP Order Reason"),
        concat_ws('|', col("Return Reason"), col("Causes"))
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="CAS_CAUSES_RETURN_REASON_MAPPING")
).assert_no_duplicates("SAP Order Reason")

###### CAS_RETURN_REASON_DESCR (FROM MAPPING_RETURN_REASONS_INPUT)
CAS_RETURN_REASON_DESCR_MAPPING = (
    MAPPING_RETURN_REASONS_INPUT
    .select(
        col("SAP Order Reason"),
        col("Return Reason Description")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="CAS_RETURN_REASON_DESCR_MAPPING")
).assert_no_duplicates("SAP Order Reason")

###### TASSI_MODIFICATI (from TASSI MODIFICATI_INPUT and VBRK_INPUT)
TASSI_MODIFICATI_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TASSI_MODIFICATI_INPUT"
    )
)
VBRK_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "VBRK_INPUT"
    )
)
TASSI_MODIFICATI = (
    TASSI_MODIFICATI_INPUT
    .select(
        concat_ws('|', col("Company_Code"), col("Billing_Document")).alias("DOCUMENT_CODE"),
        when(
            col("Exchange_Rate_Accounting_Data").startswith('/'),
            1 / regexp_replace(regexp_replace(col("Exchange_Rate_Accounting_Data"), '/', ''), ',', '.').cast("float")
        ).otherwise(
            regexp_replace(col("Exchange_Rate_Accounting_Data"), ',', '.').cast("float")
        ).alias("Exchange_Rate_Accounting_Data")
    )
    .join(
        VBRK_INPUT.select(
            concat_ws("|", col("VKORG"), col("VBELN")).alias("DOCUMENT_CODE"),
            col("KNUMV")
        ), ["DOCUMENT_CODE"], "inner"
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TASSI_MODIFICATI")
).assert_no_duplicates("DOCUMENT_CODE")

###### TASSI_MODIFICATI_MAP (from TASSI MODIFICATI)
'''
TASSI_MODIFICATI_MAP = (
    TASSI_MODIFICATI
    .select("KNUMV", "Exchange_Rate_Accounting_Data")
    #.dropDuplicates(["KNUMV"])
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TASSI_MODIFICATI_MAP")
).assert_no_duplicates("KNUMV")'''

###### Mapping_KURRF (from VBRK_INPUT)

Mapping_KURRF = (
    VBRK_INPUT
    #.where(col("FKSTO") != 'X')
    .select("KNUMV", "KURRF")
    .dropDuplicates(["KNUMV"])
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="Mapping_KURRF")
).assert_no_duplicates("KNUMV")

###### TVV5T
TVV5T_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "TVV5T_INPUT"
    )
)
TVV5T_MAPPING = (
    TVV5T_INPUT
    .where(col("SPRAS") == "EN")
    .select(
        col("KVGR5"),
        col("BEZEI").alias("CUSTOMER_TYPE_DESCR")
    )
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="TVV5T_MAPPING")
).assert_no_duplicates("KVGR5")

################ START TRANSFORMATIONS BRONZE 2 MAPPING ####################


###### Apply mappings to PRCD_ELEMENTS
PRCD_ELEMENTS_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "PRCD_ELEMENTS_INPUT"
    )
)
PRCD_ELEMENTS = (
    PRCD_ELEMENTS_INPUT
    .where(col("KSCHL").isin([
        'MWST', 'ZNSV', 'ZIV1',
        'ZIV2', 'ZTTV', 'RENT',
        'PPNT', 'ZFR2', 'ZFR5',
        'ZFR%', 'ZWEE', 'ZNET',
        'PB00', 'ZGSV', 'ZFEE', 'ZCDS']))

    # Applymap('TASSI_MODIFICATI_MAP',KNUMV, Applymap('Mapping_KURRF',KNUMV,KKURS)) as KKURS,
    # todo check: with coalesce it is not exactly equivalent, but should work the same and be simpler
    # equivalent = use default_value in mappings, reverse order
    # .do_mapping(TASSI_MODIFICATI_MAP, col("KNUMV"), "_TASSI_MODIFICATI_MAP_KKURS", mapping_name="TASSI_MODIFICATI_MAP (temp)")
    .do_mapping(Mapping_KURRF, col("KNUMV"), "_Mapping_KURRF_KKURS", mapping_name="Mapping_KURRF (temp)")
    #.withColumn("KKURS", coalesce(col("_TASSI_MODIFICATI_MAP_KKURS"), col("_Mapping_KURRF_KKURS"), col("KKURS")))
    .withColumn("KKURS", coalesce(col("_Mapping_KURRF_KKURS"), col("KKURS")))
    .withColumn("key", concat_ws('|', col("KNUMV"), col("KPOSN"), col("STUNR"), col("KSCHL")))
)

# Create the right join with max ZAEHK
PRCD_ELEMENTS_ZAEHK = (
    PRCD_ELEMENTS
    .groupBy("key")
    .agg(max("ZAEHK").alias("ZAEHK"))
)

# Perform the right join
PRCD_ELEMENTS = PRCD_ELEMENTS.join(PRCD_ELEMENTS_ZAEHK, ["key", "ZAEHK"], how="right").save_to_s3_parquet(
    path=s3_target_path.format(table_name="PRCD_ELEMENTS")
)

###### CUTOFF DATE
CUTOFF_DATE_INPUT = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "CUTOFF_DATE_INPUT"
    )
)
_CUTOFF_DATE = (
    CUTOFF_DATE_INPUT
    #.withColumn("CUTOFF_DATE", to_date("CUTOFF_DATE", "dd/MM/yyyy"))

    # get CUTOFF_DATE of the "previous" row
    .withColumn("START_DATE", lag("CUTOFF_DATE").over(Window.orderBy("CUTOFF_DATE")))
    .withColumn("START_DATE", date_add(col("START_DATE"), 1))
    .withColumn("START_DATE", coalesce(col("START_DATE"), to_date(lit("2000-01-01"))))
)
###### COMMERCIAL MAP DATE
COMMERCIAL_MAP_DATE = (
    spark.sql("select explode(sequence(to_date('2000-01-01'), to_date('3000-01-01'), interval 1 day)) as Date")

    # tiene solo current year!
    .where(year(col("Date")) == year(current_date()))

    # aggiunge colonna "MONTH"
    .join(_CUTOFF_DATE, col("Date").between(col("START_DATE"), col("CUTOFF_DATE")))

    .select("Date", "MONTH")
).save_to_s3_parquet(
    path=s3_target_path.format(table_name="COMMERCIAL_MAP_DATE")
)

###### CREATE DF MAPPING
INV_TYPE = [
    ("ZIV", 4), ("ZWIF", 4), ("ZWMF", 4), ("ZF2", 4), ("ZF3", 4), ("ZFBG", 4),
    ("ZL2", 5), ("ZG2", 6), ("ZG4", 6), ("ZRE", 7), ("ZL3|Z04", 12), ("ZL3|Z09", 17),
    ("ZL3", 14), ("ZG3|Z04", 13), ("ZG3|Z09", 16), ("ZG3", 15)
]

INV_SIGN = [
    ("ZS1", -1), ("ZS2", 1), ("ZS3", 1), ("ZS4", -1), ("ZIV", 1), ("ZF2", 1),
    ("ZF3", 1), ("ZWIF", 1), ("ZWMF", 1), ("ZFBG", 1), ("ZL2", 1), ("ZG2", -1),
    ("ZG4", -1), ("ZRE", -1), ("ZL3", 1), ("ZG3", -1)
]

ORD_TYPE = [
    ("B", 4), ("C", 4), ("L", 5), ("K", 6), ("H", 7), ("ZCRE|Z04", 13), ("ZCRE|Z09", 16),
    ("ZCRE", 15), ("ZDRE|Z04", 12), ("ZDRE|Z09", 17), ("ZDRE", 14)
]

ORD_SIGN = [
    ("C", 1), ("L", 1), ("B", 1), ("K", -1), ("H", -1)
]

PosServizio = [
    ("YFOC|ZKL", 1), ("ZCN|ZG2T", 1), ("ZDN|ZL2T", 1), ("ZFOC|ZKL", 1),
    ("ZOR|ZTAX", 1), ("ZORB|ZOR1", 1), ("ZSBU|ZTAX", 1), ("ZSUR|ZTAX", 1), ("ZOR|ZTAP", 1)
]

# Create DataFrames for each mapping
INV_TYPE = spark.createDataFrame(INV_TYPE, ["COD_SAP", "COD_CAS"]).assert_no_duplicates("COD_SAP")
INV_TYPE.save_to_s3_parquet(
    path=s3_target_path.format(table_name="INV_TYPE")
)
INV_SIGN = spark.createDataFrame(INV_SIGN, ["COD_SAP", "SIGN"]).assert_no_duplicates("COD_SAP")
INV_SIGN.save_to_s3_parquet(
    path=s3_target_path.format(table_name="INV_SIGN")
)
ORD_TYPE = spark.createDataFrame(ORD_TYPE, ["COD_SAP", "COD_CAS"]).assert_no_duplicates("COD_SAP")
ORD_TYPE.save_to_s3_parquet(
    path=s3_target_path.format(table_name="ORD_TYPE")
)
ORD_SIGN = spark.createDataFrame(ORD_SIGN, ["COD_SAP", "SIGN"]).assert_no_duplicates("COD_SAP")
ORD_SIGN.save_to_s3_parquet(
    path=s3_target_path.format(table_name="ORD_SIGN")
)
PosServizio = spark.createDataFrame(PosServizio, ["OrderTypeItemCategory", "Value"]).assert_no_duplicates("OrderTypeItemCategory")
PosServizio.save_to_s3_parquet(
    path=s3_target_path.format(table_name="PosServizio")
)


###### SALE_NETWORKS
'''
mapping_sales_office_uk_df = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "Mapping_SalesOffice_UK"
    )
).withColumn("Flag_UK", lit(1))
sale_networks_df = spark.createDataFrame([], schema="SALES_OFFICE string, NETWORK_ID string, MARKET_BUDGET string")
'''
'''
SALE_NETWORKS:
LOAD *,
 SubField(NETWORK_ID,'-@-',1) as US_COMPANY,
  SubField(NETWORK_ID,'-@-',1) as COMPANY_ID,
SubField(NETWORK_ID,'-@-',2) as US_NETWORK;
LOAD Distinct
	SALES_OFFICE as SALES_OFFICE_KEY, //k
    'SAP-'&SubField(Minstring(NETWORK_ID),'-@-',1) as COMM_ORGANIZATION_ID,
    if(SALES_OFFICE='H046','46-@-18',
    if(SALES_OFFICE='H347','47-@-87',
    if(SALES_OFFICE='H660','69-@-81',
    Minstring(NETWORK_ID)))) as NETWORK_ID
FROM [lib://Prj_Anagrafiche/QVD02/SALE_NETWORKS.QVD]
(qvd)
where MARKET_BUDGET<>0  and ApplyMap('Mapping_SalesOffice_UK',SALES_OFFICE,0)=0
Group by SALES_OFFICE;
'''
'''
# Initial transformation of sale_networks_df
sale_networks_transformed = (
    sale_networks_df
    .join(mapping_sales_office_uk_df, ["SALES_OFFICE"], "left")
    .where((col("MARKET_BUDGET") != 0) & (expr("COALESCE(Flag_UK, 0) == 0")))
    .groupBy("SALES_OFFICE")
    .agg(
        min(col("NETWORK_ID")).alias("NETWORK_ID"),
        concat_ws("-", expr("substring_index(min(NETWORK_ID), '-@-', 1)")).alias("COMM_ORGANIZATION_ID")
    )
    .withColumn(
        "NETWORK_ID",
        when(col("SALES_OFFICE") == 'H046', '46-@-18')
        .when(col("SALES_OFFICE") == 'H347', '47-@-87')
        .when(col("SALES_OFFICE") == 'H660', '69-@-81')
        .otherwise(col("NETWORK_ID"))
    )
    .withColumnRenamed("SALES_OFFICE", "SALES_OFFICE_KEY")    # todo: check!!!
    .withColumn("COMM_ORGANIZATION_ID", concat_ws("-", lit("SAP"), substring_index(col("NETWORK_ID"), "-@-", 1)))
    .withColumn("US_COMPANY", substring_index(col("NETWORK_ID"), "-@-", 1))
    .withColumn("COMPANY_ID", substring_index(col("NETWORK_ID"), "-@-", 1))
    .withColumn("US_NETWORK", substring_index(col("NETWORK_ID"), "-@-", 2))
)

# Create static inline data
static_inline_data = [
    ("H019", "11", "SAP-11", "11", "19", "11-@-19"),
    ("H668", "68", "SAP-68", "68", "81", "68-@-81")
]

static_inline_df = spark.createDataFrame(static_inline_data, ["SALES_OFFICE_KEY", "COMPANY_ID", "COMM_ORGANIZATION_ID", "US_COMPANY", "US_NETWORK", "NETWORK_ID"])

# Load additional data from Excel
# todo load from excel
additional_data = [
    ("SALES_OFFICE1|LINE1", "80", "SAP-80", "80", "NETWORK1", "80-@-NETWORK1"),
    ("SALES_OFFICE2|LINE2", "80", "SAP-80", "80", "NETWORK2", "80-@-NETWORK2")
]

additional_df = spark.createDataFrame(additional_data, ["SALES_OFFICE_KEY", "COMPANY_ID", "COMM_ORGANIZATION_ID", "US_COMPANY", "US_NETWORK", "NETWORK_ID"])

SALE_NETWORKS = (
    sale_networks_transformed
    .unionByName(static_inline_df)
    .unionByName(additional_df)
)
SALE_NETWORKS.save_to_s3_parquet(
    path=s3_target_path.format(table_name="SALE_NETWORKS")
)

sap_dm_sale_networks = (
    SALE_NETWORKS
)
sap_dm_sale_networks.save_to_s3_parquet(
    path=s3_target_path.format(table_name="sap_dm_sale_networks")
)'''

###### CDHDR_CDPOS_VERKBELEG_VBAK_LIFSK_CMGST (from CDPOS and CDHDR)
CDPOS = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "CDPOS"
    )
)
CDHDR = read_data_from_s3_parquet(
    path = s3_source_path.format(
        table_name  = "CDHDR"
    )
)
CDHDR_CDPOS_VERKBELEG_VBAK_LIFSK_CMGST = (
    CDPOS.alias("CDPOS")

    .where(col("OBJECTCLAS") == "VERKBELEG")
    .where(col("TABNAME") == "VBAK")
    .where(col("FNAME").isin(["LIFSK", "CMGST"]))

    .join(CDHDR.alias("CDHDR"), ["OBJECTCLAS", "OBJECTID", "CHANGENR"], "inner")

    .select(
        col("CDPOS.OBJECTID"),
        col("CDPOS.CHANGENR"),
        col("CDPOS.FNAME"),
        col("CDPOS.VALUE_NEW"),
        col("CDPOS.VALUE_OLD"),
        col("CDHDR.USERNAME"),
        col("CDHDR.UDATE"),
        col("CDHDR.UTIME"),
    )
)
CDHDR_CDPOS_VERKBELEG_VBAK_LIFSK_CMGST.save_to_s3_parquet(
    path=s3_target_path.format(table_name="CDHDR_CDPOS_VERKBELEG_VBAK_LIFSK_CMGST")
)

###### CDHDR_CDPOS_BLOCK
'''
CDHDR_CDPOS:
LOAD distinct
OBJECTID as ORDER_NUMBER,
CHANGENR,
if( (FNAME='LIFSK' and len(trim(VALUE_NEW))>0) or (FNAME='CMGST' and Match(VALUE_NEW,'B','C')>0)  ,USERNAME,Null()) as ORDER_BLOCKING_USER,
Date(if( (FNAME='LIFSK' and len(trim(VALUE_NEW))>0) or (FNAME='CMGST' and Match(VALUE_NEW,'B','C')>0)  ,UDATE,Null())) as ORDER_BLOCKING_DATE,
if( (FNAME='LIFSK' and len(trim(VALUE_OLD))>0) or (FNAME='CMGST' and Match(VALUE_OLD,'B','C')>0 and Match(VALUE_NEW,'B','C')=0)  ,USERNAME,Null()) as ORDER_UNBLOCKING_USER,
Date(if( (FNAME='LIFSK' and len(trim(VALUE_OLD))>0) or (FNAME='CMGST' and Match(VALUE_OLD,'B','C')>0 and Match(VALUE_NEW,'B','C')=0)  ,UDATE,Null())) as ORDER_UNBLOCKING_DATE
FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/$(vEnv)/QVD01/CDHDR_CDPOS_VERKBELEG_VBAK_LIFSK_CMGST.qvd]
(qvd);
'''


# Define the conditions
condition_order_blocking = ((col("FNAME") == "LIFSK") & (length(trim(col("VALUE_NEW"))) > 0)) | \
                           ((col("FNAME") == "CMGST") & (col("VALUE_NEW").isin("B", "C")))
condition_order_unblocking = ((col("FNAME") == "LIFSK") & (length(trim(col("VALUE_OLD"))) > 0)) | \
                             ((col("FNAME") == "CMGST") & (col("VALUE_OLD").isin("B", "C")) & (~col("VALUE_NEW").isin("B", "C")))

# Apply the transformations
_CDHDR_CDPOS = (
    CDHDR_CDPOS_VERKBELEG_VBAK_LIFSK_CMGST
    .withColumn("ORDER_BLOCKING_USER", when(condition_order_blocking, col("USERNAME")).otherwise(lit(None).cast(StringType())))
    .withColumn("ORDER_BLOCKING_DATE", date_format(when(condition_order_blocking, col("UDATE")).otherwise(lit(None)), 'yyyy-MM-dd'))
    .withColumn("ORDER_UNBLOCKING_USER", when(condition_order_unblocking, col("USERNAME")).otherwise(lit(None).cast(StringType())))
    .withColumn("ORDER_UNBLOCKING_DATE", date_format(when(condition_order_unblocking, col("UDATE")).otherwise(lit(None)), 'yyyy-MM-dd'))
    .select("OBJECTID", "CHANGENR", "ORDER_BLOCKING_USER", "ORDER_BLOCKING_DATE", "ORDER_UNBLOCKING_USER", "ORDER_UNBLOCKING_DATE")
    .distinct()
    .withColumnRenamed("OBJECTID", "ORDER_NUMBER")
)


'''
LAST_BLOCK:
Mapping Load
ORDER_NUMBER&'|'&num#(Maxstring(CHANGENR)) as LAST_BLOCK,
1 as FLAG_LAST_BLOCK
Resident CDHDR_CDPOS where len(ORDER_BLOCKING_USER)>0
Group By ORDER_NUMBER;

LAST_UNBLOCK:
Mapping Load
ORDER_NUMBER&'|'&num#(Maxstring(CHANGENR)) as LAST_UNBLOCK,
1 as FLAG_LAST_UNBLOCK
Resident CDHDR_CDPOS where len(ORDER_UNBLOCKING_USER)>0
Group By ORDER_NUMBER;
'''

LAST_BLOCK = (
    _CDHDR_CDPOS
    .where(trim(col("ORDER_BLOCKING_USER")) != "")
    .groupBy("ORDER_NUMBER")
    .agg(max("CHANGENR").alias("LAST_BLOCK_CHANGENR"))
    .withColumn("LAST_BLOCK", concat_ws("|", col("ORDER_NUMBER"), col("LAST_BLOCK_CHANGENR")))
    .withColumn("FLAG_LAST_BLOCK", lit(1))
    .select("LAST_BLOCK", "FLAG_LAST_BLOCK")
)

# Calculate LAST_UNBLOCK
LAST_UNBLOCK = (
    _CDHDR_CDPOS
    .where(trim(col("ORDER_UNBLOCKING_USER")) != "")
    .groupBy("ORDER_NUMBER")
    .agg(max("CHANGENR").alias("LAST_UNBLOCK_CHANGENR"))
    .withColumn("LAST_UNBLOCK", concat_ws("|", col("ORDER_NUMBER"), col("LAST_UNBLOCK_CHANGENR")))
    .withColumn("FLAG_LAST_UNBLOCK", lit(1))
    .select("LAST_UNBLOCK", "FLAG_LAST_UNBLOCK")
)

'''
Left Join(ORD)

BLOCK:
Load
ORDER_NUMBER,
ORDER_BLOCKING_USER,
ORDER_BLOCKING_DATE
Resident CDHDR_CDPOS
where Applymap('LAST_BLOCK',ORDER_NUMBER&'|'&num#(CHANGENR),0)=1 and len(trim(ORDER_BLOCKING_USER))>0;
'''

CDHDR_CDPOS_BLOCK = (
    _CDHDR_CDPOS
    .join(LAST_BLOCK, concat_ws("|", col("ORDER_NUMBER"), col("CHANGENR")) == col("LAST_BLOCK"), "leftsemi")
    .select("ORDER_NUMBER", "ORDER_BLOCKING_USER", "ORDER_BLOCKING_DATE")
)
CDHDR_CDPOS_BLOCK.save_to_s3_parquet(
    path=s3_target_path.format(table_name="CDHDR_CDPOS_BLOCK")
)

###### CDHDR_CDPOS_UNBLOCK
'''
Left Join(ORD)

UNBLOCK:
Load
ORDER_NUMBER,
ORDER_UNBLOCKING_USER,
ORDER_UNBLOCKING_DATE
Resident CDHDR_CDPOS
where Applymap('LAST_UNBLOCK',ORDER_NUMBER&'|'&num#(CHANGENR),0)=1 and len(trim(ORDER_UNBLOCKING_USER))>0;
'''

CDHDR_CDPOS_UNBLOCK = (
    _CDHDR_CDPOS
    .join(LAST_UNBLOCK, concat_ws("|", col("ORDER_NUMBER"), col("CHANGENR")) == col("LAST_UNBLOCK"), "leftsemi")
    .select("ORDER_NUMBER", "ORDER_UNBLOCKING_USER", "ORDER_UNBLOCKING_DATE")
)

CDHDR_CDPOS_UNBLOCK.save_to_s3_parquet(
    path=s3_target_path.format(table_name="CDHDR_CDPOS_UNBLOCK")
)