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


LIMIT_DATE = "2024-01-01"


################ START PROCESSING BRONZE - MAPPING 2 INV ####################

# BRONZE
VBAP_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBAP_INPUT")
)
LIKP_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="LIKP_INPUT")
)
VBAK_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBAK_INPUT")
)
VBPA_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBPA_INPUT")
)
VBRP_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBRP_INPUT")
)
VBFA_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBFA_INPUT")
)
VBRK_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBRK_INPUT")
)
T042ZT_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="T042ZT_INPUT")
)
T052_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="T052_INPUT")
)
VBEP_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VBEP_INPUT")
)
VTTK_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VTTK_INPUT")
)
VTTP_INPUT = read_data_from_s3_parquet(
    path=s3_bronze_path.format(table_name="VTTP_INPUT")
)


qlik_cst_pbcs_transfer_price_costs = read_data_from_redshift_table("dwa.pbcs_cst_ft_transfer_price_costs")
qlik_cst_pbcs_freight_in = read_data_from_redshift_table("dwa.pbcs_cst_ft_freight_in")
qlik_cst_pbcs_std_costs_commerciale = read_data_from_redshift_table("dwa.pbcs_cst_ft_std_costs_commerciale")
qlik_cst_pbcs_stdindef_tca_costs = read_data_from_redshift_table("dwa.pbcs_cst_ft_stdindef_tca_costs")

# SILVER
TVAK_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVAK_MAPPING")
)
TVAKT_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVAKT_MAPPING")
)
ORD_TYPE = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="ORD_TYPE")
)
CAS_CAUSES_RETURN_REASON_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="CAS_CAUSES_RETURN_REASON_MAPPING")
)
CAS_RETURN_REASON_DESCR_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="CAS_RETURN_REASON_DESCR_MAPPING")
)
TVLST_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVLST_MAPPING")
)
DD07T_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="DD07T_MAPPING")
)
T176T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="T176T_MAPPING")
)
Company_Currency_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="Company_Currency_MAPPING")
)
Currency_Map = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="Currency_Map")
)
Currency_Map_Local=Currency_Map.withColumn("exchange_to_local", when(col("exchange_value")==1 , 1).otherwise(1/col("exchange_value")))
Currency_Map_Local=Currency_Map_Local.select(col("key_currency"),col("exchange_to_local")).drop(col("exchange_value"))
PosServizio = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="PosServizio")
)
PRCD_ELEMENTS = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="PRCD_ELEMENTS")
)
CDHDR_CDPOS_BLOCK = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="CDHDR_CDPOS_BLOCK")
)
CDHDR_CDPOS_UNBLOCK = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="CDHDR_CDPOS_UNBLOCK")
)
KVGR5_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="KVGR5_MAPPING")
)
KVGR3_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="KVGR3_MAPPING")
)
TVGRT_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVGRT_MAPPING")
)
KNA1_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="KNA1_MAPPING")
)
TVV3T_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVV3T_MAPPING")
)
TVV5T_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVV5T_MAPPING")
)
KNA1_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="KNA1_MAPPING")
)
T052U_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="T052U_MAPPING")
)  # INV
INV_TYPE = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="INV_TYPE")
)  # INV
INV_SIGN = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="INV_SIGN")
)  # INV
LFA1_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="LFA1_MAPPING")
)  # INV
KNVV_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="KNVV_MAPPING")
)  # INV
VKGRP_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="VKGRP_MAPPING")
)  # INV
TVLKT_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="TVLKT_MAPPING")
)  # INV
VSART_DESCR_MAPPING = read_data_from_s3_parquet(
    path=s3_mapping_path.format(table_name="VSART_DESCR_MAPPING")
)  # INV
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
LINE_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "LINE_MAPPING"
    )
)


VBRK = (
    VBRK_INPUT
    .where(col("BUCHK") == 'C')
    #.where(col("FKDAT") >= LIMIT_DATE)

    #Applymap('DD07T',VBTYP) as DOC_TYPE_DESCR,
    .do_mapping(DD07T_MAPPING, col("VBTYP"), "DOC_TYPE_DESCR", mapping_name="DD07T_MAPPING")

    #Applymap('T052U',ZTERM) as PAYMENT_EXPIRING_DESCR,
    .do_mapping(T052U_MAPPING, col("ZTERM"), "PAYMENT_EXPIRING_DESCR", mapping_name="T05T052U_MAPPING2U")

    #Applymap('INV_TYPE',FKART) as DOC_TYPE_CODE_SINGLE,
    .do_mapping(INV_TYPE, col("FKART"), "DOC_TYPE_CODE_SINGLE", mapping_name="INV_TYPE")

    #Applymap('INV_SIGN',FKART,1) as MOLTIPLICATORE_RESO,
    .do_mapping(INV_SIGN, col("FKART"), "MOLTIPLICATORE_RESO", default_value=lit(1), mapping_name="T05T052U_MAPPING2U")

    #if(match(FKART,'ZS1','ZS2','ZS3','ZS4') or FKSTO='X','No','Yes') as "OLD PERIMETER QLIK"
    .withColumn(
        "OLD PERIMETER QLIK",
        when(col("FKART").isin(['ZS1', 'ZS2', 'ZS3', 'ZS4']), lit("No"))
        .when(col("FKSTO") == "X", lit("No"))
        .otherwise(lit("Yes"))
    )

    # Where Len(Applymap('INV_TYPE',FKART,Null()))>0 or match(FKART,'ZS1','ZS2','ZS3','ZS4') ;
    .where(
        (length(col("DOC_TYPE_CODE_SINGLE")) > 0) |
        (col("FKART").isin(['ZS1', 'ZS2', 'ZS3', 'ZS4']))
    )

    .select(
        lit(1).alias("DATA_TYPE"),
        lit("Invoiced").alias("TYPE"),
        lit("Invoiced + Delivered").alias("POT_TYPE"),
        lit("Invoiced").alias("POTENTIAL STATUS"),
        col("VBELN").alias("DOCUMENT_CODE"),
        col("FKDAT").alias("DOCUMENT_DATE"),
        to_date(col("FKDAT")).alias("TIME_ID_FACT"),
        to_date(col("FKDAT")).alias("TIME_ID"),
        col("FKSTO").alias("CANCELLED"),
        col("DOC_TYPE_DESCR").alias("DOC_TYPE_DESCR"),
        col("VKORG").alias("COMM_ORGANIZATION"),
        col("BELNR").alias("FISCAL DOCUMENT"),
        col("VKORG").alias("SALES_ORGANIZATION"),
        col("VTWEG").alias("DISTR_CHANNEL"),
        col("SPART").alias("DIVISION"),
        col("KURRF").alias("KURRF"),
        col("ZTERM").alias("PAYMENT_EXPIRING_CODE"),
        col("PAYMENT_EXPIRING_DESCR").alias("PAYMENT_EXPIRING_DESCR"),
        col("LAND1").alias("LAND1"),
        col("FKART").alias("INVOICE_TYPE"),
        col("KNUMV"),
        col("DOC_TYPE_CODE_SINGLE").alias("DOC_TYPE_CODE_SINGLE"),
        col("MOLTIPLICATORE_RESO").alias("MOLTIPLICATORE_RESO"),
        col("SFAKN").alias("CANCELLED_INVOICE"),
        col("OLD PERIMETER QLIK").alias("OLD PERIMETER QLIK")
    )
)

print("VBRK:")


_VBRP = (
    VBRP_INPUT

    .withColumn("POSPA", coalesce(col("POSPA"), col("POSNR")))

    .withColumn("SALES_OFFICE_VBRP", when(length(trim(col("VKBUR"))) == 0, lit(None)).otherwise(col("VKBUR")))

    .select(
        col("VBELN").alias("DOCUMENT_CODE"),
        col("NETWR").alias("DOC_TOTAL_AMOUNT"),
        col("POSNR").alias("POSNR"),
        col("MATNR").alias("PRODUCT_CODE"),
        col("MATNR").alias("MATNR"),
        col("VGBEL").alias("DESPATCHING_NOTE"),
        col("AUBEL").alias("ORDER_NUMBER"),
        col("AUPOS").alias("ORDER_LINE"),
        col("VGBEL").alias("REF_DESPATCH_NOTE"),
        col("AUBEL").alias("REF_ORDER_NUMBER"),
        col("POSPA").alias("POSPA"),
        col("AUPOS").alias("REF_ORDER_LINE"),
        col("FKIMG").alias("INV QTY FABS"),
        col("SALES_OFFICE_VBRP").alias("SALES_OFFICE_VBRP")
    )

)

VBRK = (
    VBRK

    .join(_VBRP, ["DOCUMENT_CODE"], "inner")
)
VBRK = VBRK.localCheckpoint()

print("VBRK (join VBRP):")



_VBAK = (
    VBAK_INPUT

    # where Applymap('TVAK',AUART,0)<>0;
    .do_mapping(TVAK_MAPPING, col("AUART"), "_TVAK_AUART", default_value=lit('0'), mapping_name="TVAK_MAPPING (temp)")
    .where(col("_TVAK_AUART") != '0').drop("_TVAK_AUART")

    #Applymap('TVAKT',AUART) as ORDER_TYPE_DESCR,
    .do_mapping(TVAKT_MAPPING, col("AUART"), "ORDER_TYPE_DESCR", mapping_name="TVAKT_MAPPING")

    #if(AUART='ZBGR',0,1) as FLAG_BGRADE,
    .withColumn("FLAG_BGRADE", when(col("AUART") == "ZBGR", lit(0)).otherwise(lit(1)))

    #if(AUART='ZBGR','B','A') as "A/B_GRADE",
    .withColumn("A/B_GRADE", when(col("AUART") == "ZBGR", lit("B")).otherwise(lit("A")))

    #DATE(DAYSTART(VDATU)) as ORD_REQUESTED_DELIVERY_DATE,
    .withColumn("ORD_REQUESTED_DELIVERY_DATE", to_date(col("VDATU")))

    #Monthname(DAYSTART(VDATU)) as ORDER_REQUESTED_DELIVERY_MONTH_YEAR,
    .withColumn("ORDER_REQUESTED_DELIVERY_MONTH_YEAR", date_format(col("ORD_REQUESTED_DELIVERY_DATE"), 'MMM'))

    #Applymap('TVLST',LIFSK) as SAP_ORDER_BLOCK_DELIVERY_TXT,
    .do_mapping(TVLST_MAPPING, col("LIFSK"), "SAP_ORDER_BLOCK_DELIVERY_TXT", mapping_name="TVLST_MAPPING")

    #if(len(trim(LIFSK))>0,'Delivery Block', if(FAKSK='C','Billing Block', if(CMGST='C' or CMGST='B','Credit Block'))) as "BLOCKING TYPE",
    .withColumn("BLOCKING TYPE",
        when(trim(col("LIFSK")) != "", lit("Delivery Block"))
        .when(col("FAKSK") == "C", lit("Billing Block"))
        .when(col("CMGST").isin(["C", "B"]), lit("Credit Block"))
        .otherwise(lit(None))
    )

    #Applymap('T176T',BSARK) as ORDER_SOURCE_DESCR
    .do_mapping(T176T_MAPPING, col("BSARK"), "ORDER_SOURCE_DESCR", mapping_name="T176T_MAPPING")

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("AUART").alias("ORDER_TYPE_CODE"),
        col("ORDER_TYPE_DESCR").alias("ORDER_TYPE_DESCR"),
        col("FLAG_BGRADE").alias("FLAG_BGRADE"),
        col("A/B_GRADE").alias("A/B_GRADE"),
        col("AUGRU").alias("ORDER_REASON"),
        col("BSTNK").alias("ORDER_CUSTOMER_REFERENCE_1"),
        col("AUDAT").alias("ORDER_DATE"),
        col("AUDAT").alias("ORDER_CUSTOMER_DATE"),
        col("ERNAM").alias("ORDER_CREATION_USER"),
        col("VDATU").alias("ORDER_REQUESTED_DELIVERY_DATE"),
        col("ORD_REQUESTED_DELIVERY_DATE").alias("ORD_REQUESTED_DELIVERY_DATE"),
        col("ORDER_REQUESTED_DELIVERY_MONTH_YEAR").alias("ORDER_REQUESTED_DELIVERY_MONTH_YEAR"),
        col("LIFSK").alias("ORDER_BLOCKING_CODE"),
        concat(col("LIFSK"), lit(""), col("FAKSK")).alias("ORDER_BLOCKING_DESCR"),
        col("LIFSK").alias("SAP_ORDER_BLOCK_DELIVERY"),
        col("CMGST").alias("SAP_ORDER_BLOCK_CREDIT"),
        col("SAP_ORDER_BLOCK_DELIVERY_TXT").alias("SAP_ORDER_BLOCK_DELIVERY_TXT"),
        col("BLOCKING TYPE").alias("BLOCKING TYPE"),
        col("VSBED").alias("WAREHOUSE_CLASSIFICATION"),
        col("VBTYP").alias("CATEGORY"),
        col("BSARK").alias("ORDER_SOURCE"),
        col("SUBMI").alias("COLLECTIVE_NUMBER"),
        col("ORDER_SOURCE_DESCR").alias("ORDER_SOURCE_DESCR"),
        #col("VKORG").alias("company"),
        #col("VKBUR").alias("network"),
    )
)

VBRK = (
    VBRK

    .join(_VBAK, ["ORDER_NUMBER"], "left")
)
VBRK = VBRK.localCheckpoint()
print("VBRK (join VBAK):")


_VBAP = (
    VBAP_INPUT

    #Applymap('ZWW_OTC_LOGAREA',ZZLAREA) as AREA_DESCR
    .do_mapping(ZWW_OTC_LOGAREA_MAPPING, col("ZZLAREA"), "AREA_DESCR", mapping_name="ZWW_OTC_LOGAREA_MAPPING")

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("LGORT").alias("ORDER_DELIVERY_WAREHOUSE"),
        col("WERKS").alias("PLANT"),
        col("VSTEL").alias("SHIPPING POINT"),
        col("ZZLAREA").alias("AREA_CODE"),
        col("AREA_DESCR").alias("AREA_DESCR"),
        col("SPART").alias("SAP_DIVISION"),
        col("PSTYV").alias("PSTYV")
    )
)


VBRK = (
    VBRK

    .join(_VBAP, ["ORDER_NUMBER", "ORDER_LINE"], "left")
)
VBRK = VBRK.localCheckpoint()
print("VBRK (join VBAP):")


_VBEP = (
    VBEP_INPUT

    .where(col("BMENG") > 0)

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("EDATU").alias("CONFIRMED DATE"),
        col("EZEIT").alias("CONFIRMED TIME"),
        col("TDDAT").alias("PLANNED_DISPATCH_DATE"),
        to_date(col("EDATU")).alias("FIRST_REQ_DEL_DATE")
    )

    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(
        concat_ws(",", collect_set("CONFIRMED DATE")).alias("CONFIRMED DATE"),
        concat_ws(",", collect_set("CONFIRMED TIME")).alias("CONFIRMED TIME"),
        concat_ws(",", collect_set("PLANNED_DISPATCH_DATE")).alias("PLANNED_DISPATCH_DATE"),
        min("FIRST_REQ_DEL_DATE").alias("FIRST_REQ_DEL_DATE")
    )
)

VBRK = (
    VBRK

    .join(_VBEP, ["ORDER_NUMBER", "ORDER_LINE"], "left")
)
VBRK = VBRK.localCheckpoint()
print("VBRK (join VBEP):")




def extract_vbpa_using_number_line(self, target_name, parw_value):
    _vbpa = (
        VBPA_INPUT
        .where(col("PARVW") == lit(parw_value))
        .select(
            col("VBELN").alias("DOCUMENT_CODE"),
            col("POSNR").alias("POSPA"),
            col("KUNNR").alias(target_name),
        )
    )

    return self.join(_vbpa, ["DOCUMENT_CODE", "POSPA"], "left")

setattr(DataFrame, "extract_vbpa_using_number_line", extract_vbpa_using_number_line)

def extract_vbpa_using_number(self, target_name, parw_value):
    _vbpa = (
        VBPA_INPUT
        .where(col("PARVW") == lit(parw_value))
        .where(col("POSNR") == lit('000000'))
        .select(
            col("VBELN").alias("DOCUMENT_CODE"),
            col("KUNNR").alias(target_name),
        )
    )

    return self.join(_vbpa, ["DOCUMENT_CODE"], "left")

setattr(DataFrame, "extract_vbpa_using_number", extract_vbpa_using_number)


VBRK = (
    VBRK
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

    .extract_vbpa_using_number(target_name = "PICKING_CARRIER", parw_value = "xxx")  # ex SP
    #Applymap('LFA1',LIFNR,Null()) as PICKING_CARRIER_DESCR
    .do_mapping(LFA1_MAPPING, col("PICKING_CARRIER"), "PICKING_CARRIER_DESCR", mapping_name="LFA1_MAPPING")


)
VBRK = VBRK.localCheckpoint()
print("VBRK (join VBPA):")





INV_SALES_OFFICE = (
    VBRK

    # where Applymap('PosServizio',ORDER_TYPE_CODE&'|'&PSTYV,0)=0 ;
    .do_mapping(PosServizio, concat_ws("|", col("ORDER_TYPE_CODE"), col("PSTYV")), "_PosServizio", default_value = lit(0), mapping_name="PosServizio (temp)")
    .where(col("_PosServizio") == 0).drop("_PosServizio")

    # ApplyMap('INV_TYPE',INVOICE_TYPE&'|'&ORDER_REASON,Null()) as DOC_TYPE_CODE_CONCATENATE,
    .do_mapping(INV_TYPE, concat_ws("|", col("INVOICE_TYPE"), col("ORDER_REASON")), "DOC_TYPE_CODE_CONCATENATE", default_value = lit(None), mapping_name="INV_TYPE")

    .withColumn("k_PRICE_CONDITION", concat_ws("|", col("KNUMV"), col("POSNR")))

    # todo use coalesceEmpty also here?
    .withColumn("DEST_ID", coalesce(col("k_SHIP_POS"), col("k_SHIP_HEADER")))
    .withColumn("SALE_ZONE_CODE", coalesce(col("k_SALE_ZONE_POS"), col("k_SALE_ZONE_HEADER")))
    .withColumn("SALE_DIRECTOR_CODE", coalesce(col("k_SALE_DIRECTOR_POS"), col("k_SALE_DIRECTOR_HEADER")))
    .withColumn("RSM_CODE", coalesce(col("k_RSM_POS"), col("k_RSM_HEADER")))

    .withColumn("INV_ID", coalesceEmpty("k_SOLD_POS", "k_SOLD_HEADER"))
    .withColumn("PURCHASE_GROUP_CODE", coalesceEmpty("k_PURCHASE_POS", "k_PURCHASE_HEADER"))
    .withColumn("MAIN_GROUP_CODE", coalesceEmpty("k_MAIN_POS", "k_MAIN_HEADER"))
    .withColumn("SUPER_GROUP_CODE", coalesceEmpty("k_SUPER_POS", "k_SUPER_HEADER"))
    .withColumn("SALESMAN_CODE", coalesceEmpty("k_SALE_MAN_POS", "k_SALE_MAN_HEADER"))

    .withColumn(
        "DOC_TYPE_CODE_ORIG",
        when((col("INVOICE_TYPE") == 'ZG2') & (upper(col("COLLECTIVE_NUMBER")) == 'PEX'), lit(7))
        .otherwise(coalesce(col("DOC_TYPE_CODE_CONCATENATE"), col("DOC_TYPE_CODE_SINGLE")))
    )

    .do_mapping(KNVV_MAPPING, concat_ws("|", col("INV_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")), "SALES_OFFICE_KNVV", mapping_name="KNVV_MAPPING")

    .withColumn("SALES_OFFICE", coalesce(col("SALES_OFFICE_VBRP"), col("SALES_OFFICE_KNVV")))

    .withColumn("INV_CUSTOMER_KEY", concat_ws("|", col("INV_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .withColumn("DEST_CUSTOMER_KEY", concat_ws("|", col("DEST_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .do_mapping(VKGRP_MAPPING, concat_ws("|", col("DEST_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")), "SALES_GROUP_KEY", default_value=lit('Not Defined'), mapping_name="VKGRP_MAPPING")
    .do_mapping(KVGR5_MAPPING, col("INV_CUSTOMER_KEY"), "CUSTOMER_TYPE_CODE", mapping_name="KVGR5_MAPPING")
    .do_mapping(KVGR5_MAPPING, col("DEST_CUSTOMER_KEY"), "DEST_CUSTOMER_TYPE_CODE", mapping_name="KVGR5_MAPPING")
    .do_mapping(KVGR3_MAPPING, col("INV_CUSTOMER_KEY"), "CUSTOMER_TYPE_CODE_LEV3", mapping_name="KVGR3_MAPPING")
    .do_mapping(KVGR3_MAPPING, col("DEST_CUSTOMER_KEY"), "DEST_CUSTOMER_TYPE_CODE_LEV3", mapping_name="KVGR3_MAPPING")
    .do_mapping(TVGRT_MAPPING, col("SALES_GROUP_KEY"), "SALES GROUP", mapping_name="TVGRT_MAPPING")

    #.join(Mapping_SalesOffice_UK.withColumn("__presence_salesoffice_uk", lit(True)), ["SALES_OFFICE"], "left")

    #.do_mapping(LINE_MAPPING, col("PRODUCT_CODE"), "__line_code", default_value=lit(None), mapping_name="LINE_MAPPING")

    #.withColumn("SALES_OFFICE_KEY",
    #    when(col("__presence_salesoffice_uk"), concat(col("SALES_OFFICE"), lit("|"), col("__line_code")))
    #    .otherwise(col("SALES_OFFICE"))
    #)
    #.drop("__presence_salesoffice_uk", "__line_code")
)

mapping_company_network = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier_uk")

    .withColumnRenamed("SALES_ORGANIZATION", "_SALES_ORGANIZATION_MAPPING")
    .withColumnRenamed("SALES_OFFICE", "_SALES_OFFICE_MAPPING")
    .withColumnRenamed("LINE", "_LINE_MAPPING")

    .assert_no_duplicates("_SALES_ORGANIZATION_MAPPING", "_SALES_OFFICE_MAPPING", "_LINE_MAPPING")    # dato che mappiamo sap->plm, verifica duplicati su chiave sap
)

INV_SALES_OFFICE = (
    INV_SALES_OFFICE

    .do_mapping(LINE_MAPPING, col("PRODUCT_CODE"), "__line_code", default_value=lit("999"), mapping_name="LINE_MAPPING")

    .join(
        mapping_company_network,
        (col("SALES_ORGANIZATION") == col("_SALES_ORGANIZATION_MAPPING")) &
        (col("SALES_OFFICE") == col("_SALES_OFFICE_MAPPING")) &
        ((col("__line_code") == col("_LINE_MAPPING")) | (col("_LINE_MAPPING").isNull())),
        "left"
    )

    .drop("_SALES_ORGANIZATION_MAPPING", "_SALES_OFFICE_MAPPING", "_LINE_MAPPING")
)
INV_SALES_OFFICE = INV_SALES_OFFICE.localCheckpoint()


print("INV_SALES_OFFICE:")



_INV_SALES_OFFICE_MAX_DOC_CANCELLED_INVOICES = (
    INV_SALES_OFFICE

    .groupBy(col("DOCUMENT_CODE").alias("CANCELLED_INVOICE"))
    .agg(
        lit(1).alias("FLAG_CANCELLED_INVOICE_VALID"),
        max(col("DOC_TYPE_CODE_ORIG")).alias("DOC_TYPE_CODE_CANCELLED")
    )
)

INV_SALES_OFFICE = (
    INV_SALES_OFFICE

    .join(_INV_SALES_OFFICE_MAX_DOC_CANCELLED_INVOICES, ['CANCELLED_INVOICE'], "left")
)

#INV_SALES_OFFICE = INV_SALES_OFFICE.localCheckpoint()
print("INV_SALES_OFFICE (join _INV_SALES_OFFICE_MAX_DOC_CANCELLED_INVOICES):")


INV = (
    INV_SALES_OFFICE

    # non è cancellata oppure flag settato
    .where((coalesce(col("CANCELLED_INVOICE"), lit("")) == "") | (col("FLAG_CANCELLED_INVOICE_VALID") == 1))

    .withColumn("DOC_TYPE_CODE", coalesce(col("DOC_TYPE_CODE_CANCELLED"), col("DOC_TYPE_CODE_ORIG")))

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

    .do_mapping(Company_Currency_MAPPING, col("SALES_ORGANIZATION"), "CURRENCY_CODE", default_value=lit("EUR"), mapping_name="Company_Currency_MAPPING")

    .do_mapping(Currency_Map, concat(col("CURRENCY_CODE"), lit("|"), year(col("DOCUMENT_DATE"))), "EXCHANGE", default_value=lit(1), mapping_name="Currency_Map")
    .do_mapping(Currency_Map_Local, concat(col("CURRENCY_CODE"), lit("|"), year(col("DOCUMENT_DATE"))), "EXCHANGE_TO_LOCAL", default_value=lit(1), mapping_name="Currency_Map_To_Local")
)
INV = INV.localCheckpoint()


PRCD_ELEMENTS = (
    PRCD_ELEMENTS
    .withColumn("k_PRICE_CONDITION", concat_ws("|", col("KNUMV"), col("KPOSN")))
    .withColumn("EXCHANGE_RATE_SAP", when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS")))
    .withColumn("KWERT_EXCHANGE_RATE_SAP", col("KWERT") * col("EXCHANGE_RATE_SAP"))
)

PRCD_ELEMENTS_BASE = (
    PRCD_ELEMENTS
    .select("k_PRICE_CONDITION").distinct()
).cache()



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
        abs(col("KWERT_EXCHANGE_RATE_SAP")).alias("INV NSV LC TEMP"),
        col("EXCHANGE_RATE_SAP").alias("EXCHANGE_RATE_SAP")
    )
)

# ORD IC DISCOUNT/SURCHARGE
_DISCOUNT_SURCHARGE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZCDS')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT").alias("INV IC DISCOUNT/SURCHARGE")
    )
)


# ORD GSV LC TOT
_GSV = (
    PRCD_ELEMENTS
    .where(col("KSCHL").isin('ZTTV', 'ZIV1', 'ZIV2'))
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("INV GSV LC TEMP")
    )
)


# ORD GSV LC TOT
_GSV_BONUS = (
    PRCD_ELEMENTS
    .where(col("KSCHL").isin('RENT', 'PPNT'))
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("INV GSV PROMO BONUS LC TEMP")
    )
    .distinct()
)


# _TRANSPORT_COST
_TRANSPORT_COST = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZNET')
    .select(
        "k_PRICE_CONDITION",
        when(col("KSCHL") == "ZFR%", abs("KWERT"))
            .otherwise(abs("KWERT_EXCHANGE_RATE_SAP"))
            .alias("INV TRANSPORT COST LC TEMP")
    )
)



# _RAEE
_RAEE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZWEE')
    .select(
        "k_PRICE_CONDITION",
        abs(col("KWERT")).alias("INV RAEE LC")
    )
)



# REMOVABLE_CHARGES_TOT
_LIST_PRICE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZNET')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("list price tot")
    )
)


_FORCED_PRICE = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'PB00')
    .select(
        "k_PRICE_CONDITION",
        when(col("KWERT") > 0, 1).otherwise(0).alias("PRICE_FORCED"),
        col("KWERT_EXCHANGE_RATE_SAP").alias("FORCED_PRICE_TOT")
    )
)



_TOTAL_AMOUNT = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZGSV')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("total amount")
    )
)



_REMOVABLE_CHARGES = (
    PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZFEE')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("removable charges lc")
    )
)


PRCD_ELEMENTS_ALL = (
    PRCD_ELEMENTS_BASE
    .join(_VAT, ["k_PRICE_CONDITION"], "left")
    .join(_NSV, ["k_PRICE_CONDITION"], "left")
    .join(_DISCOUNT_SURCHARGE, ["k_PRICE_CONDITION"], "left")
    .join(_GSV, ["k_PRICE_CONDITION"], "left")
    .join(_GSV_BONUS, ["k_PRICE_CONDITION"], "left")
    .join(_TRANSPORT_COST, ["k_PRICE_CONDITION"], "left")
    .join(_RAEE, ["k_PRICE_CONDITION"], "left")
    .join(_LIST_PRICE, ["k_PRICE_CONDITION"], "left")
    .join(_FORCED_PRICE, ["k_PRICE_CONDITION"], "left")
    .join(_TOTAL_AMOUNT, ["k_PRICE_CONDITION"], "left")
    .join(_REMOVABLE_CHARGES, ["k_PRICE_CONDITION"], "left")
).assert_no_duplicates("k_PRICE_CONDITION")

INV = (
    INV

    .join(PRCD_ELEMENTS_ALL, ["k_PRICE_CONDITION"], "left")
)
INV = INV.localCheckpoint()

print("INV (join PRCD_ELEMENTS):")



_VBFA = (
    VBFA_INPUT

    .where(col("VBTYP_V") == "M")

    .select(
        col("VBELN").alias("DOCUMENT_CODE"),
        col("POSNN").alias("POSNR"),
        col("VBELV").alias("REF_INVOICE")
    )

    .distinct()
)

INV = (
    INV

    .join(_VBFA, ["DOCUMENT_CODE", "POSNR"], "left")
)

#INV = INV.localCheckpoint()
print("INV (join _VBFA):")


_VBFA = (
    VBFA_INPUT

    .where(col("VBTYP_N") == "Q")

    .select(
        col("VBELV").alias("DESPATCHING_NOTE"),
        col("VBELN").alias("PICKING_NUMBER")
    )

    .groupBy("DESPATCHING_NOTE")
    .agg(
        concat_ws(",", collect_set(col("PICKING_NUMBER"))).alias("PICKING_NUMBER")    # collect_set() -> no duplicates
    )
)

INV = (
    INV

    .join(_VBFA, ["DESPATCHING_NOTE"], "left")
)

#INV = INV.localCheckpoint()
print("INV (join _VBFA 2):")



_VBRK = (
    INV

    .select(
        col("DOCUMENT_CODE").alias("REF_INVOICE"),
        col("TIME_ID").alias("REF_INVOICE_DATE")
    )

    .groupBy("REF_INVOICE")    # todo check: not present in as-is, added to avoid exploding number of rows!!!
    .agg(
        max("REF_INVOICE_DATE").alias("REF_INVOICE_DATE")
    )
)


INV = (
    INV

    .join(_VBRK, ["REF_INVOICE"], "left")
)


#INV = INV.localCheckpoint()
print("INV (join VBRK):")


_T052 = (
    T052_INPUT

    .select(
        col("ZTERM").alias("PAYMENT_EXPIRING_CODE"),
        col("ZLSCH").alias("PAYMENT_TYPE_CODE"),
        col("ZPRZ1").alias("PAYMENT_EXPIRING_DISC")
    )

    .drop_duplicates_ordered(["PAYMENT_EXPIRING_CODE"], ["PAYMENT_TYPE_CODE", "PAYMENT_EXPIRING_DISC"])
    .assert_no_duplicates("PAYMENT_EXPIRING_CODE")
)


INV = (
    INV

    .join(_T052, ["PAYMENT_EXPIRING_CODE"], "left")
)

#INV = INV.localCheckpoint()
print("INV (join _T052):")



_T042ZT = (
    T042ZT_INPUT

    .where(col("SPRAS") == "E")

    .select(
        col("LAND1").alias("LAND1"),
        col("ZLSCH").alias("PAYMENT_TYPE_CODE"),
        col("TEXT2").alias("PAYMENT_TYPE_DESCR")
    )
)


INV = (
    INV

    .join(_T042ZT, ["LAND1", "PAYMENT_TYPE_CODE"], "left")
)

#INV = INV.localCheckpoint()
print("INV (join _T042ZT):")





_LIKP = (
    LIKP_INPUT

    .where(length(trim(col("WADAT_IST"))) > 0)

    .do_mapping(TVLKT_MAPPING, col("LFART"), "SHIPMENT_DESCR", mapping_name="TVLKT_MAPPING")
    .do_mapping(TVROT_MAPPING, col("ROUTE"), "ROUTING_DESCR", mapping_name="TVROT_MAPPING")

    .select(
        col("VBELN").alias("DESPATCHING_NOTE"),
        col("WADAT_IST").alias("DESPATCHING_DATE"),
        col("LFART").alias("SHIPMENT_CODE"),
        col("KODAT").alias("PICKING_DATE"),
        col("KODAT").alias("PICKING_DATE_R"),
        col("SHIPMENT_DESCR").alias("SHIPMENT_DESCR"),
        col("ROUTE").alias("ROUTING"),
        col("ROUTING_DESCR").alias("ROUTING_DESCR"),
        col("LFDAT").alias("CONF_DEL_DATE"),
        col("ZZBOOK_SLOT").alias("BOOKING_STATUS"),
        to_date(col("ZZDATE_SLOT")).alias("APPOINTMENT"),
        to_date(col("ZZDATE_SLOT")).alias("BOOKING_SLOT")
    )
)


INV = (
    INV

    .join(_LIKP, ["DESPATCHING_NOTE"], "left")
)

INV = INV.localCheckpoint()
print("INV (join _LIKP):")



_VTTP = (
    VTTP_INPUT

    .select(
        col("TKNUM").alias("TKNUM"),
        #col("TPNUM").alias("TPNUM"),    # not needed
        col("VBELN").alias("DESPATCHING_NOTE"),
    )

    .join(VTTK_INPUT.select("TKNUM", "VSART"), ["TKNUM"], "left")

    .do_mapping(VSART_DESCR_MAPPING, col("VSART"), "__VSART_DESCR_MAPPING", default_value=lit("Not Defined"), mapping_name="VSART_DESCR_MAPPING")
    .withColumn("__VSART_DESCR_MAPPING", concat_ws(" - ", col("VSART"), col("__VSART_DESCR_MAPPING")))

    .groupBy("DESPATCHING_NOTE")
    .agg(
        concat_ws(" ; ", collect_set(col("__VSART_DESCR_MAPPING"))).alias("SHIPPING TYPE")    # collect_set() -> no duplicates
    )

)

INV = (
    INV

    .join(_VTTP, ["DESPATCHING_NOTE"], "left")
)

INV = INV.localCheckpoint()
print("INV (join _VTTP):")




_LIKP = (
    LIKP_INPUT

    .where(trim(col("WADAT_IST")) != "")

    .select(
        col("VBELN").alias("REF_DESPATCH_NOTE"),
        col("WADAT_IST").alias("REF_DESPATCH_DATE")
    )
)


INV = (
    INV

    .join(_LIKP, ["REF_DESPATCH_NOTE"], "left")
)
INV = INV.localCheckpoint()

print("INV (join _LIKP):")


INV = (
    INV
    .join(CDHDR_CDPOS_BLOCK, ["ORDER_NUMBER"], "left")
    .join(CDHDR_CDPOS_UNBLOCK, ["ORDER_NUMBER"], "left")
)
INV = INV.localCheckpoint()

print("INV (join CDHDR):")


needed_keys_code_network_product_year = (
    INV
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

#freight_in.show()

needed_keys_product_quarter = (
    INV
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

#cost_commerciale.show()

needed_keys_product_year = (
    INV
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
print("stdindef_tca:", stdindef_tca)

#stdindef_tca.show()
INV_WITH_COST = (
    INV#.select("company_haier", "network_haier", "product_code", "DOCUMENT_DATE")

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
INV_WITH_COST = INV_WITH_COST.localCheckpoint()

print("INV_WITH_COST:", INV)


INV_CALCOLI = (
    INV_WITH_COST

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

    #"EXCHANGE" as CURRENCY_EURO_EXCH,
    .withColumn("CURRENCY_EURO_EXCH", col("EXCHANGE"))

    #if(		Match(DOC_TYPE_CODE,5,6)  Or DOC_TYPE_CODE >7 ,0, "INV QTY FABS"*MOLTIPLICATORE_RESO) as "INV QTY",
    .withColumn(
        "INV QTY",
        when((col("DOC_TYPE_CODE") >= 5) & (col("DOC_TYPE_CODE") != 7), lit(0))
        .otherwise(col("INV QTY FABS") * col("MOLTIPLICATORE_RESO"))
    )

    #Applymap('CAS_CAUSES_RETURN_REASON',if(ApplyMap('ORD_TYPE',CATEGORY)=7 or Match(ORDER_TYPE_CODE,'ZREB','ZREG')>0 or (INVOICE_TYPE='ZG2' and COLLECTIVE_NUMBER='PEX'),ORDER_REASON,NUll()),'Not Defined') as RETURN_REASON,
    #Applymap('CAS_RETURN_REASON_DESCR',if(ApplyMap('ORD_TYPE',CATEGORY)=7 or Match(ORDER_TYPE_CODE,'ZREB','ZREG')>0 or (INVOICE_TYPE='ZG2' and COLLECTIVE_NUMBER='PEX'),ORDER_REASON,Null()),'Not Defined') as RETURN_REASON_DESCR,

    #ApplyMap('ORD_TYPE',CATEGORY)=7
    .do_mapping(ORD_TYPE, col("CATEGORY"), "__CAS_CAUSES_RETURN_REASON_category", mapping_name="__CAS_CAUSES_RETURN_REASON_category")

    .withColumn("__order_reason",
        when(col("__CAS_CAUSES_RETURN_REASON_category") == 7, col("ORDER_REASON"))
        .when(col("ORDER_TYPE_CODE").isin(['ZREB','ZREG']), col("ORDER_REASON"))
        .when((col("INVOICE_TYPE") == 'ZG2') & (col("COLLECTIVE_NUMBER") == 'PEX'), col("ORDER_REASON"))
        .otherwise(lit(None))
    )
    .do_mapping(CAS_CAUSES_RETURN_REASON_MAPPING, col("__order_reason"), "RETURN_REASON", default_value="Not Defined", mapping_name="RETURN_REASON")
    .do_mapping(CAS_RETURN_REASON_DESCR_MAPPING, col("__order_reason"), "RETURN_REASON_DESCR", default_value="Not Defined", mapping_name="RETURN_REASON_DESCR")

    .drop("__CAS_CAUSES_RETURN_REASON_category", "__order_reason")


    #1 as "FLAG SAP"
    .withColumn("FLAG SAP", lit(1))


    #coalesce("INV RAEE LC",0) / EXCHANGE 									as "INV RAEE €",
    .withColumn("INV RAEE €", coalesce(col("INV RAEE LC"), lit(0)) * col("EXCHANGE"))
    #"INV TRANSPORT COST LC TEMP" * MOLTIPLICATORE_RESO						as "INV TRANSPORT COST LC",
    .withColumn("INV TRANSPORT COST LC", col("INV TRANSPORT COST LC TEMP") * col("MOLTIPLICATORE_RESO"))
    #"INV TRANSPORT COST LC TEMP" *MOLTIPLICATORE_RESO / EXCHANGE 				as "INV TRANSPORT COST €",
    .withColumn("INV TRANSPORT COST €", col("INV TRANSPORT COST LC") * col("EXCHANGE"))


    #if(DOC_TYPE_CODE<=7, (coalesce("INV GSV LC TEMP","INV GSV PROMO BONUS LC TEMP")*MOLTIPLICATORE_RESO) +Alt("INV IC DISCOUNT/SURCHARGE"*Alt(EXCHANGE_RATE_SAP,1)*MOLTIPLICATORE_RESO,0),0) as "INV GSV LC",
    .withColumn("INV GSV LC",
        when(col("DOC_TYPE_CODE") <= 7,
            (coalesce(col("INV GSV LC TEMP"), col("INV GSV PROMO BONUS LC TEMP")) * col("MOLTIPLICATORE_RESO")) +
            (coalesce(col("INV IC DISCOUNT/SURCHARGE"), lit(0)) * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)) * col("MOLTIPLICATORE_RESO"))
            )
        .otherwise(lit(0))
    )

    #coalesce("INV GSV LC TEMP","INV GSV PROMO BONUS LC TEMP")*MOLTIPLICATORE_RESO as "INV GSV PROMOTIONAL & BONUS LC",
    .withColumn(
        "INV GSV PROMOTIONAL & BONUS LC",
        coalesce(col("INV GSV LC TEMP"), col("INV GSV PROMO BONUS LC TEMP")) * col("MOLTIPLICATORE_RESO")
    )

    #(("INV NSV LC TEMP"*MOLTIPLICATORE_RESO) +Alt("INV IC DISCOUNT/SURCHARGE"*Alt(EXCHANGE_RATE_SAP,1)*MOLTIPLICATORE_RESO,0)) as "INV NSV LC",
    .withColumn("INV NSV LC",
        (col("INV NSV LC TEMP") * col("MOLTIPLICATORE_RESO")) +
        coalesce(
            col("INV IC DISCOUNT/SURCHARGE") * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)) * col("MOLTIPLICATORE_RESO"),
            lit(0)
        )
    )


    #LIST_PRICE_TOT as LIST_PRICE,
    .withColumn("list price", col("list price tot"))
    #FORCED_PRICE_TOT as FORCED_PRICE,
    .withColumn("FORCED_PRICE", col("FORCED_PRICE_TOT"))

    #TRANSPORT_COST_UNIT as TRANSPORT_COST,
    .withColumn("TRANSPORT_COST", col("TRANSPORT_COST_UNIT"))
    #TRANSPORT_COST_UNIT* ALT([INV QTY],0)  as "TRANSPORT COST LC",
    # per chi leggerà questo codice, ci spiace davvero tanto
    .withColumn("TRANSPORT COST LC", col("TRANSPORT_COST_UNIT") *col("EXCHANGE_TO_LOCAL") * coalesce(col("INV QTY"), lit(0)))
    #TRANSPORT_COST_UNIT* ALT([INV QTY],0) /EXCHANGE as "TRANSPORT COST €",
    .withColumn("TRANSPORT COST €", col("TRANSPORT COST LC") * col("EXCHANGE"))

    #TRANSPORT_COST_INFRA_UNIT as TRANSPORT_COST_INFRA,
    .withColumn("TRANSPORT_COST_INFRA", col("TRANSPORT_COST_INFRA_UNIT"))
    #TRANSPORT_COST_INFRA_UNIT* ALT([INV QTY],0)  as "TRANSPORT COST INFRA LC",
    # questo modo di lavorare non ci appartiene
    .withColumn("TRANSPORT COST INFRA LC", col("TRANSPORT_COST_INFRA_UNIT") *col("EXCHANGE_TO_LOCAL")*coalesce(col("INV QTY"), lit(0)))
    #TRANSPORT_COST_INFRA_UNIT* ALT([INV QTY],0) /EXCHANGE as "TRANSPORT COST INFRA €",
    .withColumn("TRANSPORT COST INFRA €", col("TRANSPORT COST INFRA LC") * col("EXCHANGE"))

    #(CONS_COST_OF_SALES_UNIT+TCA_UNIT+TRANSPORT_COST_UNIT+TRANSPORT_COST_INFRA_UNIT)*"INV QTY" as "INV CP LC",
    .withColumn("INV CP LC", (col("CONS_COST_OF_SALES_UNIT")+col("TCA_UNIT")+col("TRANSPORT_COST_UNIT")+col("TRANSPORT_COST_INFRA_UNIT"))*col("EXCHANGE_TO_LOCAL")*col("INV QTY"))
    #(COMM_COST_OF_SALES_UNIT+TRANSPORT_COST_UNIT)*"INV QTY" as "INV TP LC",
    .withColumn("INV TP LC", (col("COMM_COST_OF_SALES_UNIT")+col("TRANSPORT_COST_UNIT"))*col("EXCHANGE_TO_LOCAL")*col("INV QTY"))
    #CONS_COST_OF_SALES_UNIT as CONS_COST_OF_SALES,
    # ma è figlio di tante decisioni sbagliate
    .withColumn("CONS_COST_OF_SALES", col("CONS_COST_OF_SALES_UNIT")*col("EXCHANGE_TO_LOCAL"))
    #COMM_COST_OF_SALES_UNIT as COMM_COST_OF_SALES,
    .withColumn("COMM_COST_OF_SALES", col("COMM_COST_OF_SALES_UNIT")*col("EXCHANGE_TO_LOCAL"))
    #TCA_UNIT as TCA,
    .withColumn("TCA", col("TCA_UNIT")*col("EXCHANGE_TO_LOCAL"))


    .withColumn("REMOVABLE CHARGES LC", col("REMOVABLE CHARGES LC") * col("MOLTIPLICATORE_RESO"))
    .withColumn("REMOVABLE CHARGES €", col("REMOVABLE CHARGES LC") * col("EXCHANGE"))
    #VAT_AMOUNT_1_TOT as VAT_AMOUNT_1,
    .withColumn("VAT_AMOUNT_1", col("VAT_AMOUNT_1_TOT"))

    #Applymap('KNA1',PURCHASE_GROUP_CODE) as PURCHASE_GROUP_DESCR,
    #Applymap('KNA1',MAIN_GROUP_CODE) as MAIN_GROUP_DESCR,
    #Applymap('KNA1',SUPER_GROUP_CODE) as SUPER_GROUP_DESCR,
    #Applymap('KNA1',SALE_ZONE_CODE) as SALE_ZONE_DESCR,
    #Applymap('KNA1',SALESMAN_CODE) as SALESMAN_NAME,
    #Applymap('KNA1',SALE_DIRECTOR_CODE) as  SALE_DIRECTOR,
    #Applymap('KNA1',RSM_CODE) as  RSM;
    .do_mapping(KNA1_MAPPING, col("PURCHASE_GROUP_CODE"), "PURCHASE_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("MAIN_GROUP_CODE"), "MAIN_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SUPER_GROUP_CODE"), "SUPER_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALE_ZONE_CODE"), "SALE_ZONE_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALESMAN_CODE"), "SALESMAN_NAME", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALE_DIRECTOR_CODE"), "SALE_DIRECTOR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("RSM_CODE"), "RSM", mapping_name="KNA1")


    #IF(DATA_TYPE=2,ORDER_REQUESTED_DELIVERY_DATE,IF(DATA_TYPE=4,DOCUMENT_DATE,MAKEDATE(1900,1,1))) as MRDD,
    .withColumn(
        "MRDD",
        when(col("DATA_TYPE") == 2, col("ORDER_REQUESTED_DELIVERY_DATE"))
        .when(col("DATA_TYPE") == 4, col("DOCUMENT_DATE"))
        .otherwise(to_date(lit("1900-01-01")))
    )
    #[POTENTIAL STATUS] as "ORDER STATUS",
    .withColumn("ORDER STATUS", col("POTENTIAL STATUS"))
    #Alt(FORCED_PRICE,0)+Alt(LIST_PRICE,0) as NET_PRICE,
    .withColumn("NET_PRICE", coalesce(col("FORCED_PRICE"), lit(0)) - coalesce(col("list price"), lit(0)))
    #ORDER_BLOCKING_DESCR as "ORDER BLOCKING",
    .withColumn("ORDER BLOCKING", col("ORDER_BLOCKING_DESCR"))

    #ALT([INV NSV LC],0)/EXCHANGE as "INV NSV €",
    .withColumn("INV NSV €", coalesce(col("INV NSV LC"), lit(0)) * col("EXCHANGE"))
    #ALT([INV GSV LC],0)/EXCHANGE as "INV GSV €",
    # sappiamo perfettamente che fa abbastanza schifo
    .withColumn("INV GSV €", coalesce(col("INV GSV LC"), lit(0)) * col("EXCHANGE"))
    #ALT([INV GSV PROMOTIONAL & BONUS LC],0)/EXCHANGE as "INV GSV PROMOTIONAL & BONUS €",
    .withColumn("INV GSV PROMOTIONAL & BONUS €", coalesce(col("INV GSV PROMOTIONAL & BONUS LC"), lit(0)) * col("EXCHANGE"))
    #ma ad un certo punto, uno puo, e deve, solo adeguarsi
    #ALT([INV CP LC],0)/EXCHANGE as "INV CP €",
    .withColumn("INV CP €", coalesce(col("INV CP LC"), lit(0)) * col("EXCHANGE"))
    #ALT([INV TP LC],0)/EXCHANGE as "INV TP €",
    .withColumn("INV TP €", coalesce(col("INV TP LC"), lit(0)) * col("EXCHANGE"))


    #"INV GSV LC" +  alt("INV TRANSPORT COST LC" ,0)					as "INV GSV + TRANSPORT LC" ,
    .withColumn("INV GSV + TRANSPORT LC", col("INV GSV LC") + coalesce("INV TRANSPORT COST LC", lit(0)))
    #ALT([INV GSV LC],0)/EXCHANGE +  alt("INV TRANSPORT COST €",0)  	as "INV GSV + TRANSPORT €" ,
    .withColumn("INV GSV + TRANSPORT €", coalesce("INV GSV LC", lit(0)) * col("EXCHANGE") + coalesce("INV TRANSPORT COST €", lit(0)))


    #(ALT([INV NSV LC],0)-ALT([INV TP LC],0)) 			as "INV GM TP LC",
    .withColumn("INV GM TP LC", coalesce(col("INV NSV LC"), lit(0)) - coalesce(col("INV TP LC"), lit(0)))
    #(ALT([INV NSV LC],0)-ALT([INV TP LC],0))/EXCHANGE 	as "INV GM TP €",
    .withColumn("INV GM TP €", (coalesce(col("INV NSV LC"), lit(0)) - coalesce(col("INV TP LC"), lit(0))) * col("EXCHANGE"))
    #(ALT([INV NSV LC],0)-ALT([INV CP LC],0)) 			as "INV GM CP LC",
    .withColumn("INV GM CP LC", coalesce(col("INV NSV LC"), lit(0)) - coalesce(col("INV CP LC"), lit(0)))
    #(ALT([INV NSV LC],0)-ALT([INV CP LC],0))/EXCHANGE 	as "INV GM CP €";
    .withColumn("INV GM CP €", (coalesce(col("INV NSV LC"), lit(0)) - coalesce(col("INV CP LC"), lit(0))) * col("EXCHANGE"))

    .withColumn("INV IC DISCOUNT/SURCHARGE LC", col("INV IC DISCOUNT/SURCHARGE") * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)))
    .withColumn("INV IC DISCOUNT/SURCHARGE €", coalesce(col("INV IC DISCOUNT/SURCHARGE LC"), lit(0)) * col("EXCHANGE"))

    #ORDER_NUMBER as "ORDER NUMBER",
    #[INV QTY] as "POT QTY",
    #[INV NSV LC] as "POT NSV LC",
    #[INV NSV €] as "POT NSV €",
    #[INV GSV LC] as "POT GSV LC",
    #[INV GSV €] as "POT GSV €",
    #[INV GM TP LC] as "POT GM TP LC",
    #[INV GM TP €] as "POT GM TP €",
    #[INV GM CP LC] as "POT GM CP LC",
    #[INV GM CP €] as "POT GM CP €",
    .withColumn("ORDER NUMBER", col("ORDER_NUMBER"))
    .withColumn("POT QTY", col("INV QTY"))
    .withColumn("POT NSV LC", col("INV NSV LC"))
    .withColumn("POT NSV €", col("INV NSV €"))
    .withColumn("POT GSV LC", col("INV GSV LC"))
    .withColumn("POT GSV €", col("INV GSV €"))
    .withColumn("POT GM TP LC", col("INV GM TP LC"))
    .withColumn("POT GM TP €", col("INV GM TP €"))
    .withColumn("POT GM CP LC", col("INV GM CP LC"))
    .withColumn("POT GM CP €", col("INV GM CP €"))

    #[INV QTY] as "INV + DEL QTY",
    #[INV NSV LC] as "INV + DEL NSV LC",
    #[INV NSV €] as "INV + DEL NSV €",
    #[INV GSV LC] as "INV + DEL GSV LC",
    #[INV GSV €] as "INV + DEL GSV €",
    #[INV GM TP LC] as "INV + DEL GM TP LC",
    #[INV GM TP €] as "INV + DEL GM TP €",
    #[INV GM CP LC] as "INV + DEL GM CP LC",
    #[INV GM CP €] as "INV + DEL GM CP €",
    .withColumn("INV + DEL QTY", col("INV QTY"))
    .withColumn("INV + DEL NSV LC", col("INV NSV LC"))
    .withColumn("INV + DEL NSV €", col("INV NSV €"))
    .withColumn("INV + DEL GSV LC", col("INV GSV LC"))
    .withColumn("INV + DEL GSV €", col("INV GSV €"))
    .withColumn("INV + DEL GM TP LC", col("INV GM TP LC"))
    .withColumn("INV + DEL GM TP €", col("INV GM TP €"))
    .withColumn("INV + DEL GM CP LC", col("INV GM CP LC"))
    .withColumn("INV + DEL GM CP €", col("INV GM CP €"))

    #ALT([INV BDG COMMISSIONS LC],0) + ALT([INV BDG SERVICE COSTS LC],0) + ALT([INV BDG FREIGHT OUT LC],0) + ALT([INV BDG ROYALTIES LC],0) + ALT([INV BDG ADJ VARIAB LC],0) + ALT([INV BDG CABINET FEE LC],0) AS [INV BDG VAR EXPENSES LC],
    #ALT([INV BDG COMMISSIONS LC],0) / EXCHANGE AS [INV BDG COMMISSIONS €],
    #ALT([INV BDG SERVICE COSTS LC],0) / EXCHANGE AS [INV BDG SERVICE COSTS €],
    #ALT([INV BDG FREIGHT OUT LC],0) / EXCHANGE AS [INV BDG FREIGHT OUT €],
    #ALT([INV BDG ROYALTIES LC],0) / EXCHANGE AS [INV BDG ROYALTIES €],
    #ALT([INV BDG ADJ VARIAB LC],0) / EXCHANGE AS [INV BDG ADJ VARIAB €],
    #ALT([INV BDG CABINET FEE LC],0) / EXCHANGE AS [INV BDG CABINET FEE €]
    #.withColumn("INV BDG VAR EXPENSES LC",
    #            coalesce(col("INV BDG COMMISSIONS LC"), lit(0)) +
    #            coalesce(col("INV BDG SERVICE COSTS LC"), lit(0)) +
    #            coalesce(col("INV BDG FREIGHT OUT LC"), lit(0)) +
    #            coalesce(col("INV BDG ROYALTIES LC"), lit(0)) +
    #            coalesce(col("INV BDG ADJ VARIAB LC"), lit(0)) +
    #            coalesce(col("INV BDG CABINET FEE LC"), lit(0)))
    #.withColumn("INV BDG COMMISSIONS €", col("INV BDG COMMISSIONS LC") / col("EXCHANGE"))
    #.withColumn("INV BDG SERVICE COSTS €", col("INV BDG SERVICE COSTS LC") / col("EXCHANGE"))
    #.withColumn("INV BDG FREIGHT OUT €", col("INV BDG FREIGHT OUT LC") / col("EXCHANGE"))
    #.withColumn("INV BDG ROYALTIES €", col("INV BDG ROYALTIES LC") / col("EXCHANGE"))
    #.withColumn("INV BDG ADJ VARIAB €", col("INV BDG ADJ VARIAB LC") / col("EXCHANGE"))
    #.withColumn("INV BDG CABINET FEE €", col("INV BDG CABINET FEE LC") / col("EXCHANGE"))


    #ALT([INV BDG VAR EXPENSES LC],0) / EXCHANGE AS [INV BDG VAR EXPENSES €],
    #ALT([INV GM CP €],0) - (ALT([INV BDG VAR EXPENSES LC],0) / EXCHANGE) AS [INV CTF BDG CP €],
    #ALT([INV GM TP €],0) - (ALT([INV BDG VAR EXPENSES LC],0) / EXCHANGE) AS [INV CTF BDG TP €],
    #ALT([INV GM CP LC],0) - ALT([INV BDG VAR EXPENSES LC],0) AS [INV CTF BDG CP LC],
    #ALT([INV GM TP LC],0) - ALT([INV BDG VAR EXPENSES LC],0) AS [INV CTF BDG TP LC]
    #.withColumn("INV BDG VAR EXPENSES €",
    #    col("INV BDG VAR EXPENSES LC") / col("EXCHANGE"))
    #.withColumn("INV CTF BDG CP €",
    #    coalesce(col("INV GM CP €"), lit(0)) - (col("INV BDG VAR EXPENSES LC") / col("EXCHANGE")))
    #.withColumn("INV CTF BDG TP €",
    #    coalesce(col("INV GM TP €"), lit(0)) - (col("INV BDG VAR EXPENSES LC") / col("EXCHANGE")))
    #.withColumn("INV CTF BDG CP LC",
    #    coalesce(col("INV GM CP LC"), lit(0)) - coalesce(col("INV BDG VAR EXPENSES LC"), lit(0)))
    #.withColumn("INV CTF BDG TP LC",
    #    coalesce(col("INV GM TP LC"), lit(0)) - coalesce(col("INV BDG VAR EXPENSES LC"), lit(0)))

    #.withColumn("COMPANY_CODE", col("COMPANY"))
    #.withColumn("NETWORK_CODE", col("NETWORK"))



    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    .distinct()

    .withColumn("key_fact", concat_ws("###", lit("INVOICE"), col("DOCUMENT_CODE"), col("POSNR"), col("REF_INVOICE")))
    .assert_no_duplicates("key_fact")
)

INV_CALCOLI = INV_CALCOLI.localCheckpoint()

INV_CALCOLI = INV_CALCOLI.columns_to_lower()
print(f"INV_CALCOLI OK")


##### Save Golden Sources
list_sap_cols_inv = ["order_number", "ref_despatch_note", "payment_type_code", "payment_expiring_code", "ref_invoice", "document_code", "posnr", "cancelled_invoice", "pospa", "order_line", "data_type", "document_date", "time_id", "cancelled", "comm_organization", "fiscal document", "sales_organization", "distr_channel", "division", "kurrf", "invoice_type", "knumv", "doc_type_code_single", "moltiplicatore_reso", "doc_total_amount", "product_code", "ref_order_number", "ref_order_line", "inv qty fabs", "plant", "area_code", "picking_carrier", "dest_id", "inv_id", "main_group_code", "purchase_group_code", "rsm_code", "sale_director_code", "sale_zone_code", "salesman_code", "super_group_code", "sales_office", "company", "network", "flag_cancelled_invoice_valid", "doc_type_code", "currency_code", "exchange", "inv ic discount/surcharge", "inv gsv promo bonus lc temp", "inv raee lc", "list price tot", "total amount", "removable charges lc", "picking_number", "key_cost_commercial", "key_cost_freight_in", "key_cost_transfer_price", "key_cost_stdindef_tca", "inv ic discount/surcharge lc", "inv ic discount/surcharge €", "inv qty", "inv raee €", "inv transport cost lc", "inv transport cost €", "inv gsv lc", "inv gsv €", "inv gsv promotional & bonus lc", "inv gsv promotional & bonus €", "inv nsv lc", "inv nsv €", "removable charges €", "key_fact"]
inv_gs = (
    INV_CALCOLI
    .select(list_sap_cols_inv)
    .save_data_to_redshift_table("dwa.sap_bil_ft_invoice")
)

##### Save remaining cols
list_dims_cols_inv = list(set(INV_CALCOLI.columns).difference(list_sap_cols_inv))+['key_fact']

inv_dim = (
    INV_CALCOLI
    .select(list_dims_cols_inv)
    .save_data_to_redshift_table("dwa.sap_bil_dim_invoice")
)