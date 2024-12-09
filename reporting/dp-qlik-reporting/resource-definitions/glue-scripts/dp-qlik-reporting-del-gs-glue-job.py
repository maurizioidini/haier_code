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



def date_to_qlik_num(c):
    return datediff(c, lit("1899-12-30"))


################ START PROCESSING BRONZE - MAPPING 2 DEL ####################

# BRONZE
# Bronze tables
VBAP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VBAP_INPUT")
)
LIKP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="LIKP_INPUT")
)
VBAK_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VBAK_INPUT")
)
LIPS_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="LIPS_INPUT")
)
VBPA_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VBPA_INPUT")
)
VBRP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VBRP_INPUT")
)
VBFA_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VBFA_INPUT")
)
VBEP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VBEP_INPUT")
)
VTTK_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VTTK_INPUT")
)
VTTP_INPUT = read_data_from_s3_parquet(
    path = s3_bronze_path.format(table_name="VTTP_INPUT")
)


qlik_cst_pbcs_transfer_price_costs = read_data_from_redshift_table("dwa.pbcs_cst_ft_transfer_price_costs")
qlik_cst_pbcs_freight_in = read_data_from_redshift_table("dwa.pbcs_cst_ft_freight_in")
qlik_cst_pbcs_std_costs_commerciale = read_data_from_redshift_table("dwa.pbcs_cst_ft_std_costs_commerciale")
qlik_cst_pbcs_stdindef_tca_costs = read_data_from_redshift_table("dwa.pbcs_cst_ft_stdindef_tca_costs")


#SILVER
TVAK_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="TVAK_MAPPING")
)
TVAKT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="TVAKT_MAPPING")
)
COMMERCIAL_MAP_DATE = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="COMMERCIAL_MAP_DATE")
)
ORD_TYPE = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="ORD_TYPE")
)
CAS_CAUSES_RETURN_REASON_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="CAS_CAUSES_RETURN_REASON_MAPPING")
)
CAS_RETURN_REASON_DESCR_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="CAS_RETURN_REASON_DESCR_MAPPING")
)
TVLST_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="TVLST_MAPPING")
)
DD07T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="DD07T_MAPPING")
)
ORD_SIGN = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="ORD_SIGN")
)
T176T_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="T176T_MAPPING")
)
ZWW_OTC_LOGAREA_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="ZWW_OTC_LOGAREA_MAPPING")
)
TVROT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="TVROT_MAPPING")
)
SALE_NETWORKS = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="SALE_NETWORKS")
)
Company_Currency_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="Company_Currency_MAPPING")
)
Currency_Map = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="Currency_Map")
)
Currency_Map_Local=Currency_Map.withColumn("exchange_to_local", when(col("exchange_value")==1 , 1).otherwise(1/col("exchange_value")))
Currency_Map_Local=Currency_Map_Local.select(col("key_currency"),col("exchange_to_local")).drop(col("exchange_value"))
PosServizio = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="PosServizio")
)
PRCD_ELEMENTS = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="PRCD_ELEMENTS")
)
CDHDR_CDPOS_BLOCK = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="CDHDR_CDPOS_BLOCK")
)
CDHDR_CDPOS_UNBLOCK = read_data_from_s3_parquet(
    path = s3_mapping_path.format(table_name="CDHDR_CDPOS_UNBLOCK")
)
TVLKT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVLKT_MAPPING"
    )
)
LINE_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "LINE_MAPPING"
    )
)
LFA1_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "LFA1_MAPPING"
    )
)
VSART_DESCR_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "VSART_DESCR_MAPPING"
    )
)
KNA1_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KNA1_MAPPING"
    )
)
TVGRT_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "TVGRT_MAPPING"
    )
)
KVGR3_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KVGR3_MAPPING"
    )
)
KVGR5_MAPPING = read_data_from_s3_parquet(
    path = s3_mapping_path.format(
        table_name  = "KVGR5_MAPPING"
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


OPEN_DELIVERY = (
    LIKP_INPUT

    .where(col("WADAT_IST").isNotNull())
    .select(col("VBELN").alias("CONSEGNA"))

    .join(
        LIPS_INPUT.select(
            col("VBELN").alias("CONSEGNA"),
            col("POSNR").alias("RIGA_CONSEGNA"),
            col("LFIMG").alias("DEL QTY")
        ),
        ["CONSEGNA"], "left"
    )

    .join(
        VBRP_INPUT     # todo check
            .select(
                col("VGBEL").alias("CONSEGNA"),
                col("VGPOS").alias("RIGA_CONSEGNA"),
                col("FKIMG").alias("INV QTY")
            )
            .groupBy("CONSEGNA", "RIGA_CONSEGNA")
            .agg(sum(col("INV QTY")).alias("INV QTY"))
        ,
        ["CONSEGNA", "RIGA_CONSEGNA"], "left"
    )

    #"DEL QTY"- Alt("INV QTY",0) as "DEL QTY"
    .withColumn("DEL QTY", col("DEL QTY") - coalesce(col("INV QTY"), lit(0)))

    .where(col("DEL QTY") > 0)

    .select("CONSEGNA", "RIGA_CONSEGNA", "DEL QTY")
)

OPEN_DELIVERY = OPEN_DELIVERY.localCheckpoint()
print(f"OPEN_DELIVERY OK: {OPEN_DELIVERY.count()} rows")

LIKP = (
    LIKP_INPUT

    .where(col("WADAT_IST").isNotNull())  # it is a timestamp

    .do_mapping(DD07T_MAPPING, col("VBTYP"), "DOC_TYPE_DESCR", mapping_name="DD07T_MAPPING")
    .do_mapping(TVLKT_MAPPING, col("LFART"), "SHIPMENT_DESCR", mapping_name="TVLKT_MAPPING")
    .do_mapping(TVROT_MAPPING, col("ROUTE"), "ROUTING_DESCR", mapping_name="TVROT_MAPPING")

    .select(
        lit("3").alias("DATA_TYPE"),
        lit("Delivered").alias("TYPE"),
        lit("Invoiced + Delivered").alias("POT_TYPE"),
        lit("Delivered").alias("POTENTIAL STATUS"),
        col("VBELN").alias("DESPATCHING_NOTE"),
        current_date().alias("DOCUMENT_DATE_ORIGINAL"),
        col("DOC_TYPE_DESCR").alias("DOC_TYPE_DESCR"),
        col("WADAT_IST").alias("DESPATCHING_DATE"),
        col("LFART").alias("SHIPMENT_CODE"),
        col("SHIPMENT_DESCR").alias("SHIPMENT_DESCR"),
        col("KODAT").alias("PICKING_DATE"),
        col("KODAT").alias("PICKING_DATE_R"),
        col("LFDAT").alias("CONF_DEL_DATE"),
        col("ROUTE").alias("ROUTING"),
        col("ROUTING_DESCR").alias("ROUTING_DESCR"),
        col("ZZBOOK_SLOT").alias("BOOKING_STATUS"),
        to_date(col("ZZDATE_SLOT")).alias("APPOINTMENT"),
        to_date(col("ZZDATE_SLOT")).alias("BOOKING_SLOT")
    )
)

print("LIKP", LIKP)
LIKP = LIKP.localCheckpoint()
print(f"LIKP OK: {LIKP.count()} rows")


_VTTP = (
    VTTP_INPUT

    .select(
        col("TKNUM").alias("TKNUM"),
        #col("TPNUM").alias("TPNUM"),    # not needed
        col("VBELN").alias("DESPATCHING_NOTE"),
    )

    .join(VTTK_INPUT.select("TKNUM", "VSART"), ["TKNUM"], "left")

    .do_mapping(VSART_DESCR_MAPPING, col("VSART"), "__VSART_DESCR_MAPPING", default_value=lit("Not Defined"), mapping_name="VSART_DESCR_MAPPING")

    .groupBy("DESPATCHING_NOTE")
    .agg(
        concat_ws(" ; ", collect_set(concat(col("VSART"), lit(" - "), col("__VSART_DESCR_MAPPING")))).alias("SHIPPING TYPE")    # collect_set() -> no duplicates
    )

)



LIKP = (
    LIKP

    .join(_VTTP, ["DESPATCHING_NOTE"], "left")
)

LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _VTTP) OK: {LIKP.count()} rows")


_LIPS = (
    LIPS_INPUT

    .withColumn("CONSEGNA", col("VBELN"))
    .withColumn("RIGA_CONSEGNA", col("POSNR"))

    .join(OPEN_DELIVERY, ["CONSEGNA", "RIGA_CONSEGNA"], "inner")  # extract "DEL QTY" (inner join, already filtered >0)

    .withColumnRenamed("DEL QTY", "DEL QTY FABS")
    .withColumn("% DEL QTY FABS", col("DEL QTY FABS") / col("LFIMG"))

    .do_mapping(ZWW_OTC_LOGAREA_MAPPING, col("ZZLAREA"), "AREA_DESCR", mapping_name="ZWW_OTC_LOGAREA_MAPPING")

    .withColumn("SALES_OFFICE", col("VKBUR"))
    .withColumn("PRODUCT_CODE", col("MATNR"))

    .select(
        col("VBELN").alias("DESPATCHING_NOTE"),
        col("POSNR").alias("POSNR"),
        col("MATNR").alias("PRODUCT_CODE"),
        col("VGBEL").alias("ORDER_NUMBER"),
        col("VGPOS").alias("ORDER_LINE"),
        col("LGORT").alias("ALLOCATION WAREHOUSE"),
        col("ZZLAREA").alias("AREA_CODE"),
        col("AREA_DESCR").alias("AREA_DESCR"),
        col("LFIMG").alias("ORIGINAL DEL QTY"),
        col("DEL QTY FABS").alias("DEL QTY FABS"),
        col("% DEL QTY FABS").alias("% DEL QTY FABS"),
        col("SALES_OFFICE").alias("SALES_OFFICE"),
    )
)

LIKP = (
    LIKP

    .join(_LIPS, ["DESPATCHING_NOTE"], "right")
)

LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _LIPS) OK: {LIKP.count()} rows")

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

LIKP = (
    LIKP

    .join(_VBFA, ["DESPATCHING_NOTE"], "left")
)

LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _VBFA) OK: {LIKP.count()} rows")

_VBAK = (
    VBAK_INPUT

    #where and SPART<>'SE'
    .where(col("SPART") != "SE")

    # where Applymap('TVAK',AUART,0)<>0 and Match(AUART,'ZKL','ZFOC','YFOC','ZWBF','ZWPC','ZWPR')=0
    .where(~col("AUART").isin(['ZKL','ZFOC','YFOC','ZWBF','ZWPC','ZWPR']))
    .do_mapping(TVAK_MAPPING, col("AUART"), "_TVAK_AUART", default_value=lit(""), mapping_name="TVAK_MAPPING (temp)")
    .where(col("_TVAK_AUART") != "")
    .drop("_TVAK_AUART")


    #ApplyMap('ORD_TYPE',VBTYP) as DOC_TYPE_CODE,
    .do_mapping(ORD_TYPE, col("VBTYP"), "DOC_TYPE_CODE", mapping_name="ORD_TYPE")

    #Applymap('TVAKT',AUART) as ORDER_TYPE_DESCR,
    .do_mapping(TVAKT_MAPPING, col("AUART"), "ORDER_TYPE_DESCR", mapping_name="TVAKT_MAPPING")

    #if(AUART='ZBGR',0,1) as FLAG_BGRADE,
    .withColumn("FLAG_BGRADE", when(col("AUART") == "ZBGR", lit(0)).otherwise(lit(1)))

    #if(AUART='ZBGR','B','A') as "A/B_GRADE",
    .withColumn("A/B_GRADE", when(col("AUART") == "ZBGR", lit("B")).otherwise(lit("A")))

    .do_mapping(ORD_TYPE, col("VBTYP"), "_ord_type_map", mapping_name="ORD_TYPE")
    .withColumn("_cas_return_reason_key",
        when(col("_ord_type_map") == "7", col("AUGRU"))
        .when(col("AUART").isin(['ZREB','ZREG']), col("AUGRU"))
        .otherwise(lit(None))
    )

    #Applymap('CAS_CAUSES_RETURN_REASON',if(ApplyMap('ORD_TYPE',VBTYP)=7  or Match(AUART,'ZREB','ZREG')>0 ,AUGRU,NUll()),'Not Defined')  as RETURN_REASON,
    .do_mapping(CAS_CAUSES_RETURN_REASON_MAPPING, col("_cas_return_reason_key"), "RETURN_REASON", default_value=lit("Not Defined"), mapping_name="CAS_CAUSES_RETURN_REASON_MAPPING")

    #Applymap('CAS_RETURN_REASON_DESCR',if(ApplyMap('ORD_TYPE',VBTYP)=7  or Match(AUART,'ZREB','ZREG')>0 ,AUGRU,Null()),'Not Defined') as RETURN_REASON_DESCR,
    .do_mapping(CAS_RETURN_REASON_DESCR_MAPPING, col("_cas_return_reason_key"), "RETURN_REASON_DESCR", default_value=lit("Not Defined"), mapping_name="CAS_RETURN_REASON_DESCR")

    #DATE(DAYSTART(VDATU)) as ORD_REQUESTED_DELIVERY_DATE,
    .withColumn("ORD_REQUESTED_DELIVERY_DATE", to_date(col("VDATU")))

    #Monthname(DAYSTART(VDATU)) as ORDER_REQUESTED_DELIVERY_MONTH_YEAR,
    .withColumn("ORDER_REQUESTED_DELIVERY_MONTH_YEAR", date_format(col("ORD_REQUESTED_DELIVERY_DATE"), 'MMM'))

    #LIFSK & ' ' & FAKSK as ORDER_BLOCKING_DESCR,
    .withColumn("ORDER_BLOCKING_DESCR", concat(col("LIFSK"), lit(" "), col("FAKSK")))

    #Applymap('TVLST',LIFSK) as SAP_ORDER_BLOCK_DELIVERY_TXT,
    .do_mapping(TVLST_MAPPING, col("LIFSK"), "SAP_ORDER_BLOCK_DELIVERY_TXT", mapping_name="TVLST_MAPPING")

    #if(len(trim(LIFSK))>0,'Delivery Block', if(FAKSK='C','Billing Block', if(CMGST='C' or CMGST='B','Credit Block'))) as "BLOCKING TYPE",
    .withColumn("BLOCKING TYPE",
        when(trim(col("LIFSK")) != "", lit("Delivery Block"))
        .when(col("FAKSK") == "C", lit("Billing Block"))
        .when(col("CMGST").isin(["C", "B"]), lit("Credit Block"))
        .otherwise(lit(None))
    )

    #ApplyMap('ORD_SIGN',VBTYP) as MOLTIPLICATORE_RESO,
    .do_mapping(ORD_SIGN, col("VBTYP"), "MOLTIPLICATORE_RESO", mapping_name="ORD_SIGN")

    #Applymap('T176T',BSARK) as ORDER_SOURCE_DESCR,
    .do_mapping(T176T_MAPPING, col("BSARK"), "ORDER_SOURCE_DESCR", mapping_name="T176T")

    #if(match(AUART,'ZKA','ZKB','ZKE','ZKR'),'No','Yes') as "OLD PERIMETER QLIK"
    .withColumn("OLD PERIMETER QLIK", ~col("AUART").isin('ZKA','ZKB','ZKE','ZKR'))   #
    .withColumn("OLD PERIMETER QLIK", when(col("OLD PERIMETER QLIK"), lit("Yes")).otherwise(lit("No")))

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("AUART").alias("ORDER_TYPE_CODE"),
        col("DOC_TYPE_CODE").alias("DOC_TYPE_CODE"),
        col("ORDER_TYPE_DESCR").alias("ORDER_TYPE_DESCR"),
        col("FLAG_BGRADE").alias("FLAG_BGRADE"),
        col("A/B_GRADE").alias("A/B_GRADE"),
        col("RETURN_REASON").alias("RETURN_REASON"),
        col("RETURN_REASON_DESCR").alias("RETURN_REASON_DESCR"),
        col("BSTNK").alias("ORDER_CUSTOMER_REFERENCE_1"),
        col("AUDAT").alias("ORDER_DATE"),
        col("AUDAT").alias("ORDER_CUSTOMER_DATE"),
        col("ERNAM").alias("ORDER_CREATION_USER"),
        col("VDATU").alias("ORDER_REQUESTED_DELIVERY_DATE"),
        col("ORDER_REQUESTED_DELIVERY_MONTH_YEAR").alias("ORDER_REQUESTED_DELIVERY_MONTH_YEAR"),
        col("LIFSK").alias("ORDER_BLOCKING_CODE"),
        col("ORDER_BLOCKING_DESCR").alias("ORDER_BLOCKING_DESCR"),
        col("LIFSK").alias("SAP_ORDER_BLOCK_DELIVERY"),
        col("CMGST").alias("SAP_ORDER_BLOCK_CREDIT"),
        col("SAP_ORDER_BLOCK_DELIVERY_TXT").alias("SAP_ORDER_BLOCK_DELIVERY_TXT"),
        col("BLOCKING TYPE").alias("BLOCKING TYPE"),
        col("KNUMV").alias("KNUMV"),
        col("VKORG").alias("SALES_ORGANIZATION"),
        col("SPART").alias("DIVISION"),
        col("VTWEG").alias("DISTR_CHANNEL"),
        col("MOLTIPLICATORE_RESO").alias("MOLTIPLICATORE_RESO"),
        col("VSBED").alias("WAREHOUSE_CLASSIFICATION"),
        col("VBTYP").alias("CATEGORY"),
        col("BSARK").alias("ORDER_SOURCE"),
        col("OLD PERIMETER QLIK").alias("OLD PERIMETER QLIK"),
        #col("VKORG").alias("company"),
        #col("VKBUR").alias("network"),
    )
)


mapping_company_network = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier_uk")

    .withColumnRenamed("SALES_ORGANIZATION", "_SALES_ORGANIZATION_MAPPING")
    .withColumnRenamed("SALES_OFFICE", "_SALES_OFFICE_MAPPING")
    .withColumnRenamed("LINE", "_LINE_MAPPING")

    .assert_no_duplicates("_SALES_ORGANIZATION_MAPPING", "_SALES_OFFICE_MAPPING", "_LINE_MAPPING")    # dato che mappiamo sap->plm, verifica duplicati su chiave sap
)

LIKP = (
    LIKP

    .join(_VBAK, ["ORDER_NUMBER"], "inner")

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


LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _VBAK and mapping_company_network) OK: {LIKP.count()} rows")

_VBAP = (
    VBAP_INPUT

    .where(coalesce(trim(col("ABGRU")),lit("")) == "")

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("LGORT").alias("ORDER_DELIVERY_WAREHOUSE"),
        col("WERKS").alias("PLANT"),
        col("VSTEL").alias("SHIPPING POINT"),
        col("KWMENG").alias("ORD QTY ORIG"),
        col("PSTYV").alias("PSTYV")
    )
)

LIKP = (
    LIKP
    .join(_VBAP, ['ORDER_NUMBER', 'ORDER_LINE'], "inner")
)


#LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _VBAP) OK: {LIKP.count()} rows")

_VBEP = (
    VBEP_INPUT

    .where(col("BMENG") > 0)

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        col("EDATU").alias("CONFIRMED DATE"),
        col("EZEIT").alias("CONFIRMED TIME"),
        col("TDDAT").alias("PLANNED_DISPATCH_DATE")
    )

    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(
        concat_ws(",", collect_set("CONFIRMED DATE")).alias("CONFIRMED DATE"),
        concat_ws(",", collect_set("CONFIRMED TIME")).alias("CONFIRMED TIME"),
        concat_ws(",", collect_set("PLANNED_DISPATCH_DATE")).alias("PLANNED_DISPATCH_DATE")
    )
)


LIKP = (
    LIKP

    .join(_VBEP, ["ORDER_NUMBER", "ORDER_LINE"], "left")
)

#LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _VBEP 1) OK: {LIKP.count()} rows")

_VBEP = (
    VBEP_INPUT

    .select(
        col("VBELN").alias("ORDER_NUMBER"),
        col("POSNR").alias("ORDER_LINE"),
        to_date(col("EDATU")).alias("FIRST_REQ_DEL_DATE")
    )

    .groupBy("ORDER_NUMBER", "ORDER_LINE")
    .agg(
        min("FIRST_REQ_DEL_DATE").alias("FIRST_REQ_DEL_DATE")
    )
)

LIKP = (
    LIKP

    .join(_VBEP, ["ORDER_NUMBER", "ORDER_LINE"], "left")
)

LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join _VBEP 2) OK: {LIKP.count()} rows")


def extract_vbpa_using_number_line(self, target_name, parw_value):
    _vbpa = (
        VBPA_INPUT
        .where(col("PARVW") == lit(parw_value))
        .select(
            col("VBELN").alias("DESPATCHING_NOTE"),
            col("POSNR").alias("POSNR"),
            col("KUNNR").alias(target_name),
        )
    )

    return self.join(_vbpa, ["DESPATCHING_NOTE", "POSNR"], "left")

setattr(DataFrame, "extract_vbpa_using_number_line", extract_vbpa_using_number_line)

def extract_vbpa_using_number(self, target_name, parw_value):
    _vbpa = (
        VBPA_INPUT
        .where(col("PARVW") == lit(parw_value))
        .where(col("POSNR") == lit('000000'))
        .select(
            col("VBELN").alias("DESPATCHING_NOTE"),
            col("KUNNR").alias(target_name),
        )
    )

    return self.join(_vbpa, ["DESPATCHING_NOTE"], "left")

setattr(DataFrame, "extract_vbpa_using_number", extract_vbpa_using_number)


LIKP = (
    LIKP
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

    .extract_vbpa_using_number(target_name = "PICKING_CARRIER", parw_value = "xxx")   # ex SP
    #Applymap('LFA1',LIFNR,Null()) as PICKING_CARRIER_DESCR
    .do_mapping(LFA1_MAPPING, col("PICKING_CARRIER"), "PICKING_CARRIER_DESCR", mapping_name="LFA1_MAPPING")


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


LIKP = (
    LIKP

    .extract_vbpa_using_number_line(target_name = "k_SALE_DIRECTOR_POS", parw_value = "YA")
    .extract_vbpa_using_number(target_name = "k_SALE_DIRECTOR_HEADER", parw_value = "YA")

    .extract_vbpa_using_number_line(target_name = "k_RSM_POS", parw_value = "YC")
    .extract_vbpa_using_number(target_name = "k_RSM_HEADER", parw_value = "YC")
)

LIKP = LIKP.localCheckpoint()
print(f"LIKP (after join VBPA) OK: {LIKP.count()} rows")


DEL = (
    LIKP

    #where Applymap('PosServizio',ORDER_TYPE_CODE&'|'&PSTYV,0)=0
    .do_mapping(PosServizio, concat(col("ORDER_TYPE_CODE"), lit("|"), year(col("PSTYV"))), "_presence_PosServizio", default_value=lit(0), mapping_name="PosServizio")
    .where(col("_presence_PosServizio") == 0).drop("_presence_PosServizio")



    #if(Match(DOC_TYPE_CODE,5,6) or Match(ORDER_TYPE_CODE,'ZKA','ZKB'),0, "DEL QTY FABS"*MOLTIPLICATORE_RESO) as "DEL QTY",
    .withColumn(
        "DEL QTY",
        when((col("DOC_TYPE_CODE").isin([5, 6])) | (col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"])), lit(0))
        .otherwise(col("DEL QTY FABS") * col("MOLTIPLICATORE_RESO"))
    )

    #if(Match(ORDER_TYPE_CODE,'ZKA','ZKB')>0, "DEL QTY FABS"*MOLTIPLICATORE_RESO,0) as "DEL QTY CONSIGNMENT",
    .withColumn(
        "DEL QTY CONSIGNMENT",
        when(col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"]), col("DEL QTY FABS") * col("MOLTIPLICATORE_RESO")).otherwise(lit(0))
    )

    #"% DEL QTY FABS" as "% DEL QTY",
    .withColumn(
        "% DEL QTY",
        col("% DEL QTY FABS")
    )

    #if(Match(ORDER_TYPE_CODE,'ZKA','ZKB')=0,KNUMV&'|'&ORDER_LINE,NUll()) as k_PRICE_CONDITION,
    .withColumn(
        "k_PRICE_CONDITION",
        when(~col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"]), concat_ws("|", col("KNUMV"), col("ORDER_LINE"))).otherwise(lit(None))
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


    #Applymap('Company_Currency',COMPANY_ID,20) as CURRENCY_CODE,
    .do_mapping(Company_Currency_MAPPING, col("SALES_ORGANIZATION"), "CURRENCY_CODE", default_value=lit("EUR"), mapping_name="Company_Currency_MAPPING")

    #Applymap('Currency_Map',Applymap('Company_Currency',COMPANY_ID,20)&'|'&Year(DOCUMENT_DATE_ORIGINAL),1) as "EXCHANGE",
    .do_mapping(Currency_Map, concat(col("CURRENCY_CODE"), lit("|"), year(col("DOCUMENT_DATE_ORIGINAL"))), "EXCHANGE", default_value=lit(1), mapping_name="Currency_Map")
    .do_mapping(Currency_Map_Local, concat(col("CURRENCY_CODE"), lit("|"), year(col("DOCUMENT_DATE_ORIGINAL"))), "EXCHANGE_TO_LOCAL", default_value=lit(1), mapping_name="Currency_Map_To_Local")

    .do_mapping(COMMERCIAL_MAP_DATE, current_date(), "_time_id", default_value=lit(current_date()), mapping_name="COMMERCIAL_MAP_DATE")

    .withColumn("TIME_ID", when(col("ORDER_TYPE_CODE").isin(["ZKA", "ZKB"]), to_date("DESPATCHING_DATE")).otherwise(col("_time_id")))
    .withColumn("TIME_ID_FACT", col("TIME_ID"))
    .withColumn("DOCUMENT_DATE", col("TIME_ID"))

    .drop("_time_id")
)

DEL = DEL.localCheckpoint()
print(f"DEL OK: {DEL.count()} rows")

_PRCD_ELEMENTS = (
    PRCD_ELEMENTS
    .withColumn("k_PRICE_CONDITION", concat_ws("|", col("KNUMV"), col("KPOSN")))
    .withColumn("EXCHANGE_RATE_SAP", when(col("KKURS") < 0, 1 / abs(col("KKURS"))).otherwise(col("KKURS")))
    .withColumn("KWERT_EXCHANGE_RATE_SAP", col("KWERT") * col("EXCHANGE_RATE_SAP"))
)

PRCD_ELEMENTS_BASE = (
    _PRCD_ELEMENTS
    .select("k_PRICE_CONDITION").distinct()
).cache()


# VAT_PERCENTAGE_1, VAT_AMOUNT_1_TOT, VAT_REASON_CODE
_VAT = (
    _PRCD_ELEMENTS
    .where(col("KSCHL") == 'MWST')
    .select(
        "k_PRICE_CONDITION",
        col("KBETR").alias("VAT_PERCENTAGE_1"),
        col("KWERT").alias("VAT_AMOUNT_1_TOT"),
        col("MWSK1").alias("VAT_REASON_CODE")
    )
)

# DEL NSV LC TOT, EXCHANGE_RATE_SAP
_NSV = (
    _PRCD_ELEMENTS
    .where(col("KSCHL").isin('ZNSV', 'ZIV1', 'ZIV2'))
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("DEL NSV LC TOT"),
        "EXCHANGE_RATE_SAP"
    )
)

# DEL IC DISCOUNT/SURCHARGE
_DISCOUNT_SURCHARGE = (
    _PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZCDS')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT").alias("DEL IC DISCOUNT/SURCHARGE")
    )
)

# DEL RAEE LC
_RAEE = (
    _PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZWEE')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT").alias("DEL RAEE LC")
    )
)

# DEL GSV LC TOT
_GSV = (
    _PRCD_ELEMENTS
    .where(col("KSCHL").isin('ZTTV', 'ZIV1', 'ZIV2'))
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("DEL GSV LC TOT")
    )
)

# LIST_PRICE_TOT
_LIST_PRICE = (
    _PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZNET')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("LIST_PRICE_TOT")
    )
)

# FORCED_PRICE_TOT, PRICE_FORCED
_FORCED_PRICE = (
    _PRCD_ELEMENTS
    .where(col("KSCHL") == 'PB00')
    .select(
        "k_PRICE_CONDITION",
        when(col("KWERT") > 0, lit(1)).otherwise(lit(0)).alias("PRICE_FORCED"),
        col("KWERT_EXCHANGE_RATE_SAP").alias("FORCED_PRICE_TOT")
    )
)

# removable charges tot
_REMOVABLE_CHARGES = (
    _PRCD_ELEMENTS
    .where(col("KSCHL") == 'ZFEE')
    .select(
        "k_PRICE_CONDITION",
        col("KWERT_EXCHANGE_RATE_SAP").alias("removable charges tot")
    )
)

# Join all DataFrames to DEL DataFrame
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

DEL = (
    DEL

    .join(PRCD_ELEMENTS_ALL, ["k_PRICE_CONDITION"], "left")
)


DEL = DEL.localCheckpoint()
print(f"DEL (after join PRCD_ELEMENTS_ALL) OK: {LIKP.count()} rows")

DEL = (
    DEL
    .join(CDHDR_CDPOS_BLOCK, ["ORDER_NUMBER"], "left")
    .join(CDHDR_CDPOS_UNBLOCK, ["ORDER_NUMBER"], "left")
)

#DEL = DEL.localCheckpoint()
print(f"DEL (after join CDHDR) OK: {DEL.count()} rows")

needed_keys_code_network_product_year = (
    DEL
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
    DEL
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
    DEL
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

#stdindef_tca.show()
DEL_WITH_COST = (
    DEL#.select("company_haier", "network_haier", "product_code", "DOCUMENT_DATE")

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

DEL_WITH_COST.localCheckpoint()
print(f"DEL_WITH_COST OK: {DEL_WITH_COST.count()} rows")

current_month_start = date_trunc('month', lit(current_date()))


DEL_CALCOLI = (
    DEL_WITH_COST

    #"EXCHANGE" as CURRENCY_EURO_EXCH,
    .withColumn("CURRENCY_EURO_EXCH", col("EXCHANGE"))


    #REMOVABLE_CHARGES_TOT*"% DEL QTY"/"DEL QTY" as REMOVABLE_CHARGES,
    .withColumn("removable charges", col("removable charges tot") * col("% DEL QTY"))

    .withColumn("INV_CUSTOMER_KEY", concat_ws("|", col("INV_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .withColumn("DEST_CUSTOMER_KEY", concat_ws("|", col("DEST_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .withColumn("SALES_GROUP_KEY", concat_ws("|", col("DEST_ID"), col("SALES_ORGANIZATION"), col("DISTR_CHANNEL"), col("DIVISION")))
    .do_mapping(KVGR5_MAPPING, col("INV_CUSTOMER_KEY"), "CUSTOMER_TYPE_CODE", mapping_name="KVGR5_MAPPING")
    .do_mapping(KVGR5_MAPPING, col("DEST_CUSTOMER_KEY"), "DEST_CUSTOMER_TYPE_CODE", mapping_name="KVGR5_MAPPING")
    .do_mapping(KVGR3_MAPPING, col("INV_CUSTOMER_KEY"), "CUSTOMER_TYPE_CODE_LEV3", mapping_name="KVGR3_MAPPING")
    .do_mapping(KVGR3_MAPPING, col("DEST_CUSTOMER_KEY"), "DEST_CUSTOMER_TYPE_CODE_LEV3", mapping_name="KVGR3_MAPPING")
    .do_mapping(TVGRT_MAPPING, col("SALES_GROUP_KEY"), "SALES GROUP", mapping_name="TVGRT_MAPPING")

    .do_mapping(KNA1_MAPPING, col("PURCHASE_GROUP_CODE"), "PURCHASE_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("MAIN_GROUP_CODE"), "MAIN_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SUPER_GROUP_CODE"), "SUPER_GROUP_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALE_ZONE_CODE"), "SALE_ZONE_DESCR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALESMAN_CODE"), "SALESMAN_NAME", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("SALE_DIRECTOR_CODE"), "SALE_DIRECTOR", mapping_name="KNA1")
    .do_mapping(KNA1_MAPPING, col("RSM_CODE"), "RSM", mapping_name="KNA1")

    #1 as "FLAG SAP"
    .withColumn("FLAG SAP", lit(1))


    #TRANSPORT_COST_UNIT as TRANSPORT_COST,
    .withColumn("TRANSPORT_COST", col("TRANSPORT_COST_UNIT")*col("EXCHANGE_TO_LOCAL"))
    #TRANSPORT_COST_INFRA_UNIT as TRANSPORT_COST_INFRA,
    .withColumn("TRANSPORT_COST_INFRA", col("TRANSPORT_COST_INFRA_UNIT")*col("EXCHANGE_TO_LOCAL"))
    #(CONS_COST_OF_SALES_UNIT+TCA_UNIT+TRANSPORT_COST_UNIT+TRANSPORT_COST_INFRA_UNIT)*"DEL QTY" as "DEL CP LC",
    .withColumn("DEL CP LC", (col("CONS_COST_OF_SALES_UNIT") + col("TCA_UNIT") + col("TRANSPORT_COST_UNIT") + col("TRANSPORT_COST_INFRA_UNIT"))*col("EXCHANGE_TO_LOCAL") * col("DEL QTY"))
    #(COMM_COST_OF_SALES_UNIT+TRANSPORT_COST_UNIT)*"DEL QTY" as "DEL TP LC",
    .withColumn("DEL TP LC", (col("COMM_COST_OF_SALES_UNIT") + col("TRANSPORT_COST_UNIT"))*col("EXCHANGE_TO_LOCAL") * col("DEL QTY"))
    #CONS_COST_OF_SALES_UNIT as CONS_COST_OF_SALES,
    .withColumn("CONS_COST_OF_SALES", col("CONS_COST_OF_SALES_UNIT")*col("EXCHANGE_TO_LOCAL"))
    #COMM_COST_OF_SALES_UNIT as COMM_COST_OF_SALES,
    .withColumn("COMM_COST_OF_SALES", col("COMM_COST_OF_SALES_UNIT")*col("EXCHANGE_TO_LOCAL"))
    #TCA_UNIT as TCA,
    .withColumn("TCA", col("TCA_UNIT")*col("EXCHANGE_TO_LOCAL"))



    #[POTENTIAL STATUS] as "ORDER STATUS",
    .withColumn("ORDER STATUS", col("POTENTIAL STATUS"))
    #IF(DATA_TYPE=2,ORDER_REQUESTED_DELIVERY_DATE,IF(DATA_TYPE=4,DOCUMENT_DATE_ORIGINAL,MAKEDATE(1900,1,1))) as MRDD,
    .withColumn(
        "MRDD",
        when(col("DATA_TYPE") == 2, col("ORDER_REQUESTED_DELIVERY_DATE"))
        .when(col("DATA_TYPE") == 4, col("DOCUMENT_DATE_ORIGINAL"))
        .otherwise(to_date(lit("1900-01-01")))
    )
    #ORDER_BLOCKING_DESCR as "ORDER BLOCKING",
    .withColumn("ORDER BLOCKING", col("ORDER_BLOCKING_DESCR"))
    #("DEL NSV LC TOT"+Alt("DEL IC DISCOUNT/SURCHARGE"*Alt(EXCHANGE_RATE_SAP,1)*MOLTIPLICATORE_RESO,0))/"ORD QTY ORIG"*"DEL QTY" as "DEL NSV LC",
    .withColumn("DEL NSV LC", (col("DEL NSV LC TOT") + coalesce(col("DEL IC DISCOUNT/SURCHARGE") * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)) * col("MOLTIPLICATORE_RESO"), lit(0))) / col("ORD QTY ORIG") * col("DEL QTY"))
    #("DEL GSV LC TOT"+Alt("DEL IC DISCOUNT/SURCHARGE"*Alt(EXCHANGE_RATE_SAP,1)*MOLTIPLICATORE_RESO,0))/"ORD QTY ORIG"*"DEL QTY" as "DEL GSV LC",
    .withColumn("DEL GSV LC", (col("DEL GSV LC TOT") + coalesce(col("DEL IC DISCOUNT/SURCHARGE") * coalesce(col("EXCHANGE_RATE_SAP"), lit(1)) * col("MOLTIPLICATORE_RESO"), lit(0))) / col("ORD QTY ORIG") * col("DEL QTY"))
    #"LIST_PRICE TOT"/"ORD QTY ORIG" as LIST_PRICE,

    .withColumn("DEL IC DISCOUNT/SURCHARGE", coalesce(col("DEL IC DISCOUNT/SURCHARGE"), lit(0)))
    .withColumn("DEL IC DISCOUNT/SURCHARGE LC", col("DEL IC DISCOUNT/SURCHARGE") * col("EXCHANGE_RATE_SAP"))
    .withColumn("DEL IC DISCOUNT/SURCHARGE €", coalesce(col("DEL IC DISCOUNT/SURCHARGE LC"), lit(0)) * col("EXCHANGE"))
    .withColumn("LIST_PRICE", col("LIST_PRICE_TOT") / col("ORD QTY ORIG"))
    #FORCED_PRICE_TOT/"ORD QTY ORIG" as FORCED_PRICE,
    .withColumn("FORCED_PRICE", col("FORCED_PRICE_TOT") / col("ORD QTY ORIG"))

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

   .withColumn("DEL NSV €", col("DEL NSV LC") * col("EXCHANGE"))
    .withColumn("DEL GSV €", col("DEL GSV LC") * col("EXCHANGE"))
    .withColumn("DEL TP €", col("DEL TP LC") * col("EXCHANGE"))
    .withColumn("DEL CP €", col("DEL CP LC") * col("EXCHANGE"))
    .withColumn("NET_PRICE", coalesce(col("FORCED_PRICE"), lit(0)) + coalesce(col("LIST_PRICE"), lit(0)))


    #ORDER_NUMBER as "ORDER NUMBER",
    .withColumn("ORDER NUMBER", col("ORDER_NUMBER"))
    #"DEL QTY" as "INV + DEL QTY",
    .withColumn("INV + DEL QTY", col("DEL QTY"))
    #"DEL QTY" as "POT QTY",
    .withColumn("POT QTY", col("DEL QTY"))
    #"DEL NSV €" as "INV + DEL NSV €",
    .withColumn("INV + DEL NSV €", col("DEL NSV €"))
    #"DEL NSV LC" as "INV + DEL NSV LC",
    .withColumn("INV + DEL NSV LC", col("DEL NSV LC"))
    #"DEL NSV €" as "POT NSV €",
    .withColumn("POT NSV €", col("DEL NSV €"))
    #"DEL NSV LC" as "POT NSV LC",
    .withColumn("POT NSV LC", col("DEL NSV LC"))
    #"DEL GSV €" as "INV + DEL GSV €",
    .withColumn("INV + DEL GSV €", col("DEL GSV €"))
    #"DEL GSV LC" as "INV + DEL GSV LC",
    .withColumn("INV + DEL GSV LC", col("DEL GSV LC"))
    #"DEL GSV €" as "POT GSV €",
    .withColumn("POT GSV €", col("DEL GSV €"))
    #"DEL GSV LC" as "POT GSV LC",
    .withColumn("POT GSV LC", col("DEL GSV LC"))

    #if(Alt("DEL NSV €",0)=0,0,"DEL NSV €"-(Alt("DEL TP €",0))) as "DEL GM TP €",
    .withColumn("DEL GM TP €", when(coalesce(col("DEL NSV €"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV €") - coalesce(col("DEL TP €"), lit(0))))
    #if(Alt("DEL NSV LC",0)=0,0,"DEL NSV LC"-(Alt("DEL TP LC",0))) as "DEL GM TP LC",
    .withColumn("DEL GM TP LC", when(coalesce(col("DEL NSV LC"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV LC") - coalesce(col("DEL TP LC"), lit(0))))
    #if(Alt("DEL NSV €",0)=0,0,"DEL NSV €"-(Alt("DEL CP €",0))) as "DEL GM CP €",
    .withColumn("DEL GM CP €", when(coalesce(col("DEL NSV €"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV €") - coalesce(col("DEL CP €"), lit(0))))
    #if(Alt("DEL NSV LC",0)=0,0,"DEL NSV LC"-(Alt("DEL CP LC",0))) as "DEL GM CP LC",
    .withColumn("DEL GM CP LC", when(coalesce(col("DEL NSV LC"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV LC") - coalesce(col("DEL CP LC"), lit(0))))

    #if(Alt("DEL NSV €",0)=0,0,"DEL NSV €"-(Alt("DEL TP €",0))) as "INV + DEL GM TP €",
    .withColumn("INV + DEL GM TP €", when(coalesce(col("DEL NSV €"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV €") - coalesce(col("DEL TP €"), lit(0))))
    #if(Alt("DEL NSV LC",0)=0,0,"DEL NSV LC"-(Alt("DEL TP LC",0))) as "INV + DEL GM TP LC",
    .withColumn("INV + DEL GM TP LC", when(coalesce(col("DEL NSV LC"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV LC") - coalesce(col("DEL TP LC"), lit(0))))
    #if(Alt("DEL NSV €",0)=0,0,"DEL NSV €"-(Alt("DEL CP €",0))) as "INV + DEL GM CP €",
    .withColumn("INV + DEL GM CP €", when(coalesce(col("DEL NSV €"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV €") - coalesce(col("DEL CP €"), lit(0))))
    #if(Alt("DEL NSV LC",0)=0,0,"DEL NSV LC"-(Alt("DEL CP LC",0))) as "INV + DEL GM CP LC",
    .withColumn("INV + DEL GM CP LC", when(coalesce(col("DEL NSV LC"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV LC") - coalesce(col("DEL CP LC"), lit(0))))

    #if(Alt("DEL NSV €",0)=0,0,"DEL NSV €"-(Alt("DEL TP €",0))) as "POT GM TP €",
    .withColumn("POT GM TP €", when(coalesce(col("DEL NSV €"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV €") - coalesce(col("DEL TP €"), lit(0))))
    #if(Alt("DEL NSV LC",0)=0,0,"DEL NSV LC"-(Alt("DEL TP LC",0))) as "POT GM TP LC",
    .withColumn("POT GM TP LC", when(coalesce(col("DEL NSV LC"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV LC") - coalesce(col("DEL TP LC"), lit(0))))
    #if(Alt("DEL NSV €",0)=0,0,"DEL NSV €"-(Alt("DEL CP €",0))) as "POT GM CP €",
    .withColumn("POT GM CP €", when(coalesce(col("DEL NSV €"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV €") - coalesce(col("DEL CP €"), lit(0))))
    #if(Alt("DEL NSV LC",0)=0,0,"DEL NSV LC"-(Alt("DEL CP LC",0))) as "POT GM CP LC",
    .withColumn("POT GM CP LC", when(coalesce(col("DEL NSV LC"), lit(0)) == 0, lit(0)).otherwise(col("DEL NSV LC") - coalesce(col("DEL CP LC"), lit(0))))

    #.withColumn("COMPANY_CODE", col("COMPANY"))
    #.withColumn("NETWORK_CODE", col("NETWORK"))

    .withColumn("DEL RAEE LC", coalesce(col("DEL RAEE LC"), lit(0)))
    .withColumn("DEL RAEE €", col("DEL RAEE LC") * col("EXCHANGE"))


    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    .distinct()

    .withColumn("key_fact", concat_ws("###", lit("DELIVERY"), col("DESPATCHING_NOTE"), col("POSNR")))
    .assert_no_duplicates("key_fact")
)


DEL_CALCOLI = DEL_CALCOLI.columns_to_lower().localCheckpoint()
print(f"DEL_CALCOLI OK: {DEL_CALCOLI.count()} rows")


##### Save Golden Sources
list_sap_cols_del = ["order_number", "order_line", "despatching_note", "posnr", "data_type", "type", "despatching_date", "shipment_code", "picking_date", "conf_del_date", "routing", "booking_status", "appointment", "booking_slot", "shipping type", "product_code", "area_code", "original del qty", "del qty fabs", "sales_office", "picking_number", "return_reason", "order_customer_reference_1", "sap_order_block_delivery", "knumv", "sales_organization", "division", "distr_channel", "category", "company", "network", "plant", "shipping point", "pstyv", "confirmed date", "confirmed time", "planned_dispatch_date", "first_req_del_date", "picking_carrier", "del qty", "del qty consignment", "dest_id", "inv_id", "main_group_code", "purchase_group_code", "rsm_code", "sale_director_code", "sale_zone_code", "salesman_code", "super_group_code", "currency_code", "exchange", "time_id", "document_date", "exchange_rate_sap", "del raee lc", "list_price_tot", "key_cost_commercial", "key_cost_freight_in", "key_cost_transfer_price", "key_cost_stdindef_tca", "del ic discount/surcharge lc", "del ic discount/surcharge €", "del raee €", "del nsv lc", "del nsv €", "del gsv lc", "del gsv €", "removable charges", "key_fact"]
ord_gs = (
    DEL_CALCOLI
    .select(list_sap_cols_del)
    .save_data_to_redshift_table("dwa.sap_del_ft_delivery")
)

##### Save remaining cols
# DEL_CALCOLI.columns \ list_sap_cols_del U {key_fact}
list_dims_cols_del = list(set(DEL_CALCOLI.columns).difference(list_sap_cols_del))+['key_fact']

ord_dim = (
    DEL_CALCOLI
    .select(list_dims_cols_del)
    .save_data_to_redshift_table("dwa.sap_del_dim_delivery")
)