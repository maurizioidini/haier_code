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






INVOICE_CUSTOMERS = (
  read_data_from_redshift_table("datalake_ods.oracle_dwh_dm_customers")    # todo FROM [lib://Project_Files/Framework_HORSA\Projects\Anagrafiche\Data\PROD\QVD01/DM_CUSTOMERS.QVD] //CAS

  .select(
    #CUSTOMER_ID AS _Invoice_Customers.Key,
    col("CUSTOMER_ID").alias("INV_ID"), col("CUSTOMER_CODE"), col("CUSTOMER_NAME"), col("CUSTOMER_NAME2"), col("CUSTOMER_SHORT_NAME"), col("ADDRESS"), col("ADDRESS2"), col("POSTAL_CODE"), col("TOWN"),
    col("TOWN2"), col("COUNTRY_CODE"), col("COUNTRY_DESCR"), col("COUNTRY_SIGN"), col("COUNTRY_ISO_CODE"), col("COUNTRY_CURRENCY"), col("PROVINCE_CODE"), col("PROVINCE_DESCR"), col("TOWN_CODE"),
    col("TOWN_DESCR"), col("CUSTOMER_ISO_CODE"), col("CUSTOMER_ISO_DESC"), col("CUSTOMER_ISO_CURRENCY"), col("IT_VAT_CODE"), col("VAT_CODE"), col("CANDY_CUST_GROUP"), col("SUPPLIER"),
    col("CUSTOMER_AT_CUSTOMER"), col("CUSTOMER_CURRENCY_CODE"), col("CUSTOMER_CURRENCY_SIGN"), col("CURRENCY_EURO_EXCHANGE"), col("CURRENCY_DEC_DIGITS"), col("INVOICE_IN_EURO"), col("VAT_REASON_CODE"),
    col("VAT_PERC_VALUE"), col("VAT_DESCR"), col("CUSTOMER_CLASS_CODE"), col("CUSTOMER_CLASS_DESCR"), col("CUSTOMER_TYPE_CODE"), col("CUSTOMER_TYPE_DESCR"), col("PURCHASE_GROUP_CODE"), col("PURCHASE_GROUP_DESCR"),
    col("SUPER_GROUP_CODE"),
    coalesce(col("SUPER_GROUP_DESCR"), lit("0")).alias("SUPER_GROUP_DESCR"),
    col("MAIN_GROUP_CODE"), col("MAIN_GROUP_DESCR"), col("SUB_GROUP_CODE"), col("CUSTOMER_STATUS_CODE"), col("CUSTOMER_STATUS_DESCR"),
    to_date(col("CUSTOMER_STATUS_DATE")).alias("CUSTOMER_STATUS_DATE"),
    col("CUSTOMER_WAREHOUSE_CODE"), col("CUSTOMER_WAREHOUSE_DESCR"), col("CUSTOMER_WAREHOUSE_TYPE"), col("CUSTOMER_TRAVEL_WAREHOUSE"), col("CUSTOMER_WAREHOUSE_DOC"), col("CUSTOMER_WAREHOUSE_COMPANY"),
    col("CUSTOMER_WAREHOUSE_SUPPLIER"), col("NEW_CUSTOMER_CODE"), col("ADDRESS_DELIVERY"), col("SUPPLIER_CUSTOMER"), col("VIP_CUSTOMER"), col("POTENTIAL_BUILD_IN"), col("POTENTIAL_FREE_STD"), col("POTENTIAL_FLOORCARE"),
    to_date(col("CUSTOMER_CREATION_DATE")).alias("CUSTOMER_CREATION_DATE"),
    to_date(col("CUSTOMER_UPDATE_DATE")).alias("CUSTOMER_UPDATE_DATE"),
    col("CUSTOMER_COMPANY_TYPE"), col("REGION"), col("SUPER_MAIN_GROUP_CODE"), col("CHANNEL"), col("DIRECT_WH"), col("CREDIT_CONTROL_CODE"), col("CREDIT_CONTROL_DESCR"), col("ADDRESS_EMAIL"), col("COMM_TELEPHONE"),
    col("PERIOD"), col("INTER_FLAG"), col("DEMERGE_FLAG"), col("TAX_OFFICE"), col("CUSTOMER_NAME_ORIG"),
    concat(col("SUPER_GROUP_CODE"), lit(' - '), col("SUPER_GROUP_DESCR")).alias("SUPER GROUP"),
    concat(col("MAIN_GROUP_CODE"), lit(' - '), col("MAIN_GROUP_DESCR")).alias("MAIN GROUP"),
    concat(col("PURCHASE_GROUP_CODE"), lit(' - '), col("PURCHASE_GROUP_DESCR")).alias("PURCHASE GROUP"),
    concat(col("CUSTOMER_CODE"), lit('-'), col("CUSTOMER_NAME")).alias("INVOICE CUSTOMER"),
    col("CUSTOMER_COMPANY"),
    col("SAP_CODE"),
  )
)


INVOICE_CUSTOMERS_TEMP = (
    read_data_from_redshift_table("datalake_ods.oracle_dwh_dm_customers")    # todo FROM [lib://Project_Files/Framework_HORSA\Projects\Anagrafiche\Data\PROD\QVD01/DM_CUSTOMERS.QVD] //CAS

    .select(
        #CUSTOMER_COMPANY&'-@-'&CUSTOMER_CODE AS _Invoice_Customers.Key,
        concat(col("CUSTOMER_COMPANY"), lit('-@-'), col("CUSTOMER_CODE")).alias("INV_ID"),
        col("CUSTOMER_CODE"), col("CUSTOMER_NAME"), col("CUSTOMER_NAME2"), col("CUSTOMER_SHORT_NAME"), col("ADDRESS"), col("ADDRESS2"), col("POSTAL_CODE"), col("TOWN"), col("TOWN2"), col("COUNTRY_CODE"),
        col("COUNTRY_DESCR"), col("COUNTRY_SIGN"), col("COUNTRY_ISO_CODE"), col("COUNTRY_CURRENCY"), col("PROVINCE_CODE"), col("PROVINCE_DESCR"), col("TOWN_CODE"), col("TOWN_DESCR"), col("CUSTOMER_ISO_CODE"),
        col("CUSTOMER_ISO_DESC"), col("CUSTOMER_ISO_CURRENCY"), col("IT_VAT_CODE"), col("VAT_CODE"), col("CANDY_CUST_GROUP"), col("SUPPLIER"), col("CUSTOMER_AT_CUSTOMER"), col("CUSTOMER_CURRENCY_CODE"),
        col("CUSTOMER_CURRENCY_SIGN"), col("CURRENCY_EURO_EXCHANGE"), col("CURRENCY_DEC_DIGITS"), col("INVOICE_IN_EURO"), col("VAT_REASON_CODE"), col("VAT_PERC_VALUE"), col("VAT_DESCR"),
        col("CUSTOMER_CLASS_CODE"), col("CUSTOMER_CLASS_DESCR"), col("CUSTOMER_TYPE_CODE"), col("CUSTOMER_TYPE_DESCR"), col("PURCHASE_GROUP_CODE"), col("PURCHASE_GROUP_DESCR"), col("SUPER_GROUP_CODE"),
        coalesce(col("SUPER_GROUP_DESCR"), lit("0")).alias("SUPER_GROUP_DESCR"),
        col("MAIN_GROUP_CODE"), col("MAIN_GROUP_DESCR"), col("SUB_GROUP_CODE"), col("CUSTOMER_STATUS_CODE"), col("CUSTOMER_STATUS_DESCR"), col("CUSTOMER_STATUS_DATE"), col("CUSTOMER_WAREHOUSE_CODE"),
        col("CUSTOMER_WAREHOUSE_DESCR"), col("CUSTOMER_WAREHOUSE_TYPE"), col("CUSTOMER_TRAVEL_WAREHOUSE"), col("CUSTOMER_WAREHOUSE_DOC"), col("CUSTOMER_WAREHOUSE_COMPANY"), col("CUSTOMER_WAREHOUSE_SUPPLIER"),
        col("NEW_CUSTOMER_CODE"), col("ADDRESS_DELIVERY"), col("SUPPLIER_CUSTOMER"), col("VIP_CUSTOMER"), col("POTENTIAL_BUILD_IN"), col("POTENTIAL_FREE_STD"), col("POTENTIAL_FLOORCARE"), col("CUSTOMER_CREATION_DATE"),
        col("CUSTOMER_UPDATE_DATE"), col("CUSTOMER_COMPANY_TYPE"), col("REGION"), col("SUPER_MAIN_GROUP_CODE"), col("CHANNEL"), col("DIRECT_WH"), col("CREDIT_CONTROL_CODE"), col("CREDIT_CONTROL_DESCR"),
        col("ADDRESS_EMAIL"), col("COMM_TELEPHONE"), col("PERIOD"), col("INTER_FLAG"), col("DEMERGE_FLAG"), col("TAX_OFFICE"), col("CUSTOMER_NAME_ORIG"),
        concat(col("SUPER_GROUP_CODE"), lit(' - '), col("SUPER_GROUP_DESCR")).alias("SUPER GROUP"),
        concat(col("MAIN_GROUP_CODE"), lit(' - '), col("MAIN_GROUP_DESCR")).alias("MAIN GROUP"),
        concat(col("PURCHASE_GROUP_CODE"), lit(' - '), col("PURCHASE_GROUP_DESCR")).alias("PURCHASE GROUP"),
        concat(col("CUSTOMER_CODE"), lit('-'), col("CUSTOMER_NAME")).alias("INVOICE CUSTOMER"),
        col("CUSTOMER_COMPANY"),
        col("SAP_CODE"),
        lit("").alias("SOURCE")
    )
)
INVOICE_CUSTOMERS = INVOICE_CUSTOMERS.unionByName(INVOICE_CUSTOMERS_TEMP, allowMissingColumns=True)


INVOICE_CUSTOMERS = INVOICE_CUSTOMERS.unionByName(
    spark.createDataFrame([{"INV_ID": "NOT DEFINED"}])
    , allowMissingColumns=True
)


'''
MAPPING_VAT_SUPERGROUPCODE = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["IT_VAT_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("IT_VAT_CODE"),
        col("SUPER_GROUP_CODE"),
    )
)

MAPPING_VAT_SUPERGROUPDESCR = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["IT_VAT_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("IT_VAT_CODE"),
        col("SUPER_GROUP_DESCR"),
    )
)


MAPPING_VAT_MAINGROUPCODE = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["IT_VAT_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("IT_VAT_CODE"),
        col("MAIN_GROUP_CODE"),
    )
)


MAPPING_VAT_MAINGROUPDESCR = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["IT_VAT_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("IT_VAT_CODE"),
        col("MAIN_GROUP_DESCR"),
    )
)



MAPPING_VAT_PURCHASEGROUPCODE = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["IT_VAT_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("IT_VAT_CODE"),
        col("PURCHASE_GROUP_CODE"),
    )
)



MAPPING_VAT_PURCHASEGROUPDESCR = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["IT_VAT_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("IT_VAT_CODE"),
        col("PURCHASE_GROUP_DESCR"),
    )
)


MAPPING_COUNTRY = (
    INVOICE_CUSTOMERS

    .drop_duplicates_ordered(keyCols=["COUNTRY_ISO_CODE"], orderCols=[col("CUSTOMER_UPDATE_DATE").desc()])
    .select(
        col("COUNTRY_ISO_CODE"),
        col("COUNTRY_DESCR"),
    )
)

Country = (
    spark.createDataFrame([{"LAND1": "", "LANDX": ""}]) # FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/PROD/QVD01/T005T.qvd](qvd);

    .select(
        col("LAND1"),
        col("LANDX"),
    )
).assert_no_duplicates("LAND1")

PROV_DESCR = (
    spark.createDataFrame([{"LAND1": "", "LANDX": "", "BLAND": "", "SPRAS": ""}]) # FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/PROD/QVD01/T005U.qvd](qvd)

    .where(col("SPRAS") == "E")
    .select(
        concat(col("LAND1"), lit("|"), col("BLAND")).alias("KEY"),
        col("LANDX"),
    )
).assert_no_duplicates("KEY")


  # todo bisogna convertire nomi inglesi -> tedeschi (cds view in inglese)
CUSTOMER_SAP = (
    read_data_from_redshift_table("mtd.sap_dm_i_customer_kna1") # FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/PROD/QVD01/KNA1.qvd]

    .do_mapping(PROV_DESCR, concat(col("LAND1"), lit("|"), col("REGIO")), "PROVINCE_DESCR")
    .select(
        col("KUNNR").alias("INV_ID"),
        col("KUNNR").alias("CUSTOMER_ID"),
        col("KUNNR").alias("CUSTOMER_CODE"),
        col("NAME1").alias("CUSTOMER_NAME"),
        col("NAME2").alias("CUSTOMER_NAME2"),
        col("SORTL").alias("CUSTOMER_SHORT_NAME"),
        col("STRAS").alias("ADDRESS"),
        col("ADRNR").alias("k_ADRNR"),
        col("PSTLZ").alias("POSTAL_CODE"),
        col("ORT01").alias("TOWN"),
        col("LAND1").alias("COUNTRY_ISO_CODE"),
        col("REGIO").alias("PROVINCE_CODE"),
        col("PROVINCE_DESCR"),
        col("CITYC").alias("TOWN_CODE"),
        col("STCEG").alias("IT_VAT_CODE"),
        col("STCEG").alias("VAT_CODE"),
        col("LIFNR").alias("SUPPLIER"),
        col("UWAER").alias("CUSTOMER_CURRENCY_CODE"),
        col("WERKS").alias("CUSTOMER_WAREHOUSE_CODE"),
        col("ERDAT").alias("CUSTOMER_CREATION_DATE"),
        concat(col("KUNNR"), lit('-'), col("NAME1")).alias("INVOICE CUSTOMER"),
        lit('SAP').alias("SOURCE")
    )
)

TEMP = (
    spark.createDataFrame([{}])  # FROM [lib://Project_Files_Production/Framework_HORSA/Projects/SAP/Data/PROD/QVD01/ADRC.qvd]

    .do_mapping("Country", col("COUNTRY"), "COUNTRY_DESCR")

    .select(
        col("ADDRNUMBER").alias("k_ADRNR"),
        col("CITY2").alias("TOWN2"),
        col("COUNTRY").alias("COUNTRY_CODE"),
        col("COUNTRY_DESCR")
    )
)

CUSTOMER_SAP = (
  CUSTOMER_SAP
  #.join( TEMP, "k_ADRNR", "left")
)


INVOICE_CUSTOMERS = INVOICE_CUSTOMERS.unionByName(CUSTOMER_SAP, allowMissingColumns=True)

del CUSTOMER_SAP'''

INVOICE_CUSTOMERS.save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_invoice_customers", truncate=True)




DELIVERY_CUSTOMERS = (
    INVOICE_CUSTOMERS

    .select(
        col("INV_ID").alias("DEST_ID"),
        col("CUSTOMER_CODE").alias("DEST_CODE"),
        col("CUSTOMER_NAME").alias("DEST_NAME"),
        col("CUSTOMER_NAME2").alias("DEST_NAME2"),
        col("CUSTOMER_SHORT_NAME").alias("DEST_SHORT_NAME"),
        col("ADDRESS").alias("DEST_ADDRESS"),
        col("ADDRESS2").alias("DEST_ADDRESS2"),
        col("POSTAL_CODE").alias("DEST_POST_CODE"),
        col("TOWN").alias("DEST_TOWN"),
        col("TOWN2").alias("DEST_TOWN2"),
        col("COUNTRY_CODE").alias("DEST_COUNTRY_CODE"),
        col("COUNTRY_DESCR").alias("DEST_COUNTRY_DESCR"),
        col("COUNTRY_SIGN").alias("DEST_COUNTRY_SIGN"),
        col("COUNTRY_ISO_CODE").alias("DEST_COUNTRY_ISO_CODE"),
        col("COUNTRY_CURRENCY").alias("DEST_COUNTRY_CURRENCY"),
        col("PROVINCE_CODE").alias("DEST_PROVINCE_CODE"),
        col("PROVINCE_DESCR").alias("DEST_PROVINCE_DESCR"),
        col("TOWN_CODE").alias("DEST_TOWN_CODE"),
        col("TOWN_DESCR").alias("DEST_TOWN_DESCR"),
        col("CUSTOMER_ISO_CODE").alias("DEST_CUSTOMER_ISO_CODE"),
        col("CUSTOMER_ISO_DESC").alias("DEST_CUSTOMER_ISO_DESC"),
        col("CUSTOMER_ISO_CURRENCY").alias("DEST_CUSTOMER_ISO_CURRENCY"),
        col("VAT_CODE").alias("DEST_VAT_CODE"),
        col("IT_VAT_CODE").alias("DEST_IT_VAT_CODE"),
        col("CANDY_CUST_GROUP").alias("DEST_CANDY_CUST_GROUP"),
        col("SUPPLIER").alias("DEST_SUPPLIER"),
        col("CUSTOMER_AT_CUSTOMER").alias("DEST_CUSTOMER_AT_CUSTOMER"),
        col("CUSTOMER_CURRENCY_CODE").alias("DEST_CUSTOMER_CURRENCY_CODE"),
        col("CUSTOMER_CURRENCY_SIGN").alias("DEST_CUSTOMER_CURRENCY_SIGN"),
        col("CURRENCY_EURO_EXCHANGE").alias("DEST_CURRENCY_EURO_EXCHANGE"),
        col("CURRENCY_DEC_DIGITS").alias("DEST_CURRENCY_DEC_DIGITS"),
        col("INVOICE_IN_EURO").alias("DEST_INVOICE_IN_EURO"),
        col("VAT_REASON_CODE").alias("DEST_VAT_REASON_CODE"),
        col("VAT_PERC_VALUE").alias("DEST_VAT_PERC_VALUE"),
        col("VAT_DESCR").alias("DEST_VAT_DESCR"),
        col("CUSTOMER_CLASS_CODE").alias("DEST_CUSTOMER_CLASS_CODE"),
        col("CUSTOMER_CLASS_DESCR").alias("DEST_CUSTOMER_CLASS_DESCR"),
        col("CUSTOMER_TYPE_CODE").alias("DEST_CUSTOMER_TYPE_CODE"),
        col("CUSTOMER_TYPE_DESCR").alias("DEST_CUSTOMER_TYPE_DESCR"),
        col("PURCHASE_GROUP_CODE").alias("DEST_PURCHASE_GROUP_CODE"),
        col("PURCHASE_GROUP_DESCR").alias("DEST_PURCHASE_GROUP_DESCR"),
        col("SUPER_GROUP_CODE").alias("DEST_SUPER_GROUP_CODE"),
        col("SUPER_GROUP_DESCR").alias("DEST_SUPER_GROUP_DESCR"),
        col("MAIN_GROUP_CODE").alias("DEST_MAIN_GROUP_CODE"),
        col("MAIN_GROUP_DESCR").alias("DEST_MAIN_GROUP_DESCR"),
        col("SUB_GROUP_CODE").alias("DEST_SUB_GROUP_CODE"),
        col("CUSTOMER_STATUS_CODE").alias("DEST_CUSTOMER_STATUS_CODE"),
        col("CUSTOMER_STATUS_DESCR").alias("DEST_CUSTOMER_STATUS_DESCR"),
        col("CUSTOMER_STATUS_DATE").alias("DEST_CUSTOMER_STATUS_DATE"),
        col("CUSTOMER_WAREHOUSE_CODE").alias("DEST_CUSTOMER_WAREHOUSE_CODE"),
        col("CUSTOMER_WAREHOUSE_DESCR").alias("DEST_CUSTOMER_WAREHOUSE_DESCR"),
        col("CUSTOMER_WAREHOUSE_TYPE").alias("DEST_CUSTOMER_WAREHOUSE_TYPE"),
        col("CUSTOMER_TRAVEL_WAREHOUSE").alias("DEST_CUSTOMER_TRAVEL_WAREHOUSE"),
        col("CUSTOMER_WAREHOUSE_DOC").alias("DEST_CUSTOMER_WAREHOUSE_DOC"),
        col("CUSTOMER_WAREHOUSE_COMPANY").alias("DEST_CUSTOMER_WAREHOUSE_COMPANY"),
        col("CUSTOMER_WAREHOUSE_SUPPLIER").alias("DEST_CUSTOMER_WAREHOUSE_SUPPLIER"),
        col("NEW_CUSTOMER_CODE").alias("DEST_NEW_CUSTOMER_CODE"),
        col("ADDRESS_DELIVERY").alias("DEST_ADDRESS_DELIVERY"),
        col("SUPPLIER_CUSTOMER").alias("DEST_SUPPLIER_CUSTOMER"),
        col("VIP_CUSTOMER").alias("DEST_VIP_CUSTOMER"),
        col("POTENTIAL_BUILD_IN").alias("DEST_POTENTIAL_BUILD_IN"),
        col("POTENTIAL_FREE_STD").alias("DEST_POTENTIAL_FREE_STD"),
        col("POTENTIAL_FLOORCARE").alias("DEST_POTENTIAL_FLOORCARE"),
        col("CUSTOMER_CREATION_DATE").alias("DEST_CUSTOMER_CREATION_DATE"),
        col("CUSTOMER_UPDATE_DATE").alias("DEST_CUSTOMER_UPDATE_DATE"),
        col("CUSTOMER_COMPANY_TYPE").alias("DEST_CUSTOMER_COMPANY_TYPE"),
        col("REGION").alias("DEST_REGION"),
        col("SUPER_MAIN_GROUP_CODE").alias("DEST_SUPER_MAIN_GROUP_CODE"),
        col("CHANNEL").alias("DEST_CHANNEL"),
        col("DIRECT_WH").alias("DEST_DIRECT_WH"),
        col("CREDIT_CONTROL_CODE").alias("DEST_CREDIT_CONTROL_CODE"),
        col("CREDIT_CONTROL_DESCR").alias("DEST_CREDIT_CONTROL_DESCR"),
        col("ADDRESS_EMAIL").alias("DEST_ADDRESS_EMAIL"),
        col("COMM_TELEPHONE").alias("DEST_COMM_TELEPHONE"),
        col("PERIOD").alias("DEST_PERIOD"),
        col("INTER_FLAG").alias("DEST_INTER_FLAG"),
        col("DEMERGE_FLAG").alias("DEST_DEMERGE_FLAG"),
        col("TAX_OFFICE").alias("DEST_TAX_OFFICE"),
        col("CUSTOMER_NAME_ORIG").alias("DEST_CUSTOMER_NAME_ORIG"),
        concat(col("CUSTOMER_CODE"), lit('-'), col("CUSTOMER_NAME")).alias("DELIVERY CUSTOMER"),
        col("CUSTOMER_COMPANY"),
        col("SAP_CODE").alias("DEST_SAP_CODE"),
        col("SOURCE").alias("DEST_SOURCE")
  )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_delivery_customers", truncate=True)





####### ANAGRAFICHE #############

COMM_COMPANIES = (
    read_data_from_redshift_table("mtd.sap_dm_i_companycode_t001")  # FROM [lib://Project_Files/Framework_HORSA\Projects\Anagrafiche\Data\PROD/QVD01/DM_COMM_COMPANIES.QVD]

    .select(
        #CODE  as _Comm_Companies.Key,
        col("company").alias("COMPANY_ID"),
        lit("companycodename").alias("COMPANY_DESC"), #col("DESCRIPTION").alias("COMPANY_DESC"),
        concat(col("company"), lit('-'), lit("companycodename")).alias("COMPANY"), #concat(col("company"), lit('-'), col("DESCRIPTION")).alias("COMPANY"),
        col("COUNTRY"),
        col("LANGUAGE"),
        lit("todo").alias("PRODUCER") ,
        lit("todo").alias("VENDOR"),
        col("CURRENCY"),
        lit("todo").alias("DECIMAL_DIGITS"),
        lit("todo").alias("SUPPLIER_COMPANY"),
        lit("todo").alias("CUSTOMER_COMPANY"),
        lit("todo").alias("PRODUCT_COMPANY"),
        lit("todo").alias("MATERIAL_COMPANY"),
        lit("todo").alias("MANAGE_MATERIAL"),
        lit("todo").alias("DWH_SERVER"),
        lit("todo").alias("ACCOUNTING_CONPAMY"),
        lit("todo").alias("SAP_CODE")
    )
    .withColumn("COMPANY_ID", regexp_replace("COMPANY_ID", "^[CH]", ""))
)





COMM_COMPANIES = COMM_COMPANIES.unionByName(
    spark.createDataFrame([{"COMPANY_ID": "NOT DEFINED"}]),
    allowMissingColumns=True
).save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_comm_companies", truncate=True)






fakedata = {x: "" for x in [
        "COMM_ORGANIZATION_ID",
        "COMPANY_CODE",
        "COMPANY_DESCRIPTION",
        "COMPANY_COUNTRY",
        "COMPANY_LANGUAGE",
        "COMPANY_PRODUCER",
        "COMPANY_VENDOR",
        "COMPANY_CURRENCY",
        "CURRENCY_INTER_SIGN",
        "CURRENCY_DECIMAL_DIGITS",
        "COMPANY_DECIMAL_DIGITS",
        "COMPANY_SUPPLIER_COMPANY",
        "COMPANY_CUSTOMER_COMPANY",
        "COMPANY_PRODUCT_COMPANY",
        "COMPANY_MATERIAL_COMPANY",
        "COMPANY_MANAGE_MATERIAL",
        "COMM_DIVISION_CODE",
        "COMM_DIVISION_DESCR",
        "COMM_MARKET_CODE",
        "COMM_MARKET_DESCR",
        "SALE_NETWORK_CODE",
        "SALE_NETWORK_DESCR",
        "HYPERION_CODE",
        "SALE_ZONE_MANAGER",
        "CHANNEL_CODE",
        "CHANNEL_DESCR",
        "WMA_ASA_AREA",
        "WMA_ASA_AREA_DESC",
        "SALE_ZONE_CODE",
        "SALE_ZONE_DESCR",
        "SALESMAN_CODE",
        "SALESMAN_NAME"]}



COMM_ORGANIZATIONS = (
    spark.createDataFrame([fakedata])     # FROM [lib://Project_Files/Framework_HORSA\Projects\Anagrafiche\Data\PROD/QVD01/DM_COMM_ORGANIZATIONS.QVD]

    .select(
        #COMM_ORGANIZATION_ID AS _Comm_Organizations.Key,
        col("COMM_ORGANIZATION_ID"),
        col("COMPANY_CODE"),
        col("COMPANY_DESCRIPTION"),
        col("COMPANY_COUNTRY"),
        col("COMPANY_LANGUAGE"),
        col("COMPANY_PRODUCER"),
        col("COMPANY_VENDOR"),
        col("COMPANY_CURRENCY"),
        col("CURRENCY_INTER_SIGN"),
        col("CURRENCY_DECIMAL_DIGITS"),
        col("COMPANY_DECIMAL_DIGITS"),
        col("COMPANY_SUPPLIER_COMPANY"),
        col("COMPANY_CUSTOMER_COMPANY"),
        col("COMPANY_PRODUCT_COMPANY"),
        col("COMPANY_MATERIAL_COMPANY"),
        col("COMPANY_MANAGE_MATERIAL"),
        col("COMM_DIVISION_CODE"),
        col("COMM_DIVISION_DESCR"),
        col("COMM_MARKET_CODE"),
        col("COMM_MARKET_DESCR"),
        col("SALE_NETWORK_CODE"),
        col("SALE_NETWORK_DESCR"),
        col("HYPERION_CODE"),
        coalesce(col("SALE_ZONE_CODE"), lit("NOT DEFINED")).alias("SALE_ZONE_CODE"),
        col("SALE_ZONE_DESCR"),
        col("SALE_ZONE_MANAGER"),
        coalesce(col("SALESMAN_CODE"), lit("NOT DEFINED")).alias("SALESMAN_CODE"),
        col("SALESMAN_NAME"),
        col("CHANNEL_CODE"),
        col("CHANNEL_DESCR"),
        col("WMA_ASA_AREA"),
        col("WMA_ASA_AREA_DESC")
    )
)


'''
TEMP = (
    COMM_ORGANIZATIONS  # FROM [lib://Project_Files/Framework_HORSA\Projects\Anagrafiche\Data\PROD/QVD01/DM_COMM_COMPANIES.QVD]

    .select(
        #'SAP-'&CODE  as _Comm_Organizations.Key,
        concat(lit('SAP-'), col("CODE")).alias("COMM_ORGANIZATION_ID"),
        col("DESCRIPTION").alias("COMPANY_DESCRIPTION")
    )
)


COMM_ORGANIZATIONS = COMM_ORGANIZATIONS.unionByName(TEMP, allowMissingColumns=True)
'''

COMM_ORGANIZATIONS.save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_comm_organizations", truncate=True)




####### ANAGRAFICHE ##########



SALE_NETWORKS_CAS_INPUT = read_data_from_redshift_table("datalake_ods.oracle_dwh_dw_sale_networks")

SALE_NETWORKS_CAS_ALL = (
    SALE_NETWORKS_CAS_INPUT  # FROM [lib://Project_Files/Framework_HORSA\Projects\Anagrafiche\Data\PROD/QVD01/DM_SALE_NETWORKS.QVD]   //cas

    .select(
        concat_ws("-@-", col("company"), col("code")).alias("_Sale_Networks_Key"),
        concat_ws("-@-", col("company"), col("code")).alias("NETWORK_ID"),
        col("code").alias("NET_CODE"),
        col("description").alias("NETWORK_DESC"),
        concat_ws("-", col("code"), col("description")).alias("NETWORK"),
        col("market_budget").alias("MARKET_BUDGET"),
        col("sap_code").alias("SALES_OFFICE"),
        col("sap_distr").alias("DISTRIBUTION_CHANNEL"),
        col("sap_division").alias("SALES_DIVISION"),
        col("sap_account").alias("SAP_ACCOUNT"),
        lit('ALL').alias("SALES TYPE"),
    )
).localCheckpoint()

SALE_NETWORKS_CAS_SALES_TYPE = (
    SALE_NETWORKS_CAS_ALL
    .withColumn("SALES TYPE",
        when(col("NET_CODE").isin([1, 2]), lit("ITC"))
        .otherwise(lit("NO ITC"))
    )
)

SALE_NETWORKS_CAS = (
    SALE_NETWORKS_CAS_SALES_TYPE
    .unionByName(SALE_NETWORKS_CAS_ALL)
).save_data_to_redshift_table("dwe.qlk_anagrafica_sale_networks_cas", truncate=True)









RETURN_REASONS = (
    read_data_from_redshift_table("man.qlk_sls_mapping_return_reasons")
    .withColumnRenamed("return reason", "MOTIVE")
    .withColumnRenamed("return reason description", "MOTIVE_DESCR")
    .withColumnRenamed("causes", "CAUSE")

    .select(
        col("MOTIVE"),
        coalesce(col("MOTIVE_DESCR"), lit('TO BE DEFINED')).alias("MOTIVE_DESCR"),
        col("CAUSE"),
        lit("").alias("CAUSE_DESCR"),
        lit("").alias("VERSION"),
        lit("").alias("STATUS"),
        lit("").alias("TIMESTAMP"),
        lit("").alias("ID")
    )

    .withColumn("RETURN TYPE",
        when(col("MOTIVE").cast("int").isin([60, 70, 80, 90, 100]), "GOOD RETURN")
        .when(col("MOTIVE").cast("int").isin([20, 30, 40, 50]), "BAD RETURN")
        .otherwise("TO BE DEFINED")
    )
)



RETURN_REASONS.save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_return_reasons", truncate=True)




COUNTIES = (
    read_data_from_redshift_table("crt.sap_countrytext")

    .select(
        col("countryname").alias("COUNTRY_DESCR"),
        col("country").alias("COUNTRY_ISO_CODE"),
    )
).save_data_to_redshift_table("dwe.qlk_anagrafica_countries", truncate=True)






fakedata = {x: "" for x in [
    "SUPPLIER_ID", "SUPPLIER_COMPANY", "SUPPLIER_CODE", "SUPPLIER_DESC", "ADDRESS1", "ADDRESS2", "POSTCODE", "ADDRESS3", "ADDRESS4", "FISCAL", "LANGUAGE", "CURRENCY",
    "PAYTERMS", "MANAGEMENT_CODE", "CLASSIFICATION", "JOURNAL_CODE", "CREATION_DATE", "ISO_CODE", "COUNTRY_CODE", "BANK_CODE", "ACCOUNT_CODE", "FORNAR", "VARIANCE_DATE",
    "BUYER_CODE", "VAT_CODE", "TELEPHONE", "FAX", "TELEX", "CALLBACK", "QUALITY_CHECK", "COMMODITY_CODE", "CUSTOMER_CODE", "ALTERNATIVE_ADDRESS", "ALTERNATIVE_POSTCODE",
    "CONTACT", "PAYMENT_CONTACT", "NOTE", "GROUP_CODE", "END_DATE", "FINAL_PAYMENT", "OTHER_BANK_CODE", "COUNTRY_DESCRIPTION", "MANAGEMENT_DESCR", "HIST_PAYM_TYPES",
    "HIST_PAYM_EXPIRING", "CLASSIFICATION_DESCR", "FORMAIL", "FORWFLAG", "FOREDI", "SUPPLIER_DESC2", "SUPPLIER_DESC3", "SAP_CODE", "SAP_TAX_TYPE", "SAP_TAX_CODE"
]}

Suppliers = (
    spark.createDataFrame([fakedata])   # todo FROM [lib://Prj_Anagrafiche/QVD01/DM_SUPPLIERS.QVD]


    .select(
        "SUPPLIER_ID",
        "SUPPLIER_COMPANY",
        "SUPPLIER_CODE",
        "SUPPLIER_DESC",
        "ADDRESS1",
        "ADDRESS2",
        "POSTCODE",
        "ADDRESS3",
        "ADDRESS4",
        "FISCAL",
        "LANGUAGE",
        "CURRENCY",
        "PAYTERMS",
        "MANAGEMENT_CODE",
        "CLASSIFICATION",
        "JOURNAL_CODE",
        "CREATION_DATE",
        "ISO_CODE",
        "COUNTRY_CODE",
        "BANK_CODE",
        "ACCOUNT_CODE",
        "FORNAR",
        "VARIANCE_DATE",
        "BUYER_CODE",
        "VAT_CODE",
        "TELEPHONE",
        "FAX",
        "TELEX",
        "CALLBACK",
        "QUALITY_CHECK",
        "COMMODITY_CODE",
        "CUSTOMER_CODE",
        "ALTERNATIVE_ADDRESS",
        "ALTERNATIVE_POSTCODE",
        "CONTACT",
        "PAYMENT_CONTACT",
        "NOTE",
        "GROUP_CODE",
        "END_DATE",
        "FINAL_PAYMENT",
        "OTHER_BANK_CODE",
        "COUNTRY_DESCRIPTION",
        "MANAGEMENT_DESCR",
        "HIST_PAYM_TYPES",
        "HIST_PAYM_EXPIRING",
        "CLASSIFICATION_DESCR",
        "FORMAIL",
        "FORWFLAG",
        "FOREDI",
        "SUPPLIER_DESC2",
        "SUPPLIER_DESC3",
        "SAP_CODE",
        "SAP_TAX_TYPE",
        "SAP_TAX_CODE"
    )

).save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_suppliers", truncate=True)




fakedata = {x: "" for x in [
    "PART_ID","YEAR","COST_P1","COST_P2","COST_P2A","COST_P4","COST_P5","COST_P6","COST_NDE","COST_ACC_AMI","COST_ACC_AMS","COST_ACC_IND","COST_ACC_MOP","COST_ACC_RET",
    "COST_ACC_TRA","COST_ACC_VAR","COST_ADM","COST_AMM","COST_APP","COST_ETU","COST_IMP","COST_INC_MAT","COST_LOCK","COST_MATERIAL_BUY","COST_REB","COST_REC","COST_STK","DECISION_CODE"
]}

STANDARD_COST = (

    spark.createDataFrame([fakedata])   # todo FROM [lib://Prj_Anagrafiche/QVD01/DM_STD_COST.QVD]

    .select(
        "PART_ID",
        "YEAR",
        "COST_P1",
        "COST_P2",
        "COST_P2A",
        "COST_P4",
        "COST_P5",
        "COST_P6",
        "COST_NDE",
        "COST_ACC_AMI",
        "COST_ACC_AMS",
        "COST_ACC_IND",
        "COST_ACC_MOP",
        "COST_ACC_RET",
        "COST_ACC_TRA",
        "COST_ACC_VAR",
        "COST_ADM",
        "COST_AMM",
        "COST_APP",
        "COST_ETU",
        "COST_IMP",
        "COST_INC_MAT",
        "COST_LOCK",
        "COST_MATERIAL_BUY",
        "COST_REB",
        "COST_REC",
        "COST_STK",
        "DECISION_CODE"
    )

).save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_standard_cost", truncate=True)


RETURN_REASONS_FINAL = (                # QUESTA SERVE DAVVERO A GIULIA
    read_data_from_redshift_table("man.qlk_sls_mapping_return_reasons")

    .select(
        col("return reason").alias("motive"),
        col("return reason description").alias("motive_descr"),
        col("causes").alias("cause"),
        col("sap order reason").alias("sap_order_reason"),
        concat_ws("|", col("return reason"), col("causes")).alias("_KEY_RETURN_REASON")
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_anagrafiche_return_reasons_final", truncate=True)


#### ANAGRAFICHE PRECEDENTEMENTE PRESENTI IN HETL2, PORTATE QUI PER SEMPLICITA'


INVOICE_CUSTOMERS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_invoice_customers")   # FROM FROM [lib://Prj_Anagrafiche/QVD02/INVOICE_CUSTOMERS.QVD]

    .where(col("SOURCE") != "SAP")

    .select(
        "INV_ID",
        "PURCHASE_GROUP_CODE",
        "PURCHASE_GROUP_DESCR",
        "SUPER_GROUP_CODE",
        "SUPER_GROUP_DESCR",
        "MAIN_GROUP_CODE",
        "MAIN_GROUP_DESCR",
        "CUSTOMER_TYPE_CODE",
        "CUSTOMER_TYPE_DESCR"
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl2_invoice_customers", truncate=True)



DELIVERY_CUSTOMERS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_delivery_customers")   # FROM FROM [lib://Prj_Anagrafiche/QVD02/DELIVERY_CUSTOMERS.QVD]

    .where(col("DEST_SOURCE") != "SAP")

    .select(
        "DEST_ID",
        "DEST_CUSTOMER_TYPE_CODE",
        "DEST_CUSTOMER_TYPE_DESCR",
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl2_delivery_customers", truncate=True)

# COMM_ORGANIZATIONS --> richiesta tabella anagrafica TVKO (manca tabella SAP)

COMM_ORGANIZATIONS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_comm_organizations")     # FROM FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\COMM_ORGANIZATIONS.QVD]

    .select(
        "COMM_ORGANIZATION_ID",
        "SALE_ZONE_CODE",
        "SALE_ZONE_DESCR",
        "SALESMAN_CODE",
        "SALESMAN_NAME",
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl2_comm_organizations", truncate=True)
