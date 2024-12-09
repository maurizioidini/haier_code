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







def select_product(self, product_type):
    return self.select(
        "LINE_CODE",
        "PRODUCT_ID",
        col("product_code").alias("PRODUCT CODE"),
        col("product_descr").alias("PRODUCT DESCR"),
        "PRODUCT_SHORT_DESCR", "PRODUCT_TYPE_CODE", "PRODUCT_TYPE_DESCR", "IND_DIVISION_CODE", "IND_DIVISION_DESCR", "IND_MARKET_CODE", "IND_MARKET_DESCR", "BRAND_CODE",
        "BRAND_DESCR", "LINE_SIGN", "LINE_DESCR", "IND_STRUCTURE_CODE", "IND_STRUCTURE_DESCR", "IND_FAMILY_CODE", "IND_FAMILY_DESCR", "COM_STRUCTURE_CODE",
        "COM_STRUCTURE_DESCR", "COM_FAMILY_CODE", "COM_FAMILY_DESCR", "MACRO_STRUCTURE_CODE", "MACRO_STRUCTURE_DESCR", "PRODUCER_CODE", "PRODUCER_DESCR", "PRODUCTION_CODE",
        "PRODUCTION_DESCR", "START_PROD_DATE", "END_PROD_DATE", "SUPPLIER_CODE", "SUPPLIER_DESCR", "CUSTOMER_BRAND_CODE", "CUSTOMER_BRAND_DESCR", "STOCK_CODE",
        "STOCK_DESCR", "ORM_CLASS_CODE", "ORM_CLASS_DESCR", "TRANSPORT_CLASS_CODE", "TRANSPORT_CLASS_DESCR", "QUALITY_GROUP", "FORECAST_QTY", "DAILY_PRODUCTION",
        "ROYALTIES", "EAN_CODE", "NET_WEIGHT", "GROSS_WEIGHT", "HEIGHT_MM", "WIDTH_MM", "DEPTH_MM", "VOLUME_M3", "CREATION_DATE",
        "UPDATE_DATE", "DELETE_DATE", "USERSTAMP", "PRODUCT_OLD_CODE", "MACRO_GROUP_CODE", "MACRO_GROUP_DESCR", "PRODUCT_OLD_SH_DESCR", "PRODUCT_OLD_DESCR",
        "ACTIVE_AREAS", "PRODUCT_NEW_CODE", "PRODUCT_NEW_SH_DESCR", "PRODUCT_NEW_DESCR", "PRODUCT_OLD_END_DATE", "PRODUCT_NEW_START_DATE", "PRODUCT_ORDER_CODE", "FACTORY_STRUCTURE",
        "NORMAL_TYPOLOGY", "SPECIAL_TYPOLOGY", "NORMAL_TYPOLOGY_DESCR", "SPECIAL_TYPOLOGY_DESCR", "FOB", "SECTOR", "PACKAGING_PAPER", "PACKAGING_PLASTIC", "PACKAGING_WOOD",
        "PACKAGING_POLYESTER", "PACKAGING_GLASS", "PACKAGING_METAL", "MONTHS_LIFE", "LOT", "PRODUCT_SUB", "PRODUCT_NRT", "PRODUCT_PEP", "PRODUCT_D16",
        "PRODUCT_D16_N", "PACKAGING_OTHER_PLASTIC", "PACKAGING_ALUMINIUM", "BATTERIES_NUMBER", "TOTAL_BATTERY_WEIGHTS", "DATE_RT", "BUSINESS_SECTOR_CODE", "BUSINESS_SECTOR_DESCR", "BATTERY_TYPE",
        "BATTERY_TYPE_DESCR",
        col("COUNTRY_CODE").alias("COUNTRY_CODE_prod"),
        col("COUNTRY_DESCR").alias("COUNTRY_DESCR_prod"),
        "COLOR", "MARKET", "VOLTAGE", "FREQUENCY", "PRODUCT_TYPE_FK", "FIRST_PROD_DATE", "POWER", col("CAP_COTTON").alias("KG LB"),
        col("CAP_COTTON_WASH").alias("KG WD DRY"),
        col("CAP_COTTON_DRY").alias("KG WD WASH"),
        "ENERGY_CLASS", "ELETTRONICA", "ESTETICA", "TYPOLOGY_CODE", "TYPOLOGY_DESC", "CONNECTIVITY", "CONNECT_DES", "QUALITY_GROUP_DESCR", "MODEL_TYPE",
        "BASE_MODEL", "COMM_MANUF_DATE", "PROTOTYPE_DATE", "PRESERIES_DATE_TEC", "PRESERIES_DATE_IND", "MODEL_TYPE_DESCR", "COMM_MANUF_DATE_FIRST", "PROTOTYPE_DATE_FIRST", "PRESERIES_DATE_TEC_FIRST",
        "PRESERIES_DATE_IND_FIRST", "MOTOR", lit("todo").alias("AGGREGATION_GFK"),
        col("product_id").alias("PRODUCT"), lit("todo").alias("FACTORY"), lit("todo").alias("SUPPLIERS"),
        col("ind_division_code").alias("HAIER DIVISION"), lit("todo").alias("_ORIGINAL_HAIER_DIVISION"), "COMM_MANUF_DATE_BASE_MODEL", "START_PROD_DATE_BASE_MODEL",
        lit("todo").alias("PRODUCTSORT"), lit("todo").alias("PRODUCT1"), lit("PRODUCT1").alias("PRODUCT LEVEL 1"), lit("PRODUCT2").alias("PRODUCT LEVEL 2"), lit("todo").alias("POLAND1"),
        lit("todo").alias("POLAND2"), lit("todo").alias("POLAND3"), lit("todo").alias("ITALY1"), lit("todo").alias("ITALY2"), lit("todo").alias("ITALY3"),
        "NR_RT_FROM_PIWEB", "RT_CREATION_DATE_FROM_PIWEB", "NR_REM", "FLAG_BOM", "DATE_FROM_5_TO_1", "INSERTION_MD", "FLAG_KIT", "MARKE_PL", "HAIER_CODE", "HAIER_CUST_MOD",
        "HAIER_COMM_MOD", "OWNER", "PLM_CHECK", "NEW_ENERGY_LABEL", lit("todo").alias("TECHNOLOGY"),
        lit(product_type).alias("PRODUCT TYPE"), lit("todo").alias("IOT APP"), lit("todo").alias("NEW_BRAND")
    )



oracle_product = read_data_from_redshift_table("dwa.oracle_product").limit(100)   # todo remove

# todo remove ??
#for x in ['START_PROD_DATE', 'END_PROD_DATE', 'CREATION_DATE', 'UPDATE_DATE', 'DELETE_DATE', 'PRODUCT_OLD_END_DATE', 'PRODUCT_NEW_START_DATE', 'PRODUCT_PEP', 'DATE_RT', 'FIRST_PROD_DATE', 'COMM_MANUF_DATE', 'PROTOTYPE_DATE', 'PRESERIES_DATE_TEC', 'PRESERIES_DATE_IND', 'COMM_MANUF_DATE_FIRST', 'PROTOTYPE_DATE_FIRST', 'PRESERIES_DATE_TEC_FIRST', 'PRESERIES_DATE_IND_FIRST', 'COMM_MANUF_DATE_BASE_MODEL', 'START_PROD_DATE_BASE_MODEL', 'RT_CREATION_DATE_FROM_PIWEB', 'DATE_FROM_5_TO_1', 'INSERTION_MD']:
#    oracle_product = oracle_product.withColumn(x, when(col(x).cast("string") < '1900', lit(None)).otherwise(col(x)))
oracle_product = oracle_product.localCheckpoint()

PRODUCTS = select_product(
    oracle_product.where(col("PRODUCT_TYPE_CODE").isin([1]) | col("PRODUCT_ID").startswith("1-@-") | col("PRODUCT_ID").startswith("2-@-"))
    , "ONLY FG (1)"
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product.where(col("PRODUCT_TYPE_CODE").isin([2]))
        , "PROGRAMS ACC. (2)")
    , allowMissingColumns=True
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product.where(col("PRODUCT_TYPE_CODE").isin([3]))
        , "ACCESSORIES (3)")
    , allowMissingColumns=True
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product.where(col("PRODUCT_TYPE_CODE").isin([1, 2]) | col("PRODUCT_ID").startswith("1-@-") | col("PRODUCT_ID").startswith("2-@-"))
        , "FG (1) + PROGR.ACC. (2)")
    , allowMissingColumns=True
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product.where(col("PRODUCT_TYPE_CODE").isin([1, 3]) | col("PRODUCT_ID").startswith("1-@-") | col("PRODUCT_ID").startswith("2-@-"))
        , "FG (1) + ACCESSORIES (3)")
    , allowMissingColumns=True
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product.where(col("PRODUCT_TYPE_CODE").isin([2, 3]))
        , "PROGR.ACC. (2) + ACCESSORIES (3)")
    , allowMissingColumns=True
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product.where(col("PRODUCT_TYPE_CODE").isin(["W"]))
        , "SERVICE WASHPASS")
    , allowMissingColumns=True
)

PRODUCTS = PRODUCTS.unionByName(
    select_product(oracle_product
        , "ALL")
    , allowMissingColumns=True
)

PRODUCTS_TEMP = spark.createDataFrame([
    {"PRODUCT_ID": "NOT DEFINED", "PRODUCT_TYPE_CODE": 1, "PRODUCT TYPE": "ONLY FG (1)"},
    {"PRODUCT_ID": "NOT DEFINED", "PRODUCT_TYPE_CODE": 1, "PRODUCT TYPE": "FG (1) + PROGR.ACC. (2)"},
    {"PRODUCT_ID": "NOT DEFINED", "PRODUCT_TYPE_CODE": 1, "PRODUCT TYPE": "FG (1) + ACCESSORIES (3)"},
    {"PRODUCT_ID": "NOT DEFINED", "PRODUCT_TYPE_CODE": 1, "PRODUCT TYPE": "ALL"},
])
PRODUCTS = PRODUCTS.unionByName(PRODUCTS_TEMP, allowMissingColumns=True)


PRODUCTS_TEMP = (
    PRODUCTS

    .select(
        col("BUSINESS_SECTOR_DESCR").alias("PRODUCT_ID"),
        lit(1).alias("PRODUCT_TYPE_CODE"),
        lit("ONLY FG (1)").alias("PRODUCT TYPE")
    )

    .distinct()
)
PRODUCTS = PRODUCTS.unionByName(PRODUCTS_TEMP, allowMissingColumns=True)


PRODUCTS_TEMP = (
    PRODUCTS

    .select(
        col("BUSINESS_SECTOR_DESCR").alias("PRODUCT_ID"),
        lit(1).alias("PRODUCT_TYPE_CODE"),
        lit("ALL").alias("PRODUCT TYPE")
    )

    .distinct()
)
PRODUCTS = PRODUCTS.unionByName(PRODUCTS_TEMP, allowMissingColumns=True)

MAPPING_HAIER_LINE = (
    spark.createDataFrame([{"HAIER DIVISION": "", "LINE_CODE": ""}])   # FROM [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/InputFiles/HAIER_DIVISION_MAPPING.xlsx]
    .select(
        col("HAIER DIVISION").alias("HAIER DIVISION"),
        col("LINE_CODE").alias("LINE_CODE")
    )
).assert_no_duplicates("HAIER DIVISION")
print("MAPPING_HAIER_LINE loaded!")


MAPPING_LINE_CODE_DESCR = (
    oracle_product   # FROM [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/PRODUCTS.QVD]
    .select(
        col("LINE_CODE").alias("LINE_CODE"),
        col("LINE_DESCR").alias("LINE_DESCR")
    )
    .drop_duplicates_ordered(["LINE_CODE"], ["LINE_DESCR"])   # todo remove
).assert_no_duplicates("LINE_CODE")
print("MAPPING_LINE_CODE_DESCR loaded!")


MAPPING_LINE_CODE_BS_CODE = (
    oracle_product  # FROM [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/PRODUCTS.QVD]
    .select(
        col("LINE_CODE").alias("LINE_CODE"),
        col("BUSINESS_SECTOR_CODE").alias("BUSINESS_SECTOR_CODE")
    )
    .drop_duplicates_ordered(["LINE_CODE"], ["BUSINESS_SECTOR_CODE"])   # todo remove
).assert_no_duplicates("LINE_CODE")
print("MAPPING_LINE_CODE_BS_CODE loaded!")


MAPPING_LINE_CODE_BS_DESCR = (
    oracle_product    # [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/PRODUCTS.QVD]
    .select(
        col("LINE_CODE").alias("LINE_CODE"),
        col("BUSINESS_SECTOR_DESCR").alias("BUSINESS_SECTOR_DESCR")
    )
    .drop_duplicates_ordered(["LINE_CODE"], ["BUSINESS_SECTOR_DESCR"])   # todo remove
).assert_no_duplicates("LINE_CODE")
print("MAPPING_LINE_CODE_BS_DESCR loaded!")


MAPPING_LINE_CODE_IND_DIVISION_CODE = (
    oracle_product    # [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/PRODUCTS.QVD]
    .select(
        col("LINE_CODE").alias("LINE_CODE"),
        col("IND_DIVISION_CODE").alias("IND_DIVISION_CODE")
    )
    .drop_duplicates_ordered(["LINE_CODE"], ["IND_DIVISION_CODE"])   # todo remove
).assert_no_duplicates("LINE_CODE")
print("MAPPING_LINE_CODE_IND_DIVISION_CODE loaded!")


MAPPING_LINE_CODE_IND_DIVISION_DESCR = (
    oracle_product    # [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/PRODUCTS.QVD]
    .select(
        col("LINE_CODE").alias("LINE_CODE"),
        col("IND_DIVISION_DESCR").alias("IND_DIVISION_DESCR")
    )
    .drop_duplicates_ordered(["LINE_CODE"], ["IND_DIVISION_DESCR"])   # todo remove
).assert_no_duplicates("LINE_CODE")
print("MAPPING_LINE_CODE_IND_DIVISION_DESCR loaded!")


MAPPING_LINE_CODE_FOB = (
    oracle_product    # [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/PRODUCTS.QVD]
    .select(
        col("LINE_CODE").alias("LINE_CODE"),
        col("FOB").alias("FOB")
    )
    .drop_duplicates_ordered(["LINE_CODE"], ["FOB"])   # todo remove
).assert_no_duplicates("LINE_CODE")
print("MAPPING_LINE_CODE_FOB loaded!")


PRODUCTS_TEMP = spark.createDataFrame([
    ("HAIER_BUDGET_CAC", 1, "ONLY FG (1)", "Commercial AC"),
    ("HAIER_BUDGET_CAC", 1, "FG (1) + PROGR.ACC. (2)", "Commercial AC"),
    ("HAIER_BUDGET_CAC", 1, "FG (1) + ACCESSORIES (3)", "Commercial AC"),
    ("HAIER_BUDGET_CAC", 1, "ALL", "Commercial AC"),
    ("HAIER_BUDGET_COO", 1, "ONLY FG (1)", "Cooking"),
    ("HAIER_BUDGET_COO", 1, "FG (1) + PROGR.ACC. (2)", "Cooking"),
    ("HAIER_BUDGET_COO", 1, "FG (1) + ACCESSORIES (3)", "Cooking"),
    ("HAIER_BUDGET_COO", 1, "ALL", "Cooking"),
    ("HAIER_BUDGET_DW", 1, "ONLY FG (1)", "Dishwasher"),
    ("HAIER_BUDGET_DW", 1, "FG (1) + PROGR.ACC. (2)", "Dishwasher"),
    ("HAIER_BUDGET_DW", 1, "FG (1) + ACCESSORIES (3)", "Dishwasher"),
    ("HAIER_BUDGET_DW", 1, "ALL", "Dishwasher"),
    ("HAIER_BUDGET_FRE", 1, "ONLY FG (1)", "Freezer"),
    ("HAIER_BUDGET_FRE", 1, "FG (1) + PROGR.ACC. (2)", "Freezer"),
    ("HAIER_BUDGET_FRE", 1, "FG (1) + ACCESSORIES (3)", "Freezer"),
    ("HAIER_BUDGET_FRE", 1, "ALL", "Freezer"),
    ("HAIER_BUDGET_HAC", 1, "ONLY FG (1)", "Air Conditioning"),
    ("HAIER_BUDGET_HAC", 1, "FG (1) + PROGR.ACC. (2)", "Air Conditioning"),
    ("HAIER_BUDGET_HAC", 1, "FG (1) + ACCESSORIES (3)", "Air Conditioning"),
    ("HAIER_BUDGET_HAC", 1, "ALL", "Air Conditioning"),
    ("HAIER_BUDGET_IT", 1, "ONLY FG (1)", "IT"),
    ("HAIER_BUDGET_IT", 1, "FG (1) + PROGR.ACC. (2)", "IT"),
    ("HAIER_BUDGET_IT", 1, "FG (1) + ACCESSORIES (3)", "IT"),
    ("HAIER_BUDGET_IT", 1, "ALL", "IT"),
    ("HAIER_BUDGET_MP", 1, "ONLY FG (1)", "Mobile Phone"),
    ("HAIER_BUDGET_MP", 1, "FG (1) + PROGR.ACC. (2)", "Mobile Phone"),
    ("HAIER_BUDGET_MP", 1, "FG (1) + ACCESSORIES (3)", "Mobile Phone"),
    ("HAIER_BUDGET_MP", 1, "ALL", "Mobile Phone"),
    ("HAIER_BUDGET_OTH", 1, "ONLY FG (1)", "Others"),
    ("HAIER_BUDGET_OTH", 1, "FG (1) + PROGR.ACC. (2)", "Others"),
    ("HAIER_BUDGET_OTH", 1, "FG (1) + ACCESSORIES (3)", "Others"),
    ("HAIER_BUDGET_OTH", 1, "ALL", "Others"),
    ("HAIER_BUDGET_REF", 1, "ONLY FG (1)", "Refrigerator"),
    ("HAIER_BUDGET_REF", 1, "FG (1) + PROGR.ACC. (2)", "Refrigerator"),
    ("HAIER_BUDGET_REF", 1, "FG (1) + ACCESSORIES (3)", "Refrigerator"),
    ("HAIER_BUDGET_REF", 1, "ALL", "Refrigerator"),
    ("HAIER_BUDGET_TV", 1, "ONLY FG (1)", "TV"),
    ("HAIER_BUDGET_TV", 1, "FG (1) + PROGR.ACC. (2)", "TV"),
    ("HAIER_BUDGET_TV", 1, "FG (1) + ACCESSORIES (3)", "TV"),
    ("HAIER_BUDGET_TV", 1, "ALL", "TV"),
    ("HAIER_BUDGET_WH", 1, "ONLY FG (1)", "Water Heater"),
    ("HAIER_BUDGET_WH", 1, "FG (1) + PROGR.ACC. (2)", "Water Heater"),
    ("HAIER_BUDGET_WH", 1, "FG (1) + ACCESSORIES (3)", "Water Heater"),
    ("HAIER_BUDGET_WH", 1, "ALL", "Water Heater"),
    ("HAIER_BUDGET_WM", 1, "ONLY FG (1)", "Washing Machine"),
    ("HAIER_BUDGET_WM", 1, "FG (1) + PROGR.ACC. (2)", "Washing Machine"),
    ("HAIER_BUDGET_WM", 1, "FG (1) + ACCESSORIES (3)", "Washing Machine"),
    ("HAIER_BUDGET_WM", 1, "ALL", "Washing Machine")
], schema="PRODUCT_ID string, PRODUCT_TYPE_CODE int, PRODUCT_TYPE string, HAIER_DIVISION string")

PRODUCTS_TEMP = (
    PRODUCTS_TEMP
    .withColumnRenamed("PRODUCT_TYPE", "PRODUCT TYPE")
    .withColumnRenamed("HAIER_DIVISION", "HAIER DIVISION")

    .do_mapping(MAPPING_HAIER_LINE, col("HAIER DIVISION"), "LINE_CODE", default_value=lit(current_date()), mapping_name="MAPPING_HAIER_LINE")

    .do_mapping(MAPPING_LINE_CODE_DESCR, col("LINE_CODE"), "LINE_DESCR", default_value=lit('NOT DEFINED'), mapping_name="MAPPING_LINE_CODE_DESCR")
    .do_mapping(MAPPING_LINE_CODE_BS_CODE, col("LINE_CODE"), "BUSINESS_SECTOR_CODE", default_value=lit('NOT DEFINED'), mapping_name="MAPPING_LINE_CODE_BS_CODE")
    .do_mapping(MAPPING_LINE_CODE_BS_DESCR, col("LINE_CODE"), "BUSINESS_SECTOR_DESCR", default_value=lit('NOT DEFINED'), mapping_name="MAPPING_LINE_CODE_BS_DESCR")
    .do_mapping(MAPPING_LINE_CODE_IND_DIVISION_CODE, col("LINE_CODE"), "IND_DIVISION_CODE", default_value=lit('NOT DEFINED'), mapping_name="MAPPING_LINE_CODE_IND_DIVISION_CODE")
    .do_mapping(MAPPING_LINE_CODE_IND_DIVISION_DESCR, col("LINE_CODE"), "IND_DIVISION_DESCR", default_value=lit('NOT DEFINED'), mapping_name="MAPPING_LINE_CODE_IND_DIVISION_DESCR")
    .do_mapping(MAPPING_LINE_CODE_FOB, col("LINE_CODE"), "FOB", default_value=lit('NOT DEFINED'), mapping_name="MAPPING_LINE_CODE_FOB")

    .withColumn("LINE_DESCR", when(col("LINE_CODE").isNull(), lit("NOT DEFINED")).otherwise(col("LINE_DESCR")))
    .withColumn("BUSINESS_SECTOR_CODE", when(col("LINE_CODE").isNull(), lit("NOT DEFINED")).otherwise(col("BUSINESS_SECTOR_CODE")))
    .withColumn("BUSINESS_SECTOR_DESCR", when(col("LINE_CODE").isNull(), lit("NOT DEFINED")).otherwise(col("BUSINESS_SECTOR_DESCR")))
    .withColumn("IND_DIVISION_CODE", when(col("LINE_CODE").isNull(), lit("NOT DEFINED")).otherwise(col("IND_DIVISION_CODE")))
    .withColumn("IND_DIVISION_DESCR", when(col("LINE_CODE").isNull(), lit("NOT DEFINED")).otherwise(col("IND_DIVISION_DESCR")))
    .withColumn("FOB", when(col("LINE_CODE").isNull(), lit("NOT DEFINED")).otherwise(col("FOB")))

)

PRODUCTS = PRODUCTS.unionByName(PRODUCTS_TEMP, allowMissingColumns=True).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_products_gs", truncate=True)





###########################
######## Companies ########
###########################


COMM_COMPANIES_INPUT = read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_comm_companies").localCheckpoint()


COMPANIES = (
    COMM_COMPANIES_INPUT  # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\COMM_COMPANIES.QVD](qvd);

    .select(
        "COMPANY_ID",
        "COMPANY_DESC",
        concat_ws('-', col("COMPANY_ID"), col("COMPANY_DESC")).alias("COMPANY"),
        "COUNTRY",
        "LANGUAGE",
        "PRODUCER",
        "VENDOR",
        "DECIMAL_DIGITS",
        "SUPPLIER_COMPANY",
        "CUSTOMER_COMPANY",
        "PRODUCT_COMPANY",
        "MATERIAL_COMPANY",
        "MANAGE_MATERIAL",
        "DWH_SERVER",
        "ACCOUNTING_CONPAMY"
    )

).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_companies_gs", truncate=True)




###############################
######## Del Customers ########
###############################



DEL_CUSTOMERS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_delivery_customers")   # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\DELIVERY_CUSTOMERS.QVD](qvd);

    .select(
        "DEST_ID",
        "DEST_CODE",
        "DEST_NAME",
        "DEST_NAME2",
        "DEST_SHORT_NAME",
        "DEST_ADDRESS",
        "DEST_ADDRESS2",
        "DEST_POST_CODE",
        "DEST_TOWN",
        "DEST_TOWN2",
        "DEST_COUNTRY_CODE",
        "DEST_COUNTRY_DESCR",
        "DEST_COUNTRY_SIGN",
        "DEST_COUNTRY_ISO_CODE",
        "DEST_COUNTRY_CURRENCY",
        "DEST_PROVINCE_CODE",
        "DEST_PROVINCE_DESCR",
        "DEST_TOWN_CODE",
        "DEST_TOWN_DESCR",
        "DEST_CUSTOMER_ISO_CODE",
        "DEST_CUSTOMER_ISO_DESC",
        "DEST_CUSTOMER_ISO_CURRENCY",
        "DEST_VAT_CODE",
        "DEST_IT_VAT_CODE",
        "DEST_CANDY_CUST_GROUP",
        "DEST_SUPPLIER",
        "DEST_CUSTOMER_AT_CUSTOMER",
        "DEST_CUSTOMER_CURRENCY_CODE",
        "DEST_CUSTOMER_CURRENCY_SIGN",
        "DEST_CURRENCY_EURO_EXCHANGE",
        "DEST_CURRENCY_DEC_DIGITS",
        "DEST_INVOICE_IN_EURO",
        "DEST_CUSTOMER_CLASS_CODE",
        "DEST_CUSTOMER_CLASS_DESCR",
        "DEST_PURCHASE_GROUP_CODE",
        "DEST_PURCHASE_GROUP_DESCR",
        "DEST_SUPER_GROUP_CODE",
        "DEST_SUPER_GROUP_DESCR",
        "DEST_MAIN_GROUP_CODE",
        "DEST_MAIN_GROUP_DESCR",
        "DEST_SUB_GROUP_CODE",
        "DEST_CUSTOMER_STATUS_CODE",
        "DEST_CUSTOMER_STATUS_DESCR",
        "DEST_CUSTOMER_STATUS_DATE",
        "DEST_CUSTOMER_WAREHOUSE_CODE",
        "DEST_CUSTOMER_WAREHOUSE_DESCR",
        "DEST_CUSTOMER_WAREHOUSE_TYPE",
        "DEST_CUSTOMER_TRAVEL_WAREHOUSE",
        "DEST_CUSTOMER_WAREHOUSE_DOC",
        "DEST_CUSTOMER_WAREHOUSE_COMPANY",
        "DEST_CUSTOMER_WAREHOUSE_SUPPLIER",
        "DEST_NEW_CUSTOMER_CODE",
        "DEST_ADDRESS_DELIVERY",
        "DEST_SUPPLIER_CUSTOMER",
        "DEST_VIP_CUSTOMER",
        "DEST_POTENTIAL_BUILD_IN",
        "DEST_POTENTIAL_FREE_STD",
        "DEST_POTENTIAL_FLOORCARE",
        "DEST_CUSTOMER_CREATION_DATE",
        "DEST_CUSTOMER_UPDATE_DATE",
        "DEST_CUSTOMER_COMPANY_TYPE",
        "DEST_REGION",
        "DEST_SUPER_MAIN_GROUP_CODE",
        "DEST_CHANNEL",
        "DEST_DIRECT_WH",
        "DEST_CREDIT_CONTROL_CODE",
        "DEST_CREDIT_CONTROL_DESCR",
        "DEST_ADDRESS_EMAIL",
        "DEST_COMM_TELEPHONE",
        "DEST_PERIOD",
        "DEST_INTER_FLAG",
        "DEST_DEMERGE_FLAG",
        "DEST_TAX_OFFICE",
        "DEST_CUSTOMER_NAME_ORIG",
        "DELIVERY CUSTOMER",
        "CUSTOMER_COMPANY"
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_del_customers_gs", truncate=True)


MAPPING_DEST_CODE = (
    DEL_CUSTOMERS

    .select(
        "DEST_ID",
        "DEST_CODE",
    )

    .drop_duplicates_ordered(["DEST_ID"], ["DEST_CODE"])   # todo remove
).assert_no_duplicates("DEST_ID")




INV_CUSTOMERS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_invoice_customers")    #FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\INVOICE_CUSTOMERS.QVD](qvd);

    .select(
        "INV_ID",
        "CUSTOMER_CODE",
        "CUSTOMER_NAME",
        "CUSTOMER_NAME2",
        "CUSTOMER_SHORT_NAME",
        "ADDRESS",
        "ADDRESS2",
        "POSTAL_CODE",
        "TOWN",
        "TOWN2",
        "COUNTRY_CODE",
        "COUNTRY_DESCR",
        "COUNTRY_SIGN",
        "COUNTRY_ISO_CODE",
        "COUNTRY_CURRENCY",
        "PROVINCE_CODE",
        "PROVINCE_DESCR",
        "TOWN_CODE",
        "TOWN_DESCR",
        "CUSTOMER_ISO_CODE",
        "CUSTOMER_ISO_DESC",
        "CUSTOMER_ISO_CURRENCY",
        "VAT_CODE",
        "IT_VAT_CODE",
        "CANDY_CUST_GROUP",
        "SUPPLIER",
        "CUSTOMER_AT_CUSTOMER",
        "CUSTOMER_CURRENCY_CODE",
        "CUSTOMER_CURRENCY_SIGN",
        "CURRENCY_EURO_EXCHANGE",
        "CURRENCY_DEC_DIGITS",
        "INVOICE_IN_EURO",
        "CUSTOMER_CLASS_CODE",
        "CUSTOMER_CLASS_DESCR",
        "PURCHASE_GROUP_CODE",
        "PURCHASE_GROUP_DESCR",
        "SUPER_GROUP_CODE",
        "SUPER_GROUP_DESCR",
        "MAIN_GROUP_CODE",
        "MAIN_GROUP_DESCR",
        "SUB_GROUP_CODE",
        "CUSTOMER_STATUS_CODE",
        "CUSTOMER_STATUS_DESCR",
        "CUSTOMER_STATUS_DATE",
        "CUSTOMER_WAREHOUSE_CODE",
        "CUSTOMER_WAREHOUSE_DESCR",
        "CUSTOMER_WAREHOUSE_TYPE",
        "CUSTOMER_TRAVEL_WAREHOUSE",
        "CUSTOMER_WAREHOUSE_DOC",
        "CUSTOMER_WAREHOUSE_COMPANY",
        "CUSTOMER_WAREHOUSE_SUPPLIER",
        "NEW_CUSTOMER_CODE",
        "ADDRESS_DELIVERY",
        "SUPPLIER_CUSTOMER",
        "VIP_CUSTOMER",
        "POTENTIAL_BUILD_IN",
        "POTENTIAL_FREE_STD",
        "POTENTIAL_FLOORCARE",
        "CUSTOMER_CREATION_DATE",
        "CUSTOMER_UPDATE_DATE",
        "CUSTOMER_COMPANY_TYPE",
        "REGION",
        "SUPER_MAIN_GROUP_CODE",
        "CHANNEL",
        "DIRECT_WH",
        "CREDIT_CONTROL_CODE",
        "CREDIT_CONTROL_DESCR",
        "ADDRESS_EMAIL",
        "COMM_TELEPHONE",
        "PERIOD",
        "INTER_FLAG",
        "DEMERGE_FLAG",
        "TAX_OFFICE",
        "CUSTOMER_NAME_ORIG",
        "SUPER GROUP",
        "MAIN GROUP",
        "PURCHASE GROUP",
        "INVOICE CUSTOMER",
        "CUSTOMER_COMPANY"
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_inv_customers_gs", truncate=True)



MAPPING_INV_CODE = (
    INV_CUSTOMERS

    .select(
        "INV_ID",
        "CUSTOMER_CODE",
    )

    .drop_duplicates_ordered(["INV_ID"], ["CUSTOMER_CODE"])   # todo remove
).assert_no_duplicates("INV_ID")







####################################
######## Comm Organizations ########
####################################




COMM_ORGANIZATIONS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_comm_organizations")  # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\COMM_ORGANIZATIONS.QVD](qvd);

    .select(
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
        "WMA_ASA_AREA_DESC"
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_comm_organizations_gs", truncate=True)






################################
######## Return Reasons ########
################################

RETURN_REASONS_INPUT = read_data_from_redshift_table("dwe.qlk_salesanalysis_anagrafiche_return_reasons")

RETURN_REASONS_TEMP = (
    RETURN_REASONS_INPUT    # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\RETURN_REASONS.QVD](qvd);

    .select(
        col("MOTIVE").alias("MOTIVE"),
        col("MOTIVE_DESCR").alias("MOTIVE_DESCR"),
        col("CAUSE").alias("CAUSE"),
        lit("CAUSE_DESCR").alias("CAUSE_DESCR"),
        lit("_KEY_RETURN_REASON").alias("_KEY_RETURN_REASON")
    )
)
RETURN_REASONS = RETURN_REASONS_TEMP



RETURN_REASONS_TEMP = (
    RETURN_REASONS_INPUT    # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Anagrafiche\Data\$(vEnvPROD)/QVD02\RETURN_REASONS.QVD](qvd);

    .select(
        col("MOTIVE").alias("MOTIVE"),
        col("MOTIVE_DESCR").alias("MOTIVE_DESCR"),
        col("CAUSE").alias("CAUSE"),
        lit("CAUSE_DESCR").alias("CAUSE_DESCR"),
        concat_ws("|", col("MOTIVE"), col("CAUSE")).alias("_KEY_RETURN_REASON")
    )
)
RETURN_REASONS = RETURN_REASONS.unionByName(RETURN_REASONS_TEMP, allowMissingColumns=True).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_return_reasons_gs", truncate=True)





###################################
######## DW_EXCHANGE_RATES ########
###################################


DW_EXCHANGE_RATES = (
    spark.createDataFrame([{"EXCHANGE_RATE": "", "CURRENCY": "", "CURRENCY_CODE": "", "YEAR_MONTH_NUMBER": "", "EXCHANGE_VALUE": ""}])   # FROM [lib://Project_Files/Framework_HORSA/Projects/Anagrafiche/Data/$(vEnvPROD)/QVD02/DW_EXCHANGE_RATES.QVD](qvd)

    .where(col("EXCHANGE_RATE").isin(['ANNUAL STD','ACTUAL HAIER','ACTUAL ECB']))

    .select(
        col("EXCHANGE_RATE").alias("EXCHANGE RATE"),
        "CURRENCY",
        col("CURRENCY_CODE").alias("_DW_EXCHANGE_RATES_CURRENCY_CODE"),
        col("YEAR_MONTH_NUMBER").alias("_DW_EXCHANGE_RATES_YEAR_MONTH_NUMBER"),
        col("EXCHANGE_VALUE").alias("_EXCHANGE_VALUE")
    )
).save_data_to_redshift_table("dwe.qlk_salesanalysis_hetl3_dw_exchange_rates_gs", truncate=True)




MAPPING_CURRENCY_CODE_YEAR_MONTH_NUMBER = (
    DW_EXCHANGE_RATES

    .select(
        concat_ws("-@-", col("_DW_EXCHANGE_RATES_CURRENCY_CODE"), col("_DW_EXCHANGE_RATES_YEAR_MONTH_NUMBER")).alias("KEY"),
        concat_ws("-@-", col("_DW_EXCHANGE_RATES_CURRENCY_CODE"), col("_DW_EXCHANGE_RATES_YEAR_MONTH_NUMBER")).alias("VALUE")
    )
    .drop_duplicates_ordered(["KEY"], ["VALUE"])   # todo remove
).assert_no_duplicates("KEY")





#########################
######## Mapping ########
#########################



Mapping_BUSINESS_SECTOR_CODE = (
    PRODUCTS
    .select(
        col("PRODUCT_ID"),
        col("BUSINESS_SECTOR_CODE")
    )
    .drop_duplicates_ordered(["PRODUCT_ID"], ["BUSINESS_SECTOR_CODE"])   # todo remove
).assert_no_duplicates("PRODUCT_ID")


Mapping_FOB = (
    PRODUCTS
    .select(
        col("PRODUCT CODE"),
        col("FOB")
    )
    .drop_duplicates_ordered(["PRODUCT CODE"], ["FOB"])   # todo remove
).assert_no_duplicates("PRODUCT CODE")


Mapping_IND_DIVISION_CODE = (
    PRODUCTS
    .select(
        col("PRODUCT_ID"),
        col("IND_DIVISION_CODE")
    )
    .drop_duplicates_ordered(["PRODUCT_ID"], ["IND_DIVISION_CODE"])   # todo remove
).assert_no_duplicates("PRODUCT_ID")


Mapping_IND_DIVISION_DESCR = (
    PRODUCTS
    .select(
        col("PRODUCT_ID"),
        col("IND_DIVISION_DESCR")
    )
    .drop_duplicates_ordered(["PRODUCT_ID"], ["IND_DIVISION_DESCR"])   # todo remove
).assert_no_duplicates("PRODUCT_ID")


Mapping_HAIER_DIVISION = (
    PRODUCTS
    .select(
        col("PRODUCT_ID"),
        col("HAIER DIVISION")
    )
    .drop_duplicates_ordered(["PRODUCT_ID"], ["HAIER DIVISION"])   # todo remove
).assert_no_duplicates("PRODUCT_ID")


MAPPING_INVOICE_CUSTOMER_ID = (
    spark.createDataFrame([{"CUSTOMER_COMPANY": "", "CUSTOMER_CODE": "", "INV_ID": "", }])
    .select(
        concat_ws("-@-", col("CUSTOMER_COMPANY"), col("CUSTOMER_CODE")).alias("key"),
        col("INV_ID")
    )
).assert_no_duplicates("key")


MAPPING_BUSINESS_SECTOR_DESCR = (
    PRODUCTS
    .select(
        col("PRODUCT_ID"),
        col("BUSINESS_SECTOR_DESCR")
    )
    .drop_duplicates_ordered(["PRODUCT_ID"], ["BUSINESS_SECTOR_DESCR"])   # todo remove
).assert_no_duplicates("PRODUCT_ID")





############################
######## Orders SAP ########
############################



FACTS_Tmp = (
    #read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_orders")
    read_data_from_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_orders_gs")


    .withColumn(
        "DIV",
        when((col("cas_sap") == "sap") & (col("division") == "FG"), lit(1))
        .when(col("cas_sap") == "cas", lit(1))
        .otherwise(0)
    )
    .where(col("DIV") == 1)  # Filtra solo dove DIV == 1
    .drop("DIV")
    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    #.do_mapping(Mapping_IND_DIVISION_CODE, col("PRODUCT_ID"), "IND_DIVISION_CODE", default_value=lit(0), mapping_name="Mapping_IND_DIVISION_CODE")
    #.do_mapping(Mapping_IND_DIVISION_DESCR, col("PRODUCT_ID"), "IND_DIVISION_DESCR", default_value=lit(0), mapping_name="Mapping_IND_DIVISION_DESCR")
    #.do_mapping(Mapping_BUSINESS_SECTOR_CODE, col("PRODUCT_ID"), "BUSINESS_SECTOR_CODE", default_value=lit(0), mapping_name="Mapping_BUSINESS_SECTOR_CODE")
    #.do_mapping(MAPPING_BUSINESS_SECTOR_DESCR, col("PRODUCT_ID"), "BUSINESS_SECTOR_DESCR", default_value=lit(0), mapping_name="MAPPING_BUSINESS_SECTOR_DESCR")
    .do_mapping(MAPPING_DEST_CODE, col("DEST_ID"), "DEST_CODE", default_value=lit(0), mapping_name="MAPPING_DEST_CODE")
    .do_mapping(MAPPING_INV_CODE, col("INV_ID"), "CUSTOMER_CODE", default_value=lit(0), mapping_name="MAPPING_INV_CODE")

    .select(
        "*",
        #col("DESPATCHING_DATE").alias("_EXCHANGE_DATE"),
        #col("DESPATCHING_DATE").alias("_DESPATCHING_DATE"),
        #col("TRANSPORT_COST").alias("_TRANSPORT_COST"),
        #col("TRANSPORT_COST_INFRA").alias("_TRANSPORT_COST_INFRA"),
        when(col("DOC_TYPE_CODE").isin([4, 5, 6]), lit(999)).otherwise(col("RETURN_REASON")).alias("_KEY_RETURN_REASON"),
        #col("ORDER_DATE").alias("_ORDER_DATE"),
        #col("ORD_REQUESTED_DELIVERY_DATE").alias("_ORD_REQUESTED_DELIVERY_DATE"),
        #col("MRDD").alias("_MRDD"),
        #col("DELIVERY_DATE_REQUESTED").alias("_DELIVERY_DATE_REQUESTED"),
        #col("PICKING_DATE").alias("_PICKING_DATE"),
        #col("EST_AVAILABLE_DATE_TO_WARE").alias("_EST_AVAILABLE_DATE_TO_WARE"),
        #col("START_DATE_NSV_REVISED").alias("_START_DATE_NSV_REVISED"),
        #col("START_DATE_TPRICE_REVISED").alias("_START_DATE_TPRICE_REVISED"),
        #col("APPOINTMENT").alias("_APPOINTMENT"),
        #col("APPOINTMENT_DATE").alias("_APPOINTMENT_DATE"),
        #col("APPOINTMENT_TIME").alias("APPOINTMENT TIME"),
        #col("ORDER_DELIVERY_TERM").alias("ORDER DELIVERY TERM"),
        #col("PLANNED_DISPATCH_DATE").alias("_PLANNED_DISPATCH_DATE"),
        #col("BOOKING_STATUS").alias("BOOKING STATUS"),
        #col("CONTAINER_NUMBER").alias("CONTAINER NUMBER"),
        #col("BOOKING_UPDATE").alias("BOOKING UPDATE"),
        #col("INSTOCK NSV LC").alias("_INSTOCK NSV LC"),
        #col("INSTOCK GSV LC").alias("_INSTOCK GSV LC"),
        #col("INSTOCK GM CP LC").alias("_INSTOCK GM CP LC"),
        #col("INSTOCK GM TP LC").alias("_INSTOCK GM TP LC"),
        #col("REPLENISHMENT NSV LC").alias("_REPLENISHMENT NSV LC"),
        #col("REPLENISHMENT GSV LC").alias("_REPLENISHMENT GSV LC"),
        #col("REPLENISHMENT GM CP LC").alias("_REPLENISHMENT GM CP LC"),
        #col("REPLENISHMENT GM TP LC").alias("_REPLENISHMENT GM TP LC"),
        #col("ORM NSV LC").alias("_ORM NSV LC"),
        #col("ORM GSV LC").alias("_ORM GSV LC"),
        #col("ORM GM CP LC").alias("_ORM GM CP LC"),
        #col("ORM GM TP LC").alias("_ORM GM TP LC"),
        #col("UNALLOCATED NSV LC").alias("_UNALLOCATED NSV LC"),
        #col("UNALLOCATED GSV LC").alias("_UNALLOCATED GSV LC"),
        #col("UNALLOCATED GM CP LC").alias("_UNALLOCATED GM CP LC"),
        #col("UNALLOCATED GM TP LC").alias("_UNALLOCATED GM TP LC"),
        #col("INSTOCK QTY").alias("_INSTOCK QTY"),
        #col("INSTOCK NSV €").alias("_INSTOCK NSV €"),
        #col("INSTOCK GSV €").alias("_INSTOCK GSV €"),
        #col("INSTOCK GM CP €").alias("_INSTOCK GM CP €"),
        #col("INSTOCK GM TP €").alias("_INSTOCK GM TP €"),
        #col("REPLENISHMENT QTY").alias("_REPLENISHMENT QTY"),
        #col("REPLENISHMENT NSV €").alias("_REPLENISHMENT NSV €"),
        #col("REPLENISHMENT GSV €").alias("_REPLENISHMENT GSV €"),
        #col("REPLENISHMENT GM CP €").alias("_REPLENISHMENT GM CP €"),
        #col("REPLENISHMENT GM TP €").alias("_REPLENISHMENT GM TP €"),
        #col("ORM QTY").alias("_ORM QTY"),
        #col("ORM NSV €").alias("_ORM NSV €"),
        #col("ORM GSV €").alias("_ORM GSV €"),
        #col("ORM GM CP €").alias("_ORM GM CP €"),
        #col("ORM GM TP €").alias("_ORM GM TP €"),
        #col("UNALLOCATED QTY").alias("_UNALLOCATED QTY"),
        #col("UNALLOCATED NSV €").alias("_UNALLOCATED NSV €"),
        #col("UNALLOCATED GSV €").alias("_UNALLOCATED GSV €"),
        #col("UNALLOCATED GM CP €").alias("_UNALLOCATED GM CP €"),
        #col("UNALLOCATED GM TP €").alias("_UNALLOCATED GM TP €"),

        #col("ORDER_SOURCE").alias("ORDER SOURCE"),
        #col("ORDER_SOURCE_DESCR").alias("ORDER SOURCE DESC"),
    )

    .withColumn("booking_slot", col("booking_slot").cast("date"))

).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl3_facts_gs/fact_type=order", do_cast_decimal_to_double=True)

print("loaded data from qlk_salesanalysis_hetl2_orders")







################################
######## Deliveries SAP ########
################################



DELIVERIES = None   # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Sales Analysis\Data\$(vEnvFrom)/QVD02\DELIVERIES\*.QVD](qvd);

DELIVERIES = (
    #read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_deliveries")
    read_data_from_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_deliveries_gs")


    .withColumn(
        "DIV",
        when((col("cas_sap") == "sap") & (col("division") == "FG"), lit(1))
        .when(col("cas_sap") == "cas", lit(1))
        .otherwise(0)
    )
    .where(col("DIV") == 1)  # Filtra solo dove DIV == 1
    .drop("DIV")
    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    #.do_mapping(Mapping_IND_DIVISION_CODE, col("PRODUCT_ID"), "IND_DIVISION_CODE", default_value=lit(0), mapping_name="Mapping_IND_DIVISION_CODE")
    #.do_mapping(Mapping_IND_DIVISION_DESCR, col("PRODUCT_ID"), "IND_DIVISION_DESCR", default_value=lit(0), mapping_name="Mapping_IND_DIVISION_DESCR")
    #.do_mapping(Mapping_BUSINESS_SECTOR_CODE, col("PRODUCT_ID"), "BUSINESS_SECTOR_CODE", default_value=lit(0), mapping_name="Mapping_BUSINESS_SECTOR_CODE")
    #.do_mapping(MAPPING_BUSINESS_SECTOR_DESCR, col("PRODUCT_ID"), "BUSINESS_SECTOR_DESCR", default_value=lit(0), mapping_name="MAPPING_BUSINESS_SECTOR_DESCR")
    .do_mapping(MAPPING_DEST_CODE, col("DEST_ID"), "DEST_CODE", default_value=lit(0), mapping_name="MAPPING_DEST_CODE")
    .do_mapping(MAPPING_INV_CODE, col("INV_ID"), "CUSTOMER_CODE", default_value=lit(0), mapping_name="MAPPING_INV_CODE")

    .select(
        "*",

        #col("DESPATCHING_DATE").alias("_EXCHANGE_DATE"),
        #col("DESPATCHING_DATE").alias("_DESPATCHING_DATE"),
        #col("TRANSPORT_COST").alias("_TRANSPORT_COST"),
        #col("TRANSPORT_COST_INFRA").alias("_TRANSPORT_COST_INFRA"),
        when(col("DOC_TYPE_CODE").isin([4, 5, 6]), lit(999)).otherwise(col("RETURN_REASON")).alias("_KEY_RETURN_REASON"),
        #col("ORDER_DATE").alias("_ORDER_DATE"),
        #col("ORD_REQUESTED_DELIVERY_DATE").alias("_ORD_REQUESTED_DELIVERY_DATE"),
        #col("MRDD").alias("_MRDD"),
        #col("DELIVERY_DATE_REQUESTED").alias("_DELIVERY_DATE_REQUESTED"),
        #col("PICKING_DATE").alias("_PICKING_DATE"),
        #col("EST_AVAILABLE_DATE_TO_WARE").alias("_EST_AVAILABLE_DATE_TO_WARE"),
        #col("START_DATE_NSV_REVISED").alias("_START_DATE_NSV_REVISED"),
        #col("START_DATE_TPRICE_REVISED").alias("_START_DATE_TPRICE_REVISED"),
        #col("APPOINTMENT").alias("_APPOINTMENT"),
        #col("APPOINTMENT_DATE").alias("_APPOINTMENT_DATE"),
        #col("APPOINTMENT_TIME").alias("APPOINTMENT TIME"),
        #col("ORDER_DELIVERY_TERM").alias("ORDER DELIVERY TERM"),
        #col("PLANNED_DISPATCH_DATE").alias("_PLANNED_DISPATCH_DATE"),
        #col("BOOKING_STATUS").alias("BOOKING STATUS"),
        #col("CONTAINER_NUMBER").alias("CONTAINER NUMBER"),
        #col("BOOKING_UPDATE").alias("BOOKING UPDATE"),

        #col("ORDER_SOURCE").alias("ORDER SOURCE"),
        #col("ORDER_SOURCE_DESCR").alias("ORDER SOURCE DESC"),
    )
    .withColumn("confirmed date", col("confirmed date").cast("timestamp"))
    .withColumn("booking_slot", col("booking_slot").cast("date"))

).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl3_facts_gs/fact_type=delivery", do_cast_decimal_to_double=True)

print("loaded data from qlk_salesanalysis_hetl2_deliveries")






########################
######## Budget ########
########################


BUDGET = (
    read_data_from_redshift_table("dwe.qlik_cst_pbcs_budget")

    #.where(col("DIVISION") == "FG")
    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    .select(
        col("product_code"),

        col("network"),
        col("company"),

        col("sales_organization"),
        col("sales_office").alias("sales_office"),

        col("year").alias("year"),
        col("month").alias("month"),
        to_date(concat_ws("-", col("year"), col("month"), lit("1"))).alias("time_id"),
        to_date(concat_ws("-", col("year"), col("month"), lit("1"))).alias("time_id_fact"),

        col("budget_nsv_eur").alias("bdg_nsv_eur"),
        col("budget_nsv_local").alias("bdg_nsv_lc"),

        col("qty_sold").cast("int").alias("bdg_qty"),
        col("nsv_eur").alias("nsv_eur"),
        col("nsv_local").alias("nsv_lc"),

        lit(4).alias("data_type")
    )
).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl3_facts_gs/fact_type=budget", do_cast_decimal_to_double=True)

print("BUDGET:", BUDGET)




#############################
######## Invoice SAP ########
#############################




INVOICE = None   # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Sales Analysis\Data\$(vEnvStore)/QVD02\INVOICE\INVOICE_$(vInvoiceMonth).QVD](qvd);

INVOICE = (
    #read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_invoice")
    read_data_from_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_invoice_gs")


    .withColumn(
        "DIV",
        when((col("cas_sap") == "sap") & (col("division") == "FG"), lit(1))
        .when(col("cas_sap") == "cas", lit(1))
        .otherwise(0)
    )
    .where(col("DIV") == 1)  # Filtra solo dove DIV == 1
    .drop("DIV")
    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    #.do_mapping(Mapping_IND_DIVISION_CODE, col("PRODUCT_ID"), "IND_DIVISION_CODE", default_value=lit(0), mapping_name="Mapping_IND_DIVISION_CODE")
    #.do_mapping(Mapping_IND_DIVISION_DESCR, col("PRODUCT_ID"), "IND_DIVISION_DESCR", default_value=lit(0), mapping_name="Mapping_IND_DIVISION_DESCR")
    #.do_mapping(Mapping_BUSINESS_SECTOR_CODE, col("PRODUCT_ID"), "BUSINESS_SECTOR_CODE", default_value=lit(0), mapping_name="Mapping_BUSINESS_SECTOR_CODE")
    #.do_mapping(MAPPING_BUSINESS_SECTOR_DESCR, col("PRODUCT_ID"), "BUSINESS_SECTOR_DESCR", default_value=lit(0), mapping_name="MAPPING_BUSINESS_SECTOR_DESCR")
    .do_mapping(MAPPING_DEST_CODE, col("DEST_ID"), "DEST_CODE", default_value=lit(0), mapping_name="MAPPING_DEST_CODE")
    .do_mapping(MAPPING_INV_CODE, col("INV_ID"), "CUSTOMER_CODE", default_value=lit(0), mapping_name="MAPPING_INV_CODE")

    .select(
        "*",

        #col("TIME_ID").cast("timestamp").alias("_EXCHANGE_DATE"),
        #col("DESPATCHING_DATE").alias("_DESPATCHING_DATE"),
        #col("TRANSPORT_COST").alias("_TRANSPORT_COST"),
        #col("TRANSPORT_COST_INFRA").alias("_TRANSPORT_COST_INFRA"),
        when(col("DOC_TYPE_CODE").isin([4, 5, 6]), lit(999)).otherwise(col("RETURN_REASON")).alias("_KEY_RETURN_REASON"),
        #col("ORDER_DATE").alias("_ORDER_DATE"),
        #col("ORD_REQUESTED_DELIVERY_DATE").alias("_ORD_REQUESTED_DELIVERY_DATE"),
        #col("MRDD").alias("_MRDD"),
        #col("DELIVERY_DATE_REQUESTED").alias("_DELIVERY_DATE_REQUESTED"),
        #col("PICKING_DATE").alias("_PICKING_DATE"),
        #col("EST_AVAILABLE_DATE_TO_WARE").alias("_EST_AVAILABLE_DATE_TO_WARE"),
        #col("START_DATE_NSV_REVISED").alias("_START_DATE_NSV_REVISED"),
        #col("START_DATE_TPRICE_REVISED").alias("_START_DATE_TPRICE_REVISED"),
        #col("APPOINTMENT").alias("_APPOINTMENT"),
        #col("APPOINTMENT_DATE").alias("_APPOINTMENT_DATE"),
        #col("APPOINTMENT_TIME").alias("APPOINTMENT TIME"),
        #col("ORDER_DELIVERY_TERM").alias("ORDER DELIVERY TERM"),
        #col("PLANNED_DISPATCH_DATE").alias("_PLANNED_DISPATCH_DATE"),
        #col("BOOKING_STATUS").alias("BOOKING STATUS"),
        #col("CONTAINER_NUMBER").alias("CONTAINER NUMBER"),
        #col("BOOKING_UPDATE").alias("BOOKING UPDATE"),

        when(col("TIME_ID") < trunc(current_date(), "month"), col("INV QTY")).otherwise(lit(0)).alias("INV/SF QTY"),
        when(col("TIME_ID") < trunc(current_date(), "month"), col("INV NSV €")).otherwise(lit(0)).alias("INV/SF NSV €"),
        when(col("TIME_ID") < trunc(current_date(), "month"), col("INV GM CP €")).otherwise(lit(0)).alias("INV/SF GM CP €"),
        when(col("TIME_ID") < trunc(current_date(), "month"), col("INV GM TP €")).otherwise(lit(0)).alias("INV/SF GM TP €"),

        #col("ORDER_SOURCE").alias("ORDER SOURCE"),
        #col("ORDER_SOURCE_DESCR").alias("ORDER SOURCE DESC"),
    )
    .withColumn("confirmed date", col("confirmed date").cast("timestamp"))

    .withColumn("booking_slot", col("booking_slot").cast("date"))

).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl3_facts_gs/fact_type=invoice", do_cast_decimal_to_double=True)

print("loaded data from qlk_salesanalysis_hetl2_invoice")






##########################
######## Forecast ########
##########################







###############################
######## Product Facts ########
###############################



PRODUCT_FACTS = (
    PRODUCTS

    .select(
        "PRODUCT_ID",
        "IND_DIVISION_CODE",
        "IND_DIVISION_DESCR",
        "BUSINESS_SECTOR_CODE",
        "BUSINESS_SECTOR_DESCR"
    )
).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl3_product_gs/", do_cast_decimal_to_double=True)

print("loaded data from PRODUCTS")




print("finished :)")

