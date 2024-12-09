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
if INGESTION_DATE == "YESTERDAY":
    INGESTION_DATE = (datetime.today() - timedelta(days=1)).strftime('%Y/%m/%d')


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


print("BUCKET", BUCKET)


spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

DISPLAY_LOG = True






######### utility function

def filter_last_info(self, key):
    keep = self.groupBy(key).agg(max("Ingestion_Date").alias("Ingestion_Date"))

    print("filter_last_info:")
    keep.orderBy(key).show()

    return self.join(keep, key + ["Ingestion_Date"], "leftsemi")

setattr(DataFrame, "filter_last_info", filter_last_info)


def add_columns_if_not_present(self, cols, default=lit(None).cast("double")):
    for c in cols:
        if c.lower() not in {x.lower() for x in self.columns}:
            print(f">>> adding column {c} = {default}")
            self = self.withColumn(c, default)

    return self

setattr(DataFrame, "add_columns_if_not_present", add_columns_if_not_present)






####### mapping sap - cas

mapping_company_network = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier")

    .select(
        col("sales org").alias("Sales_Organization"),
        col("sales office").alias("Sales_Office"),
        col("us_company").alias("Company"),
        col("us_network").alias("Network"),
    )

    .withColumn("Sales_Organization", regexp_replace("Sales_Organization", "[^a-zA-Z0-9]", ""))
    .withColumn("Sales_Office", regexp_replace("Sales_Office", "[^a-zA-Z0-9]", ""))
    .withColumn("Company", regexp_replace("Company", "[^a-zA-Z0-9]", ""))
    .withColumn("Network", regexp_replace("Network", "[^a-zA-Z0-9]", ""))

    .withColumn("Sales_Organization", when(col("Sales_Organization") == "", lit(None)).otherwise(col("Sales_Organization")))
    .withColumn("Sales_Office", when(col("Sales_Office") == "", lit(None)).otherwise(col("Sales_Office")))
    .withColumn("Company", when(col("Company") == "", lit(None)).otherwise(col("Company")))
    .withColumn("Network", when(col("Network") == "", lit(None)).otherwise(col("Network")))

    .where(col("Company").isNotNull()).where(col("Network").isNotNull())

    .withColumn("Company", lpad("Company", 2, "0"))    # inserisci zero davanti a codici una cifra
    .withColumn("Network", lpad("Network", 2, "0"))
    
    .drop_duplicates_ordered(["Company", "Network"], ["Sales_Organization", "Sales_Office"])

    .assert_no_duplicates("Company", "Network")    # dato che mappiamo plm->sap, verifica duplicati su chiave plm
)

mapping_company_network.show(5)






######  begin

gold_pbcs_budget_fx = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_Budget_FX/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_Budget_FX"))
    .withColumnRenamed("Years", "Year")

    .filter_last_info(["Year"])

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .withColumnRenamed("BegBalance", "Beg_Balance")

    .select("Beg_Balance", "Currency", "Year", "Ingestion_Date", "Input_File_Name", "Pbcs_Source")
).save_data_to_redshift_table("dwa.pbcs_cst_ft_budget_fx")

print("gold_pbcs_budget_fx:", gold_pbcs_budget_fx)






gold_pbcs_budget = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_Budget/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_Budget"))
    .withColumnRenamed("Years", "Year")

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .withColumnRenamed("Product", "Product_Code")

    .withColumn("Company", split("Organization", "_")[0])
    .withColumn("Network", split("Organization", "_")[1])
    .withColumn("Company", lpad("Company", 2, "0"))
    .withColumn("Network", lpad("Network", 2, "0"))

    #.withColumn("Pbcs_Client", when(col("Client").contains("Export_QTY_NSV_CAS"), col("Client")).otherwise(lit(None)))
    #.withColumn("Pbcs_Customer",
    #    when(col("Pbcs_Client").isNotNull(), lit(None))
    #    .when(col("Client").startswith(concat(col("Organization"), lit("_"))), split(col("Client"), "_")[2])
    #    .otherwise(col("Client"))
    #)

    .withColumn("Pbcs_Client", col("Client"))

    .selectExpr(
        "Account", "Company", "Network", "Pbcs_Client", "Currency", "Product_Code", "Year",
        "stack(12, 1, Jan, 2, Feb, 3, Mar, 4, Apr, 5, May, 6, Jun, 7, Jul, 8, Aug, 9, Sep, 10, Oct, 11, Nov, 12, Dec) as (Month, Budget)",
        'Ingestion_Date', "Input_File_Name", "Pbcs_Source"
    )

    .filter_last_info(["Year"])

    .withColumn("Pivot_Account",
        when(col("Account") == "2000_ASO_CALC", "Gross_Sales")
        .when(col("Account") == "1095_LFG", "QTY_Sold")
        .when(col("Account") == "2090_LFG", "Net_Sales")
        .when(col("Account") == "A_NSV_MEDIO", "NSV")
        .otherwise(col("Account"))
    )
    .withColumn("Pivot_Currency", col("Currency"))
    .withColumn("Pivot", concat_ws("_", col("Pivot_Account"), col("Pivot_Currency")))
    .withColumn("Pivot", when(col("Pivot").startswith("QTY_Sold"), lit("QTY_Sold")).otherwise(col("Pivot")))
    .groupBy("Company", "Network", "Pbcs_Client", "Product_Code", "Year", "Month", 'Ingestion_Date', "Input_File_Name", "Pbcs_Source")
    .pivot("Pivot")
    .max("Budget")

    .add_columns_if_not_present(["Gross_Sales_Local", "NSV_EUR", "NSV_Local", "Net_Sales_Local", "QTY_Sold"])

    .withColumn("Budget_NSV_EUR", col("NSV_EUR") * col("QTY_Sold"))
    .withColumn("Budget_NSV_Local", col("NSV_Local") * col("QTY_Sold"))

    .join(mapping_company_network, ["Company", "Network"], "left")
).save_data_to_redshift_table("dwa.pbcs_cst_ft_budget")

print("gold_pbcs_budget:", gold_pbcs_budget)






gold_pbcs_fct = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_FCT/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_FCT"))
    .withColumnRenamed("Years", "Year")

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .withColumn("Product_Code", regexp_replace(col("Material_Sku"), "^SKU_", ""))
    .withColumn("Product_Brand", regexp_replace(col("Product_Brand"), "^Brd_", ""))
    .withColumn("Product_Brand", when(col("Product_Brand") == "None", lit(None)).otherwise(col("Product_Brand")))
    .withColumnRenamed("SalesOffices", "Sales_Office")
    .withColumn("Sales_Office", split(col("Sales_Office"), "_")[1])
    .withColumnRenamed("SourcePlant", "Source_Plant")
    .withColumn("Source_Plant", regexp_replace(col("Source_Plant"), "^SrcPlant_", ""))
    .withColumn("Source_Plant", when(col("Source_Plant") == "None", lit(None)).otherwise(col("Source_Plant")))

    .selectExpr(
        "Currency", "Product_Code", "PL_Destination_Acc", "Product_Brand", "Sales_Office", "Scenario", "Source_Plant", "Year",
        "stack(12, 1, Jan, 2, Feb, 3, Mar, 4, Apr, 5, May, 6, Jun, 7, Jul, 8, Aug, 9, Sep, 10, Oct, 11, Nov, 12, Dec) as (Month, Forecast)",
        'Ingestion_Date', "Input_File_Name", "Pbcs_Source"
    )

    #keep last ingestion_date for each ym
    .where(col("Forecast").isNotNull())

    .where(col("Month").cast("int") > substring(col("Ingestion_Date"), 5, 2).cast("int"))   # tieni solo previsioni future :)
    .filter_last_info(["Year", "Month"])

    .withColumn("Pivot_Account",
        when(col("PL_Destination_Acc") == "2090_CALC", "Gross_Sales")
        .when(col("PL_Destination_Acc") == "Dest_1095", "QTY_Sold")
        .when(col("PL_Destination_Acc") == "Dest_2000", "Net_Sales")
        .when(col("PL_Destination_Acc") == "NSV", "NSV")
        .otherwise(col("PL_Destination_Acc"))
    )
    .withColumn("Pivot_Currency", col("Currency"))
    .withColumn("Pivot", concat_ws("_", col("Pivot_Account"), col("Pivot_Currency")))
    .withColumn("Pivot", when(col("Pivot").startswith("QTY_Sold"), lit("QTY_Sold")).otherwise(col("Pivot")))
    .groupBy('Year', 'Month', 'Product_Code', 'Product_Brand', 'Sales_Office', 'Scenario', 'Source_Plant', 'Ingestion_Date', 'Input_File_Name', 'Pbcs_Source')
    .pivot("Pivot")
    .max("Forecast")

    .add_columns_if_not_present(["Gross_Sales_Local", "NSV_Local", "Net_Sales_local", "QTY_Sold"])

).save_data_to_redshift_table("dwa.pbcs_cst_ft_forecast")

print("gold_pbcs_fct:", gold_pbcs_fct)






gold_pbcs_freight_in = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_Freight_In/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_Freight_In"))
    .withColumnRenamed("Years", "Year")

    .filter_last_info(["Year"])

    .withColumnRenamed("BegBalance", "Beg_Balance")
    .withColumnRenamed("Product", "Product_Code")

    .withColumn("Client_Company", split("Client", "_")[1])
    .withColumn("Client_Network", split("Client", "_")[2])
    .withColumn("Client_Company", lpad("Client_Company", 2, "0"))
    .withColumn("Client_Network", lpad("Client_Network", 2, "0"))

    .withColumn("Company", split("Organization", "_")[0])
    .withColumn("Network", split("Organization", "_")[1])
    .withColumn("Company", lpad("Company", 2, "0"))
    .withColumn("Network", lpad("Network", 2, "0"))

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .select("Account", "Company", "Network", "Client_Company", "Client_Network", "Currency", "Product_Code", "Year", "Beg_Balance", "Ingestion_Date", "Input_File_Name", "Pbcs_Source")

    .withColumn("Pivot_Account",
        when(col("Account") == "A_CDC_1", "Customs_Cost_1")
        .when(col("Account") == "A_CDC_2", "Customs_Cost_2")
        .when(col("Account") == "A_TRC_TOT", "Transportation_Cost")
        .when(col("Account") == "A_CDC_3", "Customs_Cost_3")
        .when(col("Account") == "A_HANIC", "Handling_IC")
        .when(col("Account") == "A_NAHNW", "Handling_Network")
        .otherwise(col("Account"))
    )
    .withColumn("Pivot_Currency", col("Currency"))
    .withColumn("Pivot", concat_ws("_", col("Pivot_Account"), col("Pivot_Currency")))
    .withColumn("Pivot", when(col("Pivot").startswith("QTY_Sold"), lit("QTY_Sold")).otherwise(col("Pivot")))
    .groupBy("Client_Company", "Client_Network", 'Company', 'Network', 'Product_Code', 'Year', 'Ingestion_Date', "Input_File_Name", "Pbcs_Source")
    .pivot("Pivot")
    .max("Beg_Balance")

    .add_columns_if_not_present(["Customs_Cost_1_EUR", "Customs_Cost_2_EUR", "Customs_Cost_3_EUR", "Handling_IC_EUR", "Handling_Network_EUR", "Transportation_Cost_EUR" ])

    .withColumn("Customs_Duty_EUR", coalesce(col("Customs_Cost_1_EUR"), lit(0)) + coalesce(col("Customs_Cost_2_EUR"), lit(0)) + coalesce(col("Customs_Cost_3_EUR"), lit(0)))
    .withColumn("Freight_In_EUR", coalesce(col("Customs_Duty_EUR"), lit(0)) + coalesce(col("Handling_Network_EUR"), lit(0)) + coalesce(col("Transportation_Cost_EUR"), lit(0)))
    .withColumn("Transport_Cost_Infra_EUR", coalesce(col("Handling_IC_EUR"), lit(0)))

    .join(mapping_company_network, ["Company", "Network"], "left")
    .join(
        mapping_company_network.select(
            col("Company").alias("Client_Company"),
            col("Network").alias("Client_Network"),
            col("Sales_Organization").alias("Client_Sales_Organization"),
            col("Sales_Office").alias("Client_Sales_Office"),
        ), ["Client_Company", "Client_Network"], "left")

).save_data_to_redshift_table("dwa.pbcs_cst_ft_freight_in")

print("gold_pbcs_freight_in:", gold_pbcs_freight_in)






gold_pbcs_std_costs_commerciale = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_STD_Costs_Commerciale/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_STD_Costs_Commerciale"))
    .withColumnRenamed("Years", "Year")
    .withColumnRenamed("Result_ACE_R&D", "Result_ACE_RD")
    .withColumnRenamed("Product", "Product_Code")

    .withColumn("Quarter", regexp_extract(col("Input_File_Name"), ".*/(Q[1-4])/.*\.parquet$", 1))
    .filter_last_info(["Year", "Quarter"])

    # copia ultimo quarter in successivo

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .select('Currency', 'Platform_Support_Unit', 'Product_Code', 'Result_ACE_Conversion', 'Result_ACE_Design', 'Result_ACE_Procurement',
            'Result_ACE_RD', 'STD_Prod_Cost_Commercial', 'Year', "Quarter", "Ingestion_Date", "Input_File_Name", "Pbcs_Source")

    .withColumn("yq", concat("Year", "Quarter"))
)

max_yq = gold_pbcs_std_costs_commerciale.agg(max("yq")).collect()[0][0]
print("max_yq", max_yq)

year, quarter = map(int, max_yq.split("Q"))

if quarter == 4:
    year, quarter = year + 1, 1
else:
    year, quarter = year, quarter + 1

quarter = f"Q{quarter}"

print(f"generated year={year}, quarter={quarter}")

last_yq_data = (
    gold_pbcs_std_costs_commerciale.where(col("yq") == max_yq)
    .withColumn("Year", lit(year))
    .withColumn("Quarter", lit(quarter))
    .withColumn("yq", concat("Year", "Quarter"))
)

gold_pbcs_std_costs_commerciale_filled = (
    gold_pbcs_std_costs_commerciale
    .unionByName(last_yq_data)
    .drop("yq")
).save_data_to_redshift_table("dwa.pbcs_cst_ft_std_costs_commerciale")

print("gold_pbcs_std_costs_commerciale_filled:", gold_pbcs_std_costs_commerciale_filled)






gold_pbcs_stdindef_tca_costs = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_STDINDEF_TCA_Costs/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_STDINDEF_TCA_Costs"))
    .withColumnRenamed("Years", "Year")

    .withColumnRenamed("Product", "Product_Code")

    .filter_last_info(["Year"])

    .withColumnRenamed("BegBalance", "Beg_Balance")

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .groupBy("Currency", "Product_Code", "Year", "Input_File_Name", "Pbcs_Source", "Ingestion_Date")
    .pivot("Account")
    .sum("Beg_Balance")

    .add_columns_if_not_present(["STD_Prod_Cost_No_TCA_Definitive", "STD_Prod_Cost_No_TCA_Indicative", "TCA_Definitive", "TCA_Indicative"])

    .withColumn("STD_Prod_Cost_No_TCA", coalesce("STD_Prod_Cost_No_TCA_Definitive", "STD_Prod_Cost_No_TCA_Indicative"))
    .withColumn("TCA", coalesce("TCA_Definitive", "TCA_Indicative"))
).save_data_to_redshift_table("dwa.pbcs_cst_ft_stdindef_tca_costs")

print("gold_pbcs_stdindef_tca_costs:", gold_pbcs_stdindef_tca_costs)






gold_pbcs_transfer_price_costs = (
    read_data_from_s3_parquet(f"s3://{BUCKET}/Export_Transfer_Price_Costs/", mergeSchema=True, recursiveFileLookup=True)
    .withColumn("Input_File_Name", input_file_name())
    .withColumn("Pbcs_Source", lit("Export_Transfer_Price_Costs"))
    .withColumnRenamed("Years", "Year")

    .withColumnRenamed("Product", "Product_Code")

    .filter_last_info(["Year"])

    .withColumn("Client_Company", split("Client", "_")[1])
    .withColumn("Client_Network", split("Client", "_")[2])
    .withColumn("Client_Company", lpad("Client_Company", 2, "0"))
    .withColumn("Client_Network", lpad("Client_Network", 2, "0"))

    .withColumnRenamed("BegBalance", "Beg_Balance")

    .withColumn("Company", split("Organization", "_")[0])
    .withColumn("Network", split("Organization", "_")[1])
    .withColumn("Company", lpad("Company", 2, "0"))
    .withColumn("Network", lpad("Network", 2, "0"))

    .withColumn("Year", concat(lit("20"), substring(col("Year"), 3, 2)).cast("int"))

    .select("Account", "Beg_Balance", "Client_Company", "Client_Network", "Currency", "Company", "Network", "Product_Code", "Year", 'Ingestion_Date', "Input_File_Name", "Pbcs_Source")

    .groupBy("Company", "Network", "Client_Company", "Client_Network", "Currency", "Product_Code", "Year", 'Ingestion_Date', "Input_File_Name", "Pbcs_Source")
    .pivot("Account")
    .sum("Beg_Balance")

    .add_columns_if_not_present(["TP_Commerciale_Definitive", "TP_Commerciale_Indicative", "TP_ITCY_Definitive", "TP_ITCY_Indicative"])

    .withColumn("TP_Commerciale", coalesce("TP_Commerciale_Definitive", "TP_Commerciale_Indicative"))
    .withColumn("TP_ITCY", coalesce("TP_ITCY_Definitive", "TP_ITCY_Indicative"))

    .join(mapping_company_network, ["Company", "Network"], "left")
    .join(
        mapping_company_network.select(
            col("Company").alias("Client_Company"),
            col("Network").alias("Client_Network"),
            col("Sales_Organization").alias("Client_Sales_Organization"),
            col("Sales_Office").alias("Client_Sales_Office"),
        ), ["Client_Company", "Client_Network"], "left")
).save_data_to_redshift_table("dwa.pbcs_cst_ft_transfer_price_costs")

print("gold_pbcs_transfer_price_costs:", gold_pbcs_transfer_price_costs)



print("FINISHED OK!")
