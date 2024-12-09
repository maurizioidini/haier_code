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
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")





####################################
######## PRODUCT TYPER CODE ########
####################################
print("START Product Type Code")

df = (
    read_data_from_redshift_table("mtd.sie_dm_product_overview")
    .select(
        col("product_code").alias("product_code_over"),
        col("product_type_code")
    )
    .assert_no_duplicates("product_code_over")
)

df.createOrReplaceTempView('Product_Type_Overview')
Product_type=spark.sql('Select * from Product_Type_Overview')
#Product_type.show(1)

print("END Product Type Code")


#################################
########### INVOICE #############
#################################

print("START Invoice")

df = (
     read_data_from_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_invoice_gs")
     #read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_invoice")

    .withColumn(
        "DIV",
        when((col("cas_sap") == "sap") & (col("division") == "FG"), lit(1))
        .when(col("cas_sap") == "cas", lit(1))
        .otherwise(0)
    )
    .where(col("DIV") == 1)  # Filtra solo dove DIV == 1
    .drop("DIV")
)

df.createOrReplaceTempView('Invoice')
INVOICE=spark.sql("""SELECT
    a.product_code,
    CAST(dest_id AS INT) AS dest_id,
    CAST(inv_id AS INT) AS inv_id,
    CAST(company AS INT) AS company_id,
    NULL as `comm_organization_id`,
    TO_DATE(a.time_id, 'yyyy-MM-dd') AS invoice_date,
    CAST(order_number AS INT) AS order_number,
    CAST(network AS INT) AS network_id,
    TO_DATE(a.order_date, 'yyyy-MM-dd') AS _order_date,
    TO_DATE(a.order_requested_delivery_date, 'yyyy-MM-dd') AS order_requested_delivery_date,
    warehouse_classification,
    area_code,
    responsible_descr,
    responsible_descr1,
    responsible_descr3,
    responsible_code,
    responsible_code1,
    responsible_code3,
    order_blocking_code,
    order_customer_reference_1,
    `a/b_grade`,
    CASE WHEN `flag sap`=1 then "SAP" ELSE "DWH"  END as order_source,
    super_group_code,
    super_group_descr,
    TO_DATE(`confirmed date`, 'yyyy-MM-dd') AS `confirmed date`,
    main_group_code,
    main_group_descr,
    sale_zone_descr,
    sale_zone_code,
    salesman_name,
    CAST(`inv qty` AS DECIMAL(18,2)) AS `inv qty`,
    CAST(`inv nsv €` AS DECIMAL(18,2)) AS `inv nsv €`,
    CAST(`inv gsv €` AS DECIMAL(18,2)) AS `inv gsv €`,
    CAST(`inv gm tp €` AS DECIMAL(18,2)) AS `inv gm tp €`,
    CAST(`inv gm cp €` AS DECIMAL(18,2)) AS `inv gm cp €`,
    order_type_code,
    order_type_descr,
    order_delivery_warehouse,
    sale_director,
    rsm,
    rsm_code,
    sales_office,
    sales_organization,
    --CAST(us_network AS INT) AS us_network,
    YEAR(TO_DATE(a.time_id, 'yyyy-MM-dd')) AS year,
    MONTH(TO_DATE(a.time_id, 'yyyy-MM-dd')) AS month,
    DATE_FORMAT(TRUNC(TO_DATE(a.time_id, 'yyyy-MM-dd'), 'MM'), 'MMMM') AS invoice_month,
    DATE_FORMAT(TO_DATE(a.time_id, 'yyyy-MM-dd'), 'MMMM') AS period,
    CAST(salesman_code AS INT) AS salesman_code,
    DATE_FORMAT(TO_DATE(a.order_requested_delivery_date, 'yyyy-MM-dd'),'MMM yyyy') AS order_requested_delivery_year_month,
    CONCAT_WS(' ', purchase_group_code, purchase_group_descr) AS `purchase group`,
    CASE
        WHEN `flag sap` = '1' THEN TO_DATE(first_req_del_date, 'yyyy-MM-dd')
        ELSE TO_DATE(a.order_requested_delivery_date, 'yyyy-MM-dd')
    END AS first_req_del_date,
    market_budget,
    market_budget_descr
FROM
    Invoice a
LEFT JOIN
    Product_Type_Overview b ON a.Product_code = b.product_code_over
WHERE
    (
        CAST(`inv qty` AS DECIMAL(18,2)) <> 0
        OR CAST(`inv nsv €` AS DECIMAL(18,2)) <> 0
        OR CAST(`inv gsv €` AS DECIMAL(18,2)) <> 0
        OR CAST(`inv gm tp €` AS DECIMAL(18,2)) <> 0
        OR CAST(`inv gm cp €` AS DECIMAL(18,2)) <> 0
    )
    AND CAST(network AS INT) NOT IN (1, 2)
    AND COALESCE(CAST(product_type_code AS INT), 0) > 0;
    """)
#print(INVOICE.columns)
INVOICE=INVOICE.withColumn("source",lit("INV"))
#INVOICE.show(1)
print("END Invoice")

#################################
########### DELIVERY ############
#################################
print("START Delivery")

df=(
    read_data_from_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_deliveries_gs")
    #read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_deliveries")


    .withColumn(
        "DIV",
        when((col("cas_sap") == "sap") & (col("division") == "FG"), lit(1))
        .when(col("cas_sap") == "cas", lit(1))
        .otherwise(0)
    )
    .where(col("DIV") == 1)  # Filtra solo dove DIV == 1
    .drop("DIV")
)
df.createOrReplaceTempView('Delivery')

DELIVERY=spark.sql("""
    SELECT
    a.product_code ,
    dest_id,
    inv_id,
    company as company_id,
    NULL as `comm_organization_id`,
    order_number,
    TO_DATE(document_date, 'yyyy-MM-dd') AS invoice_date,  -- Conversione a DATE
    network as network_id,
    TO_DATE(order_date, 'yyyy-MM-dd') AS _order_date,  -- Conversione a DATE
    TO_DATE(order_requested_delivery_date, 'yyyy-MM-dd') AS order_requested_delivery_date,  -- Conversione a DATE
    warehouse_classification,
    area_code,
    `a/b_grade`,
    CASE WHEN `flag sap`=1 then "SAP" ELSE "DWH"  END as order_source,
    super_group_code,
    super_group_descr,
    main_group_code,
    main_group_descr,
    responsible_descr,
    responsible_descr1,
    responsible_descr3,
    responsible_code,
    responsible_code1,
    responsible_code3,
    order_customer_reference_1,
    order_blocking_code,
    sale_zone_descr,
    TO_DATE(`confirmed date`, 'yyyy-MM-dd') AS `confirmed date`,  -- Conversione a DATE
    sale_zone_code,
    CAST(salesman_code AS INT) AS salesman_code,  -- Conversione a INT
    salesman_name,
    CAST(`del qty` AS DECIMAL(18,4)) AS `del qty`,  -- Conversione a DECIMAL
    CAST(`del nsv €` AS DECIMAL(18,4)) AS `del nsv €`,
    CAST(`del gsv €` AS DECIMAL(18,4)) AS `del gsv €`,
    CAST(`del gm tp €` AS DECIMAL(18,4)) AS `del gm tp €`,
    CAST(`del gm cp €` AS DECIMAL(18,4)) AS `del gm cp €`,
    order_type_code,
    order_type_descr,
    order_delivery_warehouse,
    sale_director,
    rsm,
    rsm_code,
    sales_office,
    sales_organization,
    --us_network,
    YEAR(TO_DATE(document_date, 'yyyy-MM-dd')) AS year,  -- Conversione a DATE e estrazione anno
    MONTH(TO_DATE(document_date, 'yyyy-MM-dd')) AS month,  -- Conversione a DATE e estrazione mese
    DATE_FORMAT(TRUNC(TO_DATE(document_date, 'yyyy-MM-dd'), 'MM'), 'MMMM') AS invoice_month,  -- Formattazione mese
    DATE_FORMAT(TO_DATE(document_date, 'yyyy-MM-dd'), 'MMMM') AS period,  -- Formattazione mese
    DATE_FORMAT(TO_DATE(order_requested_delivery_date, 'yyyy-MM-dd'),'MMM yyyy') AS order_requested_delivery_year_month,  -- Formattazione mese di data richiesta
    CONCAT_WS(' ', purchase_group_code, purchase_group_descr) AS `purchase group`,
    CASE
        WHEN `flag sap` = '1' THEN TO_DATE(first_req_del_date, 'yyyy-MM-dd')  -- Conversione a DATE
        ELSE TO_DATE(order_requested_delivery_date, 'yyyy-MM-dd')
    END AS first_req_del_date,
    market_budget,
    market_budget_descr
FROM
    Delivery a
LEFT JOIN
    Product_Type_Overview b
    ON a.product_code = b.product_code_over
WHERE
    (
        CAST(a.`del qty` AS DECIMAL(18,4)) <> 0 OR  -- Verifica su DECIMAL per le colonne quantitative
        CAST(a.`del nsv €` AS DECIMAL(18,4)) <> 0 OR
        CAST(a.`del gsv €` AS DECIMAL(18,4)) <> 0 OR
        CAST(a.`del gm tp €` AS DECIMAL(18,4)) <> 0 OR
        CAST(a.`del gm cp €` AS DECIMAL(18,4)) <> 0
    )
    AND CAST(network AS INT) NOT IN (1, 2)
    AND COALESCE(CAST(product_type_code AS INT), 0) > 0;
    """)

DELIVERY=DELIVERY.withColumn("source",lit("DEL"))
#DELIVERY.show(1)
#print(DELIVERY.columns)
print("END Delivery")

#################################
############ ORDERS #############
#################################
print("START Orders")

df=(
    read_data_from_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_orders_gs")
    #read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_orders")

    .withColumn(
        "DIV",
        when((col("cas_sap") == "sap") & (col("division") == "FG"), lit(1))
        .when(col("cas_sap") == "cas", lit(1))
        .otherwise(0)
    )
    .where(col("DIV") == 1)  # Filtra solo dove DIV == 1
    .drop("DIV")

)
df.createOrReplaceTempView('Orders')

ORDERS=spark.sql("""WITH vMax AS (
    SELECT
        TRUNC(ADD_MONTHS(CURRENT_DATE(), 1), 'month') AS vMax1,
        TRUNC(ADD_MONTHS(CURRENT_DATE(), 2), 'month') AS vMax2,
        TRUNC(ADD_MONTHS(CURRENT_DATE(), 3), 'month') AS vMax3,
        TRUNC(ADD_MONTHS(CURRENT_DATE(), 4), 'month') AS vMax4,
        TRUNC(ADD_MONTHS(CURRENT_DATE(), 5), 'month') AS vMax5,
        TRUNC(ADD_MONTHS(CURRENT_DATE(), 6), 'month') AS vMax6
),

ORDERS AS (
    SELECT
        a.product_code,
        dest_id,
        inv_id,
        company as company_id,
    NULL as `comm_organization_id`,
        order_number,
        TO_DATE(document_date, 'yyyy-MM-dd') AS invoice_date,
        warehouse_classification,
        TO_DATE(order_date, 'yyyy-MM-dd') AS _order_date,
        TO_DATE(order_requested_delivery_date, 'yyyy-MM-dd') AS order_requested_delivery_date,
        area_code,
        `a/b_grade`,
        CASE WHEN `flag sap`='1' then "SAP" ELSE "DWH"  END as order_source,
        super_group_code,
        super_group_descr,
        main_group_code,
        main_group_descr,
        responsible_descr,
        responsible_descr1,
        responsible_descr3,
        responsible_code,
        responsible_code1,
        responsible_code3,
        order_customer_reference_1,
        order_blocking_code,
        sale_zone_descr,
        TO_DATE(`confirmed date`, 'yyyy-MM-dd') AS `confirmed date`,
        sale_zone_code,
        salesman_name,
        CAST(`ord qty` AS DECIMAL(18,4)) AS `ord qty`,
        CAST(`ord nsv €` AS DECIMAL(18,4)) AS `ord nsv €`,
        CAST(`ord gsv €` AS DECIMAL(18,4)) AS `ord gsv €`,
        CAST(`ord gm tp €` AS DECIMAL(18,4)) AS `ord gm tp €`,
        CAST(`ord gm cp €` AS DECIMAL(18,4)) AS `ord gm cp €`,
        order_type_code,
        order_type_descr,
        order_delivery_warehouse,
        sale_director,
        rsm,
        rsm_code,
        sales_office,
        sales_organization,
        TO_DATE(first_req_del_date, 'yyyy-MM-dd') AS first_req_del_date,
        network as network_id,
        YEAR(TO_DATE(document_date, 'yyyy-MM-dd')) AS year,
        MONTH(TO_DATE(document_date, 'yyyy-MM-dd')) AS month,
        CONCAT_WS(' ', purchase_group_code, purchase_group_descr) AS `purchase group`,
        DATE_FORMAT(TO_DATE(document_date, 'yyyy-MM-dd'), 'MMMM') AS period,
        CAST(salesman_code AS INT) AS salesman_code,
        DATE_FORMAT(TO_DATE(order_requested_delivery_date, 'yyyy-MM-dd'),'MMM yyyy') AS order_requested_delivery_year_month,
        CASE
            WHEN `flag sap` = '1' THEN TO_DATE(first_req_del_date, 'yyyy-MM-dd')
            ELSE TO_DATE(order_requested_delivery_date, 'yyyy-MM-dd')
        END AS FIRST_REQ_DEL_DATE_NUM,
        market_budget,
        market_budget_descr
    FROM Orders a
    LEFT JOIN Product_type_overview b
    ON a.product_code = b.product_code_over
    WHERE (
        CAST(`ord qty` AS DECIMAL(18,4)) <> 0 OR
        CAST(`ord nsv €` AS DECIMAL(18,4)) <> 0 OR
        CAST(`ord gsv €` AS DECIMAL(18,4)) <> 0 OR
        CAST(`ord gm tp €` AS DECIMAL(18,4)) <> 0 OR
        CAST(`ord gm cp €` AS DECIMAL(18,4)) <> 0
    )
    AND CAST(network AS INT) NOT IN (1, 2)
    AND COALESCE(CAST(product_type_code AS INT), 0) > 0
)

SELECT product_code,
    dest_id,
    inv_id,
    company_id,
    NULL as `comm_organization_id`,
    order_number,
    invoice_date,
    warehouse_classification,
    _order_date,
    order_requested_delivery_date,
    area_code,
    `a/b_grade`,
    order_source,
    super_group_code,
    super_group_descr,
    main_group_code,
    main_group_descr,
    responsible_descr,
    responsible_descr1,
    responsible_descr3,
    responsible_code,
    responsible_code1,
    responsible_code3,
    order_customer_reference_1,
    order_blocking_code,
    sale_zone_descr,
    `confirmed date`,
    sale_zone_code,
    salesman_name,
    order_type_code,
    order_type_descr,
    order_delivery_warehouse,
    sale_director,
    rsm,
    rsm_code,
    sales_office,
    sales_organization,
    first_req_del_date,
    network_id,
    year,
    month,
    `purchase group`,
    period,
    salesman_code,
    order_requested_delivery_year_month,
    market_budget,
    market_budget_descr,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM < (SELECT vMax1 FROM vMax) THEN `ord qty` ELSE 0 END AS `ord qty`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM < (SELECT vMax1 FROM vMax) THEN `ord nsv €` ELSE 0 END AS `ord nsv €`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM < (SELECT vMax1 FROM vMax) THEN `ord gsv €` ELSE 0 END AS `ord gsv €`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM < (SELECT vMax1 FROM vMax) THEN `ord gm tp €` ELSE 0 END AS `ord gm tp €`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM < (SELECT vMax1 FROM vMax) THEN `ord gm cp €` ELSE 0 END AS `ord gm cp €`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax1 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax2 FROM vMax) THEN `ord qty` ELSE 0 END AS `ord qty M+1`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax1 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax2 FROM vMax) THEN `ord nsv €` ELSE 0 END AS `ord nsv € M+1`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax1 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax2 FROM vMax) THEN `ord gsv €` ELSE 0 END AS `ord gsv € M+1`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax1 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax2 FROM vMax) THEN `ord gm tp €` ELSE 0 END AS `ord gm tp € M+1`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax1 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax2 FROM vMax) THEN `ord gm cp €` ELSE 0 END AS `ord gm cp € M+1`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax2 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax3 FROM vMax) THEN `ord qty` ELSE 0 END AS `ord qty M+2`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax2 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax3 FROM vMax) THEN `ord nsv €` ELSE 0 END AS `ord nsv € M+2`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax2 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax3 FROM vMax) THEN `ord gsv €` ELSE 0 END AS `ord gsv € M+2`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax2 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax3 FROM vMax) THEN `ord gm tp €` ELSE 0 END AS `ord gm tp € M+2`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax2 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax3 FROM vMax) THEN `ord gm cp €` ELSE 0 END AS `ord gm cp € M+2`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax3 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax4 FROM vMax) THEN `ord qty` ELSE 0 END AS `ord qty M+3`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax3 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax4 FROM vMax) THEN `ord nsv €` ELSE 0 END AS `ord nsv € M+3`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax3 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax4 FROM vMax) THEN `ord gsv €` ELSE 0 END AS `ord gsv € M+3`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax3 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax4 FROM vMax) THEN `ord gm tp €` ELSE 0 END AS `ord gm tp € M+3`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax3 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax4 FROM vMax) THEN `ord gm cp €` ELSE 0 END AS `ord gm cp € M+3`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax4 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax5 FROM vMax) THEN `ord qty` ELSE 0 END AS `ord qty M+4`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax4 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax5 FROM vMax) THEN `ord nsv €` ELSE 0 END AS `ord nsv € M+4`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax4 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax5 FROM vMax) THEN `ord gsv €` ELSE 0 END AS `ord gsv € M+4`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax4 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax5 FROM vMax) THEN `ord gm tp €` ELSE 0 END AS `ord gm tp € M+4`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax4 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax5 FROM vMax) THEN `ord gm cp €` ELSE 0 END AS `ord gm cp € M+4`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax5 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax6 FROM vMax) THEN `ord qty` ELSE 0 END AS `ord qty M+5`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax5 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax6 FROM vMax) THEN `ord nsv €` ELSE 0 END AS `ord nsv € M+5`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax5 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax6 FROM vMax) THEN `ord gsv €` ELSE 0 END AS `ord gsv € M+5`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax5 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax6 FROM vMax) THEN `ord gm tp €` ELSE 0 END AS `ord gm tp € M+5`,
    CASE WHEN FIRST_REQ_DEL_DATE_NUM >= (SELECT vMax5 FROM vMax) AND FIRST_REQ_DEL_DATE_NUM < (SELECT vMax6 FROM vMax) THEN `ord gm cp €` ELSE 0 END AS `ord gm cp € M+5`
    FROM ORDERS;

""")

#ORDERS.show(1)
ORDERS=ORDERS.withColumn("source",lit("ORD"))
#print(ORDERS.columns)
print("END Orders")

#################################
########### BUDGET ##############
#################################

print("START Budget")

budget_input =(
    read_data_from_redshift_table("dwe.qlik_cst_pbcs_budget")
).localCheckpoint()

print("budget_input:")
budget_input.show(3)


budget_values = (
    budget_input

    .withColumn("time_id", to_date(concat_ws("-", col("year"), col("month"), lit("01"))))

    .select("company", "network", "sales_organization", "sales_office", "product_code", "time_id", "qty_sold", "nsv_local", "nsv_eur", "budget_nsv_local", "budget_nsv_eur")

    #.groupBy("company", "network", "time_id")
    .groupBy("company", "network", "sales_organization", "sales_office", "product_code", "time_id")
    .agg(
        sum("qty_sold").alias("qty_sold"),
        sum("nsv_local").alias("nsv_local"),
        sum("nsv_eur").alias("nsv_eur"),
        sum("budget_nsv_local").alias("budget_nsv_local"),
        sum("budget_nsv_eur").alias("budget_nsv_eur"),
    )
)

print("budget_values:")
budget_values.show(3)

from datetime import datetime
from dateutil.relativedelta import relativedelta

today = datetime.today().date().replace(day=1)
next_three_months = list(enumerate([today + relativedelta(months=i) for i in range(0, 4)]))

quantities = [sum(when(col("time_id") == lit(x), col("qty_sold"))).alias(f"qty_sold_{i}") for i, x in next_three_months]
#nsv_local = [sum(when(col("time_id") == lit(x), col("nsv_local"))).alias(f"nsv_local_{i}") for i, x in next_three_months]
#nsv_eur = [sum(when(col("time_id") == lit(x), col("nsv_eur"))).alias(f"nsv_eur_{i}") for i, x in next_three_months]
#budget_nsv_local = [sum(when(col("time_id") == lit(x), col("budget_nsv_local"))).alias(f"budget_nsv_local_{i}") for i, x in next_three_months]
budget_nsv_eur = [sum(when(col("time_id") == lit(x), col("budget_nsv_eur"))).alias(f"budget_nsv_eur_{i}") for i, x in next_three_months]
all_cols = quantities + budget_nsv_eur # quantities + nsv_local + nsv_eur + budget_nsv_local + budget_nsv_eur

print("generating:",)
print(all_cols)

budget_values_pivot = (

    budget_values

    .groupBy("company", "network", "sales_organization", "sales_office", "product_code")
    .agg(*all_cols)

)

print("budget_values_pivot:")
budget_values_pivot.show(3)

company_network_data = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier_uk")
    .drop_duplicates_ordered(["company", "network"], [col("SALES_ORGANIZATION").isNull(), col("SALES_OFFICE").isNull(), "COMPANY", "LINE"])
    .select(
        "company", "network",
        "responsible_code", "responsible_descr", "responsible_code1", "responsible_descr1", "responsible_code3", "responsible_descr3", "market_budget", "market_budget_descr"
    )
    .distinct()
    .assert_no_duplicates("company", "network")
).localCheckpoint()

print("company_network_data:")
company_network_data.show(3)

budget = (
    budget_values_pivot

    .join(company_network_data, ["company", "network"], "left")
)

print("budget:")
budget.show(3)


BUDGET_UN = (
    budget

    .where(~col("network").cast("int").isin([1,2]))

    .select(
        year(current_date()).alias("year"),   # calculation date (today), NOT BUDGET REFERENCE DATE!!
        month(current_date()).alias("month"),

        col("company").alias("company_id"),
        col("company").alias("network_id"),
        "sales_organization",
        "sales_office",
        "product_code",
        "market_budget",
        "market_budget_descr",
        "RESPONSIBLE_CODE1",
        "RESPONSIBLE_DESCR1",
        "RESPONSIBLE_CODE",
        "RESPONSIBLE_DESCR",
        "RESPONSIBLE_CODE3",
        "RESPONSIBLE_DESCR3",
        col("qty_sold_0").alias("bdg_qty"),
        col("budget_nsv_eur_0").alias("bdg_nsv_€"),
        col("qty_sold_1").alias("bdg_qty_m+1"),
        col("budget_nsv_eur_1").alias("bdg_nsv_€_m+1"),
        col("qty_sold_2").alias("bdg_qty_m+2"),
        col("budget_nsv_eur_2").alias("bdg_nsv_€_m+2"),
        col("qty_sold_3").alias("bdg_qty_m+3"),
        col("budget_nsv_eur_3").alias("bdg_nsv_€_m+3"),
    )

    .withColumn("source",lit("BUD"))
).localCheckpoint()

print("budget:")
BUDGET_UN.show(3)

print("END Budget")


#################################
############# UNION #############
#################################

print("START Union")

UNION = ORDERS.unionByName(BUDGET_UN, allowMissingColumns=True).unionByName(DELIVERY, allowMissingColumns=True).unionByName(INVOICE, allowMissingColumns=True)
#UNION.show(1)
#print(UNION.columns)
print("END Union")

print("START Writing qlk_portfolio_detail_reporting")
UNION.createOrReplaceTempView('Union')
FACT_TMP2=spark.sql("""SELECT
    product_code as product_id,
    dest_id,
    network_id,
    first_req_del_date,
    inv_id,
    company_id,
    cast(NULL as int) as `comm_organization_id`,
    `order_number` as `order number`,
    _order_date  as `order date`,
    invoice_date as `invoice date`,
    year as year,
    date_format(make_date(year, month, 1), 'MMM') as month,
    order_requested_delivery_date as `ord req del date`,
    order_requested_delivery_year_month as `order request delivery year month`,
    area_code,
    `confirmed date` as `confirmed date`,
    RESPONSIBLE_CODE1 as region_code,
    RESPONSIBLE_CODE as cluster_code,
    RESPONSIBLE_CODE3 as subcluster_code,
    responsible_descr as cluster,
    responsible_descr1 as region,
    responsible_descr3 as subcluster,
    order_blocking_code as `order blocking code`,
    order_customer_reference_1 as `order customer reference`,
    order_type_code as `order type code`,
    order_type_descr as `order type`,
    order_delivery_warehouse as `order delivery warehouse`,
    `a/b_grade` as `a/b grade`,
    warehouse_classification as `warehouse classification`,
    super_group_code as `super group code`,
    concat(super_group_code, ' - ', super_group_descr) as `super group`,
    main_group_code as `main group code`,
    concat(main_group_code, ' - ' ,main_group_descr) as `main group`,
    `purchase group` as `purchase group`,
    sale_zone_code as `sale zone code`,
    first_req_del_date as `first req del date`,
    cast(first_req_del_date as int) as `first req del date_num`,
    sale_zone_descr as `sale zone`,
    salesman_name as salesman,
    cast(salesman_code as numeric) as `salesman code`,
    sale_director as `sale director`,
    rsm,
    cast(rsm_code as numeric) as `rsm code`,
    source,
    sales_office,
    sales_organization,
    order_source,
    market_budget,
    market_budget_descr,
    sum(`inv qty`) as `inv qty`,
    sum(`inv nsv €`) as `inv nsv €`,
    sum(`inv gsv €`) as `inv gsv €`,
    sum(`inv gm tp €`) as `inv gm tp €`,
    sum(`inv gm cp €`) as `inv gm cp €`,
    sum(`del qty`) as `del qty`,
    sum(`del nsv €`) as `del nsv €`,
    sum(`del gsv €`) as `del gsv €`,
    sum(`del gm tp €`) as `del gm tp €`,
    sum(`del gm cp €`) as `del gm cp €`,
    sum(`ord qty`) as `ord qty`,
    sum(`ord nsv €`) as `ord nsv €`,
    sum(`ord gsv €`) as `ord gsv €`,
    sum(`ord gm cp €`) as `ord gm cp €`,
    sum(`ord gm tp €`) as `ord gm tp €`,
    sum(`bdg_qty`) as `bdg qty`,
    sum(`bdg_nsv_€`) as `bdg nsv €`,
    sum(`bdg_qty_m+1`) as `bdg qty m+1`,
    sum(`bdg_nsv_€_m+1`) as `bdg nsv € m+1`,
    sum(`bdg_qty_m+2`) as `bdg qty m+2`,
    sum(`bdg_nsv_€_m+2`) as `bdg nsv € m+2`,
    sum(`bdg_qty_m+3`) as `bdg qty m+3`,
    sum(`bdg_nsv_€_m+3`) as `bdg nsv € m+3`,
    sum(`ord qty m+1`) as `ord qty m+1`,
    sum(`ord nsv € m+1`) as `ord nsv € m+1`,
    sum(`ord gsv € m+1`) as `ord gsv € m+1`,
    sum(`ord gm cp € m+1`) as `ord gm cp € m+1`,
    sum(`ord gm tp € m+1`) as `ord gm tp € m+1`,
    sum(`ord qty m+2`) as `ord qty m+2`,
    sum(`ord nsv € m+2`) as `ord nsv € m+2`,
    sum(`ord gsv € m+2`) as `ord gsv € m+2`,
    sum(`ord gm cp € m+2`) as `ord gm cp € m+2`,
    sum(`ord gm tp € m+2`) as `ord gm tp € m+2`,
    sum(`ord qty m+3`) as `ord qty m+3`,
    sum(`ord nsv € m+3`) as `ord nsv € m+3`,
    sum(`ord gsv € m+3`) as `ord gsv € m+3`,
    sum(`ord gm cp € m+3`) as `ord gm cp € m+3`,
    sum(`ord gm tp € m+3`) as `ord gm tp € m+3`,
    sum(`ord qty m+4`) as `ord qty m+4`,
    sum(`ord nsv € m+4`) as `ord nsv € m+4`,
    sum(`ord gsv € m+4`) as `ord gsv € m+4`,
    sum(`ord gm cp € m+4`) as `ord gm cp € m+4`,
    sum(`ord gm tp € m+4`) as `ord gm tp € m+4`,
    sum(`ord qty m+5`) as `ord qty m+5`,
    sum(`ord nsv € m+5`) as `ord nsv € m+5`,
    sum(`ord gsv € m+5`) as `ord gsv € m+5`,
    sum(`ord gm cp € m+5`) as `ord gm cp € m+5`,
    sum(`ord gm tp € m+5`) as `ord gm tp € m+5`

from
    union
where year=year(current_date())
and month=month(current_date())

group by
    area_code,
    product_code,
    dest_id,
    network_id,
    inv_id,
    first_req_del_date,
    company_id,
    `comm_organization_id`,
    `order_number`,
    year,
    date_format(make_date(year, month, 1), 'MMM'),
    order_requested_delivery_date,
    RESPONSIBLE_CODE1,
    RESPONSIBLE_CODE,
    RESPONSIBLE_CODE3,
    responsible_descr,
    responsible_descr1,
    warehouse_classification,
    order_customer_reference_1,
    invoice_date,
    responsible_descr3,
    `confirmed date`,
    _order_date,
    `a/b_grade`,
    super_group_code,
    super_group_descr,
    order_blocking_code,
    order_type_code,
    order_type_descr,
    order_requested_delivery_year_month,
    main_group_code,
    main_group_descr,
    `purchase group`,
    order_delivery_warehouse,
    sale_zone_code,
    first_req_del_date,
    sale_zone_descr,
    salesman_name,
    salesman_code,
    sale_director,
    rsm,
    rsm_code,
    source,
    sales_office,
    sales_organization,
    order_source,
    market_budget,
    market_budget_descr;
""")

print("START Writing dwe.qlk_portfolio_detail_reporting")

#FACT_TMP2.printSchema()
FACT_TMP2.save_data_to_redshift_table("dwe.qlk_portfolio_detail_reporting_gs",truncate=True)

print("END Writing qlk_portfolio_detail_reporting")
job.commit()
