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



################ set attr to dataframe class
setattr(DataFrame, "do_mapping", do_mapping)
setattr(DataFrame, "drop_duplicates_ordered", drop_duplicates_ordered)
setattr(DataFrame, "assert_no_duplicates", assert_no_duplicates)
setattr(DataFrame, "save_data_to_redshift_table", save_data_to_redshift_table)
setattr(DataFrame, "columns_to_lower", columns_to_lower)



spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


###########################################################
########### IMPORT AND MANIPULATE USEFUL TABLES ###########
###########################################################


########### import mapping_company_network
mapping_company_network = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier")

    .select(
        col("sales org").alias("SALES_ORGANIZATION"),
        col("sales office").alias("SALES_OFFICE"),
        col("us_company").alias("COMPANY"),
        col("us_network").alias("NETWORK"),
    )

    .withColumn("SALES_ORGANIZATION", regexp_replace("SALES_ORGANIZATION", "[^a-zA-Z0-9]", ""))
    .withColumn("SALES_OFFICE", regexp_replace("SALES_OFFICE", "[^a-zA-Z0-9]", ""))
    .withColumn("COMPANY", regexp_replace("COMPANY", "[^a-zA-Z0-9]", ""))
    .withColumn("NETWORK", regexp_replace("NETWORK", "[^a-zA-Z0-9]", ""))

    .withColumn("SALES_ORGANIZATION", when(col("SALES_ORGANIZATION") == "", lit(None)).otherwise(col("SALES_ORGANIZATION")))
    .withColumn("SALES_OFFICE", when(col("SALES_OFFICE") == "", lit(None)).otherwise(col("SALES_OFFICE")))
    .withColumn("COMPANY", when(col("COMPANY") == "", lit(None)).otherwise(col("COMPANY")))
    .withColumn("NETWORK", when(col("NETWORK") == "", lit(None)).otherwise(col("NETWORK")))

    .where(col("COMPANY").isNotNull()).where(col("NETWORK").isNotNull())

    .withColumn("COMPANY", lpad("COMPANY", 2, "0"))    # inserisci zero davanti a codici una cifra
    .withColumn("NETWORK", lpad("NETWORK", 2, "0"))

    .drop_duplicates_ordered(["Company", "Network"], ["Sales_Organization", "Sales_Office"])

    .assert_no_duplicates("COMPANY", "NETWORK")    # dato che mappiamo plm->sap, verifica duplicati su chiave plm
)

########### import mapping_product_code
oracle_product = read_data_from_redshift_table("dwa.oracle_product")
mapping_product_code = (
    oracle_product
    .select(
        col("product_id").alias("PRODUCT_ID"),
        col("product_code").cast("string").alias("PRODUCT CODE"),
    )
).assert_no_duplicates("PRODUCT_ID")

########### import _CUTOFF_DATE
CUTOFF_DATE_INPUT = spark.read.parquet(f"s3://{bucket_name}/bronze/CUTOFF_DATE_INPUT/")

_CUTOFF_DATE = (
    CUTOFF_DATE_INPUT
    #.withColumn("CUTOFF_DATE", to_date("CUTOFF_DATE", "dd/MM/yyyy"))

    # get CUTOFF_DATE of the "previous" row
    .withColumn("START_DATE", lag("CUTOFF_DATE").over(Window.orderBy("CUTOFF_DATE")))
    .withColumn("START_DATE", date_add(col("START_DATE"), 1))
    .withColumn("START_DATE", coalesce(col("START_DATE"), to_date(lit("2000-01-01"))))
)

########### import COMMERCIAL_MAP_DATE
COMMERCIAL_MAP_DATE = (
    spark.sql("select explode(sequence(to_date('2000-01-01'), to_date('3000-01-01'), interval 1 day)) as Date")

    # tiene solo current year!
    .where(year(col("Date")) == year(current_date()))

    # aggiunge colonna "MONTH"
    .join(_CUTOFF_DATE, col("Date").between(col("START_DATE"), col("CUTOFF_DATE")))

    .select("Date", "MONTH")
)

########### import MAPPING_COMPANY_CURRENCY
MAPPING_COMPANY_CURRENCY = (
    spark.read.parquet(f"s3://{bucket_name}/mapping/Company_Currency_MAPPING/")
    .select(
        col("COMPANY_ID").alias("KEY"),
        col("CURRENCY")
    ).distinct()
).assert_no_duplicates("KEY")


########### import pbcs_cst_ft_budget
pbcs_cst_ft_budget = read_data_from_redshift_table("dwa.pbcs_cst_ft_budget")

PRZ_BUDGET_MAPPING = (
    pbcs_cst_ft_budget
    .where(col("pbcs_client") == "Export_QTY_NSV_CAS")

    .select(
        concat_ws("-@-", col("sales_organization"), col("sales_office"), col("product_code"), col("year"), col("month")).alias("KEY"),
        col("nsv_local").alias("nsv_local"),
        #col("nsv_eur").alias("nsv_eur"),
    )
).assert_no_duplicates("KEY")

########### import mapping_DD
mapping_DD = (
    spark.createDataFrame([{"YEAR": "", "COMPANY": "", "NETWORK": "", "PRODUCT": "", "DD": ""}])
    .select(
        concat_ws("-@-", col("YEAR"), col("COMPANY"), col("NETWORK"), col("PRODUCT")).alias("KEY"),
        col("DD").alias("DD")
    )
).assert_no_duplicates("KEY")



############################################
########### IMPORT ANAGRAFICHE #############
############################################

INVOICE_CUSTOMERS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_invoice_customers")
)

DELIVERY_CUSTOMERS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_delivery_customers")
)

COMM_ORGANIZATIONS = (
    read_data_from_redshift_table("dwe.qlk_salesanalysis_hetl2_comm_organizations")
)


############################################
########### COMPUTE SALEFACTS ##############
############################################

SALEFACTS_INPUT = read_data_from_redshift_table("dwe.oracle_dm_sale_facts_filtered")

SALEFACTS = (
    SALEFACTS_INPUT

    .withColumn("area_code", col("area_code").cast("string"))
    .withColumn("document_code", col("document_code").cast("string"))
    .withColumn("ref_invoice", col("ref_invoice").cast("string"))
    .withColumn("payment_type_code", col("payment_type_code").cast("string"))
    .withColumn("payment_expiring_code", col("payment_expiring_code").cast("string"))
    .withColumn("despatching_note", col("despatching_note").cast("string"))
    .withColumn("ref_despatch_note", col("ref_despatch_note").cast("string"))
    .withColumn("order_type_code", col("order_type_code").cast("string"))
    .withColumn("shipment_code", col("shipment_code").cast("string"))
    .withColumn("order_line", col("order_line").cast("string"))
    .withColumn("return_reason", col("return_reason").cast("string"))
    .withColumn("ref_order_number", col("ref_order_number").cast("string"))
    .withColumn("ref_order_line", col("ref_order_line").cast("string"))
    .withColumn("routing", col("routing").cast("string"))
    .withColumn("picking_number", col("picking_number").cast("string"))
    .withColumn("picking_carrier", col("picking_carrier").cast("string"))
    .withColumn("order_source", col("order_source").cast("string"))

    .withColumn("DOCUMENT_DATE_NEW", lit("todo"))

    .withColumnRenamed("US_COMPANY", "COMPANY")
    .withColumnRenamed("US_NETWORK", "NETWORK")

    .withColumn("COMPANY", lpad("COMPANY", 2, "0"))
    .withColumn("NETWORK", lpad("NETWORK", 2, "0"))

    .join(mapping_company_network, ["COMPANY", "NETWORK"], "left")

    .where(col("SALES_ORGANIZATION") != "62T0")    # exclude these two companies
    .where(col("SALES_ORGANIZATION") != "6230")    # exclude these two companies

    .do_mapping(mapping_product_code, col("product"), "product_code")
    .withColumn("product", col("product_code"))

    .withColumn("PRODUCT_ID", coalesce(col("PRODUCT"), lit("NOT DEFINED")))
    .withColumn("DEST_ID", coalesce(col("CUSTOMER"), lit("NOT DEFINED")))
    .withColumn("INV_ID", coalesce(col("INV_CUSTOMER"), lit("NOT DEFINED")))
    .withColumn("COMM_ORGANIZATION_ID", coalesce(col("COMM_ORGANIZATION"), lit("NOT DEFINED")))

    .withColumn("DOCUMENT_DATE", to_date(col("DOCUMENT_DATE")))

    .do_mapping(COMMERCIAL_MAP_DATE, current_date(), "TIME_ID", default_value=lit(current_date()), mapping_name="COMMERCIAL_MAP_DATE")
    .withColumn("TIME_ID", when( col("DATA_TYPE").isin([2,3]), col("TIME_ID")).otherwise(col("DOCUMENT_DATE")  ))
    .withColumn("TIME_ID_FACT", when( col("DATA_TYPE").isin([2,3]), col("TIME_ID")).otherwise(col("DOCUMENT_DATE")  ))
    .withColumn("__DOCUMENT_DATE", when( col("DATA_TYPE").isin([2,3]), col("TIME_ID")).otherwise(col("DOCUMENT_DATE")  ))

    .do_mapping(MAPPING_COMPANY_CURRENCY, col("COMPANY"), "__CURRENCY_CODE", default_value=lit("EUR"), mapping_name="MAPPING_COMPANY_CURRENCY")

    .withColumn("ORDER NUMBER", col("ORDER_NUMBER").cast("string"))

    .withColumn("COMPANY_ID", coalesce(col("COMPANY"), lit("NOT DEFINED")))
    .withColumn("NETWORK_ID", concat_ws("-@-", col("COMPANY"), col("NETWORK")))

    .withColumn("ORD_REQUESTED_DELIVERY_DATE", to_date(col("ORDER_REQUESTED_DELIVERY_DATE")))
    .withColumn(
        "MRDD",
        when(
            col("DATA_TYPE") == 2,
            col("ORDER_REQUESTED_DELIVERY_DATE")
        ).when(
            col("DATA_TYPE") == 4,
            col("DOCUMENT_DATE")
        ).otherwise(lit("1900-01-01"))
    )

    .withColumn("ALLOCATION WAREHOUSE", col("ALLOCATION_WAREHOUSE"))

    .withColumn("A/B_GRADE", when(col("FLAG_BGRADE") == 1, "B").otherwise("A"))

    .withColumn("TYPE",
        when(col("DATA_TYPE") == 1, "Invoiced")
        .when(col("DATA_TYPE") == 2, "Open Orders")
        .when(col("DATA_TYPE") == 3, "Delivered")
        .when(col("DATA_TYPE") == 4, "Budget")
    )

    .withColumn("POT_TYPE",
        when(col("DATA_TYPE") == 1, "Invoiced + Delivered")
        .when(col("DATA_TYPE") == 2, "Open Orders")
        .when(col("DATA_TYPE") == 3, "Invoiced + Delivered")
    )

    .withColumn("POTENTIAL STATUS",
        when(col("DATA_TYPE") == "2",
            when((col("ORDER_ALLOC_STATUS") == 6) & (coalesce(col("ORDER_BLOCKING_CODE"), lit(0)) < 50), "In Picking")
            .when((coalesce(col("ORDER_BLOCKING_CODE"), lit(0)) < 50), "Not Blocked")
            .otherwise("Blocked")
        )
        .when(col("DATA_TYPE") == 1, "Invoiced")
        .when(col("DATA_TYPE") == 3, "Delivered")
    )

    .withColumn("INV QTY",
        when(col("DOC_TYPE_CODE").isin([4, 7]), coalesce(col("QUANTITY"), lit(0))).otherwise(lit(0))
    )


    .withColumn("INV NSV €",
        when(col("DOC_TYPE_CODE").isin([4, 5, 6, 7]),
            when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, lit(0))
            .otherwise(coalesceZero(col("ACC_EXTRA_AMOUNT"), col("EXTRA_AMOUNT")) / col("CURRENCY_EURO_EXCH"))
        ).otherwise(lit(0))
    )

    .withColumn("REMOVABLE_CHARGES €", coalesce(col("REMOVABLE_CHARGES"), lit(0)) / col("CURRENCY_EURO_EXCH"))

    .withColumn("INV GSV €",
        when(col("DOC_TYPE_CODE").isin([4, 5, 6, 7]) & (col("DATA_TYPE") == 1),
            when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, lit(0))
            .otherwise((coalesce(col("TOT_AMOUNT_NO_TRANSPORT"), lit(0)) - coalesce(col("REMOVABLE_CHARGES"), lit(0))) / col("CURRENCY_EURO_EXCH"))
        ).otherwise(lit(0))
    )

    .withColumn("INV GSV + TRANSPORT €",
        when(col("DOC_TYPE_CODE").isin([4, 5, 6, 7]) & (col("DATA_TYPE") == 1),
            when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, lit(0))
            .otherwise((coalesceZero(col("ACC_AMOUNT"), col("DOC_TOTAL_AMOUNT")) - coalesce(col("REMOVABLE_CHARGES"), lit(0))) / col("CURRENCY_EURO_EXCH"))
        ).otherwise(lit(0))
    )

    .withColumn("INV GSV PROMOTIONAL & BONUS €",
        when(col("DATA_TYPE") == "1",
            when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, lit(0))
            .otherwise((coalesce(col("TOT_AMOUNT_NO_TRANSPORT"), lit(0)) - coalesce(col("REMOVABLE_CHARGES"), lit(0))) / col("CURRENCY_EURO_EXCH"))
        ).otherwise(lit(0))
    )

    .withColumn("INV GSV PROMOTIONAL & BONUS LC",
        when(col("DATA_TYPE") == "1",
            when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, lit(0))
            .otherwise((coalesce(col("TOT_AMOUNT_NO_TRANSPORT"), lit(0)) - coalesce(col("REMOVABLE_CHARGES"), lit(0))))
        ).otherwise(lit(0))
    )


    .withColumn(
        "INV CP €",
        when(
            col("DOC_TYPE_CODE").isin([4, 7]),
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
                lit(0)
            ).otherwise(
                when(
                    (coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0))) == 0,
                    lit(0)
                ).otherwise(
                    ((coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0)) + coalesce(col("TRANSPORT_COST"), lit(0)) + coalesce(col("TRANSPORT_COST_INFRA"), lit(0)))
                    / col("CURRENCY_EURO_EXCH")) * coalesce(col("QUANTITY"), lit(0))
                )
            )
        ).otherwise(lit(0))
    )


    .withColumn(
        "INV TP €",
        when(
            col("DOC_TYPE_CODE").isin([4, 7]),
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
                lit(0)
            ).otherwise(
                when(
                    coalesce(
                        when(
                            col("DOCUMENT_DATE") > col("START_DATE_TPRICE_REVISED"),
                            col("REVISED_COMM_COS")
                        ).otherwise(col("COMM_COST_OF_SALES")),
                        lit(0)
                    ) == 0,
                    lit(0)
                ).otherwise(
                    ((coalesce(
                        when(
                            col("DOCUMENT_DATE") > col("START_DATE_TPRICE_REVISED"),
                            col("REVISED_COMM_COS")
                        ).otherwise(col("COMM_COST_OF_SALES")),
                        lit(0)
                    ) + coalesce(col("TRANSPORT_COST"), lit(0)))
                    / col("CURRENCY_EURO_EXCH")) * coalesce(col("QUANTITY"), lit(0))
                )
            )
        ).otherwise(lit(0))
    )

    .withColumn("DEL QTY", coalesce(col("DELIVERY_QUANTITY"), lit(0)))
).localCheckpoint()


print("calculating SALEFACTS... done step 1!!!")


SALEFACTS = (
    SALEFACTS

    .withColumn("__mapping_product_code_KEY", concat_ws('-@-', col("SALES_ORGANIZATION"), col("SALES_OFFICE"), col("product"), year("DOCUMENT_DATE"), month("DOCUMENT_DATE")))
    .do_mapping(PRZ_BUDGET_MAPPING, col("__mapping_product_code_KEY"), "__PRZ_BUDGET_MAPPING", default_value=lit(None), mapping_name="PRZ_BUDGET_MAPPING")
    .withColumn("__mapping_product_code_REVISED_KEY", concat_ws('-@-', col("SALES_ORGANIZATION"), col("SALES_OFFICE"), col("product"), year("DOCUMENT_DATE"), month("DOCUMENT_DATE")))
    .do_mapping(PRZ_BUDGET_MAPPING, col("__mapping_product_code_REVISED_KEY"), "__PRZ_BUDGET_MAPPING_REVISED", default_value=lit(None), mapping_name="PRZ_BUDGET_MAPPING")

    .withColumn(
        "DEL NSV €",
        when(
            col("DOC_TYPE_CODE").isin([4, 7]),
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
                lit(0)
            ).otherwise(
                when(
                    col("DOC_TYPE_CODE") == '4',
                    coalesce(col("DELIVERY_NSV_AMOUNT"), lit(0)) / coalesce(col("CURRENCY_EURO_EXCH"), lit(1))
                ).otherwise(
                    coalesce(coalesce(col("DELIVERY_QUANTITY"), lit(0)), lit(0)) * (
                        coalesce(
                            when(year(col("DOCUMENT_DATE")) < 2019, col("STD_SALE_PRICE"))
                            .otherwise(col("__PRZ_BUDGET_MAPPING")), lit(0)
                        ) / col("CURRENCY_EURO_EXCH")
                    )
                )
            )
        ).otherwise(lit(0))
    )

    .withColumn(
        "DEL GSV €",
        when(
            col("DATA_TYPE").isin([3]),
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
                lit(0)
            ).otherwise(
                (coalesce(col("DELIVERY_TOTAL_AMOUNT"), lit(0)) -
                 coalesce(col("TOT_AMOUNT_NO_TRANSPORT"), lit(0)) -
                 coalesce(col("REMOVABLE_CHARGES"), lit(0))) / col("CURRENCY_EURO_EXCH")
            )
        ).otherwise(lit(0))
    )
    .withColumn(
        "DEL CP €",
        when(
            coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
            lit(0)
        ).otherwise(
            when(
                (coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0))) == 0,
                lit(0)
            ).otherwise(
                ((coalesce(col("CONS_COST_OF_SALES"), lit(0)) +
                  coalesce(col("TCA"), lit(0)) +
                  coalesce(col("TRANSPORT_COST"), lit(0)) +
                  coalesce(col("TRANSPORT_COST_INFRA"), lit(0)))
                / col("CURRENCY_EURO_EXCH")) * coalesce(col("DELIVERY_QUANTITY"), lit(0))
            )
        )
    )
    .withColumn(
        "DEL TP €",
        when(
            coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
            lit(0)
        ).otherwise(
            when(
                coalesce(
                    when(
                        col("DOCUMENT_DATE") > col("START_DATE_TPRICE_REVISED"),
                        col("REVISED_COMM_COS")
                    ).otherwise(col("COMM_COST_OF_SALES")),
                    lit(0)
                ) == 0,
                lit(0)
            ).otherwise(
                ((coalesce(
                    when(
                        col("DOCUMENT_DATE") > col("START_DATE_TPRICE_REVISED"),
                        col("REVISED_COMM_COS")
                    ).otherwise(col("COMM_COST_OF_SALES")),
                    lit(0)
                ) + coalesce(col("TRANSPORT_COST"), lit(0)))
                / col("CURRENCY_EURO_EXCH")) * coalesce(col("DELIVERY_QUANTITY"), lit(0))
            )
        )
    )


    .withColumn("ORD QTY", coalesce(col("ORDER_QTY"), lit(0)))

    .withColumn("ORD NSV €",
        when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, 0)
        .otherwise(coalesce(col("ORDER_NSV_AMOUNT"), lit(0)) / col("CURRENCY_EURO_EXCH"))
    )

    .withColumn("ORD GSV €",
        when(col("DATA_TYPE") == 2,
            when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, 0)
            .otherwise((coalesce(col("ORDER_DOC_TOTAL_AMOUNT"), lit(0)) - coalesce(col("TOT_AMOUNT_NO_TRANSPORT"), lit(0)) - coalesce(col("REMOVABLE_CHARGES"), lit(0))) / col("CURRENCY_EURO_EXCH"))
        ).otherwise(0)
    )

    .withColumn(
        "ORD CP €",
        when(
            col("DOC_TYPE_CODE").isin([4, 7]),
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
                lit(0)
            ).otherwise(
                when(
                    (coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0))) == 0,
                    lit(0)
                ).otherwise(
                    ((coalesce(col("CONS_COST_OF_SALES"), lit(0)) +
                      coalesce(col("TCA"), lit(0)) +
                      coalesce(col("TRANSPORT_COST"), lit(0)) +
                      coalesce(col("TRANSPORT_COST_INFRA"), lit(0)))
                    / col("CURRENCY_EURO_EXCH")) * coalesce(col("ORDER_QTY"), lit(0))
                )
            )
        ).otherwise(lit(0))
    )
    .withColumn(
        "ORD TP €",
        when(
            (col("DOC_TYPE_CODE") == 7) & (coalesce(col("ORDER_NSV_AMOUNT"), lit(0)) == 0),
            lit(0)
        ).otherwise(
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
                lit(0)
            ).otherwise(
                when(
                    coalesce(
                        when(
                            col("DOCUMENT_DATE") > col("START_DATE_TPRICE_REVISED"),
                            col("REVISED_COMM_COS")
                        ).otherwise(col("COMM_COST_OF_SALES")),
                        lit(0)
                    ) == 0,
                    lit(0)
                ).otherwise(
                    ((coalesce(
                        when(
                            col("DOCUMENT_DATE") > col("START_DATE_TPRICE_REVISED"),
                            col("REVISED_COMM_COS")
                        ).otherwise(col("COMM_COST_OF_SALES")),
                        lit(0)
                    ) + coalesce(col("TRANSPORT_COST"), lit(0)))
                    / col("CURRENCY_EURO_EXCH")) * coalesce(col("ORDER_QTY"), lit(0))
                )
            )
        )
    )

    .withColumn("BDG QTY", coalesce(col("NETWORK_BUDGET"), lit(0)))

    .withColumn(
        "BDG NSV €",
        when(
            coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
            lit(0)
        ).otherwise(
            coalesce(col("NETWORK_BUDGET"), lit(0)) * (
                when(
                    year(col("DOCUMENT_DATE")) < 2019,
                    col("STD_SALE_PRICE")
                ).otherwise(
                    col("__PRZ_BUDGET_MAPPING")
                ) / col("CURRENCY_EURO_EXCH")
            )
        )
    )

    .withColumn("BDG AVG NSV €",
        when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, 0)
        .otherwise(coalesce(col("NETWORK_BUDGET"), lit(0)) * col("STD_SALE_PRICE") / col("CURRENCY_EURO_EXCH"))
    )

    .withColumn("BDG CP €",
        when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, 0)
        .when(coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0)) == 0, 0)
        .otherwise((coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0)) + coalesce(col("TRANSPORT_COST"), lit(0)) + coalesce(col("TRANSPORT_COST_INFRA"), lit(0))) / col("CURRENCY_EURO_EXCH") * coalesce(col("NETWORK_BUDGET"), lit(0)))
    )

    .withColumn("BDG TP €",
        when(coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0, 0)
        .when(coalesce(col("CONS_COST_OF_SALES"), lit(0)) == 0, 0)
        .otherwise((coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TRANSPORT_COST"), lit(0))) / col("CURRENCY_EURO_EXCH") * coalesce(col("NETWORK_BUDGET"), lit(0)))
    )

    .withColumn("REV QTY",
        when(col("DOCUMENT_DATE") < col("START_DATE_NSV_REVISED"),
            when(col("DOC_TYPE_CODE").isin([4, 7]), coalesce(col("QUANTITY"), lit(0))).otherwise(0)
        ).otherwise(
            coalesce(col("QUANTITY_REVISED"), lit(0))
        )
    )

    .withColumn(
        "REV NSV €",
        when(
            col("DOCUMENT_DATE") < col("START_DATE_NSV_REVISED"),  # DOCUMENT_DATE < START_DATE_NSV_REVISED
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,  # ALT(CURRENCY_EURO_EXCH, 0) = 0
                lit(0)
            ).otherwise(
                coalesce(col("ACC_EXTRA_AMOUNT"), col("EXTRA_AMOUNT")) / col("CURRENCY_EURO_EXCH")  # ALT(ACC_EXTRA_AMOUNT, EXTRA_AMOUNT) / CURRENCY_EURO_EXCH
            )
        ).otherwise(
            when(
                coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,  # ALT(CURRENCY_EURO_EXCH, 0) = 0
                lit(0)
            ).otherwise(
                coalesce(col("QUANTITY_REVISED"), lit(0)) * (  # ALT(QUANTITY_REVISED, 0)
                    coalesce(
                        when(
                            year(col("DOCUMENT_DATE")) < 2018,  # If year(DOCUMENT_DATE) < 2018
                            col("REVISED_SALE_PRICE")
                        ).otherwise(col("__PRZ_BUDGET_MAPPING_REVISED")),  # Else __PRZ_BUDGET_MAPPING_REVISED
                        lit(0)
                    ) / col("CURRENCY_EURO_EXCH")  # Division by CURRENCY_EURO_EXCH
                )
            )
        )
    )

    .withColumn(
        "REV CP €",
        when(
            coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
            lit(0)
        ).otherwise(
            when(
                (coalesce(col("CONS_COST_OF_SALES"), lit(0)) + coalesce(col("TCA"), lit(0))) == 0,
                lit(0)
            ).otherwise(
                (
                    (coalesce(col("CONS_COST_OF_SALES"), lit(0)) +
                     coalesce(col("TCA"), lit(0)) +
                     coalesce(col("TRANSPORT_COST"), lit(0)) +
                     coalesce(col("TRANSPORT_COST_INFRA"), lit(0)))
                    / col("CURRENCY_EURO_EXCH")
                ) * when(
                    col("DOCUMENT_DATE") < col("START_DATE_NSV_REVISED"),
                    when(col("DOC_TYPE_CODE").isin([4, 7]), coalesce(col("QUANTITY"), lit(0))).otherwise(lit(0))
                ).otherwise(coalesce(col("QUANTITY_REVISED"), lit(0)))
            )
        )
    )
    .withColumn(
        "REV TP €",
        when(
            coalesce(col("CURRENCY_EURO_EXCH"), lit(0)) == 0,
            lit(0)
        ).otherwise(
            when(
                col("DOCUMENT_DATE") < col("START_DATE_NSV_REVISED"),
                when(
                    coalesce(col("COMM_COST_OF_SALES"), lit(0)) == 0,
                    lit(0)
                ).otherwise(
                    (coalesce(col("COMM_COST_OF_SALES"), lit(0)) / col("CURRENCY_EURO_EXCH")) *
                    when(col("DOC_TYPE_CODE").isin([4, 7]), coalesce(col("QUANTITY"), lit(0))).otherwise(lit(0))
                )
            ).otherwise(
                when(
                    coalesce(col("TRANSFPRI_REVISED_NSV"), lit(0)) == 0,
                    lit(0)
                ).otherwise(
                    (
                        (coalesce(col("TRANSFPRI_REVISED_NSV"), lit(0)) + coalesce(col("TRANSPORT_COST"), lit(0)))
                        / col("CURRENCY_EURO_EXCH")
                    ) * coalesce(col("QUANTITY_REVISED"), lit(0))
                )
            )
        )
    )

    .do_mapping(mapping_DD, concat_ws('-@-', year("DOCUMENT_DATE"), col("COMPANY"), col("NETWORK"), col("product_code")), "DD PERCENTAGE", default_value=lit(0), mapping_name="mapping_DD_KEY")

    ########################################

    .withColumn("INV GM CP €",
        coalesce(col("INV NSV €"), lit(0)) - coalesce(col("INV CP €"), lit(0))
    )

    .withColumn("INV GM TP €",
        coalesce(col("INV NSV €"), lit(0)) - coalesce(col("INV TP €"), lit(0))
    )

    .withColumn("DEL GM CP €",
        when((col("DOC_TYPE_CODE") == 7) & (col("DEL NSV €") == 0), 0)
        .otherwise(coalesce(col("DEL NSV €"), lit(0)) - coalesce(col("DEL CP €"), lit(0)))
    )

    .withColumn("DEL GM TP €",
        when((col("DOC_TYPE_CODE") == 7) & (col("DEL NSV €") == 0), 0)
        .otherwise(coalesce(col("DEL NSV €"), lit(0)) - coalesce(col("DEL TP €"), lit(0)))
    )

    .withColumn("ORD GM CP €",
        when((col("T3316LNG") == 2) & (col("EVALUATION_TYPE") == 0), 0)
        .when(coalesce(col("ORD NSV €"), lit(0)) == 0, 0)
        .otherwise(coalesce(col("ORD NSV €"), lit(0)) - coalesce(col("ORD CP €"), lit(0)))
    )

    .withColumn("ORD GM TP €",
        when((col("T3316LNG") == 2) & (col("EVALUATION_TYPE") == 0), 0)
        .when(coalesce(col("ORD NSV €"), lit(0)) == 0, 0)
        .otherwise(coalesce(col("ORD NSV €"), lit(0)) - coalesce(col("ORD TP €"), lit(0)))
    )

    .withColumn("BDG GM CP €",
        coalesce(col("BDG NSV €"), lit(0)) - coalesce(col("BDG CP €"), lit(0))
    )

    .withColumn("BDG GM TP €",
        coalesce(col("BDG NSV €"), lit(0)) - coalesce(col("BDG TP €"), lit(0))
    )

    .withColumn("REV GM CP €",
        coalesce(col("REV NSV €"), lit(0)) - coalesce(col("REV CP €"), lit(0))
    )

    .withColumn("REV GM TP €",
        coalesce(col("REV NSV €"), lit(0)) - coalesce(col("REV TP €"), lit(0))
    )

    .withColumn(
        "ORDER STATUS",
        when(
            col("POTENTIAL STATUS").isin(['Blocked', 'Not Blocked']),
            when(
                (col("ORDER_ALLOC_STATUS") == 1) & (col("ORDER_DELIVERY_WAREHOUSE") == col("ALLOCATION WAREHOUSE")),
                lit("Deliverables")
            ).otherwise(lit("Not Deliverables"))
        ).otherwise(col("POTENTIAL STATUS"))
    )

    .withColumn(
        "ORDER_REQUESTED_DELIVERY_MONTH_YEAR",
        concat(month(col("MRDD")).cast("string"), lit(" "), year(col("MRDD")).cast("string"))
    ).withColumn(
        "ORDER_REQUESTED_DELIVERY_MONTH_YEAR_NUMERIC",
        (year(col("MRDD")) * 100 + month(col("MRDD")))
    )


    ########################################


    .withColumn("POT QTY",
        coalesce(col("INV QTY"), lit(0)) + coalesce(col("DEL QTY"), lit(0)) + coalesce(col("ORD QTY"), lit(0))
    )

    .withColumn("POT NSV €",
        coalesce(col("INV NSV €"), lit(0)) + coalesce(col("DEL NSV €"), lit(0)) + coalesce(col("ORD NSV €"), lit(0))
    )

    .withColumn("POT GSV €",
        coalesce(col("INV GSV €"), lit(0)) + coalesce(col("DEL GSV €"), lit(0)) + coalesce(col("ORD GSV €"), lit(0))
    )

    .withColumn("POT GM CP €",
        coalesce(col("INV GM CP €"), lit(0)) + coalesce(col("DEL GM CP €"), lit(0)) + coalesce(col("ORD GM CP €"), lit(0))
    )

    .withColumn("POT GM TP €",
        coalesce(col("INV GM TP €"), lit(0)) + coalesce(col("DEL GM TP €"), lit(0)) + coalesce(col("ORD GM TP €"), lit(0))
    )

    .withColumn(
        "ORD BO QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") < trunc(current_date(), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD BO NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") < trunc(current_date(), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD BO GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") < trunc(current_date(), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD BO GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") < trunc(current_date(), "month")),
            col("ORD GM CP €")
        )
    )

)

print("calculating SALEFACTS... done step 2!!!")



SALEFACTS = (
    SALEFACTS

    .withColumn(
        "ORD BO GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") < trunc(current_date(), "month")),
            col("ORD GM TP €")
        )
    )
    # M1 (current month + 1)
    .withColumn(
        "ORD M1 QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 1), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD M1 NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 1), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD M1 GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 1), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD M1 GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 1), "month")),
            col("ORD GM CP €")
        )
    )
    .withColumn(
        "ORD M1 GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 1), "month")),
            col("ORD GM TP €")
        )
    )
    # M2 (current month + 2)
    .withColumn(
        "ORD M2 QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 2), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD M2 NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 2), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD M2 GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 2), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD M2 GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 2), "month")),
            col("ORD GM CP €")
        )
    )
    .withColumn(
        "ORD M2 GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 2), "month")),
            col("ORD GM TP €")
        )
    )
    # M3 (current month + 3)
    .withColumn(
        "ORD M3 QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 3), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD M3 NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 3), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD M3 GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 3), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD M3 GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 3), "month")),
            col("ORD GM CP €")
        )
    )
    .withColumn(
        "ORD M3 GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 3), "month")),
            col("ORD GM TP €")
        )
    )
    # M4 (current month + 4)
    .withColumn(
        "ORD M4 QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 4), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD M4 NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 4), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD M4 GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 4), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD M4 GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 4), "month")),
            col("ORD GM CP €")
        )
    )
    .withColumn(
        "ORD M4 GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 4), "month")),
            col("ORD GM TP €")
        )
    )
    # M5 (current month + 5)
    .withColumn(
        "ORD M5 QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 5), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD M5 NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 5), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD M5 GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 5), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD M5 GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 5), "month")),
            col("ORD GM CP €")
        )
    )
    .withColumn(
        "ORD M5 GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 5), "month")),
            col("ORD GM TP €")
        )
    )
    # M6 (current month + 6)
    .withColumn(
        "ORD M6 QTY",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 6), "month")),
            col("ORD QTY")
        )
    )
    .withColumn(
        "ORD M6 NSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 6), "month")),
            col("ORD NSV €")
        )
    )
    .withColumn(
        "ORD M6 GSV €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 6), "month")),
            col("ORD GSV €")
        )
    )
    .withColumn(
        "ORD M6 GM CP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 6), "month")),
            col("ORD GM CP €")
        )
    )
    .withColumn(
        "ORD M6 GM TP €",
        when(
            (col("DATA_TYPE") == 2) & (trunc(col("MRDD"), "month") == trunc(add_months(current_date(), 6), "month")),
            col("ORD GM TP €")
        )
    )

)

print("calculating SALEFACTS... done step 3!!!")


SALEFACTS = (
    SALEFACTS

    .withColumn(
        "BLK QTY",
        when(
            coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) >= 50,
            col("ORD QTY")
        ).otherwise(lit(0))
    )
    .withColumn(
        "BLK NSV €",
        when(
            coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) >= 50,
            col("ORD NSV €")
        ).otherwise(lit(0))
    )
    .withColumn(
        "BLK GSV €",
        when(
            coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) >= 50,
            col("ORD GSV €")
        ).otherwise(lit(0))
    )
    .withColumn(
        "BLK GM CP €",
        when(
            coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) >= 50,
            col("ORD GM CP €")
        ).otherwise(lit(0))
    )
    .withColumn(
        "BLK GM TP €",
        when(
            coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) >= 50,
            col("ORD GM TP €")
        ).otherwise(lit(0))
    )

    .withColumn(
        "PCK QTY",
        when(
            (col("ORDER_ALLOC_STATUS") == 6) & (coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) < 50),
            col("ORD QTY")
        ).otherwise(lit(0))
    )
    .withColumn(
        "PCK NSV €",
        when(
            (col("ORDER_ALLOC_STATUS") == 6) & (coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) < 50),
            col("ORD NSV €")
        ).otherwise(lit(0))
    )
    .withColumn(
        "PCK GSV €",
        when(
            (col("ORDER_ALLOC_STATUS") == 6) & (coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) < 50),
            col("ORD GSV €")
        ).otherwise(lit(0))
    )
    .withColumn(
        "PCK GM CP €",
        when(
            (col("ORDER_ALLOC_STATUS") == 6) & (coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) < 50),
            col("ORD GM CP €")
        ).otherwise(lit(0))
    )
    .withColumn(
        "PCK GM TP €",
        when(
            (col("ORDER_ALLOC_STATUS") == 6) & (coalesce(col("ORDER_BLOCKING_CODE"), lit(-1)) < 50),
            col("ORD GM TP €")
        ).otherwise(lit(0))
    )

    .withColumn("INSTOCK QTY",
        when(col("ORDER_ALLOC_STATUS") == "1", col("ORD QTY")).otherwise(0)
    )
    .withColumn("INSTOCK NSV €",
        when(col("ORDER_ALLOC_STATUS") == "1", col("ORD NSV €")).otherwise(0)
    )
    .withColumn("INSTOCK GSV €",
        when(col("ORDER_ALLOC_STATUS") == "1", col("ORD GSV €")).otherwise(0)
    )
    .withColumn("INSTOCK GM CP €",
        when(col("ORDER_ALLOC_STATUS") == "1", col("ORD GM CP €")).otherwise(0)
    )
    .withColumn("INSTOCK GM TP €",
        when(col("ORDER_ALLOC_STATUS") == "1", col("ORD GM TP €")).otherwise(0)
    )
    .withColumn("REPLENISHMENT QTY",
        when(col("ORDER_ALLOC_STATUS") == "2", col("ORD QTY")).otherwise(0)
    )
    .withColumn("REPLENISHMENT NSV €",
        when(col("ORDER_ALLOC_STATUS") == "2", col("ORD NSV €")).otherwise(0)
    )
    .withColumn("REPLENISHMENT GSV €",
        when(col("ORDER_ALLOC_STATUS") == "2", col("ORD GSV €")).otherwise(0)
    )
    .withColumn("REPLENISHMENT GM CP €",
        when(col("ORDER_ALLOC_STATUS") == "2", col("ORD GM CP €")).otherwise(0)
    )
    .withColumn("REPLENISHMENT GM TP €",
        when(col("ORDER_ALLOC_STATUS") == "2", col("ORD GM TP €")).otherwise(0)
    )
    .withColumn("ORM QTY",
        when(col("ORDER_ALLOC_STATUS") == "3", col("ORD QTY")).otherwise(0)
    )
    .withColumn("ORM NSV €",
        when(col("ORDER_ALLOC_STATUS") == "3", col("ORD NSV €")).otherwise(0)
    )
    .withColumn("ORM GSV €",
        when(col("ORDER_ALLOC_STATUS") == "3", col("ORD GSV €")).otherwise(0)
    )
    .withColumn("ORM GM CP €",
        when(col("ORDER_ALLOC_STATUS") == "3", col("ORD GM CP €")).otherwise(0)
    )
    .withColumn("ORM GM TP €",
        when(col("ORDER_ALLOC_STATUS") == "3", col("ORD GM TP €")).otherwise(0)
    )
    .withColumn("UNALLOCATED QTY",
        when(col("ORDER_ALLOC_STATUS") == "5", col("ORD QTY")).otherwise(0)
    )
    .withColumn("UNALLOCATED NSV €",
        when(col("ORDER_ALLOC_STATUS") == "5", col("ORD NSV €")).otherwise(0)
    )
    .withColumn("UNALLOCATED GSV €",
        when(col("ORDER_ALLOC_STATUS") == "5", col("ORD GSV €")).otherwise(0)
    )
    .withColumn("UNALLOCATED GM CP €",
        when(col("ORDER_ALLOC_STATUS") == "5", col("ORD GM CP €")).otherwise(0)
    )
    .withColumn("UNALLOCATED GM TP €",
        when(col("ORDER_ALLOC_STATUS") == "5", col("ORD GM TP €")).otherwise(0)
    )


    .withColumn("INV + DEL QTY",
        coalesce(col("INV QTY"), lit(0)) + coalesce(col("DEL QTY"), lit(0))
    )
    .withColumn("INV + DEL NSV €",
        coalesce(col("INV NSV €"), lit(0)) + coalesce(col("DEL NSV €"), lit(0))
    )
    .withColumn("INV + DEL GSV €",
        coalesce(col("INV GSV €"), lit(0)) + coalesce(col("DEL GSV €"), lit(0))
    )
    .withColumn("INV + DEL GM CP €",
        coalesce(col("INV GM CP €"), lit(0)) + coalesce(col("DEL GM CP €"), lit(0))
    )
    .withColumn("INV + DEL GM TP €",
        coalesce(col("INV GM TP €"), lit(0)) + coalesce(col("DEL GM TP €"), lit(0))
    )

    .withColumn("ORDER BLOCKING",
        concat_ws(" - ", col("ORDER_BLOCKING_CODE"), col("ORDER_BLOCKING_DESCR"))
    )

    .withColumn(
        "BLOCKING TYPE",
        when(col("ORDER_BLOCKING_CODE") <= 18, "Not Blocked")
        .when((col("ORDER_BLOCKING_CODE") >= 50) & (col("ORDER_BLOCKING_CODE") <= 57), "Commercial Block")
        .when((col("ORDER_BLOCKING_CODE") >= 58) & (col("ORDER_BLOCKING_CODE") <= 67), "Credit Blocked")
        .when(col("ORDER_BLOCKING_CODE") > 67, "Price Missing")
    )

    .withColumn("ODE QTY",
        when((col("ORDER STATUS") =='Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD QTY")).otherwise(0)
    )

    .withColumn("ODE NSV €",
        when((col("ORDER STATUS") =='Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD NSV €")).otherwise(0)
    )

    .withColumn("ODE GSV €",
        when((col("ORDER STATUS") =='Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD GSV €")).otherwise(0)
    )

    .withColumn("ODE GM CP €",
        when((col("ORDER STATUS") =='Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD GM CP €")).otherwise(0)
    )

    .withColumn("ODE GM TP €",
        when((col("ORDER STATUS") =='Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD GM TP €")).otherwise(0)
    )

    .withColumn("OND QTY",
        when((col("ORDER STATUS") =='Not Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD QTY")).otherwise(0)
    )

    .withColumn("OND NSV €",
        when((col("ORDER STATUS") =='Not Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD NSV €")).otherwise(0)
    )

    .withColumn("OND GSV €",
        when((col("ORDER STATUS") =='Not Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD GSV €")).otherwise(0)
    )

    .withColumn("OND GM CP €",
        when((col("ORDER STATUS") =='Not Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD GM CP €")).otherwise(0)
    )

    .withColumn("OND GM TP €",
        when((col("ORDER STATUS") =='Not Deliverables') & (col("POTENTIAL STATUS") == 'Not Blocked'), col("ORD GM TP €")).otherwise(0)
    )

    .withColumn("BDG DD QTY",
        col("BDG QTY") * col("DD PERCENTAGE")
    )
)


print("calculating SALEFACTS... done step 4!!!")


SALEFACTS = (
    SALEFACTS

    .withColumn("INV NSV LC", col("INV NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV GSV LC", col("INV GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV GSV + TRANSPORT LC", col("INV GSV + TRANSPORT €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV CP LC", col("INV CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV TP LC", col("INV TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("DEL NSV LC", col("DEL NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("DEL GSV LC", col("DEL GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("DEL CP LC", col("DEL CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("DEL TP LC", col("DEL TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD NSV LC", col("ORD NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD GSV LC", col("ORD GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD CP LC", col("ORD CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD TP LC", col("ORD TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BDG NSV LC", col("BDG NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BDG CP LC", col("BDG CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BDG TP LC", col("BDG TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REV NSV LC", col("REV NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REV CP LC", col("REV CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REV TP LC", col("REV TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV GM CP LC", col("INV GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV GM TP LC", col("INV GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("DEL GM CP LC", col("DEL GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("DEL GM TP LC", col("DEL GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD GM CP LC", col("ORD GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD GM TP LC", col("ORD GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BDG GM CP LC", col("BDG GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BDG GM TP LC", col("BDG GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REV GM CP LC", col("REV GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REV GM TP LC", col("REV GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("POT NSV LC", col("POT NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("POT GSV LC", col("POT GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("POT GM CP LC", col("POT GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("POT GM TP LC", col("POT GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD BO NSV LC", col("ORD BO NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD BO GSV LC", col("ORD BO GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD BO GM CP LC", col("ORD BO GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD BO GM TP LC", col("ORD BO GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M1 NSV LC", col("ORD M1 NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M1 GSV LC", col("ORD M1 GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M1 GM CP LC", col("ORD M1 GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M1 GM TP LC", col("ORD M1 GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M2 NSV LC", col("ORD M2 NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M2 GSV LC", col("ORD M2 GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M2 GM CP LC", col("ORD M2 GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M2 GM TP LC", col("ORD M2 GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M3 NSV LC", col("ORD M3 NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M3 GSV LC", col("ORD M3 GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M3 GM CP LC", col("ORD M3 GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M3 GM TP LC", col("ORD M3 GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M4 NSV LC", col("ORD M4 NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M4 GSV LC", col("ORD M4 GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M4 GM CP LC", col("ORD M4 GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M4 GM TP LC", col("ORD M4 GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M5 NSV LC", col("ORD M5 NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M5 GSV LC", col("ORD M5 GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M5 GM CP LC", col("ORD M5 GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M5 GM TP LC", col("ORD M5 GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M6 NSV LC", col("ORD M6 NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M6 GSV LC", col("ORD M6 GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M6 GM CP LC", col("ORD M6 GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORD M6 GM TP LC", col("ORD M6 GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BLK NSV LC", col("BLK NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BLK GSV LC", col("BLK GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BLK GM CP LC", col("BLK GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("BLK GM TP LC", col("BLK GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("PCK NSV LC", col("PCK NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("PCK GSV LC", col("PCK GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("PCK GM CP LC", col("PCK GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("PCK GM TP LC", col("PCK GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INSTOCK NSV LC", col("INSTOCK NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INSTOCK GSV LC", col("INSTOCK GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INSTOCK GM CP LC", col("INSTOCK GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INSTOCK GM TP LC", col("INSTOCK GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REPLENISHMENT NSV LC", col("REPLENISHMENT NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REPLENISHMENT GSV LC", col("REPLENISHMENT GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REPLENISHMENT GM CP LC", col("REPLENISHMENT GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("REPLENISHMENT GM TP LC", col("REPLENISHMENT GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORM NSV LC", col("ORM NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORM GSV LC", col("ORM GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORM GM CP LC", col("ORM GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ORM GM TP LC", col("ORM GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("UNALLOCATED NSV LC", col("UNALLOCATED NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("UNALLOCATED GSV LC", col("UNALLOCATED GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("UNALLOCATED GM CP LC", col("UNALLOCATED GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("UNALLOCATED GM TP LC", col("UNALLOCATED GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV + DEL NSV LC", col("INV + DEL NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV + DEL GSV LC", col("INV + DEL GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV + DEL GM CP LC", col("INV + DEL GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("INV + DEL GM TP LC", col("INV + DEL GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ODE NSV LC", col("ODE NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ODE GSV LC", col("ODE GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ODE GM CP LC", col("ODE GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("ODE GM TP LC", col("ODE GM TP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("OND NSV LC", col("OND NSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("OND GSV LC", col("OND GSV €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("OND GM CP LC", col("OND GM CP €") * col("CURRENCY_EURO_EXCH"))
    .withColumn("OND GM TP LC", col("OND GM TP €") * col("CURRENCY_EURO_EXCH"))

)

print("calculating SALEFACTS... done step 5!!!")


SALEFACTS = (
    SALEFACTS

    .withColumn(
        "BDG UNIT NSV LC",
        coalesce(
            when(year(col("DOCUMENT_DATE")) < 2019, col("STD_SALE_PRICE"))
            .otherwise(col("__PRZ_BUDGET_MAPPING")),
            lit(0)
        )
    )
    .withColumn(
        "REV UNIT NSV LC",
        coalesce(
            when(year(col("DOCUMENT_DATE")) < 2018, col("REVISED_SALE_PRICE"))
            .otherwise(col("__PRZ_BUDGET_MAPPING_REVISED")),
            lit(0)
        )
    )

    .withColumn("BDG UNIT NSV €", coalesce(col("BDG UNIT NSV LC"), lit(0)) / col("CURRENCY_EURO_EXCH"))
    .withColumn("REV UNIT NSV €", coalesce(col("REV UNIT NSV LC"), lit(0)) / col("CURRENCY_EURO_EXCH"))

    .withColumn("INV BDG NSV LC", coalesce(col("BDG UNIT NSV LC"), lit(0)) * coalesce(col("INV QTY"), lit(0)))
    .withColumn("INV REV NSV LC", coalesce(col("REV UNIT NSV LC"), lit(0)) * coalesce(col("INV QTY"), lit(0)))
    .withColumn("INV BDG NSV €", coalesce(col("BDG UNIT NSV €"), lit(0)) * coalesce(col("INV QTY"), lit(0)))
    .withColumn("INV REV NSV €", coalesce(col("REV UNIT NSV €"), lit(0)) * coalesce(col("INV QTY"), lit(0)))
    .withColumn("TRANSPORT COST LC", coalesce(col("TRANSPORT_COST"), lit(0)) * coalesce(col("INV QTY"), lit(0)))
    .withColumn("TRANSPORT COST INFRA LC", coalesce(col("TRANSPORT_COST_INFRA"), lit(0)) * coalesce(col("INV QTY"), lit(0)))


    .withColumn("TRANSPORT COST €", col("TRANSPORT COST LC") / col("CURRENCY_EURO_EXCH"))
    .withColumn("TRANSPORT COST INFRA €", col("TRANSPORT COST INFRA LC") / col("CURRENCY_EURO_EXCH"))

    .withColumn(
        "PRICE VARIANCE BDG LC",
        when(
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("BDG UNIT NSV LC"), lit(0)), lit(0))) == 0,
            lit(0)
        ).otherwise(
            coalesce(col("INV NSV LC"), lit(0)) -
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("BDG UNIT NSV LC"), lit(0)), lit(0)))
        ) + when(
            col("DOC_TYPE_CODE").isin([5, 6]),
            coalesce(col("INV NSV LC"), lit(0))
        ).otherwise(lit(0))
    )
    .withColumn(
        "PRICE VARIANCE REV LC",
        when(
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("REV UNIT NSV LC"), lit(0)), lit(0))) == 0,
            lit(0)
        ).otherwise(
            coalesce(col("INV NSV LC"), lit(0)) -
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("REV UNIT NSV LC"), lit(0)), lit(0)))
        ) + when(
            col("DOC_TYPE_CODE").isin([5, 6]),
            coalesce(col("INV NSV LC"), lit(0))
        ).otherwise(lit(0))
    )
    .withColumn(
        "PRICE VARIANCE BDG €",
        when(
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("BDG UNIT NSV €"), lit(0)), lit(0))) == 0,
            lit(0)
        ).otherwise(
            coalesce(col("INV NSV €"), lit(0)) -
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("BDG UNIT NSV €"), lit(0)), lit(0)))
        ) + when(
            col("DOC_TYPE_CODE").isin([5, 6]),
            coalesce(col("INV NSV €"), lit(0))
        ).otherwise(lit(0))
    )
    .withColumn(
        "PRICE VARIANCE REV €",
        when(
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("REV UNIT NSV €"), lit(0)), lit(0))) == 0,
            lit(0)
        ).otherwise(
            coalesce(col("INV NSV €"), lit(0)) -
            (coalesce(coalesce(col("INV QTY"), lit(0)), lit(0)) * coalesce(coalesce(col("REV UNIT NSV €"), lit(0)), lit(0)))
        ) + when(
            col("DOC_TYPE_CODE").isin([5, 6]),
            coalesce(col("INV NSV €"), lit(0))
        ).otherwise(lit(0))
    )
    .withColumn(
        "INV BDG TP LC",
        when(
            col("COMM_COST_OF_SALES") == 0,
            lit(0)
        ).otherwise(
            (coalesce(col("COMM_COST_OF_SALES"), lit(0)) + coalesce(col("TRANSPORT_COST"), lit(0))) * coalesce(col("INV QTY"), lit(0))
        )
    )

    .withColumn("INV BDG TP €", coalesce(col("INV BDG TP LC"), lit(0)) / col("CURRENCY_EURO_EXCH"))

    .drop("DOCUMENT_DATE")
    .withColumnRenamed("__DOCUMENT_DATE", "DOCUMENT_DATE")

    .drop("CURRENCY_CODE")
    .withColumnRenamed("__CURRENCY_CODE", "CURRENCY_CODE")

).localCheckpoint()


print("DONE SALEFACTS!!!")



#####################################################
########### JOIN ORD/INV/DEL WITH DIMS ##############
#####################################################


INVOICES = (
    SALEFACTS   # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Sales Analysis\Data\$(vEnvFrom)/QVD_Tmp\SALEFACTS_$(vsPeriod).QVD](qvd)

    .where(col("DATA_TYPE") == 1)

    .join(INVOICE_CUSTOMERS, ["INV_ID"], "left")
    .join(DELIVERY_CUSTOMERS, ["DEST_ID"], "left")
    .join(COMM_ORGANIZATIONS, ["COMM_ORGANIZATION_ID"], "left")

    .columns_to_lower()

    .drop('__mapping_product_code_key', '__mapping_product_code_revised_key', '__prz_budget_mapping_revised', 'acc_amount', 'acc_extra_amount', 'acc_extra_net_price', 'acc_net_price', 'acc_vat_amount_1', 'acc_vat_amount_2', 'act_qty', 'act_total_cos_comp', 'act_total_cos_netw', 'act_total_cos_netw_bdg', 'act_total_cos_netw_no_exch_var', 'act_total_gsv', 'act_total_gsv_no_tranlist', 'act_total_gsv_no_trannet', 'act_total_gsv_no_transp', 'act_total_nsv', 'act_unit_gsv', 'act_unit_nsv', 'allocation warehouse', 'allocation_warehouse', 'appointment_date', 'appointment_time', 'bdg avg nsv €', 'bdg cp lc', 'bdg cp €', 'bdg dd qty', 'bdg gm cp lc', 'bdg gm cp €', 'bdg gm tp lc', 'bdg gm tp €', 'bdg nsv lc', 'bdg nsv €', 'bdg qty', 'bdg tp lc', 'bdg tp €', 'bdg unit nsv lc', 'bdg_qty', 'bdg_total_cos_comp', 'bdg_total_cos_netw', 'bdg_total_cos_prod', 'bdg_total_nsv', 'bdg_unit_cos_netw', 'bdg_unit_nsv', 'blk gm cp lc', 'blk gm cp €', 'blk gm tp lc', 'blk gm tp €', 'blk gsv lc', 'blk gsv €', 'blk nsv lc', 'blk nsv €', 'blk qty', 'booking_update', 'budqtcus', 'business_line', 'business_line_descr', 'canvas_nsv_bdg', 'canvas_nsv_trg', 'canvas_qty_bdg', 'canvas_qty_trg', 'channel_code', 'comm_organization_id', 'company_id', 'company_owner_stock_reserved', 'company_sap_code', 'container_number', 'currency_decimals', 'currency_int_sign', 'cust_sap_code', 'custom_duty', 'customer', 'customer_workplan', 'custordd_ret_codean', 'dd percentage', 'del cp lc', 'del cp €', 'del gm cp lc', 'del gm cp €', 'del gm tp lc', 'del gm tp €', 'del gsv lc', 'del gsv €', 'del nsv lc', 'del nsv €', 'del qty', 'del tp lc', 'del tp €', 'delivery_carrier', 'delivery_date_requested', 'delivery_date_type', 'delivery_doc_total_amount', 'delivery_invret_quantity', 'delivery_nsv_amount', 'delivery_quantity', 'delivery_quantity_ot3', 'delivery_status', 'delivery_total_amount', 'delivery_warehouse_code', 'delta_flag', 'doc_acc_amount', 'doc_debit_change', 'doc_type_operation', 'doc_type_sign', 'document_date_new', 'document_text', 'est_available_date_to_ware', 'euro_exchange', 'euro_exchange_rate_fix', 'evaluation_type', 'extra_amount', 'extra_net_price', 'first_day_next_month', 'flag_var_exp1', 'flag_var_exp10', 'flag_var_exp11', 'flag_var_exp12', 'flag_var_exp13', 'flag_var_exp14', 'flag_var_exp15', 'flag_var_exp2', 'flag_var_exp3', 'flag_var_exp4', 'flag_var_exp5', 'flag_var_exp6', 'flag_var_exp7', 'flag_var_exp8', 'flag_var_exp9', 'for_cos_adj', 'for_exch_var', 'for_price_variance', 'for_qty', 'for_sim_nsv', 'for_total_cos_netw', 'for_total_cos_prod', 'for_total_nsv', 'for_unit_nsv', 'free_of_charge', 'freight_in', 'gm_monthly_bdg', 'gm_monthly_rev', 'handling', 'instock gm cp lc', 'instock gm cp €', 'instock gm tp lc', 'instock gm tp €', 'instock gsv lc', 'instock gsv €', 'instock nsv lc', 'instock nsv €', 'instock qty', 'inv bdg nsv lc', 'inv bdg tp lc', 'inv bdg tp €', 'inv rev nsv lc', 'inv rev nsv €', 'inv_cust_sap_code', 'inv_customer', 'inv_main_group_code_hist', 'inv_main_group_descr_hist', 'inv_purchase_group_code_hist', 'inv_purchase_group_descr_hist', 'inv_sub_group_code_hist', 'inv_super_group_code_hist', 'inv_super_group_descr_hist', 'invoicing_type_code', 'invoicing_type_descr', 'list_price_discounted', 'list_price_net_and_forced', 'network_budget', 'network_id', 'network_sap_code', 'network_target', 'nsv_monthly_bdg', 'nsv_monthly_rev', 'ode gm cp lc', 'ode gm cp €', 'ode gm tp lc', 'ode gm tp €', 'ode gsv lc', 'ode gsv €', 'ode nsv lc', 'ode nsv €', 'ode qty', 'ond gm cp lc', 'ond gm cp €', 'ond gm tp lc', 'ond gm tp €', 'ond gsv lc', 'ond gsv €', 'ond nsv lc', 'ond nsv €', 'ond qty', 'ord bo gm cp lc', 'ord bo gm cp €', 'ord bo gm tp lc', 'ord bo gm tp €', 'ord bo gsv lc', 'ord bo gsv €', 'ord bo nsv lc', 'ord bo nsv €', 'ord bo qty', 'ord cp lc', 'ord cp €', 'ord gm cp lc', 'ord gm cp €', 'ord gm tp lc', 'ord gm tp €', 'ord gsv lc', 'ord gsv €', 'ord m1 gm cp lc', 'ord m1 gm cp €', 'ord m1 gm tp lc', 'ord m1 gm tp €', 'ord m1 gsv lc', 'ord m1 gsv €', 'ord m1 nsv lc', 'ord m1 nsv €', 'ord m1 qty', 'ord m2 gm cp lc', 'ord m2 gm cp €', 'ord m2 gm tp lc', 'ord m2 gm tp €', 'ord m2 gsv lc', 'ord m2 gsv €', 'ord m2 nsv lc', 'ord m2 nsv €', 'ord m2 qty', 'ord m3 gm cp lc', 'ord m3 gm cp €', 'ord m3 gm tp lc', 'ord m3 gm tp €', 'ord m3 gsv lc', 'ord m3 gsv €', 'ord m3 nsv lc', 'ord m3 nsv €', 'ord m3 qty', 'ord m4 gm cp lc', 'ord m4 gm cp €', 'ord m4 gm tp lc', 'ord m4 gm tp €', 'ord m4 gsv lc', 'ord m4 gsv €', 'ord m4 nsv lc', 'ord m4 nsv €', 'ord m4 qty', 'ord m5 gm cp lc', 'ord m5 gm cp €', 'ord m5 gm tp lc', 'ord m5 gm tp €', 'ord m5 gsv lc', 'ord m5 gsv €', 'ord m5 nsv lc', 'ord m5 nsv €', 'ord m5 qty', 'ord m6 gm cp lc', 'ord m6 gm cp €', 'ord m6 gm tp lc', 'ord m6 gm tp €', 'ord m6 gsv lc', 'ord m6 gsv €', 'ord m6 nsv lc', 'ord m6 nsv €', 'ord m6 qty', 'ord nsv lc', 'ord nsv €', 'ord qty', 'ord tp lc', 'ord tp €', 'order_acknowledgement', 'order_alloc_date', 'order_alloc_status', 'order_commercial_salesman', 'order_commercial_zone', 'order_currency_code', 'order_customer_contact', 'order_customer_reference_2', 'order_delivered_qty', 'order_delivery_term', 'order_despatching_term', 'order_doc_total_amount', 'order_flag_acknowledgement', 'order_flag_forced_payment', 'order_flag_invoice_in_euro', 'order_flag_no_standard', 'order_flag_payment_user', 'order_foc', 'order_forced_payment_date', 'order_forced_price', 'order_global_insurance_amount', 'order_global_insurance_code', 'order_global_transport_amount', 'order_global_transport_code', 'order_in_picking_qty', 'order_in_stock_qty', 'order_invoice_type', 'order_invoiced_qty', 'order_line_amount', 'order_line_discount', 'order_list_price', 'order_logistic_area', 'order_max_all_date', 'order_nsv_amount', 'order_number_replenishment', 'order_original_qty', 'order_packing_term', 'order_payment_date', 'order_percentage_discount', 'order_priority', 'order_qty', 'order_qty_booked', 'order_qty_not_all', 'order_quantity_orm', 'order_registration_date', 'order_replen_qty', 'order_req_del_date_status', 'order_requested_delivery_month_year_numeric', 'order_status', 'order_temporary_address_1', 'order_temporary_address_2', 'order_temporary_address_flag', 'order_temporary_name_1', 'order_temporary_name_2', 'order_temporary_town_1', 'order_temporary_town_2', 'order_temporary_zip_code', 'order_total_amount', 'order_uk_aged', 'order_uk_blocked', 'order_uk_booked_not_this_month', 'order_uk_booked_this_month', 'order_uk_not_this_month', 'order_uk_unallocated_month', 'order_unallo_qty', 'order_unit_forced_price', 'order_vat_code', 'orm gm cp lc', 'orm gm cp €', 'orm gm tp lc', 'orm gm tp €', 'orm gsv lc', 'orm gsv €', 'orm nsv lc', 'orm nsv €', 'orm qty', 'orm_date', 'orm_number', 'other_cos', 'outcome', 'packing_code', 'packing_descr', 'payment_days_1', 'payment_days_2', 'payment_days_3', 'payment_expiring_inst', 'payment_modified', 'pck gm cp lc', 'pck gm cp €', 'pck gm tp lc', 'pck gm tp €', 'pck gsv lc', 'pck gsv €', 'pck nsv lc', 'pck nsv €', 'pck qty', 'platform_code', 'platform_descr', 'platform_surname', 'price variance bdg lc', 'price variance rev lc', 'price variance rev €', 'price_variance', 'product', 'product_id', 'quantity', 'quantity_revised', 'quantity_target_revised', 'ref_order_type_code', 'ref_order_type_descr', 'replenishment gm cp lc', 'replenishment gm cp €', 'replenishment gm tp lc', 'replenishment gm tp €', 'replenishment gsv lc', 'replenishment gsv €', 'replenishment nsv lc', 'replenishment nsv €', 'replenishment qty', 'returned_quantity', 'rev cp lc', 'rev cp €', 'rev gm cp lc', 'rev gm cp €', 'rev gm tp lc', 'rev gm tp €', 'rev nsv lc', 'rev nsv €', 'rev qty', 'rev tp lc', 'rev tp €', 'rev unit nsv lc', 'rev unit nsv €', 'revised_comm_cos', 'revised_sale_price', 'rvs_total_cos_netw', 'rvs_total_nsv', 'rvs_unit_cos_netw', 'rvs_unit_nsv', 'salefact_id', 'salteam_organization', 'sim_nsv', 'special_order_code', 'special_order_descr', 'stains', 'start_date_nsv_revised', 'start_date_nsv_target_revised', 'start_date_tpr_target_revised', 'start_date_tprice_revised', 'std_cos_comp', 'std_cos_cons', 'std_cos_netw', 'std_sale_cost', 'std_sale_price', 'std_sale_price_target', 't3316lng', 'target_revised_sale_price', 'temporary_address1', 'temporary_name', 'temporary_town1', 'temporary_town2', 'temporary_zip_code', 'tgt_total_cos_netw', 'timestamp', 'tot_amount_no_transport', 'tp_itc_bdg', 'tp_itc_rev', 'transfpri_budget_nsv', 'transfpri_costadj_infra', 'transfpri_costadj_nsv', 'transfpri_excvar_infra', 'transfpri_excvar_nsv', 'transfpri_revised_nsv', 'transfpri_target_costadj_nsv', 'transfpri_target_excvar_nsv', 'transfpri_target_nsv', 'transfpri_target_revised_nsv', 'transport_code', 'transport_cost_1', 'transport_cost_2', 'transport_descr', 'transport_value_1', 'transport_value_2', 'trg_qty', 'unallocated gm cp lc', 'unallocated gm cp €', 'unallocated gm tp lc', 'unallocated gm tp €', 'unallocated gsv lc', 'unallocated gsv €', 'unallocated nsv lc', 'unallocated nsv €', 'unallocated qty', 'upd_amount', 'upd_extra_amount', 'upd_extra_net_price', 'upd_forced_price', 'upd_list_price', 'upd_net_price', 'us_cust_man', 'us_cust_zone', 'us_doc_man', 'us_doc_zone', 'value_notes', 'var_expenses10_bdg', 'var_expenses10_rev', 'var_expenses11_bdg', 'var_expenses11_rev', 'var_expenses12_bdg', 'var_expenses12_rev', 'var_expenses13_bdg', 'var_expenses13_rev', 'var_expenses14_bdg', 'var_expenses14_rev', 'var_expenses15_bdg', 'var_expenses15_rev', 'var_expenses1_bdg', 'var_expenses1_rev', 'var_expenses2_bdg', 'var_expenses2_rev', 'var_expenses3_bdg', 'var_expenses3_rev', 'var_expenses4_bdg', 'var_expenses4_rev', 'var_expenses5_bdg', 'var_expenses5_rev', 'var_expenses6_bdg', 'var_expenses6_rev', 'var_expenses7_bdg', 'var_expenses7_rev', 'var_expenses8_bdg', 'var_expenses8_rev', 'var_expenses9_bdg', 'var_expenses9_rev', 'var_nsv', 'variation_qty', 'vat_amount_2', 'vat_percentage_2', 'vat_reason_descr', 'vat_reason_perc')
).localCheckpoint()

print("INVOICE checkpointed, saving to parquet...")

INVOICES = cast_cols(INVOICES)

print("DONE INVOICE (from SALEFACTS)!!!")
print(INVOICES)


ORDERS = (
    SALEFACTS   # FROM [lib://Project_Files_Production/Framework_HORSA\Projects\Sales Analysis\Data\$(vEnvPROD)/QVD_Tmp\SALEFACTS_$(vCurrentMonth).QVD]

    .where(col("DATA_TYPE") == 2)

    .join(INVOICE_CUSTOMERS, ["INV_ID"], "left")
    .join(DELIVERY_CUSTOMERS, ["DEST_ID"], "left")
    .join(COMM_ORGANIZATIONS, ["COMM_ORGANIZATION_ID"], "left")

    .columns_to_lower()

    .drop('__mapping_product_code_key', '__mapping_product_code_revised_key', '__prz_budget_mapping_revised', 'acc_amount', 'acc_extra_amount', 'acc_extra_net_price', 'acc_net_price', 'acc_vat_amount_1', 'acc_vat_amount_2', 'act_qty', 'act_total_cos_comp', 'act_total_cos_netw', 'act_total_cos_netw_bdg', 'act_total_cos_netw_no_exch_var', 'act_total_gsv', 'act_total_gsv_no_tranlist', 'act_total_gsv_no_trannet', 'act_total_gsv_no_transp', 'act_total_nsv', 'act_unit_gsv', 'act_unit_nsv', 'allocation_warehouse', 'appointment_date', 'appointment_time', 'bdg avg nsv €', 'bdg cp lc', 'bdg cp €', 'bdg dd qty', 'bdg gm cp lc', 'bdg gm cp €', 'bdg gm tp lc', 'bdg gm tp €', 'bdg nsv lc', 'bdg nsv €', 'bdg qty', 'bdg tp lc', 'bdg tp €', 'bdg unit nsv lc', 'bdg_qty', 'bdg_total_cos_comp', 'bdg_total_cos_netw', 'bdg_total_cos_prod', 'bdg_total_nsv', 'bdg_unit_cos_netw', 'bdg_unit_nsv', 'booking_status', 'booking_update', 'budqtcus', 'business_line', 'business_line_descr', 'canvas_nsv_bdg', 'canvas_nsv_trg', 'canvas_qty_bdg', 'canvas_qty_trg', 'channel_code', 'comm_cost_of_sales', 'comm_organization', 'comm_organization_id', 'company_id', 'company_owner_stock_reserved', 'company_sap_code', 'conf_del_date', 'cons_cost_of_sales', 'container_number', 'currency_decimals', 'currency_int_sign', 'cust_sap_code', 'custom_duty', 'customer', 'customer_workplan', 'custordd_ret_codean', 'dd percentage', 'del cp lc', 'del cp €', 'del gm cp lc', 'del gm cp €', 'del gm tp lc', 'del gm tp €', 'del gsv lc', 'del gsv €', 'del nsv lc', 'del nsv €', 'del qty', 'del tp lc', 'del tp €', 'delivery_carrier', 'delivery_date_requested', 'delivery_date_type', 'delivery_doc_total_amount', 'delivery_invret_quantity', 'delivery_nsv_amount', 'delivery_quantity', 'delivery_quantity_ot3', 'delivery_status', 'delivery_total_amount', 'delivery_warehouse_code', 'delta_flag', 'despatching_date', 'despatching_note', 'doc_acc_amount', 'doc_debit_change', 'doc_total_amount', 'doc_type_operation', 'doc_type_sign', 'document_code', 'document_date_new', 'document_text', 'est_available_date_to_ware', 'euro_exchange', 'euro_exchange_rate_fix', 'evaluation_type', 'extra_amount', 'extra_net_price', 'first_day_next_month', 'flag_var_exp1', 'flag_var_exp10', 'flag_var_exp11', 'flag_var_exp12', 'flag_var_exp13', 'flag_var_exp14', 'flag_var_exp15', 'flag_var_exp2', 'flag_var_exp3', 'flag_var_exp4', 'flag_var_exp5', 'flag_var_exp6', 'flag_var_exp7', 'flag_var_exp8', 'flag_var_exp9', 'for_cos_adj', 'for_exch_var', 'for_price_variance', 'for_qty', 'for_sim_nsv', 'for_total_cos_netw', 'for_total_cos_prod', 'for_total_nsv', 'for_unit_nsv', 'free_of_charge', 'gm_monthly_bdg', 'gm_monthly_rev', 'handling', 'instock gm cp lc', 'instock gm cp €', 'instock gm tp lc', 'instock gm tp €', 'instock gsv lc', 'instock gsv €', 'instock nsv lc', 'instock nsv €', 'instock qty', 'inv + del gm cp lc', 'inv + del gm cp €', 'inv + del gm tp lc', 'inv + del gm tp €', 'inv + del gsv lc', 'inv + del gsv €', 'inv + del nsv lc', 'inv + del nsv €', 'inv + del qty', 'inv bdg nsv lc', 'inv bdg nsv €', 'inv bdg tp lc', 'inv bdg tp €', 'inv cp lc', 'inv cp €', 'inv gm cp lc', 'inv gm cp €', 'inv gm tp lc', 'inv gm tp €', 'inv gsv + transport lc', 'inv gsv + transport €', 'inv gsv lc', 'inv gsv promotional & bonus lc', 'inv gsv promotional & bonus €', 'inv gsv €', 'inv nsv lc', 'inv nsv €', 'inv qty', 'inv rev nsv lc', 'inv rev nsv €', 'inv tp lc', 'inv tp €', 'inv_cust_sap_code', 'inv_customer', 'inv_main_group_code_hist', 'inv_main_group_descr_hist', 'inv_purchase_group_code_hist', 'inv_purchase_group_descr_hist', 'inv_sub_group_code_hist', 'inv_super_group_code_hist', 'inv_super_group_descr_hist', 'invoicing_type_code', 'invoicing_type_descr', 'list_price_discounted', 'list_price_net_and_forced', 'network_budget', 'network_id', 'network_sap_code', 'network_target', 'nsv_monthly_bdg', 'nsv_monthly_rev', 'ode gm cp lc', 'ode gm cp €', 'ode gm tp lc', 'ode gm tp €', 'ode gsv lc', 'ode gsv €', 'ode nsv lc', 'ode nsv €', 'ode qty', 'ord m1 gm cp lc', 'ord m1 gm cp €', 'ord m1 gm tp lc', 'ord m1 gm tp €', 'ord m1 gsv lc', 'ord m1 gsv €', 'ord m1 nsv lc', 'ord m1 nsv €', 'ord m1 qty', 'ord m2 gm cp lc', 'ord m2 gm cp €', 'ord m2 gm tp lc', 'ord m2 gm tp €', 'ord m2 gsv lc', 'ord m2 gsv €', 'ord m2 nsv lc', 'ord m2 nsv €', 'ord m2 qty', 'ord m3 gm cp lc', 'ord m3 gm cp €', 'ord m3 gm tp lc', 'ord m3 gm tp €', 'ord m3 gsv lc', 'ord m3 gsv €', 'ord m3 nsv lc', 'ord m3 nsv €', 'ord m3 qty', 'ord m4 gm cp lc', 'ord m4 gm cp €', 'ord m4 gm tp lc', 'ord m4 gm tp €', 'ord m4 gsv lc', 'ord m4 gsv €', 'ord m4 nsv lc', 'ord m4 nsv €', 'ord m4 qty', 'ord m5 gm cp lc', 'ord m5 gm cp €', 'ord m5 gm tp lc', 'ord m5 gm tp €', 'ord m5 gsv lc', 'ord m5 gsv €', 'ord m5 nsv lc', 'ord m5 nsv €', 'ord m5 qty', 'ord m6 gm cp lc', 'ord m6 gm cp €', 'ord m6 gm tp lc', 'ord m6 gm tp €', 'ord m6 gsv lc', 'ord m6 gsv €', 'ord m6 nsv lc', 'ord m6 nsv €', 'ord m6 qty', 'order number', 'order_acknowledgement', 'order_alloc_date', 'order_alloc_status', 'order_commercial_salesman', 'order_commercial_zone', 'order_currency_code', 'order_customer_contact', 'order_customer_reference_2', 'order_delivered_qty', 'order_delivery_term', 'order_despatching_term', 'order_doc_total_amount', 'order_flag_acknowledgement', 'order_flag_forced_payment', 'order_flag_invoice_in_euro', 'order_flag_no_standard', 'order_flag_payment_user', 'order_foc', 'order_forced_payment_date', 'order_forced_price', 'order_global_insurance_amount', 'order_global_insurance_code', 'order_global_transport_amount', 'order_global_transport_code', 'order_in_picking_qty', 'order_in_stock_qty', 'order_invoice_type', 'order_invoiced_qty', 'order_line_amount', 'order_line_discount', 'order_list_price', 'order_logistic_area', 'order_max_all_date', 'order_nsv_amount', 'order_number_replenishment', 'order_original_qty', 'order_packing_term', 'order_payment_date', 'order_percentage_discount', 'order_priority', 'order_qty', 'order_qty_booked', 'order_qty_not_all', 'order_quantity_orm', 'order_registration_date', 'order_replen_qty', 'order_req_del_date_status', 'order_requested_delivery_month_year_numeric', 'order_status', 'order_temporary_address_1', 'order_temporary_address_2', 'order_temporary_address_flag', 'order_temporary_name_1', 'order_temporary_name_2', 'order_temporary_town_1', 'order_temporary_town_2', 'order_temporary_zip_code', 'order_total_amount', 'order_uk_aged', 'order_uk_blocked', 'order_uk_booked_not_this_month', 'order_uk_booked_this_month', 'order_uk_not_this_month', 'order_uk_unallocated_month', 'order_unallo_qty', 'order_unit_forced_price', 'order_vat_code', 'orm gm cp lc', 'orm gm cp €', 'orm gm tp lc', 'orm gm tp €', 'orm gsv lc', 'orm gsv €', 'orm nsv lc', 'orm nsv €', 'orm qty', 'orm_date', 'orm_number', 'other_cos', 'outcome', 'packing_code', 'packing_descr', 'payment_days_1', 'payment_days_2', 'payment_days_3', 'payment_expiring_code', 'payment_expiring_descr', 'payment_expiring_disc', 'payment_expiring_inst', 'payment_modified', 'payment_type_code', 'payment_type_descr', 'picking_carrier', 'picking_carrier_descr', 'picking_date', 'picking_date_r', 'picking_number', 'platform_code', 'platform_descr', 'platform_surname', 'pot_type', 'price variance bdg lc', 'price variance bdg €', 'price variance rev lc', 'price variance rev €', 'price_variance', 'product', 'product_id', 'quantity', 'quantity_revised', 'quantity_target_revised', 'ref_despatch_date', 'ref_despatch_note', 'ref_invoice', 'ref_invoice_date', 'ref_order_line', 'ref_order_number', 'ref_order_type_code', 'ref_order_type_descr', 'removable_charges €', 'replenishment gm cp lc', 'replenishment gm cp €', 'replenishment gm tp lc', 'replenishment gm tp €', 'replenishment gsv lc', 'replenishment gsv €', 'replenishment nsv lc', 'replenishment nsv €', 'replenishment qty', 'returned_quantity', 'rev cp lc', 'rev cp €', 'rev gm cp lc', 'rev gm cp €', 'rev gm tp lc', 'rev gm tp €', 'rev nsv lc', 'rev nsv €', 'rev qty', 'rev tp lc', 'rev tp €', 'rev unit nsv lc', 'rev unit nsv €', 'revised_comm_cos', 'revised_sale_price', 'rvs_total_cos_netw', 'rvs_total_nsv', 'rvs_unit_cos_netw', 'rvs_unit_nsv', 'salefact_id', 'salteam_organization', 'shipment_code', 'shipment_descr', 'sim_nsv', 'special_order_code', 'special_order_descr', 'stains', 'start_date_nsv_revised', 'start_date_nsv_target_revised', 'start_date_tpr_target_revised', 'start_date_tprice_revised', 'std_cos_comp', 'std_cos_cons', 'std_cos_netw', 'std_sale_cost', 'std_sale_price', 'std_sale_price_target', 't3316lng', 'target_revised_sale_price', 'temporary_address1', 'temporary_name', 'temporary_town1', 'temporary_town2', 'temporary_zip_code', 'tgt_total_cos_netw', 'timestamp', 'tot_amount_no_transport', 'total_amount', 'tp_itc_bdg', 'tp_itc_rev', 'transfpri_budget_nsv', 'transfpri_costadj_infra', 'transfpri_costadj_nsv', 'transfpri_excvar_infra', 'transfpri_excvar_nsv', 'transfpri_revised_nsv', 'transfpri_target_costadj_nsv', 'transfpri_target_excvar_nsv', 'transfpri_target_nsv', 'transfpri_target_revised_nsv', 'transport cost infra lc', 'transport cost infra €', 'transport cost lc', 'transport cost €', 'transport_code', 'transport_cost', 'transport_cost_1', 'transport_cost_2', 'transport_descr', 'transport_value_1', 'transport_value_2', 'trg_qty', 'type', 'unallocated gm cp lc', 'unallocated gm cp €', 'unallocated gm tp lc', 'unallocated gm tp €', 'unallocated gsv lc', 'unallocated gsv €', 'unallocated nsv lc', 'unallocated nsv €', 'unallocated qty', 'upd_amount', 'upd_extra_amount', 'upd_extra_net_price', 'upd_forced_price', 'upd_list_price', 'upd_net_price', 'us_cust_man', 'us_cust_zone', 'us_doc_man', 'us_doc_zone', 'value_notes', 'var_expenses10_bdg', 'var_expenses10_rev', 'var_expenses11_bdg', 'var_expenses11_rev', 'var_expenses12_bdg', 'var_expenses12_rev', 'var_expenses13_bdg', 'var_expenses13_rev', 'var_expenses14_bdg', 'var_expenses14_rev', 'var_expenses15_bdg', 'var_expenses15_rev', 'var_expenses1_bdg', 'var_expenses1_rev', 'var_expenses2_bdg', 'var_expenses2_rev', 'var_expenses3_bdg', 'var_expenses3_rev', 'var_expenses4_bdg', 'var_expenses4_rev', 'var_expenses5_bdg', 'var_expenses5_rev', 'var_expenses6_bdg', 'var_expenses6_rev', 'var_expenses7_bdg', 'var_expenses7_rev', 'var_expenses8_bdg', 'var_expenses8_rev', 'var_expenses9_bdg', 'var_expenses9_rev', 'var_nsv', 'variation_qty', 'vat_amount_1', 'vat_amount_2', 'vat_percentage_2', 'vat_reason_descr', 'vat_reason_perc')
).localCheckpoint()

print("ORDERS checkpointed, saving to parquet...")

ORDERS = cast_cols(ORDERS)

print("DONE ORDERS (from SALEFACTS)!!!")
print(ORDERS)


DELIVERIES = (
    SALEFACTS

    .where(col("DATA_TYPE") == 3)

    .join(INVOICE_CUSTOMERS, ["INV_ID"], "left")
    .join(DELIVERY_CUSTOMERS, ["DEST_ID"], "left")
    .join(COMM_ORGANIZATIONS, ["COMM_ORGANIZATION_ID"], "left")

    .columns_to_lower()

    .drop('__mapping_product_code_key', '__mapping_product_code_revised_key', '__prz_budget_mapping_revised', 'acc_amount', 'acc_extra_amount', 'acc_extra_net_price', 'acc_net_price', 'acc_vat_amount_1', 'acc_vat_amount_2', 'act_qty', 'act_total_cos_comp', 'act_total_cos_netw', 'act_total_cos_netw_bdg', 'act_total_cos_netw_no_exch_var', 'act_total_gsv', 'act_total_gsv_no_tranlist', 'act_total_gsv_no_trannet', 'act_total_gsv_no_transp', 'act_total_nsv', 'act_unit_gsv', 'act_unit_nsv', 'allocation_warehouse', 'appointment_date', 'appointment_time', 'bdg avg nsv €', 'bdg cp lc', 'bdg cp €', 'bdg dd qty', 'bdg gm cp lc', 'bdg gm cp €', 'bdg gm tp lc', 'bdg gm tp €', 'bdg nsv lc', 'bdg nsv €', 'bdg qty', 'bdg tp lc', 'bdg tp €', 'bdg unit nsv lc', 'bdg_qty', 'bdg_total_cos_comp', 'bdg_total_cos_netw', 'bdg_total_cos_prod', 'bdg_total_nsv', 'bdg_unit_cos_netw', 'bdg_unit_nsv', 'blk gm cp lc', 'blk gm cp €', 'blk gm tp lc', 'blk gm tp €', 'blk gsv lc', 'blk gsv €', 'blk nsv lc', 'blk nsv €', 'blk qty', 'booking_update', 'budqtcus', 'business_line', 'business_line_descr', 'canvas_nsv_bdg', 'canvas_nsv_trg', 'canvas_qty_bdg', 'canvas_qty_trg', 'channel_code', 'comm_organization', 'comm_organization_id', 'company_id', 'company_owner_stock_reserved', 'company_sap_code', 'container_number', 'currency_decimals', 'currency_int_sign', 'cust_sap_code', 'custom_duty', 'customer', 'customer_workplan', 'custordd_ret_codean', 'dd percentage', 'delivery_carrier', 'delivery_date_requested', 'delivery_date_type', 'delivery_doc_total_amount', 'delivery_invret_quantity', 'delivery_nsv_amount', 'delivery_quantity', 'delivery_quantity_ot3', 'delivery_status', 'delivery_total_amount', 'delivery_warehouse_code', 'delta_flag', 'doc_acc_amount', 'doc_debit_change', 'doc_total_amount', 'doc_type_operation', 'doc_type_sign', 'document_code', 'document_date_new', 'document_text', 'est_available_date_to_ware', 'euro_exchange', 'euro_exchange_rate_fix', 'evaluation_type', 'extra_amount', 'extra_net_price', 'first_day_next_month', 'flag_var_exp1', 'flag_var_exp10', 'flag_var_exp11', 'flag_var_exp12', 'flag_var_exp13', 'flag_var_exp14', 'flag_var_exp15', 'flag_var_exp2', 'flag_var_exp3', 'flag_var_exp4', 'flag_var_exp5', 'flag_var_exp6', 'flag_var_exp7', 'flag_var_exp8', 'flag_var_exp9', 'for_cos_adj', 'for_exch_var', 'for_price_variance', 'for_qty', 'for_sim_nsv', 'for_total_cos_netw', 'for_total_cos_prod', 'for_total_nsv', 'for_unit_nsv', 'free_of_charge', 'freight_in', 'gm_monthly_bdg', 'gm_monthly_rev', 'handling', 'instock gm cp lc', 'instock gm cp €', 'instock gm tp lc', 'instock gm tp €', 'instock gsv lc', 'instock gsv €', 'instock nsv lc', 'instock nsv €', 'instock qty', 'inv bdg nsv lc', 'inv bdg nsv €', 'inv bdg tp lc', 'inv bdg tp €', 'inv cp lc', 'inv cp €', 'inv gm cp lc', 'inv gm cp €', 'inv gm tp lc', 'inv gm tp €', 'inv gsv + transport lc', 'inv gsv + transport €', 'inv gsv lc', 'inv gsv promotional & bonus lc', 'inv gsv promotional & bonus €', 'inv gsv €', 'inv nsv lc', 'inv nsv €', 'inv qty', 'inv rev nsv lc', 'inv rev nsv €', 'inv tp lc', 'inv tp €', 'inv_cust_sap_code', 'inv_customer', 'inv_main_group_code_hist', 'inv_main_group_descr_hist', 'inv_purchase_group_code_hist', 'inv_purchase_group_descr_hist', 'inv_sub_group_code_hist', 'inv_super_group_code_hist', 'inv_super_group_descr_hist', 'invoicing_type_code', 'invoicing_type_descr', 'list_price_discounted', 'list_price_net_and_forced', 'network_budget', 'network_id', 'network_sap_code', 'network_target', 'nsv_monthly_bdg', 'nsv_monthly_rev', 'ode gm cp lc', 'ode gm cp €', 'ode gm tp lc', 'ode gm tp €', 'ode gsv lc', 'ode gsv €', 'ode nsv lc', 'ode nsv €', 'ode qty', 'ond gm cp lc', 'ond gm cp €', 'ond gm tp lc', 'ond gm tp €', 'ond gsv lc', 'ond gsv €', 'ond nsv lc', 'ond nsv €', 'ond qty', 'ord bo gm cp lc', 'ord bo gm cp €', 'ord bo gm tp lc', 'ord bo gm tp €', 'ord bo gsv lc', 'ord bo gsv €', 'ord bo nsv lc', 'ord bo nsv €', 'ord bo qty', 'ord cp lc', 'ord cp €', 'ord gm cp lc', 'ord gm cp €', 'ord gm tp lc', 'ord gm tp €', 'ord gsv lc', 'ord gsv €', 'ord m1 gm cp lc', 'ord m1 gm cp €', 'ord m1 gm tp lc', 'ord m1 gm tp €', 'ord m1 gsv lc', 'ord m1 gsv €', 'ord m1 nsv lc', 'ord m1 nsv €', 'ord m1 qty', 'ord m2 gm cp lc', 'ord m2 gm cp €', 'ord m2 gm tp lc', 'ord m2 gm tp €', 'ord m2 gsv lc', 'ord m2 gsv €', 'ord m2 nsv lc', 'ord m2 nsv €', 'ord m2 qty', 'ord m3 gm cp lc', 'ord m3 gm cp €', 'ord m3 gm tp lc', 'ord m3 gm tp €', 'ord m3 gsv lc', 'ord m3 gsv €', 'ord m3 nsv lc', 'ord m3 nsv €', 'ord m3 qty', 'ord m4 gm cp lc', 'ord m4 gm cp €', 'ord m4 gm tp lc', 'ord m4 gm tp €', 'ord m4 gsv lc', 'ord m4 gsv €', 'ord m4 nsv lc', 'ord m4 nsv €', 'ord m4 qty', 'ord m5 gm cp lc', 'ord m5 gm cp €', 'ord m5 gm tp lc', 'ord m5 gm tp €', 'ord m5 gsv lc', 'ord m5 gsv €', 'ord m5 nsv lc', 'ord m5 nsv €', 'ord m5 qty', 'ord m6 gm cp lc', 'ord m6 gm cp €', 'ord m6 gm tp lc', 'ord m6 gm tp €', 'ord m6 gsv lc', 'ord m6 gsv €', 'ord m6 nsv lc', 'ord m6 nsv €', 'ord m6 qty', 'ord nsv lc', 'ord nsv €', 'ord qty', 'ord tp lc', 'ord tp €', 'ord_requested_delivery_date', 'order_acknowledgement', 'order_alloc_date', 'order_alloc_status', 'order_commercial_salesman', 'order_commercial_zone', 'order_currency_code', 'order_customer_contact', 'order_customer_reference_2', 'order_delivered_qty', 'order_delivery_term', 'order_despatching_term', 'order_doc_total_amount', 'order_flag_acknowledgement', 'order_flag_forced_payment', 'order_flag_invoice_in_euro', 'order_flag_no_standard', 'order_flag_payment_user', 'order_foc', 'order_forced_payment_date', 'order_forced_price', 'order_global_insurance_amount', 'order_global_insurance_code', 'order_global_transport_amount', 'order_global_transport_code', 'order_in_picking_qty', 'order_in_stock_qty', 'order_invoice_type', 'order_invoiced_qty', 'order_line_amount', 'order_line_discount', 'order_list_price', 'order_logistic_area', 'order_max_all_date', 'order_nsv_amount', 'order_number_replenishment', 'order_original_qty', 'order_packing_term', 'order_payment_date', 'order_percentage_discount', 'order_priority', 'order_qty', 'order_qty_booked', 'order_qty_not_all', 'order_quantity_orm', 'order_registration_date', 'order_replen_qty', 'order_req_del_date_status', 'order_requested_delivery_month_year_numeric', 'order_source_descr', 'order_status', 'order_temporary_address_1', 'order_temporary_address_2', 'order_temporary_address_flag', 'order_temporary_name_1', 'order_temporary_name_2', 'order_temporary_town_1', 'order_temporary_town_2', 'order_temporary_zip_code', 'order_total_amount', 'order_uk_aged', 'order_uk_blocked', 'order_uk_booked_not_this_month', 'order_uk_booked_this_month', 'order_uk_not_this_month', 'order_uk_unallocated_month', 'order_unallo_qty', 'order_unit_forced_price', 'order_vat_code', 'orm gm cp lc', 'orm gm cp €', 'orm gm tp lc', 'orm gm tp €', 'orm gsv lc', 'orm gsv €', 'orm nsv lc', 'orm nsv €', 'orm qty', 'orm_date', 'orm_number', 'other_cos', 'outcome', 'packing_code', 'packing_descr', 'payment_days_1', 'payment_days_2', 'payment_days_3', 'payment_expiring_code', 'payment_expiring_descr', 'payment_expiring_disc', 'payment_expiring_inst', 'payment_modified', 'payment_type_code', 'payment_type_descr', 'pck gm cp lc', 'pck gm cp €', 'pck gm tp lc', 'pck gm tp €', 'pck gsv lc', 'pck gsv €', 'pck nsv lc', 'pck nsv €', 'pck qty', 'platform_code', 'platform_descr', 'platform_surname', 'price variance bdg lc', 'price variance bdg €', 'price variance rev lc', 'price variance rev €', 'price_variance', 'product', 'product_id', 'quantity', 'quantity_revised', 'quantity_target_revised', 'ref_despatch_date', 'ref_despatch_note', 'ref_invoice', 'ref_invoice_date', 'ref_order_line', 'ref_order_number', 'ref_order_type_code', 'ref_order_type_descr', 'removable_charges €', 'replenishment gm cp lc', 'replenishment gm cp €', 'replenishment gm tp lc', 'replenishment gm tp €', 'replenishment gsv lc', 'replenishment gsv €', 'replenishment nsv lc', 'replenishment nsv €', 'replenishment qty', 'returned_quantity', 'rev cp lc', 'rev cp €', 'rev gm cp lc', 'rev gm cp €', 'rev gm tp lc', 'rev gm tp €', 'rev nsv lc', 'rev nsv €', 'rev qty', 'rev tp lc', 'rev tp €', 'rev unit nsv lc', 'rev unit nsv €', 'revised_comm_cos', 'revised_sale_price', 'rvs_total_cos_netw', 'rvs_total_nsv', 'rvs_unit_cos_netw', 'rvs_unit_nsv', 'salefact_id', 'salteam_organization', 'sim_nsv', 'special_order_code', 'special_order_descr', 'stains', 'start_date_nsv_revised', 'start_date_nsv_target_revised', 'start_date_tpr_target_revised', 'start_date_tprice_revised', 'std_cos_comp', 'std_cos_cons', 'std_cos_netw', 'std_sale_cost', 'std_sale_price', 'std_sale_price_target', 't3316lng', 'target_revised_sale_price', 'temporary_address1', 'temporary_name', 'temporary_town1', 'temporary_town2', 'temporary_zip_code', 'tgt_total_cos_netw', 'timestamp', 'tot_amount_no_transport', 'total_amount', 'tp_itc_bdg', 'tp_itc_rev', 'transfpri_budget_nsv', 'transfpri_costadj_infra', 'transfpri_costadj_nsv', 'transfpri_excvar_infra', 'transfpri_excvar_nsv', 'transfpri_revised_nsv', 'transfpri_target_costadj_nsv', 'transfpri_target_excvar_nsv', 'transfpri_target_nsv', 'transfpri_target_revised_nsv', 'transport cost infra lc', 'transport cost infra €', 'transport cost lc', 'transport cost €', 'transport_code', 'transport_cost_1', 'transport_cost_2', 'transport_descr', 'transport_value_1', 'transport_value_2', 'trg_qty', 'unallocated gm cp lc', 'unallocated gm cp €', 'unallocated gm tp lc', 'unallocated gm tp €', 'unallocated gsv lc', 'unallocated gsv €', 'unallocated nsv lc', 'unallocated nsv €', 'unallocated qty', 'upd_amount', 'upd_extra_amount', 'upd_extra_net_price', 'upd_forced_price', 'upd_list_price', 'upd_net_price', 'us_cust_man', 'us_cust_zone', 'us_doc_man', 'us_doc_zone', 'value_notes', 'var_expenses10_bdg', 'var_expenses10_rev', 'var_expenses11_bdg', 'var_expenses11_rev', 'var_expenses12_bdg', 'var_expenses12_rev', 'var_expenses13_bdg', 'var_expenses13_rev', 'var_expenses14_bdg', 'var_expenses14_rev', 'var_expenses15_bdg', 'var_expenses15_rev', 'var_expenses1_bdg', 'var_expenses1_rev', 'var_expenses2_bdg', 'var_expenses2_rev', 'var_expenses3_bdg', 'var_expenses3_rev', 'var_expenses4_bdg', 'var_expenses4_rev', 'var_expenses5_bdg', 'var_expenses5_rev', 'var_expenses6_bdg', 'var_expenses6_rev', 'var_expenses7_bdg', 'var_expenses7_rev', 'var_expenses8_bdg', 'var_expenses8_rev', 'var_expenses9_bdg', 'var_expenses9_rev', 'var_nsv', 'variation_qty', 'vat_amount_1', 'vat_amount_2', 'vat_percentage_2', 'vat_reason_descr', 'vat_reason_perc')
).localCheckpoint()

print("DELIVERIES checkpointed, saving to parquet...")

DELIVERIES = cast_cols(DELIVERIES)

print("DONE DELIVERIES (from SALEFACTS)!!!")
print(DELIVERIES)

#################################################
########### CREATE CAS ORD/INV/DEL ##############
#################################################

########### CREATE LIST SAP COLS

list_sap_cols_inv = ["order_number", "ref_despatch_note", "payment_type_code", "payment_expiring_code", "ref_invoice", "document_code", "posnr", "cancelled_invoice", "pospa", "order_line", "data_type", "document_date", "time_id", "cancelled", "comm_organization", "fiscal document", "sales_organization", "distr_channel", "division", "kurrf", "invoice_type", "knumv", "doc_type_code_single", "moltiplicatore_reso", "doc_total_amount", "product_code", "ref_order_number", "ref_order_line", "inv qty fabs", "plant", "area_code", "picking_carrier", "dest_id", "inv_id", "main_group_code", "purchase_group_code", "rsm_code", "sale_director_code", "sale_zone_code", "salesman_code", "super_group_code", "sales_office", "company", "network", "flag_cancelled_invoice_valid", "doc_type_code", "currency_code", "exchange", "inv ic discount/surcharge", "inv gsv promo bonus lc temp", "inv raee lc", "list price tot", "total amount", "removable charges lc", "picking_number", "key_cost_commercial", "key_cost_freight_in", "key_cost_transfer_price", "key_cost_stdindef_tca", "inv ic discount/surcharge lc", "inv ic discount/surcharge €", "inv qty", "inv raee €", "inv transport cost lc", "inv transport cost €", "inv gsv lc", "inv gsv €", "inv gsv promotional & bonus lc", "inv gsv promotional & bonus €", "inv nsv lc", "inv nsv €", "removable charges €", "key_fact"]
list_sap_cols_ord = ["area_code", "category", "company", "confirmed date", "confirmed time", "currency_code", "data_type", "dest_id", "distr_channel", "division", "doc_type_code", "document_date", "exchange", "exchange_rate_sap", "first_req_del_date", "inv_id", "key_cost_commercial", "key_cost_freight_in", "key_cost_stdindef_tca", "key_cost_transfer_price", "key_fact", "knumv", "main_group_code", "moltiplicatore_reso", "network", "ord gsv €", "ord gsv lc", "ord ic discount/surcharge", "ord nsv €", "ord nsv lc", "ord qty", "ord qty consignment", "ord qty fabs", "ord qty orig", "ord raee lc", "ord_requested_delivery_date", "order status", "order_blocking_code", "order_blocking_date", "order_blocking_user", "order_creation_user", "order_customer_date", "order_customer_reference_1", "order_date", "order_delivery_warehouse", "order_line", "order_number", "order_requested_delivery_date", "order_requested_delivery_month_year", "order_source", "order_type_code", "order_unblocking_date", "order_unblocking_user", "plant", "product_code", "pstyv", "purchase_group_code", "removable charges tot", "rsm_code", "sale_director_code", "sale_zone_code", "sales_office", "sales_organization", "salesman_code", "sap_order_block_credit", "sap_order_block_delivery", "super_group_code", "time_id"]
list_sap_cols_del = ["order_number", "order_line", "despatching_note", "posnr", "data_type", "type", "despatching_date", "shipment_code", "picking_date", "conf_del_date", "routing", "booking_status", "appointment", "booking_slot", "shipping type", "product_code", "area_code", "original del qty", "del qty fabs", "sales_office", "picking_number", "return_reason", "order_customer_reference_1", "sap_order_block_delivery", "knumv", "sales_organization", "division", "distr_channel", "category", "company", "network", "plant", "shipping point", "pstyv", "confirmed date", "confirmed time", "planned_dispatch_date", "first_req_del_date", "picking_carrier", "del qty", "del qty consignment", "dest_id", "inv_id", "main_group_code", "purchase_group_code", "rsm_code", "sale_director_code", "sale_zone_code", "salesman_code", "super_group_code", "currency_code", "exchange", "time_id", "document_date", "exchange_rate_sap", "del raee lc", "list_price_tot", "key_cost_commercial", "key_cost_freight_in", "key_cost_transfer_price", "key_cost_stdindef_tca", "del ic discount/surcharge lc", "del ic discount/surcharge €", "del raee €", "del nsv lc", "del nsv €", "del gsv lc", "del gsv €", "removable charges", "key_fact"]

postaction_inv = """drop view if exists dwe.qlk_bil_gs_invoice;
create or replace view dwe.qlk_bil_gs_invoice as (
    select *,
    'sap' as source
    from dwa.sap_bil_ft_invoice
    union all
    select *,
    'cas' as source
    from dwa.cas_bil_ft_invoice
);"""
postaction_del = """drop view if exists dwe.qlk_del_gs_delivery;
create or replace view dwe.qlk_del_gs_delivery as (
    select *,
    'sap' as source
    from dwa.sap_del_ft_delivery
    union all
    select *,
    'cas' as source
    from dwa.cas_del_ft_delivery
);"""
postaction_ord = """drop view if exists dwe.qlk_sls_gs_order;
create or replace view dwe.qlk_sls_gs_order as (
    select *,
    'sap' as source
    from dwa.sap_sls_ft_order
    union ALL
    select *,
    'cas' as source
    from dwa.cas_sls_ft_order
);"""


########### CREATE CAS INV

CAS_INVOICES = (
    INVOICES
        .withColumn("key_fact", concat_ws("###", lit("INVOICE"), col("DOCUMENT_CODE"), col("order_line"), col("REF_INVOICE")))
        .withColumn("posnr", col("DOCUMENT_CODE")) # ??
        .withColumn('inv transport cost lc', lit(None).cast('double'))
        .withColumn('key_cost_commercial', lit(None).cast('string'))
        .withColumn('distr_channel', lit(None).cast('string'))
        .withColumn('inv gsv promo bonus lc temp', lit(None).cast('double'))
        .withColumn('forced price tot', lit(None).cast('double'))
        .withColumn('flag_cancelled_invoice_valid', lit(None).cast('int'))
        .withColumn('confirmed time', lit(None).cast('string'))
        .withColumn('kurrf', lit(None).cast('double'))
        .withColumn('rsm_code', lit(None).cast('string'))
        .withColumn('vat amount 1', lit(None).cast('double'))
        .withColumn('fiscal document', lit(None).cast('string'))
        .withColumn('matnr', lit(None).cast('string'))
        .withColumn('doc_type_code_concatenate', lit(None).cast('bigint'))
        .withColumn('exchange', lit(None).cast('double'))
        .withColumn('inv qty fabs', lit(None).cast('double'))
        .withColumn('key_cost_freight_in', lit(None).cast('string'))
        .withColumn('key_cost_transfer_price', lit(None).cast('string'))
        .withColumn('order_reason', lit(None).cast('string'))
        .withColumn('removable charges lc', lit(None).cast('double'))
        .withColumn('division', lit(None).cast('string'))
        .withColumn('sap_division', lit(None).cast('string'))
        .withColumn('confirmed date', lit(None).cast('string'))
        .withColumn('inv ic discount/surcharge', lit(None).cast('double'))
        .withColumn('cancelled_invoice', lit(None).cast('string'))
        .withColumn('plant', lit(None).cast('string'))
        .withColumn('inv raee lc', lit(None).cast('double'))
        .withColumn('invoice_type', lit(None).cast('string'))
        .withColumn('inv transport cost €', lit(None).cast('double'))
        .withColumn('booking_slot', lit(None).cast('date'))
        .withColumn('cancelled', lit(None).cast('boolean'))
        .withColumn('shipping type', lit(None).cast('string'))
        .withColumn('sap_order_block_credit', lit(None).cast('string'))
        .withColumn('total amount', lit(None).cast('double'))
        .withColumn('key_cost_stdindef_tca', lit(None).cast('string'))
        .withColumn('shipping point', lit(None).cast('string'))
        .withColumn('removable charges €', lit(None).cast('double'))
        .withColumn('exchange_rate_sap', lit(None).cast('double'))
        .withColumn('inv ic discount/surcharge lc', lit(None).cast('double'))
        .withColumn('inv ic discount/surcharge €', lit(None).cast('double'))
        .withColumn('doc_type_code_single', lit(None).cast('bigint'))
        .withColumn('land1', lit(None).cast('string'))
        .withColumn('list price tot', lit(None).cast('double'))
        .withColumn('pstyv', lit(None).cast('string'))
        .withColumn('sale_director_code', lit(None).cast('string'))
        .withColumn('moltiplicatore_reso', lit(None).cast('bigint'))
        .withColumn('inv raee €', lit(None).cast('double'))
        .withColumn('collective_number', lit(None).cast('string'))
        .withColumn('category', lit(None).cast('string'))
        .withColumn('sap_order_block_delivery', lit(None).cast('string'))
        .withColumn('pospa', lit(None).cast('string'))
        .withColumn('knumv', lit(None).cast('string'))
        .withColumn('document_date', col('document_date').cast('timestamp'))
        .withColumn('order_customer_date', col('order_customer_date').cast('timestamp'))
        .withColumn('order_requested_delivery_date', col('order_requested_delivery_date').cast('timestamp'))
        .withColumn('planned_dispatch_date', col('planned_dispatch_date').cast('string'))
        .withColumn('doc_type_code', col('doc_type_code').cast('bigint'))
        .withColumn('order_blocking_date', col('order_blocking_date').cast('string'))
        .withColumn('order_unblocking_date', col('order_unblocking_date').cast('string'))
        .select(list_sap_cols_inv)
).save_data_to_redshift_table("dwa.cas_bil_ft_invoice", postactions=postaction_inv)

list_dim_cols_inv = list(set(INVOICES.columns).difference(list_sap_cols_inv))+['key_fact']
CAS_INVOICES_DIM = (
    INVOICES
        .withColumn("key_fact", concat_ws("###", lit("INVOICE"), col("DOCUMENT_CODE"), col("order_line"), col("REF_INVOICE")))
        .select(list_dim_cols_inv)
).save_data_to_redshift_table("dwa.cas_bil_dim_invoice")


########### CREATE CAS ORD

CAS_ORDERS = (
    ORDERS
        .withColumn("key_fact", concat_ws("###", lit("ORDER"), col("order_number"), col("order_line")))
        .withColumn("exchange_rate_sap", lit(None).cast('double'))
        .withColumn("rsm_code", lit(None).cast('string'))
        .withColumn("sap_order_block_credit", lit(None).cast('string'))
        .withColumn("vat amount 1", lit(None).cast('double'))
        .withColumn("confirmed date", lit(None).cast('timestamp'))
        .withColumn("exchange", lit(None).cast('double'))
        .withColumn("ord qty orig", lit(None).cast('double'))
        .withColumn("shipping_point", lit(None).cast('string'))
        .withColumn("booking_slot_likp", lit(None).cast('date'))
        .withColumn("key_cost_transfer_price", lit(None).cast('string'))
        .withColumn("ord raee lc", lit(None).cast('double'))
        .withColumn("confirmed time", lit(None).cast('string'))
        .withColumn("sale_director_code", lit(None).cast('string'))
        .withColumn("% ord qty", lit(None).cast('double'))
        .withColumn("category", lit(None).cast('string'))
        .withColumn("key_cost_freight_in", lit(None).cast('string'))
        .withColumn("distr_channel", lit(None).cast('string'))
        .withColumn("booking_slot_vbap", lit(None).cast('timestamp'))
        .withColumn("ord qty fabs", lit(None).cast('double'))
        .withColumn("key_cost_commercial", lit(None).cast('string'))
        .withColumn("forced price", lit(None).cast('double'))
        .withColumn("list price", lit(None).cast('double'))
        .withColumn("moltiplicatore_reso", lit(None).cast('bigint'))
        .withColumn("ord ic discount/surcharge", lit(None).cast('double'))
        .withColumn("% ord qty fabs", lit(None).cast('double'))
        .withColumn("__faksk", lit(None).cast('string'))
        .withColumn("document_date_original", lit(None).cast('date'))
        .withColumn("plant", lit(None).cast('string'))
        .withColumn("pstyv", lit(None).cast('string'))
        .withColumn("sap_order_block_delivery", lit(None).cast('string'))
        .withColumn("ord qty consignment", lit(None).cast('double'))
        .withColumn("division", lit(None).cast('string'))
        .withColumn("knumv", lit(None).cast('string'))
        .withColumn("removable charges tot", lit(None).cast('double'))
        .withColumn("key_cost_stdindef_tca", lit(None).cast('string'))
        .withColumn("planned_dispatch_date", col('planned_dispatch_date').cast('timestamp'))
        .withColumn("order_customer_date", col('order_customer_date').cast('timestamp'))
        .withColumn("order_requested_delivery_date", col('order_requested_delivery_date').cast('timestamp'))
        .withColumn("doc_type_code", col('doc_type_code').cast('bigint'))
        .withColumn("order_blocking_date", col('order_blocking_date').cast('string'))
        .withColumn("order_unblocking_date", col('order_unblocking_date').cast('string'))
        .withColumn("mrdd", col('mrdd').cast('string'))
        .select(list_sap_cols_ord)
).save_data_to_redshift_table("dwa.cas_sls_ft_order", postactions=postaction_ord)


list_dim_cols_ord = list(set(ORDERS.columns).difference(list_sap_cols_ord))+['key_fact']
CAS_ORDERS_DIM = (
    ORDERS
        .withColumn("key_fact", concat_ws("###", lit("ORDER"), col("order_number"), col("order_line")))
        .select(list_dim_cols_ord)
).save_data_to_redshift_table("dwa.cas_sls_dim_order")
########### CREATE CAS DEL


CAS_DELIVERIES = (
    DELIVERIES
        .withColumn("key_fact", concat_ws("###", lit("DELIVERY"), col("DESPATCHING_NOTE"),col("order_line")))  # col("posnr"))) ???
        .withColumn('key_cost_commercial', lit(None).cast('string'))
        .withColumn('distr_channel', lit(None).cast('string'))
        .withColumn('original del qty', lit(None).cast('double'))
        .withColumn('del qty consignment', lit(None).cast('double'))
        .withColumn('del raee lc', lit(None).cast('double'))
        .withColumn('% del qty', lit(None).cast('double'))
        .withColumn('forced price tot', lit(None).cast('double'))
        .withColumn('confirmed time', lit(None).cast('string'))
        .withColumn('list price', lit(None).cast('double'))
        .withColumn('rsm_code', lit(None).cast('string'))
        .withColumn('document_date_original', lit(None).cast('date'))
        .withColumn('removable charges', lit(None).cast('double'))
        .withColumn('% del qty fabs', lit(None).cast('double'))
        .withColumn('ord qty orig', lit(None).cast('double'))
        .withColumn('exchange', lit(None).cast('double'))
        .withColumn('del raee €', lit(None).cast('double'))
        .withColumn('key_cost_freight_in', lit(None).cast('string'))
        .withColumn('key_cost_transfer_price', lit(None).cast('string'))
        .withColumn('division', lit(None).cast('string'))
        .withColumn('confirmed date', lit(None).cast('string'))
        .withColumn('plant', lit(None).cast('string'))
        .withColumn('del ic discount/surcharge €', lit(None).cast('double'))
        .withColumn('removable_charges_tot', lit(None).cast('double'))
        .withColumn('booking_slot', lit(None).cast('date'))
        .withColumn('shipping type', lit(None).cast('string'))
        .withColumn('sap_order_block_credit', lit(None).cast('string'))
        .withColumn('key_cost_stdindef_tca', lit(None).cast('string'))
        .withColumn('del qty fabs', lit(None).cast('double'))
        .withColumn('vat_amount_1_tot', lit(None).cast('double'))
        .withColumn('shipping point', lit(None).cast('string'))
        .withColumn('exchange_rate_sap', lit(None).cast('double'))
        .withColumn('net price', lit(None).cast('double'))
        .withColumn('list_price_tot', lit(None).cast('double'))
        .withColumn('forced price', lit(None).cast('double'))
        .withColumn('pstyv', lit(None).cast('string'))
        .withColumn('posnr', lit(None).cast('string'))
        .withColumn('sale_director_code', lit(None).cast('string'))
        .withColumn('moltiplicatore_reso', lit(None).cast('bigint'))
        .withColumn('category', lit(None).cast('string'))
        .withColumn('sap_order_block_delivery', lit(None).cast('string'))
        .withColumn('del ic discount/surcharge lc', lit(None).cast('double'))
        .withColumn('knumv', lit(None).cast('string'))

        .withColumn('data_type', col('data_type').cast('string'))
        .withColumn('doc_type_code', col('doc_type_code').cast('bigint'))
        .withColumn('order_customer_date', col('order_customer_date').cast('timestamp'))
        .withColumn('order_requested_delivery_date', col('order_requested_delivery_date').cast('timestamp'))
        .withColumn('planned_dispatch_date', col('planned_dispatch_date').cast('string'))
        .withColumn('order_blocking_date', col('order_blocking_date').cast('string'))
        .withColumn('order_unblocking_date', col('order_unblocking_date').cast('string'))

        .select(list_sap_cols_del)
).save_data_to_redshift_table("dwa.cas_del_ft_delivery", postactions=postaction_del)

list_dim_cols_del = list(set(DELIVERIES.columns).difference(list_sap_cols_del))+['key_fact']
CAS_DELIVERIES_DIM = (
    DELIVERIES
        .withColumn("key_fact", concat_ws("###", lit("DELIVERY"), col("DESPATCHING_NOTE"),col("order_line")))
        .select(list_dim_cols_del)
).save_data_to_redshift_table("dwa.cas_del_dim_delivery")