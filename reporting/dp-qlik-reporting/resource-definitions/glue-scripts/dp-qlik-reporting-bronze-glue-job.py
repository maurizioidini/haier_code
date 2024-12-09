import boto3
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, monotonically_increasing_id, lit, concat, substring
import pyspark.sql.functions as F
import QlikReportingUtils
from QlikReportingUtils import *
from builtins import max as python_max
from datetime import datetime

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
        "JobRole"
    ],
)

bucket_name = args["BucketName"]
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
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")

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

s3_path  = f"s3://{bucket_name}/{target_flow}"
s3_path += "/{table_name}"

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




# Start aliasing
table_name = "dwa.sap_sls_ft_i_salesdocument_vbak"
VBAK_INPUT = (
    read_data_from_redshift_table(table_name)
    #.where(~col("SalesDocument").isin(["60145106","301834","60101501","60150468","1223836","1480844","2779940","60243961","2722071","2765968","2780299","2845823",]))   # vbeln: orders with bad dates

    .select(
        col("SalesDocumentType").alias("AUART"),
        col("SalesDocumentDate").alias("AUDAT"),
        col("SDDocumentReason").alias("AUGRU"),
        col("CustomerPurchaseOrderType").alias("BSARK"),
        col("BSTNK").alias("BSTNK"),
        col("TotalCreditCheckStatus").alias("CMGST"),
        col("CreatedByUser").alias("ERNAM"),
        col("HeaderBillingBlockReason").alias("FAKSK"),
        col("SalesDocumentCondition").alias("KNUMV"),
        col("DeliveryBlockReason").alias("LIFSK"),
        col("OrganizationDivision").alias("SPART"),
        col("SDDocumentCollectiveNumber").alias("SUBMI"),
        col("SalesDocument").alias("VBELN"),
        col("SDDocumentCategory").alias("VBTYP"),
        col("RequestedDeliveryDate").alias("VDATU"),
        col("SalesOffice").alias("VKBUR"),
        col("SalesOrganization").alias("VKORG"),
        col("ShippingCondition").alias("VSBED"),
        col("DistributionChannel").alias("VTWEG"),
        col("CreationDate").alias("ERDAT")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VBAK_INPUT")
)

table_name = "dwa.sap_del_ft_zww_otc_aws_i_deliverydoc_cust_likp"
LIKP_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("DELIVERYDOCUMENT").alias("VBELN"),
        col("ACTUALGOODSMOVEMENTDATE").alias("WADAT_IST"),
        col("ZZDATE_SLOT").alias("ZZDATE_SLOT"),
        col("SDDOCUMENTCATEGORY").alias("VBTYP"),
        col("DELIVERYDOCUMENTTYPE").alias("LFART"),
        col("PROPOSEDDELIVERYROUTE").alias("ROUTE"),
        col("PICKINGDATE").alias("KODAT"),
        col("DELIVERYDATE").alias("LFDAT"),
        col("ZZBOOK_SLOT").alias("ZZBOOK_SLOT"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="LIKP_INPUT")
)

table_name = "dwa.sap_sls_ft_i_salesdocumentitem_vbap"
VBAP_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("SALESDOCUMENTRJCNREASON").alias("ABGRU"),
        col("ORDERQUANTITY").alias("KWMENG"),
        col("STORAGELOCATION").alias("LGORT"),
        col("MATERIAL").alias("MATNR"),
        col("SALESDOCUMENTITEM").alias("POSNR"),
        col("SALESDOCUMENTITEMCATEGORY").alias("PSTYV"),
        col("ROUTE").alias("ROUTE"),
        col("DIVISION").alias("SPART"),
        col("SALESDOCUMENT").alias("VBELN"),
        col("SHIPPINGPOINT").alias("VSTEL"),
        col("PLANT").alias("WERKS"),
        col("ZZLAREA").alias("ZZLAREA"),
        col("ZZSO_BOOKDATE").alias("ZZSO_BOOKDATE"),
        col("ZZSO_PBSDATE").alias("ZZSO_PBSDATE")
    )
).save_to_s3_parquet(
    path=s3_path.format( table_name="VBAP_INPUT")
)

table_name = "dwa.sap_sls_ft_zww_otc_vbbe"
VBBE_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("ETENR").alias("ETENR"),
        col("LGORT").alias("LGORT"),
        col("POSNR").alias("POSNR"),
        col("VBELN").alias("VBELN"),
        col("VMENG").alias("VMENG"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VBBE_INPUT")
)


table_name = "dwa.sap_del_ft_zww_otc_aws_i_delidocitem_cust_lips"
LIPS_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("ActualDeliveryQuantity").alias("LFIMG"),
        col("DeliveryDocument").alias("VBELN"),
        col("ReferenceSDDocument").alias("VGBEL"),
        col("ReferenceSDDocumentItem").alias("VGPOS"),
        col("DeliveryDocumentItem").alias("POSNR"),
        col("Material").alias("MATNR"),
        col("zzlarea").alias("ZZLAREA"),
        col("SalesOffice").alias("VKBUR"),
        col("StorageLocation").alias("LGORT")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="LIPS_INPUT")
)

#table_name = "dwa.sap_sls_ft_zww_otc_aws_sdcompp_cust_sod_vbpa"
#table_name = "dwa_sales.sales_doc_partner"
VBPA_INPUT = (
    read_data_from_redshift_table("dwa_sales.sales_doc_partner")
    .withColumn("__priority", lit(1))

    .unionByName(
        read_data_from_redshift_table("dwa.sap_sls_ft_zww_otc_aws_sdcompp_cust_sod_vbpa").withColumn("__priority", lit(2)),
        allowMissingColumns=True
    )

    .select(
        col("PartnerFunction").alias("PARVW"),
        col("SDDocument").alias("VBELN"),     # dipende dalla cds view...
        col("SDDocumentItem").alias("POSNR"),
        col("Customer").alias("KUNNR"),
        "__priority"
    )

    .where(col("KUNNR").isNotNull())

    .drop_duplicates_ordered(["PARVW", "VBELN", "POSNR"], ["__priority", "KUNNR"]).drop("__priority")

).save_to_s3_parquet(
    path=s3_path.format(table_name="VBPA_INPUT")
)

table_name = "dwa.sap_bil_ft_i_billingdocumentitem_vbrp"
VBRP_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("SalesDocument").alias("AUBEL"),
        col("SalesDocumentItem").alias("AUPOS"),
        col("BillingQuantity").alias("FKIMG"),
        col("Material").alias("MATNR"),
        col("NetAmount").alias("NETWR"),
        col("BillingDocumentItem").alias("POSNR"),
        col("BillingDocumentItemInPartSgmt").alias("POSPA"),
        col("BillingDocument").alias("VBELN"),
        col("ReferenceSDDocument").alias("VGBEL"),
        col("SalesOffice").alias("VKBUR"),
        col("ReferenceSDDocumentItem").alias("VGPOS")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VBRP_INPUT")
)

table_name = "dwa.sap_sls_ft_zww_otc_i_sddocmultlvlprocflow_vbfa"
VBFA_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("PrecedingDocument").alias("VBELV"),
        col("SubsequentDocument").alias("VBELN"),
        col("SubsequentDocumentCategory").alias("VBTYP_N"),
        col("PrecedingDocumentCategory").alias("VBTYP_V"),
        col("PrecedingDocumentItem").alias("POSNN"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VBFA_INPUT")
)

table_name = "dwa.sap_bil_ft_i_billingdocument_vbrk"
VBRK_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("ACCOUNTINGDOCUMENT").alias("BELNR"),
        col("BILLINGDOCUMENTTYPE").alias("FKART"),
        col("BILLINGDOCUMENTDATE").alias("FKDAT"),
        col("BILLINGDOCUMENTISCANCELLED").alias("FKSTO"),
        col("PRICINGDOCUMENT").alias("KNUMV"),
        col("ACCOUNTINGEXCHANGERATE").alias("KURRF"),
        col("COUNTRY").alias("LAND1"),
        col("CANCELLEDBILLINGDOCUMENT").alias("SFAKN"),
        col("DIVISION").alias("SPART"),
        col("BILLINGDOCUMENT").alias("VBELN"),
        col("SDDOCUMENTCATEGORY").alias("VBTYP"),
        col("SALESORGANIZATION").alias("VKORG"),
        col("DISTRIBUTIONCHANNEL").alias("VTWEG"),
        col("CUSTOMERPAYMENTTERMS").alias("ZTERM"),
        col("accountingpostingstatus").alias("BUCHK")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VBRK_INPUT")
)

table_name = "mtd.sap_dm_i_salesdocumenttypetext_tvakt"
TVAKT_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("SalesDocumentType").alias("AUART"),
        col("SalesDocumentTypeName").alias("BEZEI"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVAKT_INPUT")
)

table_name = "mtd.sap_dm_i_customerpurchaseordertypetxt_t176t"
T176T_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("CustomerPurchaseOrderType").alias("BSARK"),
        col("CustomerPurchaseOrderTypeDesc").alias("VTEXT"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="T176T_INPUT")
)

table_name = "mtd.sap_dm_i_paymenttermsconditionstext_t052u"
T052U_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("PaymentTerms").alias("ZTERM"),
        col("PaymentTermsConditionDesc").alias("TEXT1"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="T052U_INPUT")
)

table_name = "mtd.sap_dm_i_additionalcustomergroup3text_tvv3t"
TVV3T_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("AdditionalCustomerGroup3").alias("KVGR3"),
        col("AdditionalCustomerGroup3Name").alias("BEZEI"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVV3T_INPUT")
)

table_name = "mtd.sap_dm_i_paymentmethoddescription_t042zt"
T042ZT_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("CustomerCountryKey").alias("LAND1"),
        col("PrmtHbPaymentMethod").alias("ZLSCH"),
        col("PaymentMethodName").alias("TEXT2"),
        col("Language").alias("SPRAS"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="T042ZT_INPUT")
)

table_name = "mtd.sap_dm_i_deliverydocumenttypetext_tvlkt"
TVLKT_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("DeliveryDocumentType").alias("LFART"),
        col("DeliveryDocumentTypeName").alias("VTEXT"),

    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVLKT_INPUT")
)

table_name = "mtd.sap_dm_i_shippingtypetext_t173t"
T173T_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("ShippingType").alias("VSART"),
        col("ShippingTypeName").alias("BEZEI"),

    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="T173T_INPUT")
)

table_name = "mtd.sap_dm_zww_cat_i_domainfixedvaluetext_dd07t"
DD07T_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("SAPDataDictionaryDomain").alias("DOMNAME"),   # todo check!!
        col("Language").alias("DDLANGUAGE"),
        col("DomainValue").alias("DOMVALUE_L"),
        col("DomainText").alias("DDTEXT"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="DD07T_INPUT")
)

table_name = "mtd.sap_dm_i_routetext_tvrot"
TVROT_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("Route").alias("ROUTE"),
        col("RouteName").alias("BEZEI"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVROT_INPUT")
)

table_name = "mtd.sap_dm_i_deliveryblockreasontext_tvlst"
TVLST_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("LANGUAGE").alias("SPRAS"),
        col("DELIVERYBLOCKREASON").alias("LIFSP"),
        col("DELIVERYBLOCKREASONTEXT").alias("VTEXT"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVLST_INPUT")
)

table_name = "dwa.sap_sls_ft_zww_otc_logarea_cds"
ZWW_OTC_LOGAREA_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("ZZLAREA").alias("ZZLAREA"),
        col("ZZLAREAT").alias("ZZLAREAT"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="ZWW_OTC_LOGAREA_INPUT")
)

table_name = "mtd.sap_dm_i_supplier_lfa1"
LFA1_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Supplier").alias("LIFNR"),
        col("OrganizationBPName1").alias("NAME1")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="LFA1_INPUT")
)

table_name = "mtd.sap_dm_i_salesdocumenttype_tvak"
TVAK_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("SalesDocumentType").alias("AUART"),
        col("SDDocumentCategory").alias("VBTYP")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVAK_INPUT")
)

table_name = "mtd.sap_dm_i_customer_kna1"
KNA1_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("CUSTOMER").alias("KUNNR"),
        col("ORGANIZATIONBPNAME1").alias("NAME1")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="KNA1_INPUT")
)

table_name = "mtd.sap_dm_i_additionalcustomergroup5text_tvv5t"
TVV5T_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("AdditionalCustomerGroup5").alias("KVGR5"),
        col("AdditionalCustomerGroup5Name").alias("BEZEI")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVV5T_INPUT")
)

table_name ="mtd.sap_dm_zww_ftm_i_paymentterms_t052"
T052_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("PaymentTerms").alias("ZTERM"),
        col("zlsch").alias("ZLSCH"),
        col("zprz1").alias("ZPRZ1")
    )

    .distinct()
).save_to_s3_parquet(
    path=s3_path.format(table_name="T052_INPUT")
)

table_name = "mtd.sap_dm_i_salesgrouptext_tvgrt"
TVGRT_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("Language").alias("SPRAS"),
        col("SalesGroup").alias("VKGRP"),
        col("SalesGroupName").alias("BEZEI")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TVGRT_INPUT")
)

table_name = "mtd.sap_dm_zww_ptp_knvv"
KNVV_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("KUNNR").alias("KUNNR"),
        col("VKORG").alias("VKORG"),
        col("VTWEG").alias("VTWEG"),
        col("SPART").alias("SPART"),
        col("VKBUR").alias("VKBUR"),
        col("KVGR3").alias("KVGR3"),
        col("KVGR5").alias("KVGR5"),
        col("VKGRP").alias("VKGRP")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="KNVV_INPUT")
)

table_name ="dwa.sap_sls_ft_i_salesdocumentscheduleline_vbep"
VBEP_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("CONFDORDERQTYBYMATLAVAILCHECK").alias("BMENG"),
        col("DELIVERYDATE").alias("EDATU"),
        col("EZEIT").alias("EZEIT"),
        col("SALESDOCUMENTITEM").alias("POSNR"),
        col("TRANSPORTATIONPLANNINGDATE").alias("TDDAT"),
        col("SALESDOCUMENT").alias("VBELN"),
        col("SCHEDULELINE").alias("ETENR")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VBEP_INPUT")
)

table_name ="dwa.sap_del_ft_zww_dts_aws_shipment_vttk"
VTTK_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("ShipNum").alias("TKNUM"),
        col("VSART").alias("VSART")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VTTK_INPUT")
)

table_name ="dwa.sap_del_ft_zww_otc_aws_shipment_item_vttp"
VTTP_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("TKNUM").alias("TKNUM"),
        col("TPNUM").alias("TPNUM"),
        col("VBELN").alias("VBELN")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="VTTP_INPUT")
)

table_name ="dwa.sap_dto_ft_zww_cat_aws_i_chdocitem_cust_cdpos"
CDPOS = (
    read_data_from_redshift_table(table_name)
    .select(
        col("ChangeDocObject").alias("OBJECTID"),
        col("ChangeDocument").alias("CHANGENR"),
        col("ChangeDocDatabaseTableField").alias("FNAME"),
        col("ChangeDocNewFieldValue").alias("VALUE_NEW"),
        col("ChangeDocPreviousFieldValue").alias("VALUE_OLD"),
        col("ChangeDocObjectClass").alias("OBJECTCLAS"),
        col("DatabaseTable").alias("TABNAME"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="CDPOS")
)

table_name ="dwa.sap_dto_ft_i_dfs_orglmeasurechangedoc_cdhdr"
CDHDR = (
    read_data_from_redshift_table(table_name)

    .select(
        col("CreatedByUser").alias("USERNAME"),
        col("CreationDate").alias("UDATE"),
        col("CreationTime").alias("UTIME"),
        col("ChangeDocObjectClass").alias("OBJECTCLAS"),
        col("ChangeDocObject").alias("OBJECTID"),
        col("ChangeDocument").alias("CHANGENR"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="CDHDR")
)

table_name = "mtd.sap_dm_i_companycode_t001"
COMM_COMPANIES_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("companycode").alias("COMPANY_ID"),      # todo
        col("currency").alias("CURRENCY")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="COMM_COMPANIES_INPUT")
)

# OneOff-Ingestion da S3
# manual ingestion

table_name ="man.qlk_fin_cutoff_date"
CUTOFF_DATE_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("CUTOFF_DATE").alias("CUTOFF_DATE"),
        col("MONTH").alias("MONTH"),
    )
    .withColumn("CUTOFF_DATE", to_date("CUTOFF_DATE", "dd/MM/yyyy"))
    .withColumn("MONTH", to_date("MONTH", "MMM-yy"))
).save_to_s3_parquet(
    path=s3_path.format(table_name="CUTOFF_DATE_INPUT")
)

table_name ="man.qlk_sls_mapping_return_reasons"
MAPPING_RETURN_REASONS_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("SAP Order Reason").alias("SAP Order Reason"),
        col("Return Reason").alias("Return Reason"),
        col("Causes").alias("Causes"),
        col("Return Reason Description").alias("Return Reason Description"),
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="MAPPING_RETURN_REASONS_INPUT")
)

table_name ="man.qlk_sls_mapping_sales_office_network_uk"
Mapping_SalesOffice_UK = (
    read_data_from_redshift_table(table_name)
    .withColumn("Flag_UK", lit(1))
    .select("sales_office")
    .dropDuplicates(["sales_office"])
).save_to_s3_parquet(
    path=s3_path.format(table_name="Mapping_SalesOffice_UK")
)

table_name = "mtd.sie_dm_product"
LINE_INPUT = (
    read_data_from_redshift_table(table_name)
    .select(
        col("product_code").alias("PRODUCT_CODE"),
        substring(col("product_category_code"), 3, 2).alias("LINE_CODE")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="LINE_INPUT")
)

############# Currency_INPUT #################à

tcurr = (
    read_data_from_redshift_table("mtd.sap_dm_i_exchangeraterawdata_tcurr")

    .where(col("targetcurrency")== 'EUR')
    .where(col("exchangeratetype")=='EURX')
    .where(col("validitystartdate") >= lit("1970-01-01"))
    .select(
        col("validitystartdate").alias("EXCHANGE_DATE"),
        col("exchangerate").alias("EXCHANGE_VALUE"),
        col("exchangeratetype").alias("TYPE"),
        col("sourcecurrency").alias("CURRENCY"),
    )

    .where(col("EXCHANGE_VALUE").isNotNull())
)
min_max_dates = tcurr.select(min("EXCHANGE_DATE").alias("min_date"), max("EXCHANGE_DATE").alias("max_date")).collect()
min_date = min_max_dates[0]["min_date"].date()
max_date = python_max(min_max_dates[0]["max_date"].date(), datetime.today().date())

date_df = spark.createDataFrame(
    [(min_date, max_date)],
    ["start_date", "end_date"]
).select(
    explode(expr("sequence(start_date, end_date, interval 1 day)")).alias("EXCHANGE_DATE")
)
print("tcurr min_date:", min_date, "max_date:", max_date)

currencies = tcurr.select("CURRENCY").distinct()

# 4. Effettua un prodotto cartesiano tra le date generate e le valute
date_currency_df = date_df.crossJoin(currencies)

tcurr_full = date_currency_df.join(tcurr, on=["EXCHANGE_DATE", "CURRENCY"], how="left").orderBy("EXCHANGE_DATE")

window_spec = Window.partitionBy("CURRENCY").orderBy("EXCHANGE_DATE").rowsBetween(Window.unboundedPreceding, 0)

# Usa la funzione last() per riempire i valori NULL con l'ultimo disponibile
tcurr_full = tcurr_full.withColumn(
    "EXCHANGE_VALUE",
    last("EXCHANGE_VALUE", ignorenulls=True).over(window_spec)
).withColumn(
    "TYPE",
    last("TYPE", ignorenulls=True).over(window_spec)
)

Currency_INPUT = (
    #spark.createDataFrame([], schema="INTERNATIONAL_SIGN string, EXCHANGE_DATE string, EXCHANGE_VALUE string, TYPE string, CURRENCY string")
    tcurr_full

    .withColumn("EXCHANGE_VALUE",
        when(col("EXCHANGE_VALUE") < 0, 1 / abs("EXCHANGE_VALUE"))
        .otherwise(col("EXCHANGE_VALUE"))
    )

    .select(
        #col("INTERNATIONAL_SIGN").alias("INTERNATIONAL_SIGN"),
        col("EXCHANGE_DATE").alias("EXCHANGE_DATE"),
        col("EXCHANGE_VALUE").alias("EXCHANGE_VALUE"),
        col("TYPE").alias("TYPE"),
        col("CURRENCY").alias("CURRENCY")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="Currency_INPUT")
)

TASSI_MODIFICATI_INPUT = (
    spark.createDataFrame([], schema="Company_Code string, Billing_Document string, Exchange_Rate_Accounting_Data string")
    .select(
        col("Company_Code").alias("Company_Code"),
        col("Billing_Document").alias("Billing_Document"),
        col("Exchange_Rate_Accounting_Data").alias("Exchange_Rate_Accounting_Data")
    )
).save_to_s3_parquet(
    path=s3_path.format(table_name="TASSI_MODIFICATI_INPUT")
)

prcd_header_deco = (
    read_data_from_redshift_table("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_header_prcd_elements")

    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")

    .withColumn("SOURCE_TABLE", lit("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_header_prcd_elements"))
    .withColumn("__priority", lit(1))
).localCheckpoint()
prcd_item_deco =(
    read_data_from_redshift_table("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_item_prcd_elements")

    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")

    .withColumn("SOURCE_TABLE", lit("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_item_prcd_elements"))
    .withColumn("__priority", lit(2))
).localCheckpoint()
prcd_inv_deco =(
    read_data_from_redshift_table("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_inv_prcd_elements")

    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")

    .withColumn("SOURCE_TABLE", lit("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_inv_prcd_elements"))
    .withColumn("__priority", lit(3))
).localCheckpoint()
prcd_bd_deco =(
    read_data_from_redshift_table("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_bd_prcd_elements")

    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")

    .withColumn("SOURCE_TABLE", lit("dwa.sap_sls_ft_zww_otc_v_konv_cust_so_bd_prcd_elements"))
    .withColumn("__priority", lit(4))
).localCheckpoint()

prcd_header = (
    read_data_from_redshift_table("datalake_ods.sap_zww_otc_v_konv_cust_so_header")
    
    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")

    .withColumn("SOURCE_TABLE", lit("datalake_ods.sap_zww_otc_v_konv_cust_so_header"))
    .withColumn("__priority", lit(5))
).localCheckpoint()
prcd_item =(
    read_data_from_redshift_table("datalake_ods.sap_zww_otc_v_konv_cust_so_item")
    
    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")

    .withColumn("SOURCE_TABLE", lit("datalake_ods.sap_zww_otc_v_konv_cust_so_item"))
    .withColumn("__priority", lit(6))
).localCheckpoint()
prcd_inv =(
    read_data_from_redshift_table("datalake_ods.sap_zww_otc_v_konv_cust_inv")
    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))
    
    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")
    
    .withColumn("SOURCE_TABLE", lit("datalake_ods.sap_zww_otc_v_konv_cust_inv"))
    .withColumn("__priority", lit(7))
).localCheckpoint()
prcd_bd =(
    read_data_from_redshift_table("datalake_ods.sap_zww_otc_v_konv_cust_bd")
    .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))
    
    .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
    .select("KNUMV", "KPOSN", "STUNR", "ZAEHK", "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")
    
    .withColumn("SOURCE_TABLE", lit("datalake_ods.sap_zww_otc_v_konv_cust_bd"))
    .withColumn("__priority", lit(8))
).localCheckpoint()

# MODIFICA 14/11/2024 PERCHE MARIA HA DETTO CHE NON CI SERVE
# prcd_po =(
#     read_data_from_redshift_table("datalake_ods.sap_zww_otc_v_konv_cust_po")
#     .where(col("KSCHL").isin(['RENT', 'PPNT', 'ZGSV', 'MWST', 'ZNSV', 'ZIV1', 'ZIV2', 'ZCDS', 'ZWEE', 'ZTTV', 'ZIV1', 'ZIV2', 'ZNET', 'PB00', 'ZFEE']))
#     .drop_duplicates_ordered(["KNUMV", "KPOSN", "STUNR", "ZAEHK"], [col("deltadate").desc()])
#     .select("KNUMV", "KPOSN", "STUNR", "ZAEHK",
#                 "KSCHL", "KWERT", "WAERS", "KBETR", "KKURS", "MWSK1")
#     .withColumn("SOURCE_TABLE", lit("datalake_ods.sap_zww_otc_v_konv_cust_po"))
# )


PRCD_ELEMENTS_INPUT = (
    prcd_header
    .unionByName(prcd_item, allowMissingColumns=True)
    .unionByName(prcd_inv, allowMissingColumns=True)
    .unionByName(prcd_bd, allowMissingColumns=True)
    
    .unionByName(prcd_header_deco, allowMissingColumns=True)
    .unionByName(prcd_item_deco, allowMissingColumns=True)
    .unionByName(prcd_inv_deco, allowMissingColumns=True)
    .unionByName(prcd_bd_deco, allowMissingColumns=True)
    
    # .unionByName(prcd_po, allowMissingColumns=True)

    .drop_duplicates_ordered(["KNUMV", "KPOSN", "KSCHL"], [col("ZAEHK").desc(), col("__priority").asc()])
    .drop("__priority")
).save_to_s3_parquet(
    path=s3_path.format(table_name="PRCD_ELEMENTS_INPUT"),
)

'''
TASSI_MODIFICATI_MAP = spark.createDataFrame([],
    schema="KNUMV string, Exchange_Rate_Accounting_Data string"
).save_to_s3_parquet(
    path=s3_path.format(table_name="TASSI_MODIFICATI_MAP")
)'''


# modifica colonne company e network per mapping uk
# 13/11/2024
# V2 (ale)
mrkt_hier = (
    read_data_from_redshift_table("dwe.qlk_sls_otc_mrkt_hier")

    .select(
        col("sales org").alias("SALES_ORGANIZATION"),
        col("sales office").alias("SALES_OFFICE"),
        col("us_company").alias("COMPANY"),
        col("us_network").alias("NETWORK"),
        col("region code").alias("RESPONSIBLE_CODE1"),
        col("region description").alias("RESPONSIBLE_DESCR1"),
        col("subcluster code").alias("RESPONSIBLE_CODE3"),
        col("subcluster description").alias("RESPONSIBLE_DESCR3"),
        col("cluster code").alias("RESPONSIBLE_CODE"),
        col("cluster description").alias("RESPONSIBLE_DESCR"),
        col("market code").alias("MARKET_BUDGET"),
        col("market description").alias("MARKET_BUDGET_DESCR")
    )
    .withColumn("SALES_ORGANIZATION", regexp_replace("SALES_ORGANIZATION", "[^a-zA-Z0-9]", ""))
    .withColumn("SALES_OFFICE", regexp_replace("SALES_OFFICE", "[^a-zA-Z0-9]", ""))
    .withColumn("COMPANY", regexp_replace("COMPANY", "[^a-zA-Z0-9]", ""))
    .withColumn("NETWORK", regexp_replace("NETWORK", "[^a-zA-Z0-9]", ""))
    .withColumn("MARKET_BUDGET", regexp_replace("MARKET_BUDGET", "^M_", ""))
    .withColumn("RESPONSIBLE_CODE1", regexp_replace("RESPONSIBLE_CODE1", "^R_", ""))
    .withColumn("RESPONSIBLE_CODE", regexp_replace("RESPONSIBLE_CODE", "^C_", ""))
    .withColumn("RESPONSIBLE_CODE3", regexp_replace("RESPONSIBLE_CODE3", "^S_", ""))

    .withColumn("SALES_ORGANIZATION", when(col("SALES_ORGANIZATION") == "", lit(None)).otherwise(col("SALES_ORGANIZATION")))
    .withColumn("SALES_OFFICE", when(col("SALES_OFFICE") == "", lit(None)).otherwise(col("SALES_OFFICE")))
    .withColumn("COMPANY", when(col("COMPANY") == "", lit(None)).otherwise(col("COMPANY")))
    .withColumn("NETWORK", when(col("NETWORK") == "", lit(None)).otherwise(col("NETWORK")))

    .withColumn("COMPANY", lpad("COMPANY", 2, "0"))    # inserisci zero davanti a codici una cifra
    .withColumn("NETWORK", lpad("NETWORK", 2, "0"))

    .distinct()

    # attenzione no chiave primaria!!
)

uk_data = (
    mrkt_hier

    .where(col("COMPANY") == "80")

    .drop("SALES_ORGANIZATION", "SALES_OFFICE").distinct()

    .assert_no_duplicates("COMPANY", "NETWORK")
)

mapping_mrkt_hier_uk = (
    read_data_from_redshift_table("man.qlk_sls_mapping_sales_office_network_uk")

    .select(
        lit("66J0").alias("SALES_ORGANIZATION"),
        col("SALES_OFFICE").alias("SALES_OFFICE"),
        col("LINE").alias("LINE"),
        lit("80").alias("COMPANY"),
        col("NETWORK").alias("NETWORK"),
    )

    .withColumn("COMPANY", lpad("COMPANY", 2, "0"))    # inserisci zero davanti a codici una cifra
    .withColumn("NETWORK", lpad("NETWORK", 2, "0"))

    .drop_duplicates_ordered(["SALES_ORGANIZATION", "SALES_OFFICE", "LINE"], [col("COMPANY").isNull(), col("NETWORK").isNull(), "COMPANY", "NETWORK"])

    .assert_no_duplicates("SALES_ORGANIZATION", "SALES_OFFICE", "LINE")

    .join(uk_data, ["COMPANY", "NETWORK"], "left")    # leftanti non torna righe, quindi matchano tutte :)

    .assert_no_duplicates("SALES_ORGANIZATION", "SALES_OFFICE", "LINE")
)

mapping_mrkt_hier_no_uk = (
    mrkt_hier

    .join(mapping_mrkt_hier_uk, ["SALES_OFFICE", "SALES_ORGANIZATION"], "leftanti")

    .withColumn("LINE", lit(None))

    .drop_duplicates_ordered(["SALES_ORGANIZATION", "SALES_OFFICE", "LINE"], [col("COMPANY").isNull(), col("NETWORK").isNull(), "COMPANY", "NETWORK"])

    .assert_no_duplicates("SALES_ORGANIZATION", "SALES_OFFICE", "LINE")
)

mapping_mrkt_hier = (
    mapping_mrkt_hier_no_uk
    .unionByName(mapping_mrkt_hier_uk)

    .select(
        'SALES_ORGANIZATION', 'SALES_OFFICE', 'LINE',
        'COMPANY', 'NETWORK',
        'RESPONSIBLE_CODE', 'RESPONSIBLE_DESCR', 'RESPONSIBLE_CODE1', 'RESPONSIBLE_DESCR1', 'RESPONSIBLE_CODE3', 'RESPONSIBLE_DESCR3',
        'MARKET_BUDGET', 'MARKET_BUDGET_DESCR'
    )

    .assert_no_duplicates("SALES_ORGANIZATION", "SALES_OFFICE", "LINE")
).save_data_to_redshift_table("dwe.qlk_sls_otc_mrkt_hier_uk")
