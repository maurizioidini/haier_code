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
setattr(DataFrame, "save_to_s3_parquet", save_to_s3_parquet)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


###### Read Gs

gs_inv = read_data_from_redshift_table("dwe.qlk_bil_gs_invoice")
gs_ord = read_data_from_redshift_table("dwe.qlk_sls_gs_order")
gs_del = read_data_from_redshift_table("dwe.qlk_del_gs_delivery")

###### Read dims SAP

dim_ord_sap = read_data_from_redshift_table("dwa.sap_sls_dim_order")
dim_ord_cas = read_data_from_redshift_table("dwa.cas_sls_dim_order")

dim_del_sap = read_data_from_redshift_table("dwa.sap_del_dim_delivery")
dim_del_cas = read_data_from_redshift_table("dwa.cas_del_dim_delivery")

dim_inv_sap = read_data_from_redshift_table("dwa.sap_bil_dim_invoice")
dim_inv_cas = read_data_from_redshift_table("dwa.cas_bil_dim_invoice")

###### Join INV

cols_sap = set(dim_inv_sap.columns)
cols_cas = set(dim_inv_cas.columns)
duplicate_cols = cols_cas.intersection(cols_sap) - {"key_fact"}

list_old_inv_cols = ['dest_id', 'inv_id', 'company', 'network', 'comm_organization', 'area_code', 'area_descr', 'doc_type_code', 'doc_type_descr', 'document_code', 'ref_invoice', 'ref_invoice_date', 'payment_type_code', 'payment_type_descr', 'payment_expiring_code', 'payment_expiring_disc', 'payment_expiring_descr', 'currency_euro_exch', 'doc_total_amount', 'vat_reason_code', 'vat_percentage_1', 'vat_amount_1', 'despatching_note', 'despatching_date', 'order_number', 'ref_despatch_note', 'ref_despatch_date', 'order_type_code', 'order_type_descr', 'shipment_code', 'shipment_descr', 'order_line', 'list_price', 'price_forced', 'forced_price', 'net_price', 'total_amount', 'comm_cost_of_sales', 'cons_cost_of_sales', 'transport_cost', 'transport_cost_infra', 'flag_bgrade', 'removable_charges', 'return_reason', 'data_type', 'order_delivery_warehouse', 'order_date', 'order_blocking_user', 'order_unblocking_date', 'order_unblocking_user', 'order_customer_reference_1', 'order_customer_date', 'order_requested_delivery_date', 'order_creation_user', 'order_blocking_code', 'order_blocking_date', 'responsible_code', 'responsible_descr', 'ref_order_number', 'ref_order_line', 'conf_del_date', 'routing', 'routing_descr', 'picking_date', 'picking_number', 'picking_carrier', 'picking_date_r', 'picking_carrier_descr', 'responsible_code1', 'responsible_descr1', 'order_blocking_descr', 'responsible_code3', 'responsible_descr3', 'appointment', 'order_source', 'order_source_descr', 'planned_dispatch_date', 'warehouse_classification', 'booking_status', 'tca', 'first_req_del_date', 'sales_organization', 'sales_office', 'product_code', 'time_id', 'time_id_fact', 'document_date', 'currency_code', 'order number', 'ord_requested_delivery_date', 'mrdd', 'a/b_grade', 'type', 'pot_type', 'potential status', 'inv qty', 'inv nsv €', 'removable_charges €', 'inv gsv €', 'inv gsv + transport €', 'inv gsv promotional & bonus €', 'inv gsv promotional & bonus lc', 'inv cp €', 'inv tp €', '__prz_budget_mapping', 'inv gm cp €', 'inv gm tp €', 'order status', 'order_requested_delivery_month_year', 'pot qty', 'pot nsv €', 'pot gsv €', 'pot gm cp €', 'pot gm tp €', 'inv + del qty', 'inv + del nsv €', 'inv + del gsv €', 'inv + del gm cp €', 'inv + del gm tp €', 'order blocking', 'blocking type', 'inv nsv lc', 'inv gsv lc', 'inv gsv + transport lc', 'inv cp lc', 'inv tp lc', 'inv gm cp lc', 'inv gm tp lc', 'pot nsv lc', 'pot gsv lc', 'pot gm cp lc', 'pot gm tp lc', 'inv + del nsv lc', 'inv + del gsv lc', 'inv + del gm cp lc', 'inv + del gm tp lc', 'bdg unit nsv €', 'inv bdg nsv €', 'transport cost lc', 'transport cost infra lc', 'transport cost €', 'transport cost infra €', 'price variance bdg €', 'purchase_group_code', 'purchase_group_descr', 'super_group_code', 'super_group_descr', 'main_group_code', 'main_group_descr', 'customer_type_code', 'customer_type_descr', 'dest_customer_type_code', 'dest_customer_type_descr', 'sale_zone_code', 'sale_zone_descr', 'salesman_code', 'salesman_name', '__cost_stdindef_tca_key', '__cost_transfer_price_key', '__cost_freight_in_key', '__cost_commercial_key', 'land1', 'posnr', 'k_price_condition', 'cancelled_invoice', 'pospa', 'cancelled', 'fiscal document', 'distr_channel', 'division', 'kurrf', 'invoice_type', 'knumv', 'doc_type_code_single', 'moltiplicatore_reso', 'old perimeter qlik', 'matnr', 'inv qty fabs', 'sales_office_vbrp', 'order_reason', 'sap_order_block_delivery', 'sap_order_block_credit', 'sap_order_block_delivery_txt', 'category', 'collective_number', 'plant', 'shipping point', 'sap_division', 'pstyv', 'confirmed date', 'confirmed time', 'k_ship_pos', 'k_ship_header', 'k_sold_pos', 'k_sold_header', 'k_purchase_pos', 'k_purchase_header', 'k_main_pos', 'k_main_header', 'k_super_pos', 'k_super_header', 'k_sale_zone_pos', 'k_sale_zone_header', 'k_sale_man_pos', 'k_sale_man_header', 'k_sale_director_pos', 'k_sale_director_header', 'k_rsm_pos', 'k_rsm_header', 'doc_type_code_concatenate', 'sale_director_code', 'rsm_code', 'doc_type_code_orig', 'sales_office_knvv', 'inv_customer_key', 'dest_customer_key', 'sales_group_key', 'customer_type_code_lev3', 'dest_customer_type_code_lev3', 'sales group', '__line_code', 'market_budget', 'market_budget_descr', 'flag_cancelled_invoice_valid', 'doc_type_code_cancelled', 'exchange', 'vat_amount_1_tot', 'inv nsv lc temp', 'exchange_rate_sap', 'inv ic discount/surcharge', 'inv gsv lc temp', 'inv gsv promo bonus lc temp', 'inv transport cost lc temp', 'inv raee lc', 'list_price_tot', 'forced_price_tot', 'removable_charges lc', 'booking_slot', 'shipping type', '__yq', 'std_prod_cost_commercial_unit', 'transport_cost_unit', 'transport_cost_infra_unit', 'comm_cost_of_sales_unit', 'tp_itcy_unit', 'cons_cost_of_sales_unit', 'tca_unit', 'return_reason_descr', 'flag sap', 'inv raee €', 'inv transport cost lc', 'inv transport cost €', 'sale_director', 'rsm', 'exchange_to_local', '__product_code_mapping_key', 'cas_sap']


df_inv = (
    gs_inv
        .withColumn("__PRODUCT_CODE_MAPPING_KEY", concat_ws('-@-', col("sales_organization"), col("SALES_OFFICE"), col("product_code"), year("DOCUMENT_DATE"), month("DOCUMENT_DATE")))
        .withColumn("cas_sap",col("source"))
        .join(dim_inv_sap, ['key_fact'], 'left')
        .join(dim_inv_cas.drop(*duplicate_cols), ['key_fact'], 'left')
        .withColumnRenamed("key_cost_stdindef_tca","__cost_stdindef_tca_key")
        .withColumnRenamed("key_cost_transfer_price","__cost_transfer_price_key")
        .withColumnRenamed("key_cost_freight_in","__cost_freight_in_key")
        .withColumnRenamed("key_cost_commercial","__cost_commercial_key")
        .withColumnRenamed("removable charges tot","removable_charges_tot")
        .withColumnRenamed("removable charges lc","removable_charges lc")
        .withColumnRenamed("list price tot","list_price_tot")
        .select(list_old_inv_cols)
        .distinct()
).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_invoice_gs")


###### Join ORD

cols_sap = set(dim_ord_sap.columns)
cols_cas = set(dim_ord_cas.columns)
duplicate_cols = cols_cas.intersection(cols_sap) - {"key_fact"}

list_old_ord_cols = ['dest_id', 'inv_id', 'company', 'network', 'area_code', 'area_descr', 'doc_type_code', 'doc_type_descr', 'currency_euro_exch', 'vat_reason_code', 'vat_percentage_1', 'order_number', 'order_type_code', 'order_type_descr', 'order_line', 'list_price', 'price_forced', 'forced_price', 'net_price', 'transport_cost_infra', 'flag_bgrade', 'removable_charges', 'return_reason', 'data_type', 'order_delivery_warehouse', 'order_date', 'order_blocking_user', 'order_unblocking_date', 'order_unblocking_user', 'order_customer_reference_1', 'order_customer_date', 'order_requested_delivery_date', 'order_creation_user', 'order_blocking_code', 'order_blocking_date', 'responsible_code', 'responsible_descr', 'freight_in', 'routing', 'routing_descr', 'responsible_code1', 'responsible_descr1', 'order_blocking_descr', 'responsible_code3', 'responsible_descr3', 'appointment', 'order_source', 'order_source_descr', 'planned_dispatch_date', 'warehouse_classification', 'tca', 'first_req_del_date', 'sales_organization', 'sales_office', 'product_code', 'time_id', 'time_id_fact', 'document_date', 'currency_code', 'ord_requested_delivery_date', 'mrdd', 'allocation warehouse', 'a/b_grade', 'potential status', '__prz_budget_mapping', 'ord qty', 'ord nsv €', 'ord gsv €', 'ord cp €', 'ord tp €', 'ord gm cp €', 'ord gm tp €', 'order status', 'order_requested_delivery_month_year', 'pot qty', 'pot nsv €', 'pot gsv €', 'pot gm cp €', 'pot gm tp €', 'ord bo qty', 'ord bo nsv €', 'ord bo gsv €', 'ord bo gm cp €', 'ord bo gm tp €', 'blk qty', 'blk nsv €', 'blk gsv €', 'blk gm cp €', 'blk gm tp €', 'pck qty', 'pck nsv €', 'pck gsv €', 'pck gm cp €', 'pck gm tp €', 'order blocking', 'blocking type', 'ond qty', 'ond nsv €', 'ond gsv €', 'ond gm cp €', 'ond gm tp €', 'ord nsv lc', 'ord gsv lc', 'ord cp lc', 'ord tp lc', 'ord gm cp lc', 'ord gm tp lc', 'pot nsv lc', 'pot gsv lc', 'pot gm cp lc', 'pot gm tp lc', 'ord bo nsv lc', 'ord bo gsv lc', 'ord bo gm cp lc', 'ord bo gm tp lc', 'blk nsv lc', 'blk gsv lc', 'blk gm cp lc', 'blk gm tp lc', 'pck nsv lc', 'pck gsv lc', 'pck gm cp lc', 'pck gm tp lc', 'ond nsv lc', 'ond gsv lc', 'ond gm cp lc', 'ond gm tp lc', 'bdg unit nsv €', 'purchase_group_code', 'purchase_group_descr', 'super_group_code', 'super_group_descr', 'main_group_code', 'main_group_descr', 'customer_type_code', 'customer_type_descr', 'dest_customer_type_code', 'dest_customer_type_descr', 'sale_zone_code', 'sale_zone_descr', 'salesman_code', 'salesman_name', '__cost_stdindef_tca_key', '__cost_transfer_price_key', '__cost_freight_in_key', '__cost_commercial_key', 'k_price_condition', 'ord qty orig', 'ord qty fabs', 'booking_slot_likp', 'confirmed date', 'confirmed time', '% ord qty fabs', 'document_date_original', 'return_reason_descr', 'distr_channel', 'division', 'sap_order_block_delivery', '__faksk', 'sap_order_block_credit', 'sap_order_block_delivery_txt', 'knumv', 'moltiplicatore_reso', 'category', 'old perimeter qlik', 'plant', 'shipping_point', 'pstyv', 'booking_slot_vbap', '__line_code', 'market_budget', 'market_budget_descr', 'k_ship_pos', 'k_ship_header', 'k_sold_pos', 'k_sold_header', 'k_purchase_pos', 'k_purchase_header', 'k_main_pos', 'k_main_header', 'k_super_pos', 'k_super_header', 'k_sale_zone_pos', 'k_sale_zone_header', 'k_sale_man_pos', 'k_sale_man_header', 'k_sale_director_pos', 'k_sale_director_header', 'k_rsm_pos', 'k_rsm_header', '% ord qty', 'ord qty consignment', 'sale_director_code', 'rsm_code', 'exchange', 'vat_amount_1_tot', 'ord nsv lc tot', 'exchange_rate_sap', 'ord ic discount/surcharge', 'ord raee lc', 'ord gsv lc tot', 'list_price_tot', 'forced_price_tot', 'removable_charges_tot', '__yq', 'std_prod_cost_commercial_unit', 'transport_cost_unit', 'transport_cost_infra_unit', 'comm_cost_of_sales_unit', 'tp_itcy_unit', 'cons_cost_of_sales_unit', 'tca_unit', 'inv_customer_key', 'dest_customer_key', 'sales_group_key', 'customer_type_code_lev3', 'dest_customer_type_code_lev3', 'sales group', 'std_prod_cost_no_tca', 'tp_commerciale', 'sale_director', 'rsm', 'booking_slot', 'flag sap', 'shipping point', 'exchange_to_local', '__product_code_mapping_key', 'cas_sap']

df_ord = (
    gs_ord
        .withColumn("__PRODUCT_CODE_MAPPING_KEY", concat_ws('-@-', col("sales_organization"), col("SALES_OFFICE"), col("product_code"), year("DOCUMENT_DATE"), month("DOCUMENT_DATE")))
        .withColumn("cas_sap",col("source"))
        .join(dim_ord_sap, ['key_fact'], 'left')
        .join(dim_ord_cas.drop(*duplicate_cols), ['key_fact'], 'left')
        .withColumnRenamed("key_cost_stdindef_tca","__cost_stdindef_tca_key")
        .withColumnRenamed("key_cost_transfer_price","__cost_transfer_price_key")
        .withColumnRenamed("key_cost_freight_in","__cost_freight_in_key")
        .withColumnRenamed("key_cost_commercial","__cost_commercial_key")
        .withColumnRenamed("removable charges tot","removable_charges_tot")
        .select(list_old_ord_cols)
        .distinct()
).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_orders_gs")

###### Join DEL

cols_sap = set(dim_del_sap.columns)
cols_cas = set(dim_del_cas.columns)
duplicate_cols = cols_cas.intersection(cols_sap) - {"key_fact"}

list_old_del_cols = ['dest_id', 'inv_id', 'company', 'network', 'area_code', 'area_descr', 'doc_type_code', 'doc_type_descr', 'currency_euro_exch', 'vat_reason_code', 'vat_percentage_1', 'despatching_note', 'despatching_date', 'order_number', 'order_type_code', 'order_type_descr', 'shipment_code', 'shipment_descr', 'order_line', 'list_price', 'price_forced', 'forced_price', 'net_price', 'comm_cost_of_sales', 'cons_cost_of_sales', 'transport_cost', 'transport_cost_infra', 'flag_bgrade', 'removable_charges', 'return_reason', 'data_type', 'order_delivery_warehouse', 'order_date', 'order_blocking_user', 'order_unblocking_date', 'order_unblocking_user', 'order_customer_reference_1', 'order_customer_date', 'order_requested_delivery_date', 'order_creation_user', 'order_blocking_code', 'order_blocking_date', 'responsible_code', 'responsible_descr', 'conf_del_date', 'routing', 'routing_descr', 'picking_date', 'picking_number', 'picking_carrier', 'picking_date_r', 'picking_carrier_descr', 'responsible_code1', 'responsible_descr1', 'order_blocking_descr', 'responsible_code3', 'responsible_descr3', 'appointment', 'order_source', 'planned_dispatch_date', 'warehouse_classification', 'booking_status', 'tca', 'first_req_del_date', 'sales_organization', 'sales_office', 'product_code', 'time_id', 'time_id_fact', 'document_date', 'currency_code', 'order number', 'mrdd', 'allocation warehouse', 'a/b_grade', 'type', 'pot_type', 'potential status', 'del qty', '__prz_budget_mapping', 'del nsv €', 'del gsv €', 'del cp €', 'del tp €', 'del gm cp €', 'del gm tp €', 'order status', 'order_requested_delivery_month_year', 'pot qty', 'pot nsv €', 'pot gsv €', 'pot gm cp €', 'pot gm tp €', 'inv + del qty', 'inv + del nsv €', 'inv + del gsv €', 'inv + del gm cp €', 'inv + del gm tp €', 'order blocking', 'blocking type', 'del nsv lc', 'del gsv lc', 'del cp lc', 'del tp lc', 'del gm cp lc', 'del gm tp lc', 'pot nsv lc', 'pot gsv lc', 'pot gm cp lc', 'pot gm tp lc', 'inv + del nsv lc', 'inv + del gsv lc', 'inv + del gm cp lc', 'inv + del gm tp lc', 'bdg unit nsv €', 'purchase_group_code', 'purchase_group_descr', 'super_group_code', 'super_group_descr', 'main_group_code', 'main_group_descr', 'customer_type_code', 'customer_type_descr', 'dest_customer_type_code', 'dest_customer_type_descr', 'sale_zone_code', 'sale_zone_descr', 'salesman_code', 'salesman_name', '__cost_stdindef_tca_key', '__cost_transfer_price_key', '__cost_freight_in_key', '__cost_commercial_key', 'k_price_condition', 'posnr', 'document_date_original', 'booking_slot', 'shipping type', 'original del qty', 'del qty fabs', '% del qty fabs', 'return_reason_descr', 'sap_order_block_delivery', 'sap_order_block_credit', 'sap_order_block_delivery_txt', 'knumv', 'division', 'distr_channel', 'moltiplicatore_reso', 'category', 'old perimeter qlik', '__line_code', 'market_budget', 'market_budget_descr', 'plant', 'shipping point', 'ord qty orig', 'pstyv', 'confirmed date', 'confirmed time', 'k_ship_pos', 'k_ship_header', 'k_sold_pos', 'k_sold_header', 'k_purchase_pos', 'k_purchase_header', 'k_main_pos', 'k_main_header', 'k_super_pos', 'k_super_header', 'k_sale_zone_pos', 'k_sale_zone_header', 'k_sale_man_pos', 'k_sale_man_header', 'k_sale_director_pos', 'k_sale_director_header', 'k_rsm_pos', 'k_rsm_header', 'del qty consignment', '% del qty', 'sale_director_code', 'rsm_code', 'exchange', 'vat_amount_1_tot', 'del nsv lc tot', 'exchange_rate_sap', 'del ic discount/surcharge', 'del raee lc', 'del gsv lc tot', 'list_price_tot', 'forced_price_tot', 'removable_charges_tot', '__yq', 'std_prod_cost_commercial_unit', 'transport_cost_unit', 'transport_cost_infra_unit', 'comm_cost_of_sales_unit', 'tp_itcy_unit', 'cons_cost_of_sales_unit', 'tca_unit', 'inv_customer_key', 'dest_customer_key', 'sales_group_key', 'customer_type_code_lev3', 'dest_customer_type_code_lev3', 'sales group', 'sale_director', 'rsm', 'flag sap', 'exchange_to_local', '__product_code_mapping_key', 'cas_sap']


df_del = (
    gs_del
        .withColumn("__PRODUCT_CODE_MAPPING_KEY", concat_ws('-@-', col("sales_organization"), col("SALES_OFFICE"), col("product_code"), year("DOCUMENT_DATE"), month("DOCUMENT_DATE")))
        .withColumn("cas_sap",col("source"))
        .join(dim_del_sap, ['key_fact'], 'left')
        .join(dim_del_cas.drop(*duplicate_cols), ['key_fact'], 'left')
        .withColumnRenamed("key_cost_stdindef_tca","__cost_stdindef_tca_key")
        .withColumnRenamed("key_cost_transfer_price","__cost_transfer_price_key")
        .withColumnRenamed("key_cost_freight_in","__cost_freight_in_key")
        .withColumnRenamed("key_cost_commercial","__cost_commercial_key")
        .withColumnRenamed("removable charges tot","removable_charges_tot")
        .withColumnRenamed("removable charges lc","removable_charges lc")
        .withColumnRenamed("list price tot","list_price_tot")
        .select(list_old_del_cols)
        .distinct()
).save_to_s3_parquet(f"s3://{bucket_name}/dwe/qlk_salesanalysis_hetl2_deliveries_gs")