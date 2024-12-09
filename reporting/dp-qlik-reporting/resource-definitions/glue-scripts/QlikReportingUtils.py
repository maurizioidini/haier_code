from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame


DEBUG_MODE          = False
URL                 = None
USER                = None
PASSWORD            = None
SPARK_OBJECT        = None
TMP_DIR             = None
IAM_ROLE            = None
GLUE_CONTEXT        = None



def read_redshift_or_empty(table, schema=None):
    if schema is None: schema=StructType([])

    try:
        return read_data_from_redshift_table(table)
    except Exception as ex:
        if not ("relation" in str(ex) and "does not exist" in str(ex)):
            raise ex   # do not catch other exceptions than "table xxx does not exist"

        print(f"read_redshift_or_empty: returning empty table! (schema={schema})")
        return SPARK_OBJECT.createDataFrame([], schema=schema)

def read_data_from_redshift_table(
        table:str
) -> DataFrame:
    """
    This method is used to read all data from a redshift table

    Args:
        table(str): name of the redshift table

    Return:
        DataFrame: pyspark dataframe object containing all records from the redshift table
    """

    if URL is None or TMP_DIR is None or SPARK_OBJECT is None or IAM_ROLE is None:
        raise Exception("Please set QlikReportingUtils global var")

    query = f"select * from {table}"
    print(f"query: {query}")

    data = (
        SPARK_OBJECT.read.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", URL)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("query", query)
        .option("tempdir", TMP_DIR)
        .option("forward_spark_s3_credentials", "true")
        .load()
    )

    if DEBUG_MODE:
        print(f"LOADED {data.count()} rows!")

    return data

def save_data_to_redshift_table(
        self,
        table_name: str,
        truncate=True,
        preactions=None,
        postactions=None
    ) -> None:
    """
    This method is used to save all data to a redshift table

    Args:
        table(str): name of the redshift table

    """
    if URL is None or GLUE_CONTEXT is None:
        raise Exception("Please set connection parameters and/or tmp dir global var")

    connection_options = {
        "url": URL,
        "user": USER,
        "password": PASSWORD,
        "redshiftTmpDir": TMP_DIR,
        "dbtable": table_name
    }

    print(f"Saving data to redshift (using AWS DynamicFrame): {table_name}...")

    data = DynamicFrame.fromDF(self, GLUE_CONTEXT, table_name)

    assert not (preactions and truncate), "'preactions' parameter overrides 'truncate' parameter!!"

    if preactions:
        connection_options["preactions"] = preactions
    elif truncate:
        connection_options["preactions"] = f"truncate table {table_name}"

    if postactions:
        connection_options["postactions"] = postactions

    if "preactions" in connection_options: print("preactions:", connection_options["preactions"])
    if "postactions" in connection_options: print("postactions:", connection_options["postactions"])

    GLUE_CONTEXT.write_dynamic_frame.from_options(
        frame=data,
        connection_type="redshift",
        connection_options = connection_options
    )

    return self

def read_data_from_s3_parquet(path:str, recursiveFileLookup=False, mergeSchema=True) -> DataFrame:
    """
    This method is used to read a parquet file in S3 and create a dataframe from it

    Args:
        path(str): path of the s3 bucket

    Returns:
        DataFrame: a spark dataframe containing data from s3 parquet

    """

    print(f"LOADING {path}... (recursiveFileLookup={recursiveFileLookup}, mergeSchema={mergeSchema})")

    data = SPARK_OBJECT.read.option("recursiveFileLookup", recursiveFileLookup).option("mergeSchema", mergeSchema).parquet(path)

    if DEBUG_MODE:
        print(f"LOADED {data.count()} rows!")
    return data

def save_to_s3_parquet(
    self,
    path: str,
    do_lower_columns=True,
    datetimeRebaseMode="LEGACY",
    do_cast_decimal_to_double=True
) -> None:

    """
    This method is used to store a dataframe to a parquet file in S3

    Args:
        dataframe(DataFrame): dataframe containing data
        path(str): path of the s3 bucket

    """

    print(f"SAVING {path}... (do_lower_columns={do_lower_columns}, datetimeRebaseMode={datetimeRebaseMode}, do_cast_decimal_to_double={do_cast_decimal_to_double})")

    if do_lower_columns: self = self.columns_to_lower()

    if do_cast_decimal_to_double:
        self = self.cast_decimal_to_double()

    writer = self.write

    if datetimeRebaseMode is not None: writer = writer.option("datetimeRebaseMode", datetimeRebaseMode)

    writer.parquet(path, mode="overwrite")

    if DEBUG_MODE:
        print(f"WRITTEN {self.count()} rows!")

    return self

# if you want to use None, pass lit(None) as default value!!
def do_mapping(
    self,
    mapping_table,
    source_mapping_col,
    dest_col_name,
    default_value=None,
    mapping_name=None,
):
    mapping_key, mapping_val = list(mapping_table.columns)

    print(
        f">>> {mapping_name or 'mapping'}: {source_mapping_col} -> {mapping_key} -> {mapping_val} -> {dest_col_name}"
    )

    mapping_table.assert_no_duplicates(mapping_key)

    mapping_table = mapping_table.drop_duplicates_ordered([mapping_key], [mapping_val])

    d = self.join(
        mapping_table.select(
            col(mapping_key).alias("___mapping_col"),
            col(mapping_val).alias(dest_col_name),
            lit(True).alias("___mapping_presence"),
        ),
        source_mapping_col == col("___mapping_col"),
        "left",
    )

    if default_value is not None:
        d = d.withColumn(
            dest_col_name,
            when(col("___mapping_presence").isNull(), default_value).otherwise(
                col(dest_col_name)
            ),
        )

    return d.drop("___mapping_col", "___mapping_presence")

def drop_duplicates_ordered(self, keyCols, orderCols):
    win = Window.partitionBy(keyCols).orderBy(orderCols)

    return (
        self.withColumn("drop_duplicates_ordered___rn", row_number().over(win))
        .where(col("drop_duplicates_ordered___rn") == 1)
        .drop("drop_duplicates_ordered___rn")
    )

def assert_no_duplicates(self, *cols):
    cols = list(cols)

    df = self.withColumn("__cnt", count("*").over(Window.partitionBy(cols))).where(col("__cnt") > 1)

    if df.count() == 0:
        return self

    cols_display = cols + ["__cnt"] + list(set(self.columns) - set(cols))

    print("ERROR: DUPLICATES FOUND!")
    df.select(cols_display).show(truncate=False)

    raise RuntimeError(f"ERROR: DUPLICATES FOUND ON TABLE!")

def read_data_from_s3_csv(path:str, sep=",", recursiveFileLookup=False) -> DataFrame:
    """
    This method is used to read a csv file in S3 and create a dataframe from it

    Args:
        path(str): path of the s3 bucket

    Returns:
        DataFrame: a spark dataframe containing data from s3 parquet

    """

    print(f"LOADING {path}... (sep='{sep}', recursiveFileLookup={recursiveFileLookup})")
    data = SPARK_OBJECT.read.option("recursiveFileLookup", recursiveFileLookup).csv(path, header=True, sep=sep)
    if DEBUG_MODE:
        print(f"LOADED {data.count()} rows!")
    return data

def cast_decimal_to_double(self):
    for field in self.schema.fields:    # cast all decimal (from cas/redshift) to double :)
        if isinstance(field.dataType, DecimalType):
            self = self.withColumn(field.name, self[field.name].cast("double"))

    return self

def coalesceZero(a, b):
    return (
        when(a.isNotNull() & (a != 0), a)
        .when(b.isNotNull() & (b != 0), b)
        .otherwise(lit(0))
    )

def coalesceEmpty(c1, c2):
    return when(col(c1).isNull() | (trim(col(c1)) == ""), col(c2)).otherwise(col(c1))

def cast_cols(df):
    all_cols = {x.lower() for x in set(df.columns)}
    int_cols = {x.lower() for x in {'bdg_qty', 'data_type', 'flag_bgrade', 'flag sap', 'price_forced', 'flag_cancelled_invoice_valid'}} & all_cols
    double_cols = {x.lower() for x in {'vat_amount_2', 'vat_reason_perc', 'acc_vat_amount_1', 'vat_percentage_2', 'acc_vat_amount_2', 'vat_percentage_1', 'BDG STD COMM €', 'BDG FM €', 'doc_type_code_concatenate', 'del tp lc', 'pck nsv €', 'removable_charges_tot', 'ord qty fabs', 'ond gm cp lc', 'del nsv lc', 'ord bo nsv €', 'transport_cost_infra', 'forced_price_tot', 'total_amount', 'std_prod_cost_no_tca', 'doc_type_code_orig', 'inv qty', 'ord gm cp lc', '% ord qty', 'forced_price', 'blk gm cp €', 'inv gm tp €', 'ond gsv lc', 'ord cp €', 'ord nsv lc', 'pck gm tp lc', 'inv transport cost lc', 'pot qty', 'inv + del gm cp €', 'pot gsv €', 'blk nsv €', 'inv gsv promotional & bonus lc', 'transport cost infra lc', 'inv nsv €', 'del nsv €', 'transport_cost_unit', 'del gm tp €', 'pot nsv lc', 'doc_total_amount', 'inv gm cp lc', 'removable_charges €', 'ord qty consignment', 'pck gm cp lc', 'pck gsv €', 'cons_cost_of_sales_unit', 'pck gm cp €', 'blk nsv lc', 'inv tp €', 'inv gsv lc temp', 'ord gsv lc tot', 'kurrf', 'ord nsv €', 'inv cp lc', 'inv transport cost €', 'del gsv lc tot', 'tca_unit', 'inv gsv + transport €', 'transport_cost_infra_unit', 'transport cost infra €', 'ord bo nsv lc', 'blk gm tp €', 'original del qty', 'inv nsv lc', 'inv gsv €', 'comm_cost_of_sales_unit', 'del gsv €', 'inv gsv + transport lc', 'inv + del gm cp lc', 'blk gsv €', 'comm_cost_of_sales', 'removable_charges lc', 'ord bo gm cp €', 'ond nsv lc', 'pck nsv lc', 'del gm tp lc', 'ord cp lc', 'del nsv lc tot', 'tp_itcy_unit', 'ord qty', 'pck gm tp €', 'ond nsv €', 'ord qty orig', 'del gsv lc', 'tp_commerciale', 'del raee lc', 'vat_percentage_1', 'inv gm tp lc', 'ord gsv lc', 'doc_type_code_cancelled', 'cons_cost_of_sales', 'inv cp €', 'ord tp €', 'ond gm tp €', 'inv gm cp €', 'ord bo gm tp lc', 'inv + del nsv €', 'ord nsv lc tot', 'inv tp lc', 'inv gsv lc', 'ord gsv €', 'tca', 'blk qty', 'std_prod_cost_commercial_unit', 'inv nsv lc temp', 'inv raee lc', 'pot gm tp €', 'ord gm cp €', 'inv gsv promotional & bonus €', 'ord gm tp €', 'ord bo gsv €', 'del qty consignment', 'pot gm cp lc', 'ord tp lc', 'inv qty fabs', 'ond gm cp €', 'list_price', 'transport cost lc', 'vat_amount_1_tot', 'pot gm cp €', 'blk gm cp lc', 'ond gsv €', 'inv + del gsv lc', '% del qty', 'list_price_tot', 'inv gsv promo bonus lc temp', 'removable_charges', 'ord bo qty', 'inv + del nsv lc', '% ord qty fabs', 'pot gm tp lc', 'del gm cp €', 'inv + del qty', 'inv + del gm tp €', 'ond qty', 'inv raee €', 'exchange', 'pot nsv €', 'freight_in', 'blk gm tp lc', 'del gm cp lc', 'ord gm tp lc', 'ord bo gm tp €', 'del cp €', 'ord bo gsv lc', 'doc_type_code_single', 'doc_type_code', 'del tp €', 'payment_expiring_disc', 'moltiplicatore_reso', 'transport cost €', 'inv + del gsv €', 'currency_euro_exch', 'vat_amount_1', 'pck qty', 'del cp lc', 'inv ic discount/surcharge', 'transport_cost', 'ord bo gm cp lc', 'pck gsv lc', 'inv transport cost lc temp', 'net_price', '% del qty fabs', 'exchange_rate_sap', 'del ic discount/surcharge', 'ord ic discount/surcharge', 'del qty fabs', 'ond gm tp lc', 'pot gsv lc', 'ord raee lc', 'inv + del gm tp lc', 'blk gsv lc', 'del qty'}} & all_cols
    date_cols = {x.lower() for x in {"mrdd", "document_date", "first_req_del_date", "planned_dispatch_date", "appointment", "order_blocking_date", "order_unblocking_date", "ref_invoice_date", 'ref_invoice_date', 'order_customer_date', 'order_requested_delivery_date', 'ord_requested_delivery_date', 'time_id_fact', 'time_id'}} & all_cols
    string_cols = {"customer_type_code", "dest_customer_type_code", "purchase_group_code", "super_group_code", "main_group_code", 'order_vat_code', 'vat_reason_code', 'area_code', 'comm_organization', 'order_number'}

    for c in all_cols:
        if c in int_cols: df = df.withColumn(c, col(c).cast("int"))
        if c in double_cols: df = df.withColumn(c, col(c).cast("double"))
        if c in date_cols: df = df.withColumn(c, col(c).cast("date"))
        if c in string_cols: df = df.withColumn(c, col(c).cast("string"))

    for field in df.schema.fields:    # cast all decimal (from cas/redshift) to double :)
        if isinstance(field.dataType, DecimalType):
            df = df.withColumn(field.name, df[field.name].cast(DoubleType()))

    df = df.columns_to_lower()    # select all columns in lowercase so mergeSchema does not explode!

    return df.localCheckpoint()

def columns_to_lower(self):
    cols = [x.lower() for x in self.columns]
    return self.select(cols)

setattr(DataFrame, "columns_to_lower", columns_to_lower)
setattr(DataFrame, "cast_decimal_to_double", cast_decimal_to_double)
setattr(DataFrame, "columns_to_lower", columns_to_lower)
