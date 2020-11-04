from pyspark.sql.functions import to_json, struct
import base64 as b64


PATH = "s3://bucket-prod/processed/table"
TABLE_NAME = "table_name"


def convert_to_json(df, partition_columns=None):
    """
    """
    if partition_columns:
        prcs_cols = set(df.columns) - set(partition_columns)
    else:
        prcs_cols = set(df.columns)

    jvalue_df = df.withColumn("JVALUE", to_json(struct([x for x in prcs_cols])))
    jvalue_df = jvalue_df.drop(*prcs_cols)
    ordered_cols = ["JVALUE"]
    if partition_columns is not None:
        ordered_cols.extend(partition_columns)

    jvalue_df = jvalue_df.select(*ordered_cols)
    return jvalue_df


df = spark.read.format('delta').load(path)

def download_key(scope, key):
    """
    :param spark:
    :param scope:
    :param key:
    :return:
    """
    secret_key = dbutils.secrets.get(scope=scope, key=key)
    return b64.b64decode(secret_key).decode("UTF-8")


db_secret_scope = "batch-user"
db_secret_key = "snowflake-pem"
pem_private_key = download_key(db_secret_scope, db_secret_key)

##Modify below 4 variables
sfDatabase = "database"
sfSchema = "schema"
mode = "append"

# only modify if you want to change the default warehouse
sfWarehouse = "AID_BATCH_SMALL_WH"

sf_options = {
    "sfURL": "https://account.region.snowflake.com",
    "sfAccount": "account",
    "sfUser": "user",
    "pem_private_key": pem_private_key,
    "sfDatabase": sfDatabase,
    "sfSchema": sfSchema,
    "sfWarehouse": sfWarehouse,
    "sfRole": "role",
}

partition_columns = ['year', 'month', 'day']
jvalue_df = convert_to_json(df, partition_columns)

# jvalue_df.printSchema()
# jvalue_df.show()
# print(jvalue_df.count())

jvalue_df.write.format("snowflake").options(**sf_options).option("dbtable", table_name).mode(mode).save()
