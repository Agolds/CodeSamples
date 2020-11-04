"""
Author: Alex Goldstein
Environment: Databricks
"""
from datetime import datetime, timedelta
from pyspark.sql.functions import min as min_
from delta.tables import *


date = datetime.strptime(getArgument("exec_date"), '%Y-%m-%d')

# Loading already aggregated table
minimum_date = DeltaTable.forPath(spark, 's3://prod-delta/processed/minimum_date')

# Loading filtered origin table
new_ids = spark.read.format('delta') \
    .load(f's3://prod-historical/processed/historical/year={date.year}/month={date.month}/day={date.day}') \
    .selectExpr('userid AS personid_m'
                , 'properties_product_guid AS deviceid_m'
                , 'time_stamp')

new_ids = new_ids.groupBy('personid_m', 'deviceid_m') \
    .agg(min_('time_stamp')) \
    .withColumnRenamed('min(time_stamp)', 'createdon_madrid')

new_ids = new_ids.where('personid_m is not null AND personid_m != "" AND deviceid_m is not null')

minimum_date.alias("minimum_date").merge(
    new_ids.alias("new_ids"),
    "minimum_date.personid_m = new_ids.personid_m AND minimum_date.deviceid_m = new_ids.deviceid_m") \
    .whenNotMatchedInsertAll() \
    .execute()
