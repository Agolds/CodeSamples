from pyspark.sql.functions import min


spark.conf.set("spark.sql.parquet.compression.codec",'snappy')
spark.conf.set("spark.sql.shuffle.partitions","2001")
spark.conf.set("spark.driver.maxResultSize", "50g")
spark.conf.set("spark.dynamicAllocation.enabled", False)

new_ids = spark.createDataFrame([("10", "lol"), ("11", "lola"), ("13", "lelo"), ("87", "pepo"), ("65", "sain")], ["personid", "deviceid"])

if len(new_ids.head(1)) > 0:
  new_ids = new_ids.agg(min(new_ids.personid))

new_ids.show()


all_distinct_products_and_users = spark.createDataFrame([("10", "lol"), ("11", "lola"), ("13", "lelo"), ("87", "pepo"), ("65", "sain")], ["personid", "deviceid"])
eco2_accounts_product = spark.createDataFrame([("10", "lol", "1", "c"), ("11", "lola", "2", "b"), ("13", "lelo", "3", "a"), ("19", "lel", "8", "a")], ["personid", "deviceid", "createdon", "extra"])

total = all_distinct_products_and_users.join(
    eco2_accounts_product,
    (all_distinct_products_and_users.personid == eco2_accounts_product.personid) &
    (all_distinct_products_and_users.deviceid ==
     eco2_accounts_product.deviceid), 'left_outer'
)

total.show()


izq = spark.createDataFrame([("10", "lol", "1"), ("11", "lola", "2"), ("13", "lelo", "3"), ("87", "pepo", "987")], ["age", "name", "equis"])
der = spark.createDataFrame([("10", "lol"), ("11", "lola"), ("13", "lelo")], ["age", "name"])

total = izq.join(
    der,
    (izq.age == der.age) &
    (izq.name == der.name), 'left_anti')

total.show()
