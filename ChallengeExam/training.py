from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, concat, col, date_format, to_timestamp, to_date
from pyspark.sql.types import StringType, FloatType, StructType, StructField, TimestampType
from datetime import datetime

now = datetime.now()
print("now =", now)
# dd/mm/YY H:M:S
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
print("date and time =", dt_string)


sizes = ['5', '23', '6', '6', '6', '6', '1']
names = ['stock', 'transaction_date', 'open_price', 'close_price', 'max_price', 'min_price', 'variation']
names2 = ['stocsk', 'tradnsaction_date', 'open_praice', 'close_dprice', 'maxa_price', 'min_pfrice', 'varhiation']
types = ['string','date','float','float','float','float','float']

"""aux = 1
init = [1]
for i in sizes:
    j = aux + int(i) + 1
    aux = j
    init.append(aux)
    if len(init) == len(sizes):
        break
print(init)"""

spark = SparkSession.builder \
    .master('local') \
    .appName('SantanderChallenge') \
    .getOrCreate()

df = spark.read \
    .option('delimiter', '\t') \
    .option('header', 'true') \
    .option('delimiter', ',') \
    .format('csv') \
    .load('C:/Users/agoldste/Downloads/wetransfer-fa877f/nyse_2012.csv')

#.INFERSCHEMA('TRUE')

col_list = df.columns
col_date = []  # Saves columns with datetime type

""" Column cast """
for i in range(len(col_list)):
    if types[i] == 'datetime' or types[i] == 'date':
        types[i] = 'string'
        col_date.append(col_list[i])
    df = df.withColumn(col_list[i], df[i].cast(types[i]))

""" Cast DatetimeType """
for i in range(len(col_date)):
    df = df.withColumn(col_date[i], to_date(df[col_date[i]], 'dd-MMM-yyyy').alias(col_date[i]))

df.show()

mySchema = StructType([StructField(c, StringType()) for c in names])
df2 = spark.createDataFrame(data=[], schema=mySchema)
df2.show()

"""colsToSelect = df2.columns

head, *tail = colsToSelect
print(head)

l = ','.join(colsToSelect)

print("\n\n")"""
#df2.select(concat(col('stock'), lit(''), col('min_price')).alias('test')).show()



lista = [df['_c0'].substr(init[0], int(sizes[0])).cast(StringType()).alias(names[0])
                         , df['_c0'].substr(init[1], int(sizes[1])).cast(StringType()).alias(names[1])
                         , df['_c0'].substr(init[2], int(sizes[2])).cast(FloatType()).alias(names[2])
                         , df['_c0'].substr(init[3], int(sizes[3])).cast(FloatType()).alias(names[3])
                         , df['_c0'].substr(init[4], int(sizes[4])).cast(FloatType()).alias(names[4])
                         , df['_c0'].substr(init[5], int(sizes[5])).cast(FloatType()).alias(names[5])
                         , df['_c0'].substr(init[6], int(sizes[6])).cast(FloatType()).alias(names[6])]
lis = []
col_date = []

for i in range(len(names)):
    if types[i] == 'timestamp':
        types[i] = 'string'
        col_date.append(names[i])
    lis.append(df['_c0'].substr(init[i], int(sizes[i])).cast(types[i]).alias(names[i]))



print(col_date)
print(lis)

df = df.select(lis)
for i in range(len(col_date)):
    df = df.withColumn(col_date[i], to_timestamp(df[col_date[i]], 'd/m/Y HH:MM:S').alias('transaction_date'))


df.show()


#df.select(lista).show()

""" for j in range(len(names)):
    df_aux = df_aux.withColumn(names2[j], lit(None))
    df_aux = df_aux.withColumnRenamed(names2[j], names[i + 1])
    df_aux.show()"""

"""df_aux1 = df_aux.select(names)
result = df2.union(df_aux)
print("\nPRUEBA:\n")
result.distinct().show(100)"""

"""df = df.select(df['_c0'].substr(init[0], int(sizes[0])).cast(StringType()).alias(names[0])
                       , df['_c0'].substr(init[1], int(sizes[1])).cast(StringType()).alias(names[1])
                       , df['_c0'].substr(init[2], int(sizes[2])).cast(FloatType()).alias(names[2])
                       , df['_c0'].substr(init[3], int(sizes[3])).cast(FloatType()).alias(names[3])
                       , df['_c0'].substr(init[4], int(sizes[4])).cast(FloatType()).alias(names[4])
                       , df['_c0'].substr(init[5], int(sizes[5])).cast(FloatType()).alias(names[5])
                       , df['_c0'].substr(init[6], int(sizes[6])).cast(FloatType()).alias(names[6]))"""
