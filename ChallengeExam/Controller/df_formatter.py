from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, to_timestamp, to_date
from datetime import datetime


class FormatDataFrame:

    @staticmethod
    def parse_nyse2012(df: DataFrame, types: str) -> DataFrame:
        """
        :param types: Column types
        :param df: Dataframe to be transformed
        :return: Dataframe transformed
        """
        types = types.split(',')
        col_list = df.columns
        col_date = []  # Saves columns with datetime type

        try:
            """ Column cast """
            for i in range(len(col_list)):
                if types[i] == 'datetime' or types[i] == 'date':
                    types[i] = 'string'
                    col_date.append(col_list[i])
                df = df.withColumn(col_list[i], df[i].cast(types[i]))

            """ Cast DateType """
            for i in range(len(col_date)):
                df = df.withColumn(col_date[i], to_date(df[col_date[i]], 'dd-MMM-yyyy').alias(col_date[i]))

            """ New column """
            now = datetime.now()
            df = df.withColumn('partition_date', lit(now))
        except Exception as e:
            print("Error transforming nyse_2012")
            print(e)

        return df

    @staticmethod
    def parse_nyse2012TS(df: DataFrame, names: str, sizes: str, types: str) -> DataFrame:
        """
        :param types: Column types
        :param df: Dataframe to be transformed
        :param names: Column names
        :param sizes: column sizes in chars
        :return: Dataframe transformed
        """
        names = names.split(',')
        sizes = sizes.split(',')
        types = types.split(',')
        init = init_array(sizes)
        col_list = []  # Saves query for each column
        col_date = []  # Saves columns with timestamp type

        try:
            """ Fixed width & Column cast """
            for i in range(len(names)):
                if types[i] == 'timestamp':
                    types[i] = 'string'
                    col_date.append(names[i])
                col_list.append(df['_c0'].substr(init[i], int(sizes[i])).cast(types[i]).alias(names[i]))

            df = df.select(col_list)

            """ Cast TimestampType """
            for i in range(len(col_date)):
                df = df.withColumn(col_date[i], to_timestamp(df[col_date[i]], 'd/m/Y HH:MM:S').alias(col_date[i]))
        except Exception as e:
            print("Error transforming nyse_2012_ts")
            print(e)

        return df


def init_array(sizes):
    """
    :param sizes: Array with column sizes
    :return: Beginning of the columns (position)
    """
    aux = 1
    init = [1]
    for i in sizes:
        j = aux + int(i) + 1
        aux = j
        init.append(aux)
        if len(init) == len(sizes):
            return init
