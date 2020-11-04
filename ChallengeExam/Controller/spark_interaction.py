from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class SparkInteraction:
    """
    Triggering spark actions
    """
    spark = SparkSession.builder \
        .master('local') \
        .appName('Challenge') \
        .getOrCreate()

    def sparkRead(self, path, format, delimiter=None, header='false') -> DataFrame:
        """
        :param format: csv, json, etc.
        :param header: True / False
        :param delimiter: ',', ' ', '|', etc.
        :param path: origin
        :return:
        """
        try:
            return self.spark.read \
                .option('delimiter', delimiter) \
                .option('header', header) \
                .format(format) \
                .load(path)
        except Exception as e:
            print("Error reading source: " + path)
            print(e)

    @staticmethod
    def sparkWrite(df: DataFrame, path, partition=None):
        """
        :return:
        """
        try:
            df.write.mode('overwrite')\
                .partitionBy(partition) \
                .parquet(path)
        except Exception as e:
            print("Error loading to destination: " + path)
            print(e)
