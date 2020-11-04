from Utils.property import PropertyGetter
from Controller.spark_interaction import SparkInteraction
from Controller.df_formatter import FormatDataFrame


class SantanderChallenge:
    """
    Main Class
    """

    @staticmethod
    def main():
        """
        :return:
        """
        # nyse_2012
        """ Extract """
        df_nyse2012 = spark.sparkRead(propGetter.get_nyse2012('origin'), propGetter.get_nyse2012('format'),
                                      propGetter.get_nyse2012('delimiter'), propGetter.get_nyse2012('header'))

        """ Transform """
        df_nyse2012 = formatDF.parse_nyse2012(df_nyse2012, propGetter.get_nyse2012('column', 'types'))

        """ Load """""
        df_nyse2012.show(truncate=False)
        spark.sparkWrite(df_nyse2012, propGetter.get_nyse2012('destination'),
                         propGetter.get_nyse2012('column', 'partition'))

        # nyse_2012_ts
        """ Extract """
        df_nyse2012TS = spark.sparkRead(propGetter.get_nyse2012_ts('origin'), propGetter.get_nyse2012_ts('format'),
                                        propGetter.get_nyse2012_ts('delimiter'), propGetter.get_nyse2012_ts('header'))

        """ Transform """
        df_nyse2012TS = formatDF.parse_nyse2012TS(df_nyse2012TS,
                                                  propGetter.get_nyse2012_ts('column', 'names'),
                                                  propGetter.get_nyse2012_ts('column', 'sizes'),
                                                  propGetter.get_nyse2012_ts('column', 'types'))

        """ Load """""
        df_nyse2012TS.show(truncate=False)
        spark.sparkWrite(df_nyse2012TS, propGetter.get_nyse2012_ts('destination'),
                         propGetter.get_nyse2012_ts('column', 'partition'))


if __name__ == '__main__':
    # Classes Instances
    spark = SparkInteraction()
    propGetter = PropertyGetter()
    formatDF = FormatDataFrame()
    SantanderChallenge.main()
