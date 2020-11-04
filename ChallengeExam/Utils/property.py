import configparser


class PropertyGetter:
    """
     :return property value
    """
    config = configparser.ConfigParser()
    config.read('../properties.ini')

    def get_nyse2012(self, arg_property, extra=''):
        """
        :return: key/values from ini file: 'properties.ini'
        """
        if extra != '':
            extra = '_' + extra

        try:
            return self.config.get(arg_property, arg_property + extra + '_nyse_2012')
        except Exception as e:
            print("Error reading key | nyse_2012")
            print(e)

    def get_nyse2012_ts(self, arg_property, extra=''):
        """
        :return: key/values from ini file: 'properties.ini'
        """
        if extra != '':
            extra = '_' + extra

        try:
            return self.config.get(arg_property, arg_property + extra + '_nyse_2012_ts')
        except Exception as e:
            print("Error reading key | nyse_2012_ts")
            print(e)
