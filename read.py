from pyspark.sql.functions import col, trim, current_timestamp, regexp_replace
from pyspark.sql.types import DecimalType, StringType, TimestampType
from constants import *


class ReadData:

    def __init__(self, api, spark):
        self.api = api
        self.spark = spark

    def get_data(self, country, parameters):

        measurements_pdf = self.api.measurements(country=country, parameter=parameters, limit=1000, df=True)

        measurements_df = self.spark.createDataFrame(measurements_pdf.astype(str))

        new_names_cols = (column.replace('.', '_') for column in measurements_df.columns)

        measurements_df = measurements_df.toDF(*new_names_cols).select(
            trim(col(location)).cast(StringType()).alias(location),
            trim(col(parameter)).cast(StringType()).alias(parameter),
            trim(col(value)).cast(DecimalType(15,10)).alias(value),
            trim(col(country_col)).cast(StringType()).alias(country_col),
            trim(col(date_utc)).cast(TimestampType()).alias(date_utc),
            trim(col(coordinates_latitude)).cast(DecimalType(15,9)).alias(coordinates_latitude),
            trim(col(coordinates_longitude)).cast(DecimalType(15,9)).alias(coordinates_longitude)
        ).groupBy(country_col, location, coordinates_latitude,
                                                coordinates_longitude, date_utc).pivot(parameter).avg(value)

        return measurements_df


