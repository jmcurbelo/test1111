import openaq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, regexp_replace
from pyspark.sql.types import DecimalType, StringType

api = openaq.OpenAQ(version='v2')

locations_pdf = api.locations(country='MX', limit=500, df=True)

measurements_pdf = api.measurements(country='MX', limit=500, df=True)

spark = SparkSession.builder.getOrCreate()

locations_df = spark.createDataFrame(locations_pdf.astype(str))

new_names_cols = (column.replace('.', '_') for column in locations_df.columns)
locations_df = locations_df.toDF(*new_names_cols)

measurements_df = spark.createDataFrame(measurements_pdf.astype(str))

locations_df.show(truncate=False)
measurements_df.show(truncate=False)

locations_df_cast = locations_df.select(
    trim(col('id')).cast(StringType()).alias('id'),
    trim(col('country')).cast(StringType()).alias('country'),
    trim(col('name')).cast(StringType()).alias('location'),
    trim(col('coordinates_latitude')).cast(DecimalType(15,9)).alias('coordinates_latitude'),
    trim(col('coordinates_longitude')).cast(DecimalType(15,9)).alias('coordinates_longitude')
)

measurements_df_cast = measurements_df.select(
    trim(col('location')).cast(StringType()).alias('location'),
    trim(col('parameter')).cast(StringType()).alias('parameter'),
    trim(col('value')).cast(DecimalType(15,10)).alias('value'),
    trim(col('country')).cast(StringType()).alias('country'))


pivot_df = measurements_df_cast.groupBy('location').pivot('parameter').avg('value')

final_df = locations_df_cast.join(pivot_df, ['location'], 'inner').dropDuplicates(['id']).withColumn(
    'execution_time', regexp_replace(current_timestamp(), '[\s|:|.]', '-'))

final_df.printSchema()
final_df.show(100, truncate=False)

"""
## con api v1
locations_df_cast = locations_df.select(
    trim(col('id')).cast(StringType()).alias('id'),
    trim(col('country')).cast(StringType()).alias('country'),
    trim(col('city')).cast(StringType()).alias('city'),
    trim(col('location')).cast(StringType()).alias('location'),
    trim(col('coordinates_latitude')).cast(DecimalType(15,9)).alias('coordinates_latitude'),
    trim(col('coordinates_longitude')).cast(DecimalType(15,9)).alias('coordinates_longitude')
)

measurements_df_cast = measurements_df.select(
    trim(col('location')).cast(StringType()).alias('location'),
    trim(col('parameter')).cast(StringType()).alias('parameter'),
    trim(col('value')).cast(DecimalType(15,10)).alias('value'),
    trim(col('country')).cast(StringType()).alias('country'),
    trim(col('city')).cast(StringType()).alias('city')
)

pivot_df = measurements_df_cast.groupBy('location').pivot('parameter').avg('value')

print(locations_df_cast.count())

final_df = locations_df_cast.join(pivot_df, ['location'], 'inner').dropDuplicates(['id']).withColumn(
    'execution_time', regexp_replace(current_timestamp(), '[\s|:|.]', '-'))

final_df.printSchema()
final_df.show(100, truncate=False)

final_df.write.mode('overwrite').partitionBy('execution_time').parquet('./output')
"""

# para ver como actualizar

#latest_pdf = api.latest(country='MX', limit=500, df=True)
#latest_df = spark.createDataFrame(latest_pdf.astype(str))
#latest_df.show(50, truncate=False)