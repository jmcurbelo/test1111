import openaq
from pyspark.sql import SparkSession
from read import ReadData
from write import Write
from update import UpdateData

api = openaq.OpenAQ(version='v2')
spark = SparkSession.builder.getOrCreate()

# create clases
read = ReadData(api, spark)
write = Write()
update = UpdateData(api, spark)

country_list = str(input('Enter a list of country comma separated\n')).split(',')

parameter_list = str(input('Enter a list of parameter comma separated\nOptions include '
                           '[pm25,pm10,so2,co,no2,o3,bc]\n')).split(',')

data = read.get_data(country_list, parameter_list)

output_path = './output'
write.write_data(read.get_data(country_list, parameter_list), 3, 'overwrite', 'parquet', output_path)

update.update_data(country_list, parameter_list)











# Write
# output_path = './output'
# write.write_data(read.get_data(country_list, parameter_list), 3, 'overwrite', 'parquet', output_path)

# para ver como actualizar
"""
latest_pdf = api.latest(country='MX', limit=500, df=True)
latest_df = spark.createDataFrame(latest_pdf.astype(str))
latest_df.show(50, truncate=False)
"""