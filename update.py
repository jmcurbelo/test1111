class UpdateData:

    def __init__(self, api, spark):
        self.api = api
        self.spark = spark

    def update_data(self, country, parameters):
        latest_pdf = self.api.measurements(country=country, parameter=parameters, limit=1000, df=True)

        latest_df = self.spark.createDataFrame(latest_pdf.astype(str))

        new_names_cols = (column.replace('.', '_') for column in latest_df.columns)

        latest_df = latest_df.toDF(*new_names_cols)

        print(latest_df.count())

        aux = self.spark.read.parquet('./output')

        print(latest_df.join(aux, ['location'], 'inner').count())

