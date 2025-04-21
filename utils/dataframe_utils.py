def read_data_spark(file_path,file_format,spark,**options):
    df = spark.read.format(file_format).options(**options).load(file_path)
    return df



