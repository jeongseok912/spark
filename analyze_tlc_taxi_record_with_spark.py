from pyspark.sql import SparkSession
from pyspark import SparkConf

'''
conf = SparkConf().setAppName("PySpark - Read TLC Taxi Record")
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', '<access_key>')
conf.set('spark.hadoop.fs.s3a.secret.key', '<secret_key>')
conf.set('spark.hadoop.fs.s3a.session.token', '<toke>')

spark = SparkSession.builder.config(conf=conf).getOrCreate()
'''
spark = SparkSession.builder.appName(
    "PySpark - Read TLC Taxi Record").getOrCreate()

file_path = 's3://tlc-taxi/2019/fhvhv_tripdata_2019-02.parquet'

df = spark.read.format('parquet').load(file_path)
df.show()
