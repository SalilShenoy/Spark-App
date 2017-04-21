from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *

S3_BUCKET = 's3a://23andme-sshenoy'

conf = (SparkConf()
        .setMaster("local")
        .setAppName("Hive Query for Parquet")
        .set("spark.executor.memory", "4g"))
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS imputed_dosage (acccession_id BIGINT, starting_pos BIGINT, allele CHAR, \
	            individual BIGINT, dosage DECIMAL)")
sqlContext.sql("LOAD DATA LOCAL INPATH 's3a://23andme-sshenoy/parquet/*.parquet' INTO TABLE dosage")

# Queries can be expressed in HiveQL.
results = sqlContext.sql("FROM dosage SELECT acccession_id").collect()
print results