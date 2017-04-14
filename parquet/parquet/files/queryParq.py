from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("parq reader")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

sqlContext = SQLContext(sc)
df = sqlContext.read.load("adult.parq")

df.show()