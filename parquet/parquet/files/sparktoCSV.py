from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

conf = (SparkConf()
         .setMaster("local")
         .setAppName("parq reader")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df.write.format('com.databricks.spark.csv').save('mycsv.csv')