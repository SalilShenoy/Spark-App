from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd

conf = (SparkConf()
         .setMaster("local")
         .setAppName("parq reader")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# DataFrames can be saved as Parquet files, maintaining the schema information.
# Load a text file and convert each line to a tuple.
#lines = sc.textFile("adult.csv")
#parts = lines.map(lambda l: l.split(","))
#adults = parts.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14]))

df = pd.read_csv('/etc/adult.data', names = ["Age", "Workclass", "fnlwgt", "Education", "Education_Num", "Martial_Status",
        "Occupation", "Relationship", "Race", "Sex", "Capital_Gain", "Capital_Loss",
        "Hours_per_week", "Country", "Target"])

#schema information
schema = StructType([
          StructField("Age", IntegerType(),True),
          StructField("Workclass", StringType(),True),
          StructField("fnlwgt", IntegerType(),True),
          StructField("Education", StringType(),True),
          StructField("Education_Num", IntegerType(),True),
          StructField("Martial_Status", StringType(),True),
          StructField("Occupation", StringType(),True),
          StructField("Relationship", StringType(),True),
          StructField("Race", StringType(),True),
          StructField("Sex", StringType(),True),
          StructField("Capital_Gain", IntegerType(), True),
          StructField("Capital_Loss", IntegerType(), True),
          StructField("Hours_per_week", IntegerType(), True),
          StructField("Country", StringType(), True),
          StructField("Target", StringType(), True)])
#create a data frame to save data as a parquet file
schemaAdult = sqlContext.createDataFrame(df, schema)
schemaAdult.write.parquet("adult.parquet")
#Query using spark
schemaAdult.registerTempTable("adults")
results = sqlContext.sql("SELECT Age FROM adults")
ages = results.map(lambda p: "Age: " + str(p.Age))
for a in ages.collect():
  print(a)
#df = sqlContext.read.load("adult.parquet")
#df = sqlContext.sql("SELECT Age FROM parquet.`adult.parquet`")
#df.show()