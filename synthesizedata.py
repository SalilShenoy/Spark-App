import argparse
import os
import csv
import random
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.storagelevel import StorageLevel


INDIVIDUAL = 2000000
START_POS = 1000000000
imputedData = Row('a_id', 's_pos', 'allele', 'individual', 'dosage')
S3_BUCKET = 's3a://mys3bucket'


class synthesizeData(object):

    def __init__(self, individual_ids):
        self.individual_ids = individual_ids.value

    def __call__(self,individual_id):
        fd = list()
        a_id = random.randint(1, 23)
        allele = random.choice('ATCG')
        dosage = random.uniform(0,2)
        for sPos in range(START_POS):
            row = imputedData(a_id, sPos, allele, individual_id, dosage)
            fd.append(row)
        return fd

def main(sc, sqlContext, args):
    numIndividuals = range(1, INDIVIDUAL)
    print 'Number of Individuals: ', len(numIndividuals)
    indvidual_rdd = sc.parallelize(numIndividuals)
    indvidual_rdd = indvidual_rdd.repartition(120)
    indvidual_rdd.persist()
    individual_ids = sc.broadcast(numIndividuals)
    imputedData = synthesizeData(individual_ids)
    imputedData_rdd = indvidual_rdd.flatMap(imputedData)
    imputedData_rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    imputed_df = imputedData_rdd.toDF()
    print 'Data: %d' % imputedData_rdd.count()
    imputed_df.printSchema()
    print 'Total rows: %d' % imputedData_rdd.count()
    for data in imputedData_rdd.collect():
        print data
    if args.format == 'parq':
        imputed_df.write.format('parquet').mode('overwrite').save(os.path.join(S3_BUCKET, 'parquet'))
    else:
        imputed_df.write.format('com.databricks.spark.csv').mode('overwrite').save(os.path.join(S3_BUCKET, 'csv'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Spark App. to create Synthetic data and store in S3')
    parser.add_argument('--format', type=str, default='csv',help='file format to store data in S3')
    args = parser.parse_args()
    conf = (SparkConf()
            .setMaster("local[*]")
            .setAppName("Synthetic Data")
            .set("spark.executor.memory", "4g"))
    conf.set('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)
    main(sc, sql_context,args)
