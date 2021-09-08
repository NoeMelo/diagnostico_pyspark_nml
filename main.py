from pyspark.sql import SparkSession

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.Transformer import Transformer

if __name__ == '__main__':
    spark: SparkSession = SparkSession \
        .builder \
        .master(SPARK_MODE) \
        .getOrCreate()
    transformer = Transformer(spark,0) # age validacion (1 o 0) -> ejercicio 5 
