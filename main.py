from pyspark.sql import SparkSession

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.Transformer import Transformer
import os
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
    spark: SparkSession = SparkSession \
        .builder \
        .master(SPARK_MODE) \
        .getOrCreate()
    transformer = Transformer(spark)
