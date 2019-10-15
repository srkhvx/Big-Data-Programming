import os

os.environ["SPARK_HOME"] = "D:\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("document.txt", 1)

    countsReduce = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    print("MAX VALUE: ", countsReduce.max())
    print("MIN VALUE: ", countsReduce.min())
    countsSort = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add).sortByKey(True)
    countsDist=lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .count()
    #countsReduce.saveAsTextFile("outputReduce")
    #countsSort.saveAsTextFile("outputSort")
    print(countsDist)
    sc.stop()