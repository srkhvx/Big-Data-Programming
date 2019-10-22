import os
import ast
os.environ["SPARK_HOME"] = "D:\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add
import pdb
vertex=1

from pyspark import SparkContext
def dfs(graph, vertex=1, path=[]):
    path += [vertex]
    try:
        graph = ast.literal_eval(graph)
    except:
        pass
    for neighbor in graph[vertex]:
        if neighbor not in path:
            path = dfs(graph, neighbor, path)

    return path


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("document1.txt", 1)
    #lines = sc.parallelize(alpha)
    lines=sc.parallelize(lines.collect())
    func=lines.map(dfs)
    print(func)
    print(func.collect()[0])
