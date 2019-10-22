import os

os.environ["SPARK_HOME"] = "D:\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add

from pyspark import SparkContext
def mergeSort(nlist1):
    try:
        data = nlist1.split(',')
        data1 = []
        for x in data:
            data1.append(int(x))
        nlist = data1
    except:
        nlist=nlist1
    if len(nlist) > 1:
        mid = len(nlist) // 2
        lefthalf = nlist[:mid]
        righthalf = nlist[mid:]

        mergeSort(lefthalf)
        mergeSort(righthalf)
        i = j = k = 0
        while i < len(lefthalf) and j < len(righthalf):
            if lefthalf[i] < righthalf[j]:
                nlist[k] = lefthalf[i]
                i = i + 1
            else:
                nlist[k] = righthalf[j]
                j = j + 1
            k = k + 1

        while i < len(lefthalf):
            nlist[k] = lefthalf[i]
            i = i + 1
            k = k + 1

        while j < len(righthalf):
            nlist[k] = righthalf[j]
            j = j + 1
            k = k + 1
    return(nlist)


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("document.txt", 1)
    print(lines.collect())
    lines=sc.parallelize(lines.collect())
    #lines = sc.parallelize(alpha)
    func=lines.map(mergeSort)
    print(func)
    print(func.collect()[0])