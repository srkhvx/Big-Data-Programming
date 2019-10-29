import os

# from pyspark.shell import spark

os.environ["SPARK_HOME"] = "D:\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add


from pyspark.sql import SparkSession
from pyspark import SparkContext

sc=SparkContext('local')


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#DATA FRAME READING
df = sqlContext.read.csv("survey.csv", inferSchema=True, header=True)
df.show()

#Create Table Name
df.createTempView(name="survey")

#DUPLICATE
df1 = sqlContext.sql("select Age,Country, COUNT(*) FROM survey group by Age,Country having COUNT(*) >1")
df1.show()

#DATAFRAME SAVING
df.write.csv('saved')

#SPLIT DATAFRAME TO 2
splits = df.randomSplit([0.5, 0.5])
print("LENGTH OF 1: ",splits[0].count())
print("LENGTH OF 2: ",splits[1].count())


#JOINING THEM
new=splits[0].unionAll(splits[1])
print("LENGTH OF JOINED: ", new.count())


new.createTempView(name="orderit")
#ORDER BY
ordered=sqlContext.sql("select * from orderit order by Country")
ordered.show()

#FETCH
print(ordered.take(12))

#
splits[0].createTempView(name="1st")
splits[1].createTempView(name="2nd")

joined=sqlContext.sql("select * from 1st INNER JOIN 2nd on 1st.Country = 2nd.Country order by 1st.Country")
joined.show()
print("LENGTH OF Joined: ",joined.count())

