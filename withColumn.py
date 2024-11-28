import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark import SparkConf
from sys import stdin

conf=SparkConf().setAppName("filter in sql").setMaster("local[*]")
spark=SparkSession.builder.config(conf=conf).getOrCreate()

data=[(1,"Alice",29),(2,"Bob",35),(3,"catherine",28),(4,"david",40)]

columns=["id", "name", "salary"]

df=spark.createDataFrame(data,schema=columns)
df1=df.withColumn(colName="id",col=col("id").cast("Integer"))
df2=df1.withColumn("salary",col("salary")*1000)
df3=df2.withColumn("country",lit("India"))
df4=df3.withColumn("copy_name",col("name"))
df5=df4.withColumnRenamed("copy_name","copy_of_names")



df5.show()

df5.printSchema()

spark.stop()