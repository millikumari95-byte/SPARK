import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)



# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
# Add the bin directory to the system PATH so the DLL can be loaded
os.environ['PATH'] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ['PATH']
os.environ['JAVA_HOME'] = r'C:\Users\Milli\.jdks\corretto-1.8.0_472'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

#spark = SparkSession.builder.getOrCreate()
#sc=SparkContext(conf=conf)
ss=SparkSession.builder.getOrCreate()
sc.textFile("file:///C:/Users/Milli/IdeaProjects/SPARK/venv/Lib/site-packages/pyspark/sql/dt.txt").foreach(print)
#ss.read.csv("file:///C:/Users/Milli/IdeaProjects/SPARK/venv/Lib/site-packages/pyspark/sql/dt.txt").show()




##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ########################
#filerdd= sc.textFile("state.txt")
#print()
#print(filerdd.collect())
#filerdd.foreach(print)
#flatrdd=filerdd.flatMap(lambda x : x.split ("~"))
#flatrdd.foreach(print)

#Statelis=flatrdd.filter(lambda x :'State'in x )
#Statelis.foreach(print)
#states=Statelis.map(lambda x : x.replace("State->",""))
#states.foreach(print)

#usdata =sc.textFile("usdata.csv")
#print()
#print("=======Raw data====")
#usdata.foreach(print)
#lendata=usdata.filter (lambda x : len(x)>200)
#print()
#print("========lendata======")
#lendata.foreach(print)

#flatdata=lendata.flatMap(lambda x :x.split(","))
#print()
#print("=====flatdata=====")
#flatdata.foreach(print)
#repdata=flatdata.map(lambda x : x.replace("-",""))
#print()
#print("=====repdata=====")
#repdata.foreach(print)

#adddata=repdata.map(lambda x : x + ",zeyo")
#print()
#print("=======adddata=====")
#adddata.foreach(print)

#adddata.saveAsTextFile("D:/rddout")
#print("=====writtend done re --go and check  ")

data = sc.textFile("dt.txt")
#print()
#print("=======Rawdata=====")
#data.foreach(print)


gymdata=data.filter(lambda x : "Gymnastics" in x)
#print()
#print("========gymdata=====")
#gymdata.foreach(print)

mapsplit=data.map(lambda x : x.split(","))
# print()
# print("======mapsplit======")
# mapsplit.foreach(print)

from collections import namedtuple

# define the namedtuple
columns = namedtuple("columns", ["id","tdate","amt","category","product","mode"])
# impose columns to the data splits
coldata = mapsplit.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))

# print()
# print("======coldata====")
# coldata.foreach(print)   # <-- use coldata instead of mapsplit

#step 4 column filter
prodfil=coldata.filter(lambda x : "Gymnastics" in x.product)
# print()
# print("=======prodfil=====")
# prodfil.foreach(print)



####dataframe
# df=prodfil.toDF()
# df.show()
# df=prodfil.toDF()
# df.show()
# df.write.parquet("file:///D:/parquetout")

#from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadCSVExample") \
    .getOrCreate()

# Read the CSV file
csvdf = (
    spark
    .read
    .format("csv")
    .option("header", "true")   # if your CSV has headers
    .option("inferSchema", "true")  # to automatically detect column types
    .load("usdata.csv")
)

# Show the data
csvdf.show()


