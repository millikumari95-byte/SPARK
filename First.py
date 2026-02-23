import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

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

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

# print()

print("=====started=====")
#
# filerdd= sc.textFile("state.txt")
# #print()
# #print(filerdd.collect())
# #filerdd.foreach(print)
#
# flatrdd=filerdd.flatMap(lambda x : x.split ("~"))
# #flatrdd.foreach(print)
#
# Statelis=flatrdd.filter(lambda x : 'State' in x )
# #Statelis.foreach(print)
#
# states=Statelis.map(lambda x : x.replace("State->",""))
# states.foreach(print)
#
# citylis=flatrdd.filter(lambda x :"city" )
# print()
# print("=======citylis======")
# citylis.foreach(print)
#
# rawlist=["state->TN~city->chennai", "state->kerala~city->Trivandrum"]
# print()
# print("=====rawlis========")
# print(rawlist)
#
# rddlis=sc.parallelize(rawlist)
# print()
# print("=======rddlis=====")
# print(rddlis.collect())
#
# flatrdd=rddlis.flatMap(lambda x : x.split("~"))
# print(flatrdd.collect())
#
# filerdd=sc.textFile("state.txt")
# print()
# print("====filerdd======")
# print(filerdd.collect())

usdata =sc.textFile("usdata.csv")
print()
print("=======Raw data====")
# #usdata.foreach(print)
#
lendata=usdata.filter (lambda x : len(x)>200)
# print()
# print("========lendata======")
lendata.foreach(print)

# flatdata=lendata.flatMap(lambda x :x.split(","))
# print()
# print("=====flatdata=====")
# flatdata.foreach(print)
#
# repdata=flatdata.map(lambda x : x.replace("-",""))
# print()
# print("=====repdata=====")
# repdata.foreach(print)
#
# adddata=repdata.map(lambda x : x + ",zeyo")
# print()
# print("=======adddata=====")
# adddata.foreach(print)
#
# adddata.saveAsTextFile("D:/rddout")
# print("=====writtend done re --go and check  ")

#step1 read the data
data = sc.textFile("dt.txt")
print()
print("=======Rawdata=====")
data.foreach(print)

#step2 filter the data
gymdata=data.filter(lambda x : "Gymnastics" in x)
print()
print("========gymdata=====")
gymdata.foreach(print)

#step3 split the data with columns
mapsplit=data.map(lambda x : x.split(","))
print()
print("======mapsplit======")
mapsplit.foreach(print)

#step4 impose columns to the data splits
####define columns using named tuple#####
from collections import namedtuple

from collections import namedtuple

# define the namedtuple
columns = namedtuple("columns", ["id","tdate","amt","category","product","mode"])

# impose columns to the data splits
coldata = mapsplit.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))

print()
print("======coldata====")
coldata.foreach(print)   # <-- use coldata instead of mapsplit

#step 4 column filter
prodfil=coldata.filter(lambda x : "Gymnastics" in x.product)
print()
print("=======prodfil=====")
prodfil.foreach(print)

####dataframe
# df=prodfil.toDF()
# df.show()
# df.write.parquet("file:///D:/parquetout")

csvdf = (
    spark
    .read
    .format("csv")
    .option("header", "true")   # ✅ Correct syntax
    .load("usdata.csv")
)

csvdf.show()

jsondf=(
       spark
       .read
       .format("json")
       .load("file4.json")

)
jsondf.show()

parquetdf=(
    spark
    .read
    .format("parquet")
    .load("file5.parquet")
)
print()
print("=====parquetdf======")
parquetdf.show()

orcdf=(
    spark
    .read
    .format("orc")
    .load("data.orc")
)
print("======orcdf=====")
orcdf.show()

orcdf.createOrReplaceTempView("varanasi")
spark.sql("select * from varanasi where id >0 ").show()

csvdf = (

    spark
    .read
    .format("csv")
    .option("header","true")
    .load("usdata.csv")

)

csvdf.show()

csvdf.createOrReplaceTempView("rishab")


spark.sql(" select * from rishab where state='LA' ").show()


Fildf = csvdf.filter("state ='LA' ")
Fildf.show()

data = [
    ("00000000","06-26-2011",200,"Exercise","GymnasticsPro","cash"),
    ("00000001","05-26-2011",300,"Exercise","Weightlifting","credit"),
    ("00000002","06-01-2011",100,"Exercise","GymnasticsPro","cash"),
    ("00000003","06-05-2011",100,"Gymnastics","Rings","credit"),
    ("00000004","12-17-2011",300,"Team Sports","Field","paytm"),
    ("00000005","02-14-2011",200,"Gymnastics",None,"cash")  # added None for missing subcategory
]

# Define column names as a list
cols = ["txnno","txndate","amount","category","subcategory","spendmode"]

# Create DataFrame
df = spark.createDataFrame(data, cols)

df.show()

df.createOrReplaceTempView("df")
spark.sql("select txndate ,amount from df ").show()

seldf=df.select("txndate","amount")
seldf.show()

dropdf=df.drop("txndate","amount")
dropdf.show()

print("=======category=Exercise")
sincol=df.filter("category='Exercise'")
sincol.show()

print("========category=Exercise and spendby=cash =======" )
mulcol=df.filter("category='Exercise' and spendmode='cash' ")
mulcol.show()

print("=======category =EXERCISE OR spendmode=CASH =======")
mulcolor=df.filter ("category ='Excercise' or spendmode ='cash' ")
mulcolor.show()

print("======Category=Exercise & Gymnastics ========")
infil=df.filter("category in ('Exercise','Gymnastics') ")
infil.show()

print("====subcategory contains Gymnastics=====")
likefil=df.filter("subcategory like '%Gymnastics%' ")
likefil.show()

print("====subcategory is null ======")
nullfil=df.filter("subcategory is null")
nullfil.show()

print("====subcategory is not  null ======")
notnullfil=df.filter("subcategory is  not null")
notnullfil.show()

print("====category not equal to exercise")
notfil=df.filter("category != 'Exercise' ")
notfil.show()

exprdf=df.selectExpr(
              "txnno",
              "txndate",
               "amount",
                "upper(category)",
                 "subcategory",
                 "spendmode"

)
exprdf.show()

exprdf=df.selectExpr(
    "txnno",
    "txndate",
    "amount + 10000 as amount ",
    "upper(category) as category " ,
    "concat (subcategory,'~zeyo')",
    "spendmode"

)
exprdf.show()

exprdf=df.selectExpr(
    "cast (txnno as int) as txnno",
    "txndate",
    "amount + 10000 as amount ",
    "upper(category) as category " ,
    "concat (subcategory,'~zeyo')",
    "spendmode"

)
exprdf.show()

exprdf=df.selectExpr(
    "cast (txnno as int) as txnno",
    "split(txndate, '-' )[2] as year ",
    "amount + 10000 as amount ",
    "upper(category) as category " ,
    "concat (subcategory,'~zeyo') as subcategory"
    "spendmode"

)
exprdf.show()

exprdf=df.selectExpr(
    "cast (txnno as int) as txnno",
    "split(txndate, '-' )[2] as year ",
    "amount + 10000 as amount ",
    "upper(category) as category " ,
    "concat (subcategory,'~zeyo') as subcategory"
    "spendmode",
     "case when spendmode='cash' then 1 else 0 end as status"
)
exprdf.show()

exprdf=df.selectExpr(
    "cast (txnno as int) as txnno",
    "split(txndate, '-' )[2] as year ",
    "amount + 10000 as amount ",
    "upper(category) as category " ,
    "concat (subcategory,'~zeyo') as subcategory"
    "spendmode",
    "case when spendmode='cash' then 1 when spendmode='credit' then 2  else 0 end as status"
)
exprdf.show()

#from pyspark.sql.functions import expr

#from pyspark.sql.functions import expr

from pyspark.sql.functions import expr, split

print("====== withColumn dataframe ======")

withexpr = (
    df.withColumn("category", expr("upper(category)"))
    .withColumn("amount", expr("amount + 1000"))
    .withColumn("subcategory", expr("concat(subcategory, '~zeyo')"))
    .withColumn("txno", expr("cast(txnno as int)"))
    .withColumn("status", expr("CASE WHEN spendmode = 'cash' THEN 1 ELSE 0 END"))
    #.withColumn("year", split(df["txndate"], "-").getItem(2))   # year part
    .withColumn("txndate", split(df["txndate"], "-").getItem(2))   # year part
    .withColumnRenamed("txndate","year")
)

withexpr.show()

####INNER JOIN

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()


innerj = cust. join(prod, ["id"], "inner")
innerj.show()