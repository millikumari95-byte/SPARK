import os
import sys
from pyspark.sql import SparkSession

# ✅ Force Spark to use your venv Python
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ✅ Correct JAVA_HOME and PATH
os.environ['JAVA_HOME'] = r'C:\Users\Milli\.jdks\corretto-1.8.0_472'
os.environ['PATH'] = r'C:\Users\Milli\.jdks\corretto-1.8.0_472\bin' + os.pathsep + os.environ['PATH']

# ================== Spark Setup ==================
spark = SparkSession.builder \
    .appName("csv-test") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Read CSV
csvdf = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:/Users/Milli/IdeaProjects/SPARK/usdata.csv")
)

csvdf.show()

# ✅ Filter example
fildf = csvdf.filter("state = 'LA'")
fildf.show()



# Sample data
data = [
    ("00000000","06-26-2011",200,"Exercise","GymnasticsPro","cash"),
    ("00000001","05-26-2011",300,"Exercise","Weightlifting","credit"),
    ("00000002","06-01-2011",100,"Exercise","GymnasticsPro","cash"),
    ("00000003","06-05-2011",100,"Gymnastics","Rings","credit"),
    ("00000004","12-17-2011",300,"Team Sports","Field","paytm"),
    ("00000005","02-14-2011",200,"Gymnastics",None,"cash")  # None for missing subcategory
]

# Define column names
cols = ["txnno","txndate","amount","category","subcategory","spendmode"]

# ✅ Create DataFrame
df = spark.createDataFrame(data, cols)

# Show DataFrame
df.show()

# # Optional: print schema to verify column types
# #df.printSchema()
#
# # df.createOrReplaceTempView("df")
# # spark.sql("select txndate ,amount from df ").show()
# # seldf=df.select("txndate","amount")
# # seldf.show()
#
# dropdf=df.drop("txndate","amount")
# dropdf.show()
#
# sincol=df.filter("category='Exercise'")
# sincol.show()
#
# print("========category=Exercise and spendby=cash =======" )
# mulcol=df.filter("category='Exercise' and spendmode='cash' ")
# mulcol.show()
#
# print("=======category =EXERCISE OR spendmode=CASH =======")
# mulcolor=df.filter ("category ='Excercise' or spendmode ='cash' ")
#
# mulcolor.show()

print("======Category=Exercise & Gymnastics ========")
infil=df.filter("category in ('Exercise','Gymnastics') ")
infil.show()
