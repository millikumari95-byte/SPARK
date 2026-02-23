import os
import sys
import shutil
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ======================================================================================
# 1. ENVIRONMENT SETUP & DEPENDENCIES
# ======================================================================================

# Setup directories for local Hadoop binaries (Required for Windows)
hadoop_home = os.path.abspath("hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
os.makedirs(hadoop_bin, exist_ok=True)

# Define file downloads
downloads = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": "data/test.txt",
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": "hadoop/bin/winutils.exe",
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": "hadoop/bin/hadoop.dll",
    "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"
}

# Download files securely
ssl_context = ssl._create_unverified_context()
for url, path in downloads.items():
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):  # Only download if file doesn't exist
        with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
            out_file.write(response.read())

# Set Environment Variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Milli\.jdks\corretto-1.8.0_472'
# CRITICAL: Add Hadoop bin to PATH so Windows can find hadoop.dll
os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ['PATH']

# ======================================================================================
# 2. SPARK SESSION INITIALIZATION
# ======================================================================================

conf = SparkConf() \
    .setAppName("USData_Processing") \
    .setMaster("local[*]") \
    .set("spark.driver.host", "localhost")

sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# ======================================================================================
# 3. RDD DATA PROCESSING
# ======================================================================================

output_path = "D:/rddout"

# Clean up output directory if it exists (prevents "File Already Exists" error)
if os.path.exists(output_path):
    shutil.rmtree(output_path)

print("\n--- Starting Data Processing ---")

# 1. Load raw text file as RDD
raw_rdd = sc.textFile("usdata.csv")

# 2. Filter: Only keep rows with length > 200 characters
filtered_rdd = raw_rdd.filter(lambda line: len(line) > 200)

# 3. FlatMap: Split lines by comma into individual elements
flattened_rdd = filtered_rdd.flatMap(lambda line: line.split(","))

# 4. Map: Remove hyphens from the data
cleaned_rdd = flattened_rdd.map(lambda item: item.replace("-", ""))

# 5. Map: Append a suffix to every element
final_rdd = cleaned_rdd.map(lambda item: item + ",zeyo")

# ======================================================================================
# 4. ACTIONS & OUTPUT
# ======================================================================================

# Show a few results in the console
print("\nSample processed data:")
for record in final_rdd.take(5):
    print(record)

# Save the final result to the D: drive
try:
    final_rdd.saveAsTextFile(output_path)
    print(f"\nSUCCESS: Data written to {output_path}")
except Exception as e:
    print(f"\nERROR saving file: {e}")

print("\n--- Processing Finished ---")