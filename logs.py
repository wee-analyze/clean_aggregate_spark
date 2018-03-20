from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession.builder.appName("iislogs").getOrCreate()

raw_data = spark.sparkContext.textFile("hdfs_path_here")

remove_info_headers = raw_data.filter(lambda x: x[0] != "#")

def extract_data(line):
    column_split = line.split(" ")
    return(column_split[0], column_split[1], column_split[2])

clean_data = remove_info_headers.map(extract_data)

# make data schema. Could have used this function. Here is an option
# from pyspark.sql import Row
#def make_table(line):
    #column_split = line.split(" ")
    #return Row(old_date = str(column_split[0]), time = str(column_split[1]), c_ip = str(column_split[2]))

#clean_logs.map(make_table)
    
#df = spark.createDataFrame(clean_logs)

# However, this is limited because if you want to use other data types like timestamps and date then you have to do more 
# processing as this is a Python code. Instead, use spark sql types



clean_logs_schema = StructType([StructField("date", StringType(), True),
           StructField("time", StringType(), True),
           StructField("c_ip", StringType(), True)])

# make dataframe by applying schema to RDD
df = spark.createDataFrame(clean_data, clean_logs_schema)
# df.printSchema()

aggregate = df.groupBy("c_ip").count()

results = aggregate.filter(aggregate["count"] > 2500)


# save to HDFS
results.coalesce(1).write.format("csv").save("path_here")
# or 
#results.repartition(1).write.format("csv").save("path_here")
