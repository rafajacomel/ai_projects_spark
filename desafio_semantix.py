from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import countDistinct
import re  

import collections

# parse file with relevant information
def parseFile(line):
    fieldsParsed = line.replace("- -","").replace('"',"").replace("[","").replace("]","").replace("GET","").replace("POST","")
    fieldsSplited = re.split(' |:',fieldsParsed)
    

    if(len(fieldsSplited) == 12 and (fieldsSplited[11].isdigit())):
        return Row(host=str(fieldsSplited[0]), day=str(fieldsSplited[2]),  url=str(fieldsSplited[8]), httpReturn=str(fieldsSplited[10]), returnedBytes=int(fieldsSplited[11]))
    else:
        if(len(fieldsSplited) == 12 and (fieldsSplited[10] == "404")):
            return Row(host=str(fieldsSplited[0]), day=str(fieldsSplited[2]),  url=str(fieldsSplited[8]), httpReturn=str(fieldsSplited[10]), returnedBytes=0)
        else:
            return Row(host="dummy", day="dummy",  url="dummy", httpReturn="dummy", returnedBytes=0)
        
# create a dataframe for a specifc file
def createFileDataFrameFormfile(spark,file):
   fileLines = spark.sparkContext.textFile(file)
   fileParsed = fileLines.map(parseFile)
   fileDataFrame = spark.createDataFrame(fileParsed).cache()
   return fileDataFrame

# calculate the number of distinct hosts
def calculateNumberOfDistinctHosts(spark):
    query = "SELECT count(distinct host) as countDistinctHosts FROM nasa_data"
    distinctHosts = spark.sql(query)
    for distinctHostsValue in distinctHosts.collect():
         print("Number of distinct hosts analyzed: " + str(distinctHostsValue.countDistinctHosts))

# calculate the total of 404 errors
def calculateTotal404Errors(spark):
    query = "SELECT count(httpReturn) as count404Errors FROM nasa_data where httpReturn = 404"
    total404Errors = spark.sql(query)
    for total404ErrorsValue in total404Errors.collect():
         print("Number of 404 errors: " + str(total404ErrorsValue.count404Errors))

# calculate the 5 url with more 404 errors
def calculateTopFiveUrls404Errors(spark):
    query = "SELECT url, count(httpReturn) as count404Errors FROM nasa_data where httpReturn = 404 GROUP BY url ORDER BY count404Errors desc LIMIT 5"
    topFiveUrls404Errors = spark.sql(query)
    for topFiveUrls404ErrorsValue in topFiveUrls404Errors.collect():
         print("Number of 404 errors for url " + topFiveUrls404ErrorsValue.url + ": " + str(topFiveUrls404ErrorsValue.count404Errors))

# calculate 404 errors per day
def calculate404ErrorsPerDay(spark):
    query = "SELECT day, count(httpReturn) as count404Errors FROM nasa_data where httpReturn = 404 GROUP BY day"
    topFiveUrls404Errors = spark.sql(query)
    for topFiveUrls404ErrorsValue in topFiveUrls404Errors.collect():
         print("Number of 404 errors for day " + topFiveUrls404ErrorsValue.day + ": " + str(topFiveUrls404ErrorsValue.count404Errors))

# calculate the total bytes returned
def calculateTotalBytesReturned(spark):
    query = "SELECT sum(returnedBytes) as sumReturnedBytes FROM nasa_data"
    sumReturnedBytes = spark.sql(query)
    for sumReturnedBytes in sumReturnedBytes.collect():
         print("Sum of total returned bytes: " + str(sumReturnedBytes.sumReturnedBytes))

# files to be analyzed
file01 = "access_log_Aug95"
file02 = "access_log_Jul95"

# create  a spark session
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("desafioSemantix").getOrCreate()

print("")
print("starting nasa report....")
# create dataframe for access_log_Aug95
print("reading file " + file01 + "..." )
dataFrameFile01 = createFileDataFrameFormfile(spark, file01)

# create dataframe for access_log_Jul95
print("reading file " + file02 + "..." )
dataFrameFile02 = createFileDataFrameFormfile(spark,file02)

# merge data from both files
print("merging files " + file01 + " and " + file02 + "....")
allDataDataFrames = dataFrameFile01.union(dataFrameFile02)

# create view with all data
print("creating view with all data...")
allDataDataFrames.createOrReplaceTempView("nasa_data")

print("")
print("Calculating numbers of distinct hosts...")
print("----------------------------------------")
calculateNumberOfDistinctHosts(spark)
print("")
print("Calculating numbers of 404 errors...")
print("------------------------------------")
calculateTotal404Errors(spark)
print("")
print("Calculating 5 hosts with higher ammount of 404 errors...")
print("--------------------------------------------------------")
calculateTopFiveUrls404Errors(spark)
print("")
print("Calculating 404 errors per day...")
print("---------------------------------")
calculate404ErrorsPerDay(spark)
print("")
print("Calculating total ammount of bytes returned...")
print("----------------------------------------------")
calculateTotalBytesReturned(spark)
spark.stop()
