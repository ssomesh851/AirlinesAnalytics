# AirlinesAnalytics


# How Apache Spark can give wings to airline analytics?

The global airline industry continues to grow rapidly, but consistent and robust profitability is yet to be seen. According to the International Air Transport Association (IATA), the industry has doubled its revenue over the past decade, from US$369 billion in 2005 to an expected $727 billion in 2015.

In the commercial aviation sector, every player in the value chain — airports, airplane manufacturers, jet engine makers, travel agents, and service companies turns a tidy profit.

All these players individually generate extremely high volumes of data due to higher churn of flight transactions. Identifying and capturing the demand is the key here which provides much greater opportunity for airlines to differentiate themselves. Hence, Aviation industries can utilize big data insights to boost up their sales and improve profit margin.

#### Big data is a term for collection of datasets so large and complex that its computing cant be handled by traditional data processing systems or on-hand DBMS tools.

#### Apache Spark is an open source, distributed cluster computing framework specifically designed for interactive queries and iterative algorithms.

The Spark DataFrame abstraction is a tabular data object similar to R's native dataframe or Pythons pandas package, but stored in the cluster environment.

According to [Fortune's](http://fortune.com/2015/09/25/apache-spark-survey/ "Fortune")
 latest survey , Apache Spark is most popular technology of 2015.

Biggest Hadoop vendor Cloudera is also saying  GoodBye to Hadoops MapReduce and Hello to Spark .

What really gives Spark the edge over Hadoop is speed. Spark handles most of its operations in memory – copying them from the distributed physical storage into far faster logical RAM memory. This reduces the amount of time consumed in writing and reading to and from slow, clunky mechanical hard drives that needs to be done under Hadoops MapReduce system.

Also, Spark includes tools (real-time processing, machine learning and interactive SQL) that are well crafted for powering business objectives such as analyzing real-time data by combining historical data from connected devices, also known as the Internet of things.

now, lets gather some insights on sample Aviation data using Apache Spark.


In this project we will understand how to query data in DataFrames using SQL as well as save output to filesystem in CSV format.

### Using Databricks File system(DBFS)

For this I am going to load a file from existing system to DBMS by Databricks , a company founded by Creators of Apache Spark and which handles Spark Development and distributions currently.

Spark community consists of roughly 600 contributors who make it the most active project in the entire Apache Software Foundation, a major governing body for open source software, in terms of number of contributors.

 'Spark.read.csv' helps us to parse and query csv data in the spark. We can use this read function for both for reading and writing csv data to and from any Hadoop compatible filesystem.

### Loading the data into Spark DataFrames

Lets load our input files into a Spark DataFrames using the spark.read.csv from Databricks.
 
While starting the databricks as shown below:
you must need to start a spark cluster and also need to create python notebook in order to write a code into it.

Lets create SparkSession by using module pyspark.sql

```
from pyspark.sql import *
spark = SparkSession.builder.appName('sr-1').getOrCreate()
```
Now lets load our csv data from Airlines.csv (Airlines csv github) file whose schema is as below


schema of aviation csv
```
Airlinedf = spark.read.csv('/FileStore/tables/aviationcsv.txt', inferSchema=True, header=True)
```
The load operation will parse the *.csv file using Databricks spark-csv library and return a dataframe with column names same as in the first header line in file.

The following are the parameters passed to load method:

1. Source: "/FileStore/tables/aviationcsv.txt"  tells spark we want to load as csv file.
2. Options:
  +  path – path of file, where it is located.
  + inferSchema - It will infer the Schema by default from csv. So we must pass True.
  +  Header: "header" -> "True" tells spark to keep first line of file to column names for resulting dataframe.

Lets see what is schema of our Dataframe
table1

Check out sample data in our dataframe

```
AirlinedF.show
```
### Querying CSV data using temporary tables:

To execute a query against a table, we call the sql() method on the pyspark.sql.

We have created airports DataFrame and loaded CSV data, to query this DF data we have to register it as temporary table called airports.

```
Airlinedf.registerTempTable("Aviation")
```
Let's find out how many airports are there in South east part in our dataset
```
Airlinedf1 = spark.sql("select AirportID, Name, Latitude, Longitude from airports where Latitude<0 and Longitude>0")
Airlinedf1.show()
[1,Goroka,-6.081689,145.391881]
[2,Madang,-5.207083,145.7887]
[3,Mount Hagen,-5.826789,144.295861]
[4,Nadzab,-6.569828,146.726242]
[5,Port Moresby Jacksons Intl,-9.443383,147.22005]
[6,Wewak Intl,-3.583828,143.669186] 
```
We can do aggregations in sql queries on Spark
We will find out how many unique cities have airports in each country
```
Aviationdf2 = spark.sql("select Country, count(distinct(City)) from airports group by Country")
Aviationdf2.show()
[Iceland,10]
[Greenland,4]
[Canada,131]
[Papua New Guinea,6] 
```

What is average Altitude (in feet) of airports in each Country?
```
Airlinedf3 = spark.sql("select Country , avg(Altitude) from Aviation group by Country")
Airlinedf3.show()
[Iceland,72.8], 
[Greenland,202.75],
[Canada,852.6666666666666], 
[Papua New Guinea,1849.0]) 
```

Now to find out in each timezones how many airports are operating?
```
Airlinedf4 = spark.sql("select Tz , count(Tz) from Aviation group by Tz")
Airlinedf4.show()
[America/Dawson_Creek,1]
[America/Coral_Harbour,3]
[America/Halifax,9]
[America/Toronto,48]
[America/Vancouver,19]
[America/Godthab,3]
[Pacific/Port_Moresby,6]
[Atlantic/Reykjavik,10]
[America/Thule,1]
[America/St_Johns,4]
[America/Winnipeg,14]
[America/Edmonton,27]
[America/Regina,10] 
```

We can also calculate average latitude and longitude for these Aviation's in each country
```
Aviationdf4 = spark.sql("select Country, avg(Latitude), avg(Longitude) from Aviation group by Country")
Aviationdf4.show()
[Iceland,65.0477736,-19.5969224]
[Greenland,67.22490275,-54.124131999999996]
[Canada,53.94868565185185,-93.950036237037]
[Papua New Guinea,-6.118766666666666,145.51532] 
```

Lets count how many different DSTs are there
```
Aviationdf5 = spark.sql("select count(distinct(DST)) from Aviation")
Aviationdf5.show()
```

###Saving data in CSV format
Till now we loaded and queried csv data. Now we will see how to save results in CSV format back to filesystem.
Suppose we want to send report to client about all airports in northwest part of all countries.
Lets calculate that first.

NorthWestAvaitiondf6 = spark.sql("select AirportID, Name, Latitude, Longitude from Aviation where Latitude>0 and Longitude<0")
NorthWestAvaitiondf6.show()
```
NorthWestAirportsdf6: org.apache.spark.sql.DataFrame = [AirportID: string, Name: string, Latitude: string, Longitude: string]
```
And save it to CSV file
```
NorthWestAviationdf6.write.format("csv").save("/FileStore/tables/NorthWestAviationdata")```

```
### Conclusion:
In this project we gathered some insights on airports data using SparkSQL and DataFrame, interactive queries
and explored csv format dataset from Spark.

