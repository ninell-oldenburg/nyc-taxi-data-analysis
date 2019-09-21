# nyc-taxi-data-analysis
# Ludwig Bald, Florence Lopez, Ninell Oldenburg
# University of Trento
# 02/10/2019
# Python3
# Apache Spark


# Databricks notebook source
df = spark.read.csv('tables/test_dataframe.csv', header=True, inferSchema=True, sep=',')
display(df)

# COMMAND ----------

# add green data 
df_green = spark.read.csv('tables/test_dataframe_green.csv', header=True, inferSchema=True, sep=',')
display(df_green)

# COMMAND ----------

# import zone_lookup dataframe 
df_zones = spark.read.csv(' tables/zone_lookup.csv', header=True, inferSchema=True, sep=',')
display(df_zones)

# COMMAND ----------

# import clustering stuff
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# COMMAND ----------

# establish k-means 
kmeans = KMeans().setK(2).setSeed(1) 

# COMMAND ----------

# before we can fit the model we have to create a vector assembler to do a feature vector 
from pyspark.ml.feature import VectorAssembler 
assembler = VectorAssembler(inputCols = ['trip_distance', 'total_amount'], outputCol = 'features')

#assemble
output = assembler.transform(df)
display(output)

# COMMAND ----------

model = kmeans.fit(output)

# COMMAND ----------

predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

display(predictions)

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# make new column in dataframe, which has the hour of the day 
from pyspark.sql.types import DateType
from pyspark.sql.functions import split

# new day and time cols (needed for fare change analysis)
splitColtime = split(df['tpep_pickup_datetime'], ' ')
df = df.withColumn('PU_day', splitColtime.getItem(0))
df = df.withColumn('PU_time', splitColtime.getItem(1))

# new hour col
splitColHour = split(df['PU_time'], ':')
df = df.withColumn('PU_hour', splitColHour.getItem(0))
display(df)

# COMMAND ----------

# cast PU_hour column to double 
from pyspark.sql.types import DoubleType
df = df.withColumn("PU_hour", df["PU_hour"].cast(DoubleType()))
display(df)

# COMMAND ----------

# do k-means with k=3 and another included feature, namely the time 
kmeans = KMeans().setK(3).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
from pyspark.ml.feature import VectorAssembler 
assembler = VectorAssembler(inputCols = ['trip_distance', 'total_amount', 'PU_hour'], outputCol = 'features')

#assemble
output = assembler.transform(df)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# k = 3 does not seem to be so great, therefore do k = 2 but with the 3 features of above 
kmeans = KMeans().setK(2).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
assembler = VectorAssembler(inputCols = ['trip_distance', 'total_amount', 'PU_hour'], outputCol = 'features')

#assemble
output = assembler.transform(df)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# do a 4th clustering with just PU_time and trip_distance 
kmeans = KMeans().setK(2).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
assembler = VectorAssembler(inputCols = ['trip_distance', 'PU_hour'], outputCol = 'features')

#assemble
output = assembler.transform(df)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

####### K-MEANS FOR GREEN DATA #######
kmeans = KMeans().setK(2).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
assembler = VectorAssembler(inputCols = ['trip_distance', 'total_amount'], outputCol = 'features')

#assemble
output = assembler.transform(df_green)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# make new column in dataframe, which has the hour of the day 
# new day and time cols (needed for fare change analysis)
splitColtime = split(df_green['lpep_pickup_datetime'], ' ')
df_green = df_green.withColumn('PU_day', splitColtime.getItem(0))
df_green = df_green.withColumn('PU_time', splitColtime.getItem(1))

# new hour col
splitColHour = split(df_green['PU_time'], ':')
df_green = df_green.withColumn('PU_hour', splitColHour.getItem(0))
display(df_green)

# COMMAND ----------

# cast PU_hour column to double 
df_green = df_green.withColumn("PU_hour", df_green["PU_hour"].cast(DoubleType()))
display(df_green)

# COMMAND ----------

# do k-means with k=3 and another included feature, namely the time 
kmeans = KMeans().setK(3).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
assembler = VectorAssembler(inputCols = ['trip_distance', 'total_amount', 'PU_hour'], outputCol = 'features')

#assemble
output = assembler.transform(df_green)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# now do k = 2 but with the 3 features of above 
kmeans = KMeans().setK(2).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
assembler = VectorAssembler(inputCols = ['trip_distance', 'total_amount', 'PU_hour'], outputCol = 'features')

#assemble
output = assembler.transform(df_green)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# do a 4th clustering with just PU_time and trip_distance 
kmeans = KMeans().setK(2).setSeed(1) 
# before we can fit the model we have to create a vector assembler to do a feature vector 
assembler = VectorAssembler(inputCols = ['trip_distance', 'PU_hour'], outputCol = 'features')

#assemble
output = assembler.transform(df_green)
display(output)

# COMMAND ----------

model = kmeans.fit(output)
predictions = model.transform(output)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------

# Shows the results
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

# descriptive statistics
from pyspark.sql.functions import rand, randn

# general descriptives
display(df.describe())

# COMMAND ----------

# descriptive statistics
from pyspark.sql.functions import rand, randn

# general descriptives
display(df_green.describe())

# COMMAND ----------

# descriptive statistics
# covariance passenger count and trip distance
df.stat.cov('passenger_count', 'trip_distance')

# COMMAND ----------

# descriptive statistics
# correlation pick-up location and drop-off location
df.stat.corr('PULocationID', 'DOLocationID')

# COMMAND ----------

# descriptive statistics
# covariance payment type and fare amount
df_green.stat.cov('payment_type', 'fare_amount')

# COMMAND ----------

# descriptive statistics
# correlation tip amount and total amount
df_green.stat.corr('tip_amount', 'total_amount')

# COMMAND ----------

# descriptive statistics
# frequency distribution in contingency table of Drivers Company and number of passengers
df.stat.crosstab("VendorID", "passenger_count").show()

# COMMAND ----------

# descriptive statistics
# frequency distribution in contingency table of Drivers Company and number of passengers
df_green.stat.crosstab("PULocationID", "RateCodeID").show()

# COMMAND ----------

# identify the top 5 drivers
# the top 2 (because we have just 2 companies here & no driver's ID) that had the most trips 
mostTrips = df.groupBy('VendorID').count()
mostTrips.show()

# COMMAND ----------

# do the same for the green_data
mostTrips_g = df_green.groupBy('VendorID').count()
mostTrips_g.show()

# COMMAND ----------

# the top 2 that had the longest distance 
maxTrip = df.groupBy('VendorID').max('trip_distance')
maxTrip.show()

# COMMAND ----------

# the top 2 that made the most money (including tips)
maxMoney = df.groupBy('VendorID').max('total_amount')
maxMoney.show()

# COMMAND ----------

# do the same for the green data 
maxMoney_g = df_green.groupBy('VendorID').max('total_amount')
maxMoney_g.show()

# COMMAND ----------

# max money in all rides 
maxMoney_all = df.groupBy('VendorID').sum('total_amount')
maxMoney_all.show()

# COMMAND ----------

# do the same for the green data 
maxMoney_all_g = df_green.groupBy('VendorID').sum('total_amount')
maxMoney_all_g.show()

# COMMAND ----------

# identify location with most pickups 
from pyspark.sql.functions import col
mostPicks = df.groupBy('PULocationID').count()
mostPicks = mostPicks.join(df_zones, mostPicks.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
mostPicks = mostPicks.sort(col('count').desc())
display(mostPicks)

# COMMAND ----------

# do the same for the green data 
mostPicks_g = df_green.groupBy('PULocationID').count()
mostPicks_g = mostPicks_g.join(df_zones, mostPicks_g.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
mostPicks_g = mostPicks_g.sort(col('count').desc())
display(mostPicks_g)

# COMMAND ----------

# identify location with most dropoffs 
mostDrops = df.groupBy('DOLocationID').count()
mostDrops = mostDrops.join(df_zones, mostDrops.DOLocationID == df_zones.LocationID).drop(df_zones.LocationID)
mostDrops = mostDrops.sort(col('count').desc())
display(mostDrops)

# COMMAND ----------

# do the same for the green data 
mostDrops_g = df_green.groupBy('DOLocationID').count()
mostDrops_g = mostDrops_g.join(df_zones, mostDrops_g.DOLocationID == df_zones.LocationID).drop(df_zones.LocationID)
mostDrops_g = mostDrops_g.sort(col('count').desc())
display(mostDrops_g)

# COMMAND ----------

# show
picksHour = df.groupBy("PU_hour").count()
picksHour = picksHour.sort(col('PU_hour').asc())
display(picksHour)

# COMMAND ----------

# quantify the total drop-offs by time of the day (per hour)
# split drop-off field and count the number of drop-offs per day

# new day and time cols (needed for fare change analysis)
splitColtime = split(df['tpep_dropoff_datetime'], ' ')
df = df.withColumn('DO_day', splitColtime.getItem(0))
df = df.withColumn('DO_time', splitColtime.getItem(1))

# new hour col
splitColHour = split(df['DO_time'], ':')
df = df.withColumn('DO_hour', splitColHour.getItem(0))

# show in ascending order
dropsHour = df.groupBy("DO_hour").count()
dropsHour = dropsHour.sort(col('DO_hour').asc())
display(dropsHour)

# COMMAND ----------

# quantify the total pick-ups by location & add the location name 
totalPicksLoc = df.groupBy('PULocationID').count()
totalPicksLoc = totalPicksLoc.join(df_zones, totalPicksLoc.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
totalPicksLoc = totalPicksLoc.sort(col('count').desc())
display(totalPicksLoc)

# COMMAND ----------

# quantify the total drop-offs by location & add the location name 
totalDropsLoc = df.groupBy('DOLocationID').count()
totalDropsLoc = totalDropsLoc.join(df_zones, totalDropsLoc.DOLocationID == df_zones.LocationID).drop(df_zones.LocationID)
totalDropsLoc = totalDropsLoc.sort(col('count').desc())
display(totalDropsLoc)

# COMMAND ----------

# quantify the total pick-ups by time and location
totalPicksTimeLoc = df.groupby("PU_hour", "PULocationID").count()
totalPicksTimeLoc = totalPicksTimeLoc.join(df_zones, totalPicksTimeLoc.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
totalPicksTimeLoc = totalPicksTimeLoc.sort(col('PU_hour').asc())
display(totalPicksTimeLoc)

# COMMAND ----------

# quantify the total drop-offs by time and location
totalDropsTimeLoc = df.groupby("DO_hour", "DOLocationID").count()
totalDropsTimeLoc = totalDropsTimeLoc.join(df_zones, totalDropsTimeLoc.DOLocationID == df_zones.LocationID).drop(df_zones.LocationID)
totalDropsTimeLoc = totalDropsTimeLoc.sort(col('DO_hour').asc())
display(totalDropsTimeLoc)

# COMMAND ----------

# fare changes throughout the year
fareChangeYear = df.groupby("PU_day").agg({"total_amount": "avg"})
fareChangeYear = fareChangeYear.sort(col('PU_day').asc())
display(fareChangeYear)

# COMMAND ----------

# fare changes throughout the day
fareChangeDay = df.groupby("PU_time").agg({"total_amount": "avg"})
fareChangeDay = fareChangeDay.sort(col('PU_time').asc())
display(fareChangeDay)

# COMMAND ----------

# fare changes grouped by hour
fareChangeHour = df.groupby("PU_hour").agg({"total_amount": "avg"})
fareChangeHour = fareChangeHour.sort(col('PU_hour').asc())
display(fareChangeHour)

# COMMAND ----------

# fare changes grouped by location
fareChangeLoc = df.groupby("PULocationID").agg({"total_amount": "avg"})
fareChangeLoc = fareChangeLoc.join(df_zones, fareChangeLoc.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
fareChangeLoc = fareChangeLoc.sort(col('PULocationID').asc())
display(fareChangeLoc)

# COMMAND ----------

# fare changes hour and location
fareChangeHourLoc = df.groupby("PU_hour", "PULocationID").agg({"total_amount": "avg"})
fareChangeHourLoc = fareChangeHourLoc.join(df_zones, fareChangeHourLoc.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
fareChangeHourLoc = fareChangeHourLoc.sort(col('PU_hour').asc(), col('PULocationID').asc())
display(fareChangeHourLoc)

# COMMAND ----------

# fare changes day and hour
fareChangeDayHour = df.groupby("PU_day", "PU_hour").agg({"total_amount": "avg"})
fareChangeDayHour = fareChangeDayHour.sort(col("PU_day").asc(), col('PU_hour').asc())
display(fareChangeDayHour)

# COMMAND ----------

# fare changes day and location
fareChangeDayLoc = df.groupby("PU_day", "PULocationID").agg({"total_amount": "avg"})
fareChangeDayLoc = fareChangeDayLoc.join(df_zones, fareChangeDayLoc.PULocationID == df_zones.LocationID).drop(df_zones.LocationID)
fareChangeDayLoc = fareChangeDayLoc.sort(col('PU_day').asc(), col('PULocationID').asc())
display(fareChangeDayLoc)

# COMMAND ----------

# fare changes day, hour and location
fareChangeDayLoc = df.groupby("PU_day", "PU_hour", "PULocationID").agg({"total_amount": "avg"})
fareChangeDayLoc = fareChangeDayLoc.sort(col('PU_day').asc(), col('PU_hour').asc(), col('PULocationID').asc())
display(fareChangeDayLoc)

# COMMAND ----------

# tip change in dependence of the number of passengers
tipChange = df.groupby("passenger_count").agg({"tip_amount": "avg"})
tipChange = tipChange.sort(col("passenger_count").asc())
display(tipChange)

# COMMAND ----------

# linear regression or fake prices within distance?
priceByDistance = df.groupby("trip_distance").agg({"fare_amount": "avg"})
priceByDistance = priceByDistance.sort(col("trip_distance").asc())
display(priceByDistance)
