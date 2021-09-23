import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

case class Station(
                    stationId:Integer,
                    name:String,
                    lat:Double,
                    long:Double,
                    dockcount:Integer,
                    landmark:String,
                    installation:String)

case class Trip(
                 tripId:Integer,
                 duration:Integer,
                 startDate:LocalDateTime,
                 startStation:String,
                 startTerminal:Integer,
                 endDate:LocalDateTime,
                 endStation:String,
                 endTerminal:Integer,
                 bikeId: Integer,
                 subscriptionType: String,
                 zipCode: String)

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
//    val cfg = new SparkConf().setAppName("Test").setMaster(args(0))
    val sc = new SparkContext(cfg)

    val tripData = sc.textFile("file:///D:/Projects/univer/big-data-labs/data/trips.csv")
//    val tripData = sc.textFile(args(1))
    val trips = tripData.map(row=>row.split(",",-1))

    val stationData = sc.textFile("file:///d:/projects/univer/big-data-labs/data/stations.csv")
//    val stationData = sc.textFile(args(2))
    val stations = stationData.map(row=>row.split(",",-1))

    val tripsInternal = trips.mapPartitions(rows => {
      val timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm")
      rows.map( row =>
        Trip(tripId = row(0).toInt,
          duration = row(1).toInt,
          startDate = LocalDateTime.parse(row(2), timeFormat),
          startStation = row(3),
          startTerminal = row(4).toInt,
          endDate = LocalDateTime.parse(row(5), timeFormat),
          endStation = row(6),
          endTerminal = row(7).toInt,
          bikeId = row(8).toInt,
          subscriptionType = row(9),
          zipCode = row(10)))})

    val stationsInternal = stations.mapPartitions(rows => {
      rows.map(row =>
        Station(stationId = row(0).toInt,
          name = row(1),
          lat = row(2).toDouble,
          long = row(3).toDouble,
          dockcount = row(4).toInt,
          landmark = row(5),
          installation = row(6)))})

    println
    println("______")
    println("Task 1")
    val bikeWithMaxMileage = tripsInternal
      .keyBy(record => record.bikeId)
      .mapValues(x => x.duration)
      .groupByKey()
      .mapValues(col => col.reduce((a, b) => a + b))
      .reduce((acc, value) => {if(acc._2 < value._2) value else acc})
    println(bikeWithMaxMileage)

    println
    println("______")
    println("Task 2")
    val longestDistanceStationsDistance = stationsInternal
      .cartesian(stationsInternal)
      .collect()
      .map(station => (station, math.sqrt(math.pow(station._1.lat - station._2.lat, 2) + math.pow(station._1.long - station._2.long, 2)) * 111.139))
      .reduce((acc, value) => {if(acc._2 < value._2) value else acc})
    println(String.format("Longest distance: " + longestDistanceStationsDistance._2) + " km from " + longestDistanceStationsDistance._1._1.name + " to " + longestDistanceStationsDistance._1._2.name)

    println
    println("______")
    println("Task 3")
    println("Bike track through stations: ")
    tripsInternal
      .filter(trip => trip.bikeId==bikeWithMaxMileage._1)
      .groupBy(_.bikeId)
      .mapValues(x => x.toList.sortWith((trip1, trip2) => trip1.startDate.compareTo(trip2.startDate) < 0))
      .foreach(x => x._2
        .take(10)
        .foreach(x => println("from " + x.startStation + " to " + x.endStation)))

    println
    println("______")
    println("Task 4")
    println(tripsInternal.keyBy(_.bikeId).groupByKey().count())

    println
    println("______")
    println("Task 5")
    tripsInternal
      .filter(trip => trip.endDate.toEpochSecond(ZoneOffset.UTC) - trip.startDate.toEpochSecond(ZoneOffset.UTC) > 3 * 60 * 60)
      .take(10)
      .foreach(println)

    sc.stop()
  }

}
