import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{Duration, LocalDateTime}

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    Logger.getLogger("org.apache.parquet").setLevel(Level.WARN)

    // for local
//    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    // for server
    val sparkConf = new SparkConf().setAppName("Test").setMaster(args(0))
    val sparkContext = new SparkContext(sparkConf)

    val sparkSession = SparkSession.builder().getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    // for local
//    val programmingLanguagesPath = "../data/programming-languages.csv"
    // for server
    val programmingLanguagesPath = args(1)

    // for local
//    val postsPath = "../data/posts_sample.xml"
    // for server
    val postsPath = args(2)

    val years = 2010 to 2020 map(_.toString)
    val topCount = 10

    val startTime = LocalDateTime.now()

    // raw rows of the programming languages data file
    val programmingLanguagesRDD = sparkContext.textFile(programmingLanguagesPath)
    // raw rows of the posts data file
    val postsRDD = sparkContext.textFile(postsPath)
    val postsCount = postsRDD.count

    // array of all the programming languages from the file
    val programmingLanguages = programmingLanguagesRDD
      // index all rows in the file
      .zipWithIndex
      // discard the first row
      .filter{ case (_, idx) => idx > 1}
      // discard row indices
      .map(_._1)
      // split each row into an array of strings
      .map{ row => row.split(",")}
      // discard the arrays with size not equal to 2
      .filter{ rowValues => rowValues.length == 2}
      // discard all elements of each array
      // except for the first one (i.e. the language name)
      .map{ rowValues => rowValues.toSeq.head.toLowerCase()}
      // convert rdd to array
      .collect()

    // array of the top topCount languages
    // by the language tags count in the file
    val topLanguagesTagsCountsInYears = postsRDD
      // index all rows in the file
      .zipWithIndex
      // discard the first two rows
      .filter{
        case (_, idx) => idx > 2 && idx<postsCount - 1
      }
      // discard row indices
      .map(_._1)
      // convert raw rows to xml elements
      .map(row => scala.xml.XML.loadString(row))
      // extract data and tags attributes
      // from each xml element
      .map(xmlElem => (
        xmlElem.attribute("CreationDate"),
        xmlElem.attribute("Tags")
      ))
      // discard elements with undefined date and/or tags
      .filter { x => x._1.isDefined && x._2.isDefined}
      // convert date and tags
      // from sequence nodes to strings
      .map{ x => (x._1.mkString, x._2.mkString)}
      // extract year from the date
      // and split the tags string to an array of tags
      .map(x => (
        x._1.substring(0, 4),
        x._2.substring(4, x._2.length - 4).split("&gt;&lt;")
      ))
      // discard the elements
      // with the year not in the specified range
      .filter(x => years.contains(x._1))
      // discard years
      .map(x => x._2)
      // flatten the rdd
      // so that each element represents a tag
      .flatMap(tags => tags.map(tag => tag))
      // discard the non-language tags
      .filter(tag => programmingLanguages.contains(tag))
      // group by tags
      .groupBy(tag => tag)
      // count tags in each group
      .map{
        case (tag, tags) => (tag, tags.count(_ => true))
      }
      // sort by tags count
      .sortBy{ case (_, count) => -count}
      // add an index which represents the rank of the language
      .zipWithIndex()
      // convert to the form: rank, language, tags count
      .map{case ((lang, tagsCount), rank) => (rank + 1, lang, tagsCount)}
      // take the first topCount elements
      .take(topCount)

    // convert to dataframe
    val dataFrame = sparkContext.parallelize(topLanguagesTagsCountsInYears).toDF("Rank", "Language", "Tags_Count")

    val stopTime = LocalDateTime.now()

    // print the dataframe as a table
    dataFrame.show()
    // save the dataframe to the report file
    dataFrame.write.parquet("report")
    // print the elapsed time taken to create the report
    println(s"duration ${ Duration.between(startTime, stopTime) }" )

    sparkContext.stop()
  }
}
