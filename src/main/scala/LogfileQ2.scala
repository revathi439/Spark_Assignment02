import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LogfileQ2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
implicit val spark: SparkSession=  SparkSession.builder()
  .master("local[*]")
  .appName("Read LogFile")
  .getOrCreate()

  // Function to data into RDD
  val LogDF = Service.readDF("src/main/resources/ghtorrent-logs.txt")
  val  ParsingDF = Service.ParsedDF(LogDF)

  // Number of lines count
  val countLinesDF = Service.Line_Count(ParsingDF)
  println("Count of Lines is : "+countLinesDF)

  //Counting number of warnings
  val WarningsCountDF = Service.Waring_Count(ParsingDF)
  println("Count of Warnings is: "+WarningsCountDF)

  //Repositories Count
  val RepositoryCountDF = Service.Repository_count(ParsingDF)
  println("Count of Repositories :"+RepositoryCountDF)

 // finding which client did the most http request
  val mostHttpRequestDF = Service.client_http_Request(ParsingDF)
  println("The client who did most http request : "+mostHttpRequestDF)

  // client failed the http request
  val failHttpReqDF = Service. client_failed_http(ParsingDF)
  println("The client who failed to the http request :"+failHttpReqDF)

//What is the most active hour of day
val activeHour = Service.active_Hour(ParsingDF)
  println("most active hour of the day: " + activeHour)

}
