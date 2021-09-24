import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

  object Logfile8 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    implicit val spark: SparkSession=  SparkSession.builder()
      .master("local[*]")
      .appName("Read LogFile")
      .getOrCreate()

    // Function to data into RDD
    val LogDF = Service1.readDF("src/main/resources/ghtorrent-logs.txt")
    val  ParsingDF = Service1.ParsedDF(LogDF)
    //What is the most active repository
 val mostactiveRepo = Service1.mostActiveRepository(ParsingDF)
 println("most active repository : " + mostactiveRepo)
  }
