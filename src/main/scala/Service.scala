import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

object Service {

// Read Log text file

  def readDF(path:String)(implicit spark:SparkSession): Dataset[String] = {
    spark.read.textFile(path)
  }

  // parsing the given LOG file
  def ParsedDF(readDF: Dataset[String])(implicit spark:SparkSession):DataFrame = {
    import spark.implicits._
    val ParseData = readDF.select(regexp_extract($"value", """([^(\s|,)]+)""",1).alias("LoggingLevel"),
      regexp_extract($"value","""([^\s]+)\+00:00""",1).as("timestamp"),
      regexp_extract($"value","""ghtorrent-([^\s]+)""",1).cast("int").as("downloaderId"),
      regexp_extract($"value","""([^\s]+).rb:""",1).alias("retrievalStage"),
    regexp_extract($"value","""([^\s]+)$""",1).as("remaining"))

    ParseData


  }

  // 2. how many lines does the RDD contain?
  def Line_Count(data:DataFrame):Long =data.count()

  //3. Count number of WARNING messages
  def Waring_Count(data:DataFrame)(implicit spark:SparkSession):Long ={
    import spark.implicits._
    val WarningMSG = data.filter($"LoggingLevel"==="WARN").distinct().count()
    WarningMSG
  }

  //4. how many repositories  where processed in total? Use the api_client lines only
  def Repository_count(data:DataFrame)(implicit spark:SparkSession):Long={
    import spark.implicits._
    val Repo = data .filter($"retrievalStage"==="api_client").distinct().count()

    Repo
  }

  //5.which client did the most http request
  def client_http_Request(data:DataFrame)(implicit spark:SparkSession):Int={
    import spark.implicits._
    val httpRequest = data.select("retrievalStage","downloaderId").filter($"retrievalStage"==="api_client")
      .groupBy("downloaderId").count()
      .sort($"count".desc).toDF()
      .first().getInt(0)
    httpRequest
  }

  //6.Which client did most FAILED HTTP requests? Use group_by to provide an answer.

  def client_failed_http(data:DataFrame)(implicit spark:SparkSession):Int={
    import spark.implicits._
    val failedHttpRequest = data.select("retrievalStage","remaining","downloaderId")
      .filter($"retrievalStage"==="api_client")
      .filter($"LoggingLevel"==="WARN")
      .groupBy("downloaderId").count()
      .sort($"count".desc).first().getInt(0)
    failedHttpRequest
  }

  //7. What is the most active hour of day.

  def active_Hour(data: DataFrame)(implicit spark:SparkSession):String = {
    import spark.implicits._
    val active = data
      .withColumn("time", functions.split($"timeStamp", "T").getItem(1))
      .groupBy($"time").count().sort($"count".desc).first().getString(0)
    active
  }

}