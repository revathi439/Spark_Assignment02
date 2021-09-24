
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
  object Service1{


    // Read Log text file

    def readDF(path:String)(implicit spark:SparkSession): Dataset[String] = {
      spark.read.textFile(path)
    }

    // parsing the given LOG file
    def ParsedDF(readDF: Dataset[String])(implicit spark:SparkSession):DataFrame = {

      val Data = readDF
        .withColumn("loggingLevel", functions.split(col("value"), ",").getItem(0))
        .withColumn("timestamp", regexp_extract(col("value"), "([^\\s]+)\\+00:00", 1))
        .withColumn("downloaderId", regexp_extract(col("value"), "ghtorrent-([^\\s]+)", 1).cast("Int"))
        .withColumn("retrievalStage", regexp_extract(col("value"), "([^\\s]+).rb:", 1))
        .withColumn("remaining", regexp_extract(col("value"), "(.*$)", 1))
        .drop("value")
      Data.cache()
    }

    // 8. What is the most active repository

    def mostActiveRepository(data: DataFrame):String = {

      val active_repository =data
        .filter(col("retrievalStage") === "api_client")
        .withColumn("repository1", split(col("remaining"), "/").getItem(4))
        .withColumn("repository2", split(col("remaining"), "/").getItem(5))
        .select(concat(col("repository1"), lit("/"), col("repository2")).as("repository")).na.drop()
        .groupBy(col("repository")).count().sort(col("count").desc).first().getString(0)
      active_repository


    }

  }
