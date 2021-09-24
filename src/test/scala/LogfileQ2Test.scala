import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
class LogfileQ2Test extends AnyFunSuite{
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark:SparkSession= SparkSession.builder()
    .master("local[*]")
    .appName("TestLogfile")
    .getOrCreate()
 val LogFile = List(("DEBUG, 2017-03-23T10:22:44+00:00, ghtorrent-7 -- ghtorrent.rb: Repo mithro/chromium-infra exists"),
   (" INFO, 2017-03-23T09:09:23+00:00, ghtorrent-40 -- ghtorrent.rb: Added issue_comment shadowsocks/shadowsocks-manager -> 143/287988535"),
   ("DEBUG, 2017-03-23T10:05:06+00:00, ghtorrent-33 -- ghtorrent.rb: User Unity-Technologies exists"),
   ("DEBUG, 2017-03-23T11:17:57+00:00, ghtorrent-39 -- retriever.rb: issues sojamo/controlp5 -> 69 exists"),
   ("DEBUG, 2017-03-23T09:14:02+00:00, ghtorrent-43 -- ghtorrent.rb: User apollographql exists"),
   ("WARN, 2017-03-23T10:13:22+00:00, ghtorrent-16 -- ghtorrent.rb: Transaction failed (51638 ms)"),
   ("DEBUG, 2017-03-23T11:02:49+00:00, ghtorrent-3 -- ghtorrent.rb: User dongzhixiong exists)"),
   ("WARN, 2017-03-23T11:07:16+00:00, ghtorrent-35-- api_client.rb: Successful request. URL: https://api.github.com/repos/fuzitu/FE-interview/issues?state=closed&per_page=100, Remaining: 3508, Total: 68 ms"),
   ("WARN, 2017-03-23T11:07:16+00:00, ghtorrent-35 -- api_client.rb: Successful request. URL: https://api.github.com/repos/fuzitu/FE-interview/issues?state=closed&per_page=100, Remaining: 3508, Total: 68 ms"))
  import spark.implicits._
  val ListdataFrame: DataFrame = LogFile.toDF
  val data1 = Service.ParsedDF(ListdataFrame.as[String])

  assert(Service.Line_Count(data1)===9)
  assert(Service.Waring_Count(data1)===3)
  assert(Service.Repository_count(data1)===2)


}
