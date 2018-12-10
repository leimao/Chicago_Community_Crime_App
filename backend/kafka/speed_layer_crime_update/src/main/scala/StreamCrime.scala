import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamCrime {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  
  // Use the following two lines if you are building for the cluster 
  // hbaseConf.set("hbase.zookeeper.quorum","class-m-0-20181017030211.us-central1-a.c.mpcs53013-2018.internal")
  // hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  
  // Use the following line if you are building for the VM
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val communityCrimeByWeatherTable = hbaseConnection.getTable(TableName.valueOf("leimao_community_crime_by_weather"))
  val weatherTable = hbaseConnection.getTable(TableName.valueOf("leimao_last_one_year_weather_chicago"))
  
  def getLatestWeather(date: String) = {
      val result = weatherTable.get(new Get(Bytes.toBytes(date)))
      System.out.println(result.isEmpty())
      if(result.isEmpty())
        None
      else
        Some(WeatherReport(
              date,
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("fog"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("rain"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("snow"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("hail"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("thunder"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("tornado")))))
  }
  
  def incrementCrimeByWeather(cr : CrimeReport) : String = {
    val date = "%04d".format(cr.year) + "%02d".format(cr.month) + "%02d".format(cr.day)
    val maybeLatestWeather = getLatestWeather(date)
    if (maybeLatestWeather.isEmpty)
      return "No weather on " + date;
    val latestWeather = maybeLatestWeather.get
    val inc = new Increment(Bytes.toBytes(cr.community))
    //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("days"), 1)
    inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("crime_sum"), 1)
    if(latestWeather.fog) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("fog_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("fog_crime_sum"), 1)
    }
    if(latestWeather.rain) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("rain_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("rain_crime_sum"), 1)
    }
    if(latestWeather.snow) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("snow_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("snow_crime_sum"), 1)
    }
    if(latestWeather.hail) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("hail_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("hail_crime_sum"), 1)
    }
    if(latestWeather.thunder) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("thunder_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("thunder_crime_sum"), 1)
    }
    if(latestWeather.tornado) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("tornado_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("tornado_crime_sum"), 1)
    }
    if ((!latestWeather.fog) && (!latestWeather.rain) && (!latestWeather.snow) && (!latestWeather.hail) && (!latestWeather.thunder) && (!latestWeather.tornado)) {
      //inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("clear_days"), 1)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("clear_crime_sum"), 1)
    }
    communityCrimeByWeatherTable.increment(inc)
    return "Updated speed layer for crime in community " + cr.community
}
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamCrime <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamCrime")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("leimao_crime_update")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
   
    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[CrimeReport]))

    // Update speed table    
    val processedCrime = kfrs.map(incrementCrimeByWeather)
    processedCrime.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}