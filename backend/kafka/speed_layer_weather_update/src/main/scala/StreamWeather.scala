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
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object StreamWeather {
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
  val table = hbaseConnection.getTable(TableName.valueOf("leimao_last_one_year_weather_chicago"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamWeather")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("leimao_weather_update")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[WeatherReport]))

    // How to write to an HBase table
    val batchStats = reports.map(wr => {
      val result = table.get(new Get(Bytes.toBytes(wr.date)))
      if (result.isEmpty()) {
        val put = new Put(Bytes.toBytes(wr.date))
        put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fog"), Bytes.toBytes(wr.fog))
        put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("rain"), Bytes.toBytes(wr.rain))
        put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("snow"), Bytes.toBytes(wr.snow))
        put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("hail"), Bytes.toBytes(wr.hail))
        put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("thunder"), Bytes.toBytes(wr.thunder))
        put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("tornado"), Bytes.toBytes(wr.tornado))
        table.put(put)
      }
      else {
        None
      }
    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
