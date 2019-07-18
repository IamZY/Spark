import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingKafkaLocationStrategyScala {

  def sendInfo(obj: Object, m: String, param: String) = {
    import java.net.InetAddress
    import java.lang.management.ManagementFactory
    val ip = InetAddress.getLocalHost.getHostAddress
    val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
    val tid = Thread.currentThread().getId
    val classname = obj.getClass.getSimpleName
    val objHash = obj.hashCode()
    val info = ip + "/" + pid + "/" + tid + "/" + classname + "@" + objHash + "/" + m + "(" + param + ")" + "\r\n"

    //发送数据给nc 服务器
    val sock = new java.net.Socket("192.168.52.154", 8888)
    val out = sock.getOutputStream
    out.write(info.getBytes())
    out.flush()
    out.close()
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreamingKafka")
//    conf.setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(2))

    // kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.52.154:9092,192.168.52.155:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic1")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      // 首选一致性
      PreferBrokers,
      //
      Subscribe[String, String](topics, kafkaParams)
    )

    val kv = stream.map(r => {
      val t = r.topic()
      val p = r.partition()
      val offset = r.offset()
      val v = r.value()
      val str = t + "/" + p + "/" + offset + "/" + v
      sendInfo(this, "map", str)
    }).print()

//    kv.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
