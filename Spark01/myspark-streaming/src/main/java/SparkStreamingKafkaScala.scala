import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingKafkaScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreamingKafka")
    conf.setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(2))

    // kafka参数
    val kafkaParams = Map[String,Object](
      "bootstrap.servers"->"192.168.52.154:9092,192.168.52.155:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic1")
    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      // 首选一致性
      PreferConsistent,
      //
      Subscribe[String,String](topics,kafkaParams)
    )

    val kv = stream.map(rdd=>{
      (rdd.key(),rdd.value())
    })

    kv.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
