import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWordCountParttionNumScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // * 动态提取cpu核数
    conf.setMaster("local[*]")
    conf.setAppName("StreamingWordsCount")
    // 流上下文
    val ssc = new StreamingContext(conf, Seconds(2));
    val lines = ssc.socketTextStream("localhost", 8888)

    lines.foreachRDD(rdd=>{
      val part = rdd.getNumPartitions
      println(part)
    })

//    result.print()
    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}
