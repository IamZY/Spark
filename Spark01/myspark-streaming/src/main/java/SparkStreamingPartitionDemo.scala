import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingPartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // * 动态提取cpu核数
    conf.setMaster("local[*]")
    conf.setAppName("StreamingWordsCount")
    conf.set("spark.streaming.blockInterval", "2000ms");
    // 流上下文
    val ssc = new StreamingContext(conf, Seconds(2));
    val lines = ssc.socketTextStream("localhost", 8888)

    //action
    lines.foreachRDD(rdd => {
      println(rdd.partitions.length)
    })

    // 启动上下文
    ssc.start()
    ssc.awaitTermination()

  }
}
