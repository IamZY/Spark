import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWordCountUpdateStateByKeyScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // * 动态提取cpu核数
    conf.setMaster("local[*]")
    conf.setAppName("StreamingWordsCount")
    // 流上下文
    val ssc = new StreamingContext(conf, Seconds(2));
    val lines = ssc.socketTextStream("192.168.52.154", 8888)

    val words = lines.flatMap(_.split(" "))
    val pair = words.map((_, 1))


    val result = pair.reduceByKey((a: Int, b: Int) => {
      a + b
    });

    def updateFunc(vs: Seq[Int], st: Option[Int]): Option[Int] = {
      var currCount = 0
      if (vs != null && !vs.isEmpty) {
        currCount = vs.sum
      }
      var newSt = currCount
      if (!st.isEmpty) {
        newSt = newSt + st.get
      }
      Some(newSt)
    }

    result.updateStateByKey(updateFunc).print()

    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}
