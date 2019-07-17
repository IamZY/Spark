import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object SparkStreamingWordCountUpdateStateByKeyScala2 {
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

    // 更新状态函数
    def updateFunc(vs: Seq[Int], st: Option[ArrayBuffer[(Long, Int)]]): Option[ArrayBuffer[(Long, Int)]] = {
      // 新状态
      val buf = new ArrayBuffer[(Long, Int)]()
      // 提取当前时间毫秒数
      var ms = System.currentTimeMillis()
      var currCount = 0
      // 当前v的数量
      if (vs != null && !vs.isEmpty) {
        currCount = vs.sum
      }
      //      var oldBuf = ArrayBuffer
      if (!st.isEmpty) {
        // 取出旧状态
        val oldBuf = st.get
        for (t <- oldBuf) {
          if (ms - t._1 < 10000) {
            buf.+=(t)
          }
        }
      }
      if (currCount != 0) {
        buf.+=((ms, currCount))
      }

      if (buf.isEmpty) {
        None
      } else {
        Some(buf)
      }

    }

    result.updateStateByKey(updateFunc).print()

    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}
