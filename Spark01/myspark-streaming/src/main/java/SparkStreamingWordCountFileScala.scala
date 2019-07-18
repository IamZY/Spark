import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object SparkStreamingWordCountFileScala {
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

    val result = pair.reduceByKey(_ + _)
    // 输出大量的小文件
    // result.saveAsTextFiles("file:///e:/streaming","dat")

    // 每一个小时保存一次结果
    result.foreachRDD(rdd => {
      val rdd2 = rdd.mapPartitionsWithIndex((idx, it) => {
        val buf: ArrayBuffer[(Int, (String, Int))] = ArrayBuffer[(Int, (String, Int))]()
        for (t <- it) {
          buf.+=((idx, t))
        }
        buf.iterator
      })

      rdd2.foreachPartition(it => {
        val now = new Date()
        val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
        // 格式化时间串
        val strDate = sdf.format(now)

        val itt = it.take(1)

        if(!itt.isEmpty){
          val par = itt.next()._1
          val file = strDate + "-" + par + ".dat"
          // 需要手动创建文件夹 向文件中追加
          val fout = new FileOutputStream(new File("e:/stream", file),true)
          for (t <- it) {
            val word = t._2._1
            val cnt = t._2._2
            fout.write((word + "\t" + cnt + "\r\n").getBytes())
            fout.flush()
          }

          fout.close()
        }


      })

    })


    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}
