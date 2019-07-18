import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
 */
object SparkStreamingSQLWordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // * 动态提取cpu核数
    conf.setMaster("local[*]")
    conf.setAppName("StreamingWordsCount")

    // SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 流上下文
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 8888)

    // 压扁变成单词流
    val words = lines.flatMap(lines=>{
      lines.split(" ")
    })

    words.foreachRDD(rdd=>{
      import spark.implicits._
      val df = rdd.toDF("word")
      df.createOrReplaceTempView("_doc")
      spark.sql("select word,count(*) from _doc group by word").show(1000,false)
    })

    // 启动上下文
    ssc.start()

    ssc.awaitTermination()

  }
}
