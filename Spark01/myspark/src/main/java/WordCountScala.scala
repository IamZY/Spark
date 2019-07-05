import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")
    // 创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file:///d:/hello.txt")

    val rdd2 = rdd1.flatMap(_.split(" "))

    val rdd3 = rdd2.map((_, 1))

    val rdd4 = rdd3.reduceByKey((_ + _))

    val arr = rdd4.collect()

    arr.foreach(println(_))

  }
}
