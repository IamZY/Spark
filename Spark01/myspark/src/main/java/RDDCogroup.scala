import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相当于mysql中的union 操作 不去重
  *
  */
object RDDCogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Seq((1,"1_1"),(1,"1_2"),(2,"2_1"),(2,"2_3")))
    val rdd2 = sc.makeRDD(Seq((1,"beijing"),(1,"上海"),(2,"广州"),(2,"深圳")))

    rdd1.cogroup(rdd2).foreach(println)

  }
}
