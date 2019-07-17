import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相当于mysql中的union 操作 不去重
  *
  */
object RDDJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Seq((1,"tom1"),(2,"tom2"),(3,"tom3")))
    val rdd2 = sc.makeRDD(Seq((1,600),(2,400),(3,900)))

    // 左外连接
//    rdd1.fullOuterJoin()
    rdd1.join(rdd2).foreach(println)


  }
}
