import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相当于mysql中的union 操作 不去重
  *
  */
object RDDUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(1 to 10)
    val rdd2 = sc.makeRDD(1 to 4)
    val rdd3 = rdd1.union(rdd2)
    // 取交集 同型集合
    val rdd4 = rdd1.intersection(rdd2)

    val rdd5 = rdd1.map(e => {
      (if (e % 2 == 0) {
        "even"
      } else {
        "odd"
      }, e)
    })

    val rdd6 = rdd5.groupByKey()

    rdd6.collect().foreach(println)

    //    rdd3.collect().foreach(println)
    //    rdd4.collect().foreach(println)
    //
    //    rdd3.distinct().foreach(println)

  }
}
