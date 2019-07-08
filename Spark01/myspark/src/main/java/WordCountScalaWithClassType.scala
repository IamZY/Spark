import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")
    // 创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("file:///d:/hello.txt")

    val rdd2: RDD[String] = rdd1.flatMap((e: String) => {
      val arr: Array[String] = e.split(" ")
      arr
    })

    //
    val rdd3: RDD[Tuple2[String, Int]] = rdd2.map(e => {
      new Tuple2[String, Int](e, 1)
    })
    //    val rdd3 = rdd2.map((_, 1))

    def add(a: Int, b: Int): Int = a + b

    val rdd4: RDD[Tuple2[String, Int]] = rdd3.reduceByKey((add _))

    val arr: Array[Tuple2[String, Int]] = rdd4.collect()

    for (t <- arr) {
      print(t._1 + "::" + t._2)
    }

  }
}
