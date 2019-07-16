import org.apache.spark.{SparkConf, SparkContext}

object MaxTempScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")
    // 创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file:///d:/temp3.dat")

    val rdd2 = rdd1.map(line=>{
      val arr = line.split(" ")
      (arr(0).toInt,arr(1).toInt)
    })

    val rdd3 = rdd2.reduceByKey((a,b)=>{
      Math.max(a,b)
    })

//    rdd3.collect().foreach(println)
    val arr = rdd3.collect();
    val arr2 = arr.sortBy(t=>t._1);

    arr2.foreach(println)

  }
}
