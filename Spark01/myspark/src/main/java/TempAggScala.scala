import org.apache.spark.{SparkConf, SparkContext}

object TempAggScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TempAggScala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("file:///d:/temp3.dat")

    val rdd2 = rdd1.map(line=>{
      val arr = line.split(" ")
      val year = arr(0).toInt
      val temp = arr(1).toInt
      //
      (year,(temp,temp,temp,1,temp.toDouble))
    })

  rdd2.reduceByKey((t1,t2)=>{
    val max = Math.max(t1._1,t2._1)
    val min = Math.min(t1._2,t2._2)
    val sum = t1._3 + t2._3
    val cnt = t1._4 + t2._4
    val avg = sum.toDouble/cnt
    (max,min,sum,cnt,avg)
  }).sortByKey().collect().foreach(println)



  }
}
