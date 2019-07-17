import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object RDDSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Seq("hello1","hello2","hello3","hello2"))
    rdd1.collect().foreach(println)
    System.out.println("===========")
    val rdd2 = rdd1.sample(true,0.1,1000)
    rdd2.collect().foreach(println)

  }
}
