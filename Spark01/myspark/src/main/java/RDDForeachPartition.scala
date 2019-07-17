import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object RDDForeachPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")
    // 创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file:///d:/source/hello.txt")

    val rdd2 = rdd1.flatMap(_.split(" "))

    val rdd3 = rdd2.map((_, 1))

    val rdd4 = rdd3.reduceByKey((_ + _))

//    val rdd5 = rdd4.mapPartitionsWithIndex((index, it) => {
//      for (e <- it) {
//        System.out.println(index + "::" + e)
//      }
//      it
//    })

//    val arr = rdd5.collect()
    rdd4.foreachPartition(it=>{
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/big13","root","123456")
      conn.setAutoCommit(false)
      val sql = "insert into wc(word,cnt) values(?,?)"
      val ppst = conn.prepareStatement(sql)
      for(t<-it){
        val word = t._1
        val cnt = t._2
        ppst.setString(1,word)
        ppst.setInt(2,cnt)
        ppst.executeUpdate()
      }
      conn.commit()
      ppst.close()
      conn.close()
      System.out.println("over..")
    })

//    arr.foreach(println(_))

  }
}
