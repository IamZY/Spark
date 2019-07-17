import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 将DataFrame保存json
  */
object SparkSQLJDBCWriteDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
    val rdd1 = spark.sparkContext.textFile("/user/custs.txt")

    import spark.implicits._

    val df1 = rdd1.map(line => {
      val arr = line.split(",")
      (arr(0).toInt, arr(1), arr(2).toInt)
    }).toDF("id", "name", "age")

    df1.show(1000, false)
    val url = "jdbc:mysql://localhost:3306/big13"
    var table = "custs"
    val prop = new Properties()
    prop.put("drivers", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "123456")
    df1.where("id > 2").write.jdbc(url, table, prop)


  }
}
