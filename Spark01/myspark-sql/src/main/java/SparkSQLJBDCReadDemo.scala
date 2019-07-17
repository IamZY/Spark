import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 将DataFrame保存json
  */
object SparkSQLJBDCReadDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()

    val url = "jdbc:mysql://localhost:3306/big13"
    var table = "custs"
    val prop = new Properties()
    prop.put("drivers", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "123456")

    val df1 = spark.read.jdbc(url, table, prop)
    df1.show(1000, false)

  }
}
