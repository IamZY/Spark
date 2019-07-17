import org.apache.spark.sql.SparkSession

object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
    spark.sql("show databases").show()
    spark.sql("use hive1").show()
    spark.sql("show tables").show()
    spark.sql("select * from usr").show()
//    spark.sql("row format delimited fields terminated by ',' stored as sequencefile;")
  }
}
