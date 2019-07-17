import org.apache.spark.sql.SparkSession

/**
  * 加载Json数据成DataFrame
  */
object SparkSQLJsonReadDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()

    val df1 = spark.read.json("file:///e:/json")

    df1.show(1000,false)

  }
}
