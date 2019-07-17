import org.apache.spark.sql.SparkSession

object SparkSQLDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()

    val rdd1 = spark.sparkContext.textFile("/user/zangyang.txt")

    val rdd2 = rdd1.flatMap(_.split(" "))
    // 导入SparkSession的隐式转换
    import spark.implicits._
    // 将rdd转换成数据框
    val df = rdd2.toDF("word")
    // 将数据框注册成临时视图
    df.createOrReplaceTempView("_doc")

    spark.sql("select word,count(*) from _doc group by word").show(1000,false)
  }
}
