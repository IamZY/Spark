import org.apache.spark.sql.SparkSession

/**
  * 将DataFrame保存json
  */
object SparkSQLJsonWriteDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").enableHiveSupport().getOrCreate()
    val rdd1 = spark.sparkContext.textFile("/user/custs.txt")

    import spark.implicits._

    val df1 = rdd1.map(line=>{
      val arr = line.split(",")
      (arr(0).toInt,arr(1),arr(2).toInt)
    }).toDF("id","name","age")

    df1.show(1000,false)
    df1.where("id > 2").write.json("file:///e:/json")



  }
}
