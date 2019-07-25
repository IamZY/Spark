import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
  * 多元分类模型计算器
  */
object MuticlassEvaluationScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mlline")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val rdd1 = spark.sparkContext.makeRDD(Array(
      (1, 1), (1, 1), (1, 0), (1, 1), (1, 1), (0, 0), (0, 1), (0, 0), (0, 1), (0, 0)
    ))

    import spark.implicits._

    val df = rdd1.map(t => (t._1.toDouble, t._2.toDouble)).toDF("label", "prediction")

    df.show(100, false)

    val e = new MulticlassClassificationEvaluator()
    e.setLabelCol("label")
    e.setPredictionCol("prediction")
    // f1,accuracy,weightedRecall
    e.setMetricName("weightedPrecision")
    val ret = e.evaluate(df)

    // 0.7083333333333333
    println(ret)

  }
}
