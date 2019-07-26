import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession

/**
  * 分词器
  */
object TokenizerScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ml_linearRegress")
    conf.setMaster("local[*]")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //数据
    val df1 = spark.createDataFrame(Seq(
      (0, "Hi ! I heard about Spark ."),
      (1, "I wish \t Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    df1.show(10, false)

    //创建分词器
    val token = new Tokenizer()
    token.setInputCol("sentence")
    token.setOutputCol("words")
    val df2 = token.transform(df1);
    df2.show(10, false)

  }
}
