import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  *
  */
object RegexTokenizerScala {
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
    val token = new RegexTokenizer()
    token.setInputCol("sentence")
    token.setOutputCol("words")
    token.setPattern("\\W")
    val df2 = token.transform(df1);
    df2.show(10, false)

    val tf = new HashingTF()
    tf.setNumFeatures(1000)
    tf.setInputCol("words")
    tf.setOutputCol("features")
    val df3 = tf.transform(df2)
    df3.show(10,false)

  }
}
