import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  *
  */
object IDFScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ml_linearRegress")
    conf.setMaster("local[*]")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //句子数据框
    val df1 = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "I Logistic regression models are neat")
    )).toDF("label", "sentence")
    df1.show(10, false)

    //分词
    val token = new Tokenizer();
    token.setInputCol("sentence")
    token.setOutputCol("words")
    val df2 = token.transform(df1)
    df2.show(10, false)

    //哈希词频
    val tf = new HashingTF()
    tf.setInputCol("words")
    tf.setOutputCol("rawFeatures")
    tf.setNumFeatures(20)
    val df3 = tf.transform(df2)
    df3.show(10, false)

    //训练idf模型
    val idf = new IDF()
    idf.setInputCol("rawFeatures")
    idf.setOutputCol("features")
    val model = idf.fit(df3)

    //应用idf模型，产生结果
    val result = model.transform(df3)
    result.show(10, false)
  }
}
