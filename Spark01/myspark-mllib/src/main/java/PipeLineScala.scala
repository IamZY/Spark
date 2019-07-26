import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SparkSession

object PipeLineScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PipelineExample")
      .master("local[4]")
      .getOrCreate()

    //数据源
    val df1 = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")


    //分词
    val token = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    //停用词
    val stop = new StopWordsRemover()
    stop.setInputCol("words")
    stop.setOutputCol("words2")

    //哈希词频
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(stop.getOutputCol)
      .setOutputCol("features")

    //逻辑回归
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    //组装管线
    val pipeline = new Pipeline().setStages(Array(token, stop, hashingTF, lr))
    val model = pipeline.fit(df1)

    //测试数据
    val test = spark.createDataFrame(Seq(
      (4L, "spark is a bigdata technolgy !"),
      (5L, "hi , "),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test).show(100,false)
  }
}
