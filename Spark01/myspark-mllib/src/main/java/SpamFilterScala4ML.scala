import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, LabeledPoint}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession


/**
  * 垃圾邮件分类
  */
object SpamFilterScala4ML {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ml_linearRegress")
    conf.setMaster("local[*]")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //哈希词频
    val tf = new HashingTF()
    tf.setInputCol("words")
    tf.setOutputCol("features")
    tf.setNumFeatures(1000)

    //垃圾邮件
    val rdd10 = spark.sparkContext.textFile("e:\\spam.txt")
    val rdd11 = rdd10.map(_.split(" "))
    val df10 = rdd11.toDF("words")

    val df11 = tf.transform(df10).map(r => {
      new LabeledPoint(1, r.getAs[SparseVector]("features"))
    })
    println("===spam====")
    df11.show(10, false)


    //常规邮件
    val rdd20 = spark.sparkContext.textFile("e:\\normal.txt")
    val rdd21 = rdd20.map(_.split(" "))
    val df20 = rdd21.toDF("words")

    val df21 = tf.transform(df20).map(r => {
      new LabeledPoint(0, r.getAs[SparseVector]("features"))
    })
    println("===normal====")
    df21.show(100, false)


    val sample = df11.union(df21)

    println("===sample====")
    sample.show(1000, false)

    //切割数据
    val Array(train, test) = sample.randomSplit(Array(0.8, 0.2))
    val lr = new LogisticRegression()
    val model = lr.fit(train)
    val result = model.transform(test)
    result.show(1000, false)

  }
}
