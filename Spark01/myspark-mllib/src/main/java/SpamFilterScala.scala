import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * 垃圾邮件分类
  */
object SpamFilterScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mlline")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 哈希词频 维度
    val tf = new HashingTF(1000)

    // 垃圾邮件
    val spam_Rdd1 = spark.sparkContext.textFile("file:///e:/spam.txt")
    // 常规邮件
    val normal_Rdd1 = spark.sparkContext.textFile("file:///e:/normal.txt")

    // 垃圾邮件向量rdd
    val spam_vec_rdd = spam_Rdd1.map(e => tf.transform(e.split(" ")))
    val normal_vec_rdd = normal_Rdd1.map(e => tf.transform(e.split(" ")))

    // 垃圾邮件的正样本
    val spam_lb_rdd = spam_vec_rdd.map(LabeledPoint(1, _))
    // 垃圾邮件的负样本
    val normal_lb_rdd = normal_vec_rdd.map(LabeledPoint(0, _))
    // 合并正负样本
    val sample_rdd = spam_lb_rdd.union(normal_lb_rdd)
    // SGD
    val model = new LogisticRegressionWithSGD().run(sample_rdd)

    val posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    println(model.predict(posTest))

    val negTest = tf.transform("Hi dad, I started studying Spark the other ...".split(" "))
    println(model.predict(negTest))


  }
}
