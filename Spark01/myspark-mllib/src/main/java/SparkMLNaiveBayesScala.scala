import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 朴素贝叶斯分类
  */
object SparkMLNaiveBayesScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ml").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val rdd1 = spark.read.format("libsvm").load("file:///e:/sample_libsvm_data.txt")

    val Array(trainData,testData) = rdd1.randomSplit(Array(0.7,0.3),seed = 1234L)
    val model = new NaiveBayes().fit(trainData)
    val ret = model.transform(testData)
    ret.select("label","prediction").show()
  }
}
