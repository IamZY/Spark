import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  *
  */
object LogisticRegressionWhiteDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mlline")

    //创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val rdd1 = spark.sparkContext.textFile("file:///e:/white.csv");

    import spark.implicits._

    val df1 = rdd1.map(line=>{
      val arr = line.split(";")
      val label = if(arr(11).toDouble > 7) 1 else 0
      val vec = Vectors.dense(
        arr(0).toDouble,
        arr(1).toDouble,
        arr(2).toDouble,
        arr(3).toDouble,
        arr(4).toDouble,
        arr(5).toDouble,
        arr(6).toDouble,
        arr(7).toDouble,
        arr(8).toDouble,
        arr(9).toDouble,
        arr(10).toDouble
      )

      (label,vec)
    }).toDF("label","features")


    val Array(trainData,testData) = df1.randomSplit(Array(0.8,0.2))

    val lr = new LogisticRegression()
    lr.setMaxIter(3)
    val model = lr.fit(trainData)

    val ret = model.transform(testData)

    ret.show(100,false)

  }
}
