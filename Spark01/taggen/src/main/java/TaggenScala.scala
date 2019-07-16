import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 标签生成
  */
object TaggenScala {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.setAppName("Taggen")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    // 加载文件
    val rdd1 = sc.textFile("file:///d:/source/temptags.txt")

    // 变换
    val rdd2 = rdd1.map(line => {
      val arr = line.split("\t")
      val busid = arr(0)
      val json = arr(1)
      val tagBuffer = parseJson(json)
      (busid, tagBuffer)
    })

    // 过滤

    val rdd3 = rdd2.filter(e => {
      e._2 != null && e._2.size > 0
    })
    // 压扁
    val rdd4 = rdd3.flatMapValues(list => list)

    val rdd5 = rdd4.map(e => (e, 1))


    // 聚合算值
    val rdd6 = rdd5.reduceByKey(_ + _)

    val rdd7 = rdd6.map(t => {
      (t._1._1, (t._1._2, t._2))
    })

    val rdd8 = rdd7.groupByKey()

    val rdd9 = rdd8.mapValues(it => it.toList.sortBy(-_._2))

    val rdd10 = rdd9.sortBy(t => -t._2(0)._2)

    rdd10.collect().foreach(println)

  }


  def parseJson(str: String): ArrayBuffer[String] = {
    val result: ArrayBuffer[String] = ArrayBuffer()
    //解析成JSONObject
    val jo1 = JSON.parseObject(str);
    //得到extInfoList数组
    val jarr = jo1.getJSONArray("extInfoList")
    if (jarr != null && jarr.size() > 0) {
      //得到第一个对象
      val firstObj = jarr.getJSONObject(0)
      val tagArr = firstObj.getJSONArray("values")
      if (tagArr != null && tagArr.size() > 0) {
        val arr = tagArr.toArray()
        for (e <- arr) {
          result.+=(e.asInstanceOf[String])
        }
      }
    }
    return result;
  }

}
