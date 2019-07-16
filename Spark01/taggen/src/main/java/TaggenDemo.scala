import com.alibaba.fastjson.JSON

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * 纯Scala版
  */
object TaggenDemo {
  def main(args: Array[String]): Unit = {
    // 加载数据
    var data = Source.fromFile("d:/source/temptags.txt").getLines().toList

    // 变换  取出商家和评论
    val list = data.map(line => {
      val arr = line.split("\t")
      val busid = arr(0)
      val json = arr(1)
      val tagArr = parseJson(json)
      (busid,tagArr)
    })

    // 先过滤
    val list2 = list.filter(t => {
      t._2 != null && t._2.size > 0
    })

    // 压扁集合 ArrayBuffer (0877,List(..,..,)) ->
    val list3 = list2.flatMap(t=>{
      val arr : ArrayBuffer[(String,String)] = ArrayBuffer()
      val busid = t._1
      val tags = t._2
      for(tag <- tags){
        arr.append((busid,tag))
      }
      arr
    })

    // 统计每个组的数量  按照元组来分组 ((78824187,服务热情),List((78824187,服务热情), (78824187,服务热情), (78824187,服务热情), (78824187,服务热情), (78824187,服务热情)))s
    val map1 = list3.groupBy(t=>t)


    // 取出map中value的长度
    val map2 = map1.mapValues(_.size)
    // 转换出list 防止数据冲掉
    val list4 = map2.toList

    val list5 = list4.map(t=>{
      val busid = t._1._1
      val tag = t._1._2
      val cnt = t._2
      (busid,(tag,cnt))
    })

    // 按busid分组
    val map3 = list5.groupBy(t=>t._1)

    // 按照评论降排
    val map4 = map3.mapValues(list=>{
      val list0 = list.map(_._2)
      list0.sortBy(- _._2)
    })

    // 按照商家排序
    val list6 = map4.toList

    val list7 = list6.sortBy(t=> -t._2(0)._2)


    for (line <- list7) {
      println(line)
    }


  }

  def parseJson(str:String) : ArrayBuffer[String] = {
    val result : ArrayBuffer[String] = ArrayBuffer()
    //解析成JSONObject
    val jo1 = JSON.parseObject(str) ;
    //得到extInfoList数组
    val jarr = jo1.getJSONArray("extInfoList")
    if(jarr != null && jarr.size() > 0){
      //得到第一个对象
      val firstObj = jarr.getJSONObject(0)
      val tagArr = firstObj.getJSONArray("values")
      if(tagArr != null && tagArr.size() > 0){
        val arr = tagArr.toArray()
        for(e <- arr){
          result.+=(e.asInstanceOf[String])
        }
      }
    }
    return result ;
  }


}
