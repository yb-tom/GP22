package com.ETL

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Exam0824 {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath) = args
    val spark =SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec","snappy")
      .appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile(inputPath)
    val rdd: RDD[String] = ds.rdd
//    val strings: Array[String] = spark.read.textFile(inputPath).collect()
    //过滤status
    val rdd1: RDD[String] = rdd.filter(x => {
      val jsonObject = JSON.parseObject(x)
      val status: Int = jsonObject.getIntValue("status")
      status == 1
    })
    //按照pois，分类businessarea，并统计每个businessarea的总数
    val rdd2_pois: RDD[List[(String, Int)]] = rdd1.map(string => {
      var list_businessarea= List[(String, Int)]()
      val jsonparse = JSON.parseObject(string)
      val regeocode = jsonparse.getJSONObject("regeocode")
      val pois = regeocode.getJSONArray("pois")
      for (item <- pois.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json_item: JSONObject = item.asInstanceOf[JSONObject]
          val businessarea: String = json_item.getString("businessarea")
          if (StringUtils.isNotBlank(businessarea) && businessarea != "[]") {
            list_businessarea:+=(businessarea,1)
          }
        }
      }
      list_businessarea
    })

    val rdd3_pois: RDD[(String, Int)] = rdd2_pois.flatMap(x=>x)
    val rdd4_pois: RDD[(String, Int)] = rdd3_pois.reduceByKey(_+_)
    rdd4_pois.collect().foreach(println)

    //按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
    val rdd2_type: RDD[List[(String, Int)]] = rdd1.map(string => {
      var list_type= List[(String, Int)]()
      val jsonparse = JSON.parseObject(string)
      val regeocode = jsonparse.getJSONObject("regeocode")
      val pois = regeocode.getJSONArray("pois")
      for (item <- pois.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json_item: JSONObject = item.asInstanceOf[JSONObject]
          val types: Array[String] = json_item.getString("type").split(";")
          types.foreach(x=>{
            list_type:+=(x,1)
          })
        }
      }
      list_type
    })
    val rdd3_type: RDD[(String, Int)] = rdd2_type.flatMap(x=>x)
    val rdd4_type: RDD[(String, Int)] = rdd3_type.reduceByKey(_+_)
    rdd4_type.collect().foreach(println)
  }

}
