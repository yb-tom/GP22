package com.ETL

import com.ETL.Txt2parquet.AD
import com.utils.{RptUtils, Utils2Type}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Demand3 {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
    val Array(inputPathDic,inputPath) = args
    val spark =SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec","snappy")
      .appName(this.getClass.getName).master("local[*]").getOrCreate()

    val lines_dic: Dataset[String] = spark.read.textFile(inputPathDic)
    import spark.implicits._
    val rdd_dic: RDD[Array[String]] = lines_dic.rdd.map(t=>t.split("\t",t.length)).filter(_.size>4)
      val map_dic: Map[String, String] = rdd_dic.map(x => {
      (x(4), x(1))
    }).collect().toMap
    val bc_value: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(map_dic)


    val df_media: DataFrame = spark.read.parquet(inputPath)
    val rdd1_media = df_media.rdd.map(row => {
      //把需要的字段全部取出
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val WinPrice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      val appid: String = row.getAs[String]("appid")
      val appname: String = row.getAs[String]("appname")
      var appname_final = bc_value.value.getOrElse(appid,appname)
//      if(appname_final!="爱奇艺" && appname_final!="腾讯新闻" && appname_final!="PPTV"){
//        appname_final="其他"
//      }
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //      ((pro, city), list1 :: list2 :: list3::Nil)
      (appname_final, list1++list2++list3)
    })
    val rdd2_media: RDD[(String, List[Double])] = rdd1_media.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_media = rdd2_media.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
    rdd3_media.collect().foreach(println)



    val df_provider: DataFrame = spark.read.parquet(inputPath)
    val rdd1_provider = df_media.rdd.map(row => {
      //把需要的字段全部取出
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val WinPrice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //      ((pro, city), list1 :: list2 :: list3::Nil)
      (adplatformproviderid, list1++list2++list3)
    })
    val rdd2_provider: RDD[(Int, List[Double])] = rdd1_provider.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_provider = rdd2_provider.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
    rdd3_provider.collect().foreach(println)

    spark.stop()
  }
}
