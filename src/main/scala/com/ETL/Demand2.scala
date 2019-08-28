package com.ETL

import com.utils.{ConnectPool, RptUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Nvl
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.spark_project.jetty.client.ConnectionPool

/**
  * 地域分布指标
  */
object Demand2 {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
    val Array(inputPath) = args
    val spark =SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec","snappy")
      .appName(this.getClass.getName).master("local[*]").getOrCreate()

    val df_area: DataFrame = spark.read.parquet(inputPath)

    /**
      * spark sql
      */
    df_area.createOrReplaceTempView("ad")
    val result: DataFrame = spark.sql("select\n" +
      "provincename,\n" +
      "cityname,\n" +
      "count(case when requestmode=1 and processnode>=1 then 1 end) request_num_total,\n" +
      "count(case when requestmode=1 and processnode>=2 then 1 end ) request_num_valid,\n" +
      "count(case when requestmode=1 and processnode=3 then 1 end) request_num_ad,\n" +
      "count(case when iseffective =1 and isbilling=1 and isbid =1 then 1 end) bid_num_participate,\n" +
      "count(case when iseffective =1 and isbilling=1 and iswin=1 and adorderid <>0 then 1 end) bid_num_success,\n" +
      "count(case when requestmode = 2 and iseffective = 1 and isbilling = 1 then 1 end ) ad_num_show,\n" +
      "count(case when requestmode = 3 and iseffective = 1 and isbilling = 1 then 1 end) ad_num_click,\n" +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end )/1000 ad_cost,\n" +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0 end )/1000 ad_payment\n" +
      "from ad\n" +
      "group by provincename,cityname")
    result.show()
//    result.coalesce(1).write.mode("overwrite").partitionBy("provincename", "cityname").json(outputPath)



    /**
      * spark core
      */
    import spark.implicits._
    val rdd1_area = df_area.rdd.map(row => {
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
      //key值 省市
      val pro: String = row.getAs[String]("provincename")
      val city: String = row.getAs[String]("cityname")
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
//      ((pro, city), list1 :: list2 :: list3::Nil)
      ((pro, city), list1++list2++list3)
    })
    val rdd2_area: RDD[((String, String), List[Double])] = rdd1_area.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_area = rdd2_area.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))

//    rdd3_area.collect().foreach(println)


    val rdd1_coop = df_area.rdd.map(row => {
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
      //key值 省市
      val ispid: Int = row.getAs[Int]("ispid")
      val ispname: String = row.getAs[String]("ispname")
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //      ((pro, city), list1 :: list2 :: list3::Nil)
      (ispname, list1++list2++list3)
    })
    val rdd2_coop= rdd1_coop.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_coop = rdd2_coop.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
//    rdd3_coop.collect().foreach(println)


    val rdd1_net = df_area.rdd.map(row => {
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
      //key值 省市
      val networkmannerid: Int = row.getAs[Int]("networkmannerid")
      val networkmannername: String = row.getAs[String]("networkmannername")
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //      ((pro, city), list1 :: list2 :: list3::Nil)
      (networkmannername, list1++list2++list3)
    })
    val rdd2_net = rdd1_net.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_net = rdd2_net.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
//    rdd3_net.collect().foreach(println)


    val rdd1_dev = df_area.rdd.map(row => {
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
      //key值 省市
      val devicetype: Int = row.getAs[Int]("devicetype")
      var devicename = ""
      if(devicetype == 1){
        devicename = "手机"
      }else if(devicetype == 2){
        devicename = "平板"
      }else{
        devicename = "其他"
      }
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //      ((pro, city), list1 :: list2 :: list3::Nil)
      (devicename, list1++list2++list3)
    })
    val rdd2_dev = rdd1_dev.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_dev = rdd2_dev.reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
    rdd3_dev.collect().foreach(println)


    val rdd1_os = df_area.rdd.map(row => {
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
      //key值 省市
      val clientId: Int = row.getAs[Int]("client")
      var clientName = ""
      if(clientId == 1){
        clientName = "Android"
      }else if(clientId == 2){
        clientName = "ios"
      }else if(clientId == 3){
        clientName = "wp"
      }
      else{
        clientName = "其他"
      }
      //创建对应的三个方法 处理九个指标
      val list1: List[Int] = RptUtils.request(requestmode, processnode)
      val list2: List[Int] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.ad_bid(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //      ((pro, city), list1 :: list2 :: list3::Nil)
      (clientName, list1++list2++list3)
    })
    val rdd2_os = rdd1_os.mapValues(x=>x.map(_.toString.toDouble))
    val rdd3_os = rdd2_os.reduceByKey((x,y)=>x.zip(y)         .map(x=>x._1+x._2))
    rdd3_os.foreachPartition(insertData)
    rdd3_os.collect().foreach(println)
    spark.stop()
  }

  def insertData(iterator: Iterator[(String,List[Double])]):Unit={
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
//    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hadoop101:3306/test", "root", "123456")
    val connection = ConnectPool.getConnection()
    iterator.foreach(data=>{
      val ps = connection.prepareStatement("insert into rdd_os(clientName,requestmode,processnode,iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment) " +
        "values (?,?,?,?,?,?,?,?,?,?)")
      ps.setString(1,data._1)
      ps.setDouble(2,data._2(0))
      ps.setDouble(3,data._2(1))
      ps.setDouble(4,data._2(2))
      ps.setDouble(5,data._2(3))
      ps.setDouble(6,data._2(4))
      ps.setDouble(7,data._2(5))
      ps.setDouble(8,data._2(6))
      ps.setDouble(9,data._2(7))
      ps.setDouble(10,data._2(8))
      ps.executeUpdate()
    })
  }
//  ConnectPool.closeCon()
}
