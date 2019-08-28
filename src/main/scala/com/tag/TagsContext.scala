package com.tag

import com.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import redis.clients.jedis.{Jedis, JedisPool}

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length!=5){
      println("目录不匹配，退出程序")
      sys.exit()
    }

    val Array(inputPath,inputPath_dict,inputPath_dev,inputPath_stop,outputPath)=args
    //创建上下文
    val spark =SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec","snappy")
      .appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    import spark.implicits._


    //广播变量
    val lines_dic: Dataset[String] = spark.read.textFile(inputPath_dict)
    val rdd_dic: RDD[Array[String]] = lines_dic.rdd.map(t=>t.split("\t",t.length)).filter(_.size>=4)
    val map_dic: Map[String, String] = rdd_dic.map(x => {
      (x(4), x(1))
    }).collect().toMap
    val bc_value: Broadcast[Map[String, String]] = sc.broadcast(map_dic)
    //redis变量
    rdd_dic.map(x=>{
      (x(4), x(1))
    }).foreachPartition(itr=>{
      val jp: JedisPool = JedisPoolTool.getJedisPoolInstance
      val jedis: Jedis = jp.getResource()
      itr.foreach(t=>{
        jedis.hset("yb",t._1,t._2)
      })
      JedisPoolTool.release(jp,jedis)



    })



    val lines_dev: Dataset[String] = spark.read.textFile(inputPath_dev)
    val rdd_dev: RDD[Array[String]] = lines_dev.rdd.map(t=>t.split("\t",t.length)).filter(_.size>=2)
    val map_dev: Map[String, String] = rdd_dev.map(x => {
      (x(0), x(1))
    }).collect().toMap
    val bc_value_dev: Broadcast[Map[String, String]] = sc.broadcast(map_dev)

    val lines_stop: Dataset[String] = spark.read.textFile(inputPath_stop)
    val rdd_stop: RDD[Array[String]] = lines_stop.rdd.map(t=>t.split("\t",t.length)).filter(_.size>=1)
    val map_stop: Map[String, String] = rdd_stop.map(x => {
      (x(0), "")
    }).collect().toMap
    val bc_value_stop: Broadcast[Map[String, String]] = sc.broadcast(map_stop)

    //读取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    val ds: Dataset[Row] = df.filter(TagUtils.uniqueUserId)
    val rdd: RDD[Row] = ds.rdd

    val rdd1: RDD[(String, List[(String, Int)])] = rdd.map(row => {
      val userId = TagUtils.getUniqueUserId(row)
      //通过row数据 打上所有标签
      val ad: List[(String, Int)] = TagsAd.makeTags(row)
      val app: List[(String, Int)] = TagsApp.makeTags(row, bc_value)
      val channel: List[(String, Int)] = TagsChannel.makeTags(row, map_dev)
      val dev: List[(String, Int)] = TagsDev.makeTags(row)
      //      val appForRedis: List[(String, Int)] = TagsAppForRedis.makeTags(row,bc_value)
      val keys: List[(String, Int)] = TagsKey.makeTags(row, bc_value_stop)
      val area: List[(String, Int)] = TagsArea.makeTags(row)

      (userId, ad ++ app ++ channel ++ dev ++ keys ++ area)
    })
    val rdd2: RDD[(String, List[(String, Int)])] = rdd1.reduceByKey((x, y) => {
      val list: List[(String, Int)] = x ++ y
      list.groupBy(_._1)
        .mapValues(_.foldLeft(0)(_ + _._2))
        .toList
    })
    rdd2.collect().foreach(println)

    /**
      * 存入hbase
      */
    rdd2.foreachPartition(x=>{
      val connection: Connection = HbaseUtil.getHBaseConnection(HbaseUtil.getHBaseConf())
      val admin: HBaseAdmin = HbaseUtil.getHBaseAdmin(connection,"tag","yb")
      x.foreach(x=>{
        val rowkey: String = x._1
        HbaseUtil.addRowData(connection,"tag",rowkey,"yb","yb_tag",x._2.toString)
      })
    })

    // HBase数据转成RDD
println("HBASE+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    val conf: Configuration = HbaseUtil.getHBaseConf()
    conf.set(TableInputFormat.INPUT_TABLE,"tag")
    val hBaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()
    // RDD数据操作
    val data: RDD[(String, String)] = hBaseRDD.map(x => {
      val result = x._2
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("yb".getBytes, "yb_tag".getBytes))
      (key, value)
    })
    data.collect().foreach(println)
    spark.stop()
  }
}
