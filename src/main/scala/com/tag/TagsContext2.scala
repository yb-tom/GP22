package com.tag

import com.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TagsContext2 {
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


    //广播变量
    val lines_dic: Dataset[String] = spark.read.textFile(inputPath_dict)
    val rdd_dic: RDD[Array[String]] = lines_dic.rdd.map(t=>t.split("\t",t.length)).filter(_.size>=4)
    val map_dic: Map[String, String] = rdd_dic.map(x => {
      (x(4), x(1))
    }).collect().toMap
    val bc_value: Broadcast[Map[String, String]] = sc.broadcast(map_dic)
    //redis变量
//    rdd_dic.map(x=>{
//      (x(4), x(1))
//    }).foreachPartition(itr=>{
//      val jp: JedisPool = JedisPoolTool.getJedisPoolInstance
//      val jedis: Jedis = jp.getResource()
//      itr.foreach(t=>{
//        jedis.hset("yb",t._1,t._2)
//      })
//      JedisPoolTool.release(jp,jedis)
//
//    })



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
    import spark.implicits._
    // 过滤符合Id的数据
    val baseRDD: RDD[(List[String], Row)] = ds.rdd.map(row => {
      val userList: List[String] = TagUtils.getAllUserId(row)
      (userList, row)
    })

    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(row => {
      //通过row数据 打上所有标签
      val ad: List[(String, Int)] = TagsAd.makeTags(row)
      val app: List[(String, Int)] = TagsApp.makeTags(row, bc_value)
      val channel: List[(String, Int)] = TagsChannel.makeTags(row, map_dev)
      val dev: List[(String, Int)] = TagsDev.makeTags(row)
      //      val appForRedis: List[(String, Int)] = TagsAppForRedis.makeTags(row,bc_value)
      val keys: List[(String, Int)] = TagsKey.makeTags(row, bc_value_stop)
      val area: List[(String, Int)] = TagsArea.makeTags(row)

      val AllTag = ad ++ app ++ channel ++ dev ++ keys ++ area
      // 保证其中一个点携带者所有标签，同时也保留所有userId
      val VD = row._1.map((_, 0)) ++ AllTag
      // 处理所有的点集合
      row._1.map(uId => {
        // 保证一个点携带标签 (uid,vd),(uid,list()),(uid,list())
        if (row._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    // vertiesRDD.take(50).foreach(println)
    // 构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      // A B C : A->B A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    //edges.take(20).foreach(println)
    // 构建图
    val graph = Graph(vertiesRDD,edges)
    // 取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    // 处理所有的标签和id
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      // 聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)

    spark.stop()
  }
}
