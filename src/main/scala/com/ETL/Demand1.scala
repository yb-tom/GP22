package com.ETL

import java.io.FileInputStream
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Demand1 {
//  要求一：
//  将统计的结果输出成 json 格式，并输出到磁盘目录。
//  {"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
//  {"ct":578,"provincename":"内蒙古自治区","cityname":"未知"}
//  {"ct":262632,"provincename":"北京市","cityname":"北京市"}
//  {"ct":1583,"provincename":"台湾省","cityname":"未知"}
//  {"ct":53786,"provincename":"吉林省","cityname":"长春市"}
//  {"ct":41311,"provincename":"吉林省","cityname":"吉林市"}
//  {"ct":15158,"provincename":"吉林省","cityname":"延边朝鲜族自治州"}
def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[2]")
    .getOrCreate()

  /**
    * sparksql 实现
    */
  val df: DataFrame = spark.read.load("D://testdata/ouptu0820")
  df.createOrReplaceTempView("ad")
  val res1: DataFrame = spark.sql("select count(1) ct ,provincename,cityname from ad group by provincename,cityname order by provincename")

  /**
    * spark core 实现
    */
  val rdd: RDD[Row] = df.rdd
  val rdd1: RDD[((Any, Any), Int)] = rdd.map(x=>((x(24),x(25)),1))
  val rdd2: RDD[((Any, Any), Iterable[((Any, Any), Int)])] = rdd1.groupBy(x=>x._1)
  val rdd3: RDD[((Any, Any), Int)] = rdd2.mapValues(_.toList.map(_._2).sum)
  val rdd4: RDD[(Int, Any, Any)] = rdd3.map(x => {
    (x._2, x._1._1, x._1._2)
  })
  rdd4.collect().foreach(println)
//  rdd4.saveAsTextFile("hdfs://hadoop101:8020/ad.txt")

  /**
    * 写入数据库
    */
  val prop = new Properties()
  val path = Thread.currentThread().getContextClassLoader.getResource("mysql.properties").getPath //文件要放到resource文件夹下
  prop.load(new FileInputStream(path))
  val url = prop.getProperty("mysql_url")
  val table = prop.getProperty("mysql_table")
  val propConect = new Properties()
  propConect.put("user",prop.getProperty("mysql_user"))
  propConect.put("password",prop.getProperty("mysql_password"))
  res1.write.mode("overwrite").jdbc(url, table, propConect)

  /**
    *  写入hdfs
    */
//  res1.coalesce(1).write.mode("overwrite").partitionBy("provincename","cityname")json("D://testdata/ouptu0820_1")
  res1.coalesce(1).write.mode("overwrite").json("D://testdata/ouptu0820_1")
  //res1.show()

  val config: Config = ConfigFactory.load("application.conf")
  val my_url: String = config.getString("jdbc_url")

  spark.stop()
  }
}
