package com.tag

import com.utils.{JedisPoolT, JedisPoolTool, JedisTool, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool}

object TagsAppForRedis extends  Tag{
  val jp: JedisPool = JedisPoolTool.getJedisPoolInstance
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    if(arg.length>1){
      val row = arg(0).asInstanceOf[Row]
      val appDict= arg(1).asInstanceOf[Broadcast[Map[String, String]]]
      val appid: String = row.getAs[String]("appid")
      val appname: String = row.getAs[String]("appname")
      var appName=""
      val jedis: Jedis = jp.getResource()
      if(jedis.hexists("yb","appid")){
        appName = jedis.hget("yb","appid")
      }else{
        appName=appname
      }
      if(StringUtils.isNotBlank(appName)){
        list:+=("APP"+appName,1)
      }
      JedisPoolTool.release(jp,jedis)
    }
    list.foreach(println(_))

    list
  }
}
