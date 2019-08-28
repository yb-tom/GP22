package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsDev extends Tag{
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    if(arg.length>1){
      val row = arg(0).asInstanceOf[Row]
      val networkmannername: String = row.getAs[String]("networkmannername")
      val devDict = arg(1).asInstanceOf[Broadcast[Map[String,String]]]
      val clientId: Int = row.getAs[Int]("client")

      val ispname: String = row.getAs[String]("ispname")
      val client = devDict.value.getOrElse(clientId+"",devDict.value.get("4").get)
      list:+=("操作系统"+client,1)
      val network = devDict.value.getOrElse(networkmannername,devDict.value.get("NETWORKOTHER").get)
      list:+=("联网方"+network,1)
      val isp = devDict.value.getOrElse(ispname,devDict.value.get("OPERATOROTHER").get)
      list:+=("运营商"+isp,1)
    }
    list
  }
}
