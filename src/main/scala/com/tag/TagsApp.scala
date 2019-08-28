package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends  Tag{
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    if(arg.length>1){
      val row = arg(0).asInstanceOf[Row]
      val appDict= arg(1).asInstanceOf[Broadcast[Map[String, String]]]
      val appid: String = row.getAs[String]("appid")
      val appname: String = row.getAs[String]("appname")
      val appName = appDict.value.getOrElse(appid,appname)
      if(StringUtils.isNotBlank(appName)){
        list:+=("APP"+appName,1)
      }
    }
    list
  }
}
