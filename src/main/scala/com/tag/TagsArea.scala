package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsArea extends Tag{
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = arg(0).asInstanceOf[Row]
    val pro: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(pro)){
      list:+=("APP"+pro,1)
    }
    if(StringUtils.isNotBlank(city)){
      list:+=("APP"+city,1)
    }
    list
  }
}
