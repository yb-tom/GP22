package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsChannel extends Tag{
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = arg(0).asInstanceOf[Row]
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+adplatformproviderid,1)
    list
  }
}
