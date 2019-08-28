package com.tag

import com.utils.Tag
import org.apache.spark.sql.Row

object BusinessTag extends Tag{
  override def makeTags(arg: Any*): List[(String, Int)] = {
    val list: List[(String, Int)] = List[(String,Int)]()
    val row: Row = arg(0).asInstanceOf[Row]
    list
  }
}
