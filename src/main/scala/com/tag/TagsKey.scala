package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKey extends Tag{
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    if(arg.length>1){
      val row = arg(0).asInstanceOf[Row]
      val bc_stop: Broadcast[Map[String, String]] = arg(1).asInstanceOf[Broadcast[Map[String, String]]]
      val keywords: String = row.getAs[String]("keywords")
      if(StringUtils.isNotBlank(keywords)){
        val strings: Array[String] = keywords.split("\\|")
        strings.filter(words=>words.length>=3 && words.length<=8 && !bc_stop.value.contains(words)).foreach(x=>{
          list:+=(("K"+x),1)
        })
      }
    }
//    list.foreach(println(_))
    list
  }
}
