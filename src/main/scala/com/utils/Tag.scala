package com.utils

/**
  * 打标签的统一接口
  */
trait Tag {
  def makeTags(arg:Any*):List[(String,Int)]
}
