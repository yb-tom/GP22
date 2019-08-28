package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {
  //过滤需要的字段
  val uniqueUserId =
    """
    | imei != '' or mac != '' or openudid != '' or androidid != '' or idfa != '' or
    | imeimd5 != '' or macmd5 != '' or openudidmd5 != '' or androididmd5 != '' or idfamd5 != '' or
    | imeisha1 != '' or macsha1 != '' or openudidsha1 != '' or androididsha1 != '' or idfasha1 != ''
    """.stripMargin


  //取出唯一不为空的id
  def getUniqueUserId(row:Row):String={
    row match{
      case v if StringUtils.isNoneBlank(v.getAs[String]("imei"))=>"IM:"+v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac"))=>"mac:"+v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid"))=>"openudid:"+v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid"))=>"androidid:"+v.getAs[String]("androidid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa"))=>"idfa:"+v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeimd5"))=>"IMmd5:"+v.getAs[String]("imeimd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macmd5"))=>"macmd5:"+v.getAs[String]("macmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidmd5"))=>"openudidmd5:"+v.getAs[String]("openudidmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididmd5"))=>"androididmd5:"+v.getAs[String]("androididmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfamd5"))=>"idfamd5:"+v.getAs[String]("idfamd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeisha1"))=>"IMsha1:"+v.getAs[String]("imeisha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macsha1"))=>"macsha1:"+v.getAs[String]("macsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidsha1"))=>"openudidsha1:"+v.getAs[String]("openudidsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididsha1"))=>"androididsha1:"+v.getAs[String]("androididsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfasha1"))=>"idfasha1:"+v.getAs[String]("idfasha1")

    }
  }

  // 获取所有Id
  def getAllUserId(row:Row):List[String]={
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+= "IM: "+row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac"))) list:+= "MC: "+row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid"))) list:+= "OD: "+row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid"))) list:+= "AD: "+row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa"))) list:+= "ID: "+row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list:+= "IMM: "+row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list:+= "MCM: "+row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) list:+= "ODM: "+row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list:+= "ADM: "+row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list:+= "IDM: "+row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) list:+= "IMS: "+row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list:+= "MCS: "+row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list:+= "ODS: "+row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list:+= "ADS: "+row.getAs[String]("androididsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list:+= "IDS: "+row.getAs[String]("idfasha1")
    list
  }
}
