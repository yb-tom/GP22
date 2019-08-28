package com.utils

object RptUtils {
  //处理请求数
  def request(requestmode:Int,processnode:Int):List[Int]={
    var origin_request_count = 0
    var effect_request_count = 0
    var ad_request_count = 0
    if(requestmode == 1){
      if(processnode >= 1 ){
        origin_request_count = 1
        if(processnode >= 2){
          effect_request_count = 1
          if(processnode == 3){
            ad_request_count =1
          }
        }
      }
    }
    List(origin_request_count,effect_request_count,ad_request_count)
  }

  //处理展示点击数
  def click(requestmode:Int,iseffective:Int):List[Int]={
    var exh_count = 0
    var click_count = 0
    if(iseffective==1){
      if(requestmode == 2){
        exh_count = 1
      }else if(requestmode == 3){
        click_count = 1
      }
    }
    List(exh_count,click_count)
  }
  //处理竞价操作
  def ad_bid(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,WinPrice:Double,adpayment:Double):List[Double]={
    var bid_join_count = 0.0
    var bid_success_count = 0.0
    var ad_consume_count = 0.0
    var ad_cost_count = 0.0
    if(iseffective==1 && isbilling==1){
      if(isbid == 1){
        bid_join_count = 1.0
      }else if(iswin == 1){
        if(adorderid != 1){
          bid_success_count = 1.0
        }else{
          ad_consume_count = WinPrice/1000
          ad_cost_count = adpayment/1000
        }
      }
    }
    List(bid_join_count,bid_success_count,ad_consume_count,ad_cost_count)
  }
}
