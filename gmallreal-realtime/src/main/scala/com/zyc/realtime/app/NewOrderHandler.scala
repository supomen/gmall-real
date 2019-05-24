package com.zyc.realtime.app

import com.zyc.realtime.bean.OrderInfo
import com.zyc.utils.MyEsUtil
import constant.GmallConstant
import org.apache.spark.streaming.dstream.DStream

object NewOrderHandler {


  def handleOrder(newOrderDSream: DStream[OrderInfo]): Unit ={
    newOrderDSream.foreachRDD{rdd=>
      rdd.foreachPartition{orderItr=>
        val orderList: List[OrderInfo] = orderItr.map { orderInfo =>
          val createtimeArray: Array[String] = orderInfo.createTime.split(" ")
          val createDate: String = createtimeArray(0)
          val createTime: String = createtimeArray(1)
          val hour: String = createTime.split(":")(0)
          val minute: String = createTime.split(":")(1)
          val sec: String = createTime.split(":")(2)

          orderInfo.createDate = createDate
          orderInfo.createHour = hour
          orderInfo.createHourMinute = hour + ":" + minute
          orderInfo
        }.toList
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_NEW_ORDER,orderList);
      }
    }
  }

}
