package com.zyc.realtime.app

import com.alibaba.fastjson.JSON
import com.zyc.realtime.bean.OrderInfo
import com.zyc.realtime.utils.MyKafkaUtil
import constant.GmallConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeNewOrderApp {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(10))

    val newOrderStringStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_NEW_ORDER,ssc)

    val newOrderDSream: DStream[OrderInfo] = newOrderStringStream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      orderInfo
    }
    NewOrderHandler.handleOrder(newOrderDSream)

    ssc.start()
    ssc.awaitTermination()
  }



}


