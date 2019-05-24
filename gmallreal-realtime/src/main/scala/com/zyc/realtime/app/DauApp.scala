package com.zyc.realtime.app



import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.zyc.realtime.StartUpLog
import com.zyc.realtime.utils.{MyKafkaUtil, RedisUtil}
import com.zyc.utils.MyEsUtil
import constant.GmallConstant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("gmallreal-realtime").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

    //测试获取到的Dstream
//    inputDstream.foreachRDD{
//      rdd => println(rdd.map(_.value()).collect().mkString("\n"))
//    }

    //转换为目标case class 并补全日期
    val startUpLogDstream = inputDstream.map {
      record => {
        val logJson = record.value()
        val startUpLog = JSON.parseObject(logJson, classOf[StartUpLog])
        //把日期补全
        val datetimeString = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startUpLog.ts))
        val datetimeArray = datetimeString.split(" ")
        startUpLog.logDate = datetimeArray(0)
        startUpLog.logHour = datetimeArray(1).split(":")(0)
        startUpLog.logHourMinute = datetimeArray(1)
        startUpLog
      }
    }

    //2.去重操作
    //性能瓶颈点,需要优化
//    startUpLogDstream.filter{
//      startUpLog => {
//        val jedis = RedisUtil.getJedisClient
//        !jedis.sismember("dau:" + startUpLog.logDate,startUpLog.mid)
//      }
//    }
    val filteredStartupLogDstream = startUpLogDstream.transform {
      rdd => {
        //driver端执行，每个时间间隔执行一次
        //获取redis中指定key的所有value
        println("过滤前" + rdd.count())
        val jedis = RedisUtil.getJedisClient
        val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val key = "dau:" + date
        val dauSet = jedis.smembers(key)
//        println(dauSet)
        //此处的dauSet需要被每个executor访问到，如果不做处理将会在每个executor上都会生成一个dauSet，会降低性能
        //故此处可以使用广播变量进行优化
        val dauBC = ssc.sparkContext.broadcast(dauSet)
        val filteredRDD = rdd.filter {
          //executor中执行
          startUpLog => {
            !dauBC.value.contains(startUpLog.mid)
          }
        }
        println("过滤后" + filteredRDD.count())
        filteredRDD
      }
    }

    //上面的去重还有一个漏洞，就是当在这5s内产生了大量的相同mid的时候，且这个id在redis中不存在的时候，这所有的数据都会通过
    //所以还需要进行一次过滤
    val groupStartupLogDstream = filteredStartupLogDstream.map(startUpLog => (startUpLog.mid,startUpLog)).groupByKey()
    val distinctDstream = groupStartupLogDstream.flatMap {
      case (mid, startUpItr) => {
        startUpItr.take(1)
      }
    }

    

    //1.将当日登录的用户保存至redis
    distinctDstream.foreachRDD{
      rdd => {
        rdd.foreachPartition {
          startUpLogItr => {
            val jedis = RedisUtil.getJedisClient
            //流式数据，这次用了，就没办法重用了，后面还需要重用，所以此处需要用一个集合接住
            val startUpLogList = startUpLogItr.toList
            for (startUpLog <- startUpLogList) {
              //设计保存的key  类型set   key:2019-xx-xx   value：mid
              //sadd  key  value
              jedis.sadd("dau:" + startUpLog.logDate, startUpLog.mid)

            }
            jedis.close()

            //将数据保存至ES中
            MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_DAU,startUpLogList)

          }
        }
      }
    }

    //


    ssc.start()
    ssc.awaitTermination()
  }
}
