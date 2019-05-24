package com.zyc.utils

import java.util.Objects

import com.zyc.bean.Movie
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

import scala.collection.mutable

object MyEsUtil {
  private val ES_HOST = "http://hadoop101"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  def insertEsBulk(indexName: String, docs: List[Any]): Unit ={
    //获取Es客户端
    val esCliect = getClient
    //Bulk API允许批量提交index和delete请求
    val bulkBuilder = new Bulk.Builder
    //每一条数据的索引的type都是一样的，所以在此处设定好默认的索引的type值
    bulkBuilder.defaultIndex(indexName).defaultType("_doc")
    for (doc <- docs){

      //创建DSL语句
      val index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }
    //    println(bulkBuilder.build())
    val items = esCliect.execute(bulkBuilder.build()).getItems
    println("保存" + items.size() + "条数据")
    esCliect.close()
  }


  def main(args: Array[String]): Unit = {
//    val esClient = getClient
//    val movie = "{\"name\":\" liulangdiqiu\"}"
//    val index = new Index.Builder(movie).index("movie_index").`type`("_doc").build()
//    esClient.execute(index)
//    esClient.close()

    //批量保存
    val movieList = List(new Movie("dianying1"),new Movie("dianying2"))
    println("数据为" + movieList)

    insertEsBulk("movie_index", movieList)
  }


}
