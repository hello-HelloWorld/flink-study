package com.study.apiStudy.sinkTest

import java.util

import com.study.apiStudy.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/*
* @author: helloWorld
* @date  : Created in 2020/7/23 14:36
*/

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //读取文件数据
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataARR: Array[String] = data.split(",")
      SensorReading(dataARR(0), dataARR(1).toLong, dataARR(2).toDouble)
    })

    //定义一个httpposts
    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))

    //定义一个ElasticsearchSinkFunction
    val esSink: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //包装写入es的数据
        val dataSource: util.HashMap[String, String] = new util.HashMap[String, String]()
        dataSource.put("sensor_id", t.id)
        dataSource.put("temp", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)
        //创建一个index request
        val indexRequest = Requests.indexRequest()
          .index("sensor_temp")
          .`type`("readingdata")
          .source(dataSource)
        //用index发送http请求
        requestIndexer.add(indexRequest)
        println(t + "发送成功")
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, esSink).build())
    environment.execute("es sink test")
  }
}
