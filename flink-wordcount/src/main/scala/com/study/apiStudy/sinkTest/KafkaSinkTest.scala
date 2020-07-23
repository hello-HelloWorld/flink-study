package com.study.apiStudy.sinkTest

import java.util.Properties

import com.study.apiStudy.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/*
* @author: helloWorld
* @date  : Created in 2020/7/23 11:30
*/

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //读取文件数据
    //    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")

    //读取kafka中的数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("topic", new SimpleStringSchema(), properties))

    val dataStram: DataStream[String] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble).toString
    })
    //输出数据到文件
    //    dataStram.writeAsCsv("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\out.txt")
    inputStream.print()
    //输出数据到kafka
    dataStram.addSink(
      new FlinkKafkaProducer011[String]("hadoop102:9092", "sink", new SimpleStringSchema())
    )

    environment.execute("flink sink test")
  }
}
