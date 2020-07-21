package com.study.apiStudy

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

/*
* 数据来源：
* 1.从集合中读取数据
* 2.从文件中读取数据
* 3.socket文本流
* 4.从kafka读取数据
* */

//输入数据的样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //1.从集合中读取数据
    val stream1: DataStream[SensorReading] = environment.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1),
      SensorReading("sensor_1", 1547718207, 37.2),
      SensorReading("sensor_1", 1547718212, 33.5),
      SensorReading("sensor_1", 1547718215, 38.1)
    ))
    //    stream1.print("stream1").setParallelism(2)

    val stream2: DataStream[Any] = environment.fromElements(1, 2.2, "aa") //可以放不同类型的数据
    //    stream2.print("stream2")

    //2.从文件中读取数据
    val stream3: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    //    stream3.print("stream3").setParallelism(2)

    //3.读取socket文本流
    val stream4: DataStream[String] = environment.socketTextStream("hadoop102", 7777)

    //4.从kafka中读取数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop104:9092")
    //properties.setProperty("group.id", "ab")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream5: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("topicb", new SimpleStringSchema(), properties))

    //5.自定义source
    val stream6: DataStream[SensorReading] = environment.addSource(new MySensorSource())
    stream6.print("stream6")

    environment.execute("source test job")
  }
}

// 实现一个自定义的 SourceFunction，自动生成测试数据
class MySensorSource() extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据源是否正常运行
  var running: Boolean = false

  override def cancel(): Unit = running = false

  //随机生成SensorReading数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数生成器
    val random: Random = new Random()
    // 随机生成 10个传感器的温度值，并且不停在之前温度基础上更新（随机上下波动）
    // 首先生成 10个传感器的初始温度
    var curtmps: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )

    //无限循环，生成随机数流
    while (true) {
      //在当前温度基础上，随机生成小微动
      curtmps = curtmps.map(
        data => (data._1, data._2 + random.nextGaussian())
      )

      //获取当前系统时间
      val curTs: Long = System.currentTimeMillis()
      //包装成样例类，用sourceContext发出数据
      curtmps.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTs, data._2))
      )
      //定义时间间隔
      Thread.sleep(1000)
    }
  }
}
