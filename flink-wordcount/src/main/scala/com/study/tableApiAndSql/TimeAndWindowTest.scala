package com.study.tableApiAndSql

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/*
* @author: helloWorld
* @date  : Created in 2020/8/10 16:07
*/

/*
* 设置时间语义
* */
object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //设置时间语义--事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据，创建datastream
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    //创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将datastream转化成table
    //把timestamp字段当成事件时间的时间戳
    val table: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    table.printSchema()
    table.toAppendStream[Row].print()
    environment.execute("tiem and window")
  }
}
