package com.study.tableApiAndSql


import java.sql.Timestamp

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/*
* @author: helloWorld
* @date  : Created in 2020/8/12 11:23
*/

/*
* udf函数
* */
object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置事件时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据，创建datastream
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000 //将sensorReading的timestamp字段作为事件时间，延迟1秒钟
      })

    //创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将datastream转换为table
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    //创建udf函数实例
    val hashCode: HashCode = new HashCode(10)
    //调用table api
    val resultTable: Table = sensorTable.select('id, 'ts, hashCode('id))

    //sql实现
    //注册表
    tableEnvironment.createTemporaryView("sensor", sensorTable)
    //注册udf函数
    tableEnvironment.registerFunction("hashCode", hashCode)
    val resultSqlTable: Table = tableEnvironment.sqlQuery("select id ,ts,hashCode(id) from sensor")

    resultTable.toAppendStream[(String, Timestamp, Int)].print("result")
    resultSqlTable.toRetractStream[Row].print("sql result")
    environment.execute("udf test")
  }
}

//自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
  // 必须要实现一个eval方法，它的参数是当前传入的字段，它的输出是一个Int类型的hash值
  def eval(str: String): Int = {
    str.hashCode * factor
  }
}
