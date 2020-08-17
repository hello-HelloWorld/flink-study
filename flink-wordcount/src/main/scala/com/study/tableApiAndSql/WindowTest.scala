package com.study.tableApiAndSql

import java.sql.Timestamp

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/*
* @author: helloWorld
* @date  : Created in 2020/8/11 16:42
*/

/*
* 窗口操作
* */
object WindowTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据，创建dataStream
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

    //创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将datastream转化成table
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    //窗口操作
    //1.1 group窗口，开一个10秒的滚动窗口，统计每个传感器的数量
    val groupResultTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw) //此处别名tw是该窗口的别名
      .groupBy('id, 'tw) //必须要加上窗口的别名
      .select('id, 'id.count, 'tw.end)

    //1.2 group窗口 sql实现
    tableEnvironment.createTemporaryView("sensor", sensorTable)
    val groupSqlResultTable: Table = tableEnvironment.sqlQuery(
      """
      select id,count(id),tumble_end(ts,interval '10' second)
      from sensor
      group by id,tumble(ts,interval '10' second)
      """.stripMargin)

    //2.1 over 窗口，对每个传感器当前行数与前两行数据的温度求平均值
    val overResultTable: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      .select('id, 'ts, 'id.count over ('w), 'temperature.avg over 'w)

    //2.2 over 窗口sql实现
    val overSqlResultTable: Table = tableEnvironment.sqlQuery(
      """
        |select id,ts,
        | count(id) over w,
        | avg(temperature) over w
        |from sensor
        |window w as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
      """.stripMargin)

    //    groupResultTable.toAppendStream[(String, Long, Timestamp)].print("group result")
    //    groupSqlResultTable.toRetractStream[Row].print("group sql result")
    //    overResultTable.toAppendStream[(String, Timestamp, Long, Double)].print("over result")
    overSqlResultTable.toAppendStream[Row].print("over sql result")

    environment.execute("window test")
  }
}
