package com.study.tableApiAndSql

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/*
* @author: helloWorld
* @date  : Created in 2020/8/12 13:27
*/

/*
* 表函数
* */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置事件时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据，创建datastream
    val inputStram: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStram.map(data => {
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

    //将datastream转换为table
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    //实例化udf函数
    val split: Split = new Split("_")
    //调用table api，tablefunction使用的时候，需要调用joinLateral方法
    val resultTable: Table = sensorTable
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)

    //sql 实现
    tableEnvironment.createTemporaryView("sensor", sensorTable)
    tableEnvironment.registerFunction("split", split)
    val resultSqlTable: Table = tableEnvironment.sqlQuery(
      """
        |select id,ts,word,length
        |from
        |sensor,lateral table(split(id)) as splitid(word,length)
      """.stripMargin)


    resultSqlTable.toAppendStream[Row].print("result sql table")
    //    resultTable.toAppendStream[Row].print("result table")
    environment.execute("table udf test")
  }
}

// 实现一个自定义TableFunction，对一个String，输出用某个分隔符切分之后的(word, wordlength)
class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    //一个str被分割成2个数据，对应着发出两条数据
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}