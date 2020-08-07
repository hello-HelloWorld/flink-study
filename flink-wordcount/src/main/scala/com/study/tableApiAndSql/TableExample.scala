package com.study.tableApiAndSql

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/8/7 17:24
*/

//tableApi和sql初识
object TableExample {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //读取数据创建dataStream
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    //    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 666)
    //将dataStream封装为样例类
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    //创建表执行环境
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    //基于数据流，转换成一张表，在进行操作
    val dataTable: Table = tableEnvironment.fromDataStream(dataStream)
    //调用tableApi，得到转换结果
//    val resultTable: Table = dataTable.select("id,temperature").filter("id=='sensor_1'")

    //或者直接写sql得到转换结果
        val resultTable: Table = tableEnvironment.sqlQuery("select id,temperature from " + dataTable + " where id= 'sensor_1'")
    //创建临时视图,即相当于创建了表，
    /*tableEnvironment.createTemporaryView("dataTable2", dataTable)
    //    tableEnvironment.registerTable("dataTable2", dataTable)
    val resultTable: Table = tableEnvironment.sqlQuery("select id, temperature from dataTable2 where id= 'sensor_1'")*/

    //转换回数据流，打印输出
    resultTable.printSchema()
    val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
    resultStream.print("result")

    environment.execute("table example job")
  }
}
