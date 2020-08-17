package com.study.tableApiAndSql

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/*
* @author: helloWorld
* @date  : Created in 2020/8/10 15:13
*/

/*
* 输出表计算的结果到文件
* */
object OutputTableTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //读取数据，创建datastream
    val inputSream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputSream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    //创建表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将datastream转换为table
    val sensotTable: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)
    //对table进行转化，得到结果表
    val resultTable: Table = sensotTable.select('id, 'temp).filter('id === "sensor_1")
    val aggResultTable: Table = sensotTable.groupBy('id).select('id, 'id.count as 'count)

    //定义一张输出表，这就是要写入数据的tableSink
    tableEnvironment.connect(new FileSystem().path("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\out2.txt"))
      .withFormat(new Csv()) //定义数据的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("temp", DataTypes.DOUBLE())) //定义表结构
      .createTemporaryTable("outPutKafkaTable")

    //将结果写入到table sink
    resultTable.insertInto("outPutKafkaTable")

    sensotTable.printSchema()
    sensotTable.toAppendStream[(String,Double,Long)].print("sensor")

    environment.execute("out put table")

  }
}
