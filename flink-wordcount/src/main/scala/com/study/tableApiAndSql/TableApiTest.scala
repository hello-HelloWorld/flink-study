package com.study.tableApiAndSql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/*
* @author: helloWorld
* @date  : Created in 2020/8/10 10:33
*/

/*
* table api学习
* */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //1.创建表环境
    //1.1 创建老版本的流查询环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //1.2 创建老版本的批式查询环境
    val batchEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnvironment: BatchTableEnvironment = BatchTableEnvironment.create(batchEnvironment)

    //1.3 创建blink版本的流查询环境
    val blinkSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkTableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, blinkSettings)

    //1.4 创建blink版本的批式查询环境
    val batchBlinkSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val batchBlinkTableEnvironment: TableEnvironment = TableEnvironment.create(batchBlinkSettings)

    //2. 从外部环境读取数据，在环境中注册表
    //2.1 连接到文件系统(CSV)
    val filePath = "E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt"
    tableEnvironment.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) //定义读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())) //定义表结构
      .createTemporaryTable("inputTable") //注册一张表

    //2.2 连接到kafka
    tableEnvironment.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers", "hadoop102:9092")
      .property("zookeeper.connect", "2181"))
      .withFormat(new Csv()) //定义读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())) //定义表结构
      .createTemporaryTable("kafkaInputTable")

    //3. 表的查询
    //3.1 简单查询，过滤投影
    val sensorTable: Table = tableEnvironment.from("inputTable")
    sensorTable.printSchema()
    val resultTable: Table = sensorTable.select('id, 'temperature).filter('id === "sensor_1")
    resultTable.printSchema()
//    resultTable.toAppendStream[(String, Double)].print("result")

    //3.2 sql查询
    val sqlResultTable: Table = tableEnvironment.sqlQuery(
      """
        |select id,temperature from inputTable where id='sensor_1'
      """.stripMargin)
    sqlResultTable.printSchema()
//    sqlResultTable.toAppendStream[(String, Double)].print("sqlResult")

    //3.3 简单聚合，统计每个传感器的个数
    val aggResultTable: Table = sensorTable.groupBy("id").select('id, 'id.count as 'count)
    val aggSqlResultTable: Table = tableEnvironment.sqlQuery("select id,count(id) as countt from inputTable group by id")
    aggResultTable.toRetractStream[(String, Long)].print("agg result")

    environment.execute("table api test")
  }
}
