package com.study.tableApiAndSql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/*
* @author: helloWorld
* @date  : Created in 2020/8/10 14:16
*/

/*
* kafka table api
* */
object KafkaTableTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //从kafka读取数据
    tableEnvironment.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers", "hadoop102:9092")
      .property("zookeeper.connect", "hadoop102:2181"))
      .withFormat(new Csv()) //定义读取数据之后的数据格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())) //定义表结构
      .createTemporaryTable("kafkaInputTable") //注册表

    //做转换操作
    val sensorTable: Table = tableEnvironment.from("kafkaInputTable")
    val resultTable: Table = sensorTable.select('id, 'temperature).filter('id === "sensor_1")
    val aggResultTable: Table = sensorTable.groupBy('id).select('id, 'id.count as 'count)

    //定义一个连接到kafka的输出表
    tableEnvironment.connect(new Kafka()
      .version("0.11")
      .topic("output")
      .property("bootstrap:servers", "hadoop102:9092")
      .property("zookeeper.connect", "hadoop102:2181"))
      .withFormat(new Csv()) //定义数据格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("temperature", DataTypes.DOUBLE())) //定义表结构
      .createTemporaryTable("kafkaOutputTable")

    //将结果表输出
    resultTable.insertInto("kafkaOutputTable")

    val table: Table = tableEnvironment.sqlQuery("select id,temperature from kafkaOutputTable")
    table.toAppendStream[(String, Double)].print("result")
    environment.execute("kafka table test")
  }
}
