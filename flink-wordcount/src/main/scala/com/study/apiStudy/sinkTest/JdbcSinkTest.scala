package com.study.apiStudy.sinkTest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.study.apiStudy.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/7/23 16:18
*/

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    val inPutSteam: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inPutSteam.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    dataStream.addSink(new MyJdbcSink())
    environment.execute("jdbc sink test")
  }

}

// 自定义一个 SinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  //首先定义sql连接，以及预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //在open生命周期方法中创建连接以及预编译语句
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root")
    insertStmt = conn.prepareStatement("insert into temp(sensor,temperature) values(?,?)")
    updateStmt = conn.prepareStatement("update temp set tempetature=? where sensor=?")
  }


  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //如果刚才没有更新数据，那么执行插入操作
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setDouble(2, value.temperature)
      insertStmt.setString(1, value.id)
      insertStmt.execute()
    }
  }

  //关闭操作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
