package com.study.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/7/22 14:47
*/

//流处理
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接收socket文本流
    val inputDS: DataStream[String] = environment.socketTextStream(host, port)

    val wordCount: DataStream[(String, Int)] = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCount.print().setParallelism(1)//设置输出并行度设置为1
    //启动executor，执行任务
    environment.execute("stream word count")
  }
}
