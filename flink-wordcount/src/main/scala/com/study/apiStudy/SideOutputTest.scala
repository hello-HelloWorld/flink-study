package com.study.apiStudy

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/7/28 17:47
*/

/*
* 侧输出流
*  用 ProcessFunction的侧输出流实现分流操作
* */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //接收数据
    val inputStream: DataStream[String] = environment.socketTextStream("master1", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    // 用 ProcessFunction的侧输出流实现分流操作
    val highTempStream: DataStream[SensorReading] = dataStream.process(new SplitTemoProcessor(30.0))

    val lowTempStream: DataStream[(String, Double, Long)] = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

    highTempStream.print("high")
    lowTempStream.print("low")
    environment.execute("side output job")
  }
}

// 自定义 ProcessFunction，用于区分高低温度的数据
class SplitTemoProcessor(temp: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 判断当前数据的温度值，如果大于阈值，输出到主流；如果小于阈值，输出到侧输出流
    if (value.temperature > temp) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[(String, Double, Long)]("low-temp"), (value.id, value.temperature, value.timestamp))
    }
  }
}
