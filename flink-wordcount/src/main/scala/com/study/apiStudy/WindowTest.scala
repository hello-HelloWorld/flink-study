package com.study.apiStudy

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/7/23 17:13
*/
/*
* 窗口函数：
* 滚动窗口
* 滑动窗口
* 会话窗口
* （时间，计数窗口）
* */
object WindowTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    // 从调用时刻开始给env创建的每一个stream追加时间特征(事件时间)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    environment.getConfig.setAutoWatermarkInterval(100L)

    val inputStram: DataStream[String] = environment.socketTextStream("hadoop102",7777)
    inputStram.map(data=>{
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0),dataArr(1).toLong,dataArr(2).toDouble)
    })




  }

}
