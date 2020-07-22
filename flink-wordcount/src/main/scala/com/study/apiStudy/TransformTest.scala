package com.study.apiStudy

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/7/21 16:07
*/

//转换算子
object TransformTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并发度
    environment.setParallelism(1)
    //读取数据
    val inputData: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")

    //1.基本转换操作
    val stream1: DataStream[SensorReading] = inputData.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })
    //    stream1.print("基本转换操作").setParallelism(2)

    //2.分组滚动聚合
    val stream2 = stream1.
      keyBy("id")
      //        .keyBy(data=>data.id)
      //      .keyBy(new MyIdSelector())
      //      .min("temperature") //取当前sensor温度最小的值
      //      .reduce(new MyReducer())//取出每个sensor最大的时间戳和最小的温度
      .reduce((curData, newData) => SensorReading(curData.id, curData.timestamp.max(newData.timestamp), curData.temperature.min(newData.temperature)))
    //        stream2.print()

    //3.分流,split和select一起用
    val splitStream: SplitStream[SensorReading] = stream1.split(data => {
      if (data.temperature > 38) {
        Seq("high")
      } else {
        Seq("low")
      }
    })
    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    val allStream: DataStream[SensorReading] = splitStream.select("low", "high")
    /*  stream1.print()
      highStream.print("high")
      lowStream.print("low")
      allStream.print("all")*/

    //4.合流，connect和map一起使用
    val waringStream: DataStream[(String, Double)] = highStream.map(
            data => (data.id, data.temperature)
//      new MyMapper()
    )
    val connectedStreams: ConnectedStreams[SensorReading, (String, Double)] = lowStream.connect(waringStream)

  val resultStream: DataStream[Object] = connectedStreams.map(
    lowTempData => (lowTempData.id, "normal"),
      warningData => (warningData._1, warningData._2, "high temp warning")
  )
    //    resultStream.print()

    //使用union进行合并
    val value: DataStream[SensorReading] = highStream.union(lowStream,allStream)
    value.print()


    environment.execute()
  }
}

// 自定义函数类，key选择器
class MyIdSelector() extends KeySelector[SensorReading, String] {
  override def getKey(in: SensorReading): String = in.id
}

class MyReducer() extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.timestamp))
  }
}

class MyMapper extends MapFunction[SensorReading, (String, Double)]() {
  override def map(t: SensorReading): (String, Double) = (t.id, t.temperature)
}
