package com.study.apiStudy


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

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
    //周期性的产生waterMarket，设置时间，单位毫秒
    //    environment.getConfig.setAutoWatermarkInterval(1000L)

    val inputStram: DataStream[String] = environment.socketTextStream("hadoop102", 7777)
    val dataStream: DataStream[SensorReading] = inputStram.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })
    //数据流的时间戳是单调递增的，没有乱序。直接使用数据的时间戳生成watermarket
    //            .assignAscendingTimestamps(_.timestamp * 1000L)
    //对于乱序数据，能估算出数据流中事件的最大延迟时间
    /* .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
     override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
   })*/
    //周期性生成watermaker
    //      .assignTimestampsAndWatermarks(new MyWMAssigner(1000L))


    val resultStream = dataStream
      .keyBy("id")
      //      .window(EventTimeSessionWindows.withGap(Time.minutes(1)))//会话窗口，前后数据的间隔超过1分钟，划分一个窗口
      //      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))//处理时间偏移
      .timeWindow(Time.seconds(15), Time.seconds(5)) //时间滑动窗口
      .allowedLateness(Time.minutes(1)) //窗口允许处理迟到多少时间的数据
      .sideOutputLateData(new OutputTag[SensorReading]("late")) //将迟到的数据放入侧输出流
      //      .reduce(new MyReducer())
      .apply(new MyWindowFun())

    dataStream.print("data")
    resultStream.getSideOutput(new OutputTag[SensorReading]("late"))
    resultStream.print("result")
    environment.execute("window test")
  }
}

//自定义一个周期性生成watermarket的assigner
class MyWMAssigner(latess: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {
  //需要两个关键参数：延迟时间，和当前所有数据中最大的时间戳,latess是最大延迟时间
  var maxTs: Long = Long.MinValue + latess

  //得到当前的waterMaker
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - latess)
  }

  //得到当前数据的时间戳
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp * 1000L)
    t.timestamp * 1000L
  }
}

//自定义一个断点式生成watermark的assigner
class MyWMAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading] {

  val latess = 1000L

  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if (t.id == "sensor_1") {
      new Watermark(l - latess)
    } else {
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000L
  }
}

// 自定义一个全窗口函数
class MyWindowFun() extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
    out.collect(window.getStart, input.size)
  }

}
