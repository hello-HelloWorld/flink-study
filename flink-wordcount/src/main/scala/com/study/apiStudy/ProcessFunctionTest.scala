package com.study.apiStudy

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/7/28 16:02
*/

/*
* ProcessFunction API
* */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //接收socket文本数据
    val inputStream: DataStream[String] = environment.socketTextStream("master1", 7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    //检测每一个温度传感器是否在10秒内连续上升
    val warningStream: DataStream[String] = dataStream.keyBy("id").process(new MyTempIncWarning(10000L))
    warningStream.print()
    environment.execute("process function job")
  }
}

// 自定义 KeyedProcessFunction
class MyTempIncWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
  //由于需要跟之前的温度做对比，所以需要保存上一个温度的状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //为了方便删除定时器，需要保存定时器的时间戳
  lazy val curTimeTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timeTs", classOf[Long]))

  override def processElement(sensorReading: SensorReading, context: KeyedProcessFunction[Tuple, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //首先取出状态
    val lastTemp: Double = lastTempState.value()
    val curTimeTs: Long = curTimeTsState.value()

    //将上次温度值的状态更新为当前数据的温度值
    lastTempState.update(sensorReading.timestamp)

    //判断当前温度值，如果比之前温度高，并且没有定时器的话，注册10秒后的定时器
    if (sensorReading.timestamp > lastTemp && curTimeTs == 0) {
      val ts: Long = context.timerService().currentProcessingTime() + interval
      //注册定时器
      context.timerService().registerProcessingTimeTimer(ts)
      //更新状态
      curTimeTsState.update(ts)
    }
    //如果温度下降，则删除定时器
    else if (sensorReading.timestamp < lastTemp) {
      //删除定时器
      context.timerService().deleteProcessingTimeTimer(curTimeTs)
      //清空状态
      curTimeTsState.clear()
    }
  }

  //定时器触发，说明10秒内没有下降的温度，报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    val key: String = ctx.getCurrentKey.asInstanceOf[Tuple1[String]].f0
    out.collect(key+"温度值连续" + interval / 1000 + "秒上升")
    curTimeTsState.clear()
  }
}
