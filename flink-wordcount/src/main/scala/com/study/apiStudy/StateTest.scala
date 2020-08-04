package com.study.apiStudy

import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.common.time.Time


/*
* @author: helloWorld
* @date  : Created in 2020/7/29 13:12
*/

/*
* 状态编程
* */
object StateTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //配置状态后端
    //    environment.setStateBackend(new MemoryStateBackend())
    //    environment.setStateBackend(new FsStateBackend("path"))
    //    environment.setStateBackend(new RocksDBStateBackend("path",true))

    //checkPoint配置
    //启用检查点，指定触发检查点的间隔时间(毫秒)
    environment.enableCheckpointing(1000L)
    //其他配置
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(10000L)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    environment.getCheckpointConfig.setPreferCheckpointForRecovery(false)
    environment.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    //配置重启策略
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    environment.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 666)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    val warningStream: DataStream[(String, Double, Double)] = dataStream
      //            .map(new MyMapper2())
      .keyBy("id")
      //      .flatMap(new TempChangeWarningWithFlatmap(10.0))
      .flatMapWithState[(String, Double, Double), Double]({
      case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
      case (inputData: SensorReading, lastTemp: Some[Double]) => {
        val diff = (inputData.temperature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
        } else {
          (List.empty, Some(inputData.temperature))
        }
      }
    })
    warningStream.print()
    environment.execute()
  }
}

// operator state示例
class MyMapper2() extends RichMapFunction[SensorReading, Long] with ListCheckpointed[java.lang.Long] {
  //  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  var count: Long = 0L

  override def map(value: SensorReading): Long = {
    count += 1
    count
  }

  override def restoreState(state: util.List[java.lang.Long]): Unit = {
    val itea = state.iterator()
    while (itea.hasNext) {
      count += itea.next()
    }

    /* for (countState<-state){
       count+=countState
     }*/
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[java.lang.Long] = {
    val stateList = new util.ArrayList[java.lang.Long]()
    stateList.add(count)
    stateList
  }
}

// keyed state定义示例
class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int] {
  //lazy val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state",classOf[Int]))
  lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("my-liststate", classOf[String]))
  lazy val myMapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("my-MapState", classOf[String], classOf[Double]))
  getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
    "my-reducingstate",
    new MyReduceFunction(),
    classOf[SensorReading]))

  var myState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
    myState.value()
    myState.update(1)
    myListState.add("hello")
    myListState.update(new util.ArrayList[String]())
    myMapState.put("sensor_1", 10.0)
    myMapState.get("sensor_1")

  }
}

// 自定义 RichFlatMapFunction，可以输出多个结果
class TempChangeWarningWithFlatmap(stand: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  //刚开始状态中没有数据，默认值为0.0
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //从状态中取出上次的温度值
    val lastTemp: Double = lastTempState.value()
    //更新温度状态
    lastTempState.update(value.temperature)

    //跟当前温度进行比较，如果大于阈值，就报警
    val abs: Double = (value.temperature - lastTemp).abs //绝对值
    if (abs > stand) {
      out.collect(value.id, lastTemp, value.temperature)
    }
  }
}

// 自定义 RichMapFunction
class TempChangeWarning(stand: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {
  //定义状态变量，上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def map(value: SensorReading): (String, Double, Double) = {
    //从状态中取出上一次温度值
    val lastTemp: Double = lastTempState.value()
    //更新状态
    lastTempState.update(value.temperature)

    //跟当前温度值计算差值，如果大于阈值，则报警
    val abs: Double = (value.temperature - lastTemp).abs //绝对值
    if (abs > stand) {
      (value.id, lastTemp, value.temperature)
    } else {
      (value.id, 0.0, 0.0)
    }
  }
}

class MyReduceFunction() extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
  }
}
