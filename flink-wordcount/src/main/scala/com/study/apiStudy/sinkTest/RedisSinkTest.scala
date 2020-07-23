package com.study.apiStudy.sinkTest

import com.study.apiStudy.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/*
* @author: helloWorld
* @date  : Created in 2020/7/23 13:44
*/

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)

    //读取文件数据
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    //定义一个redis的配置类
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()

    //定义一个redisMapper
    val redisMapper: RedisMapper[SensorReading] = new RedisMapper[SensorReading] {
      //定义保存数据到redis的命令，hset，table_name,key value
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
      }

      override def getKeyFromData(t: SensorReading): String = t.id

      override def getValueFromData(t: SensorReading): String = t.temperature.toString
    }

    dataStream.addSink(new RedisSink(conf, redisMapper))
    environment.execute("redis sink test")
  }
}
