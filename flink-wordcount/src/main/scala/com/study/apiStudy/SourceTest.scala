package com.study.apiStudy

import java.sql.Timestamp

/*
* 数据来源：
* 1.从集合中读取数据
* 2.从文件中读取数据
* 3.socket文本流
* 4.从kafka读取数据
* */

//输入数据的样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {

}
