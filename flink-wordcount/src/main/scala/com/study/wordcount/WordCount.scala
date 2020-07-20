package com.study.wordcount

import org.apache.flink.api.scala._

//批处理
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据，读取所有的数据在进行处理
    val inputPath = "E:\\develop\\idea2018.2.3\\workspace\\flink-study\\flink-wordcount\\src\\main\\resources\\word.txt"
    val inputDS: DataSet[String] = environment.readTextFile(inputPath)
    val wordCount: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" "))//分词得到word的数据集
      .map((_, 1))//转换成二元组
      .groupBy(0) //按照第一个元素就行分组
      .sum(1) //将第二个元素进行求和
    //打印输出
    wordCount.print()
  }
}
