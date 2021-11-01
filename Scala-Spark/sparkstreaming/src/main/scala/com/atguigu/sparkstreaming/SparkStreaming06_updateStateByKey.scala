package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming06_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.checkpoint("D:\\IdeaProjects\\SparkStreaming0625\\ck")

    //需求:对接hadoop102的9999端口,获取数据源,做一个流式的累加版的WordCount
    //1.对接数据源
    val lineDStrem: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //2 按照空格切分数据,并且炸开
    val wordDStream: DStream[String] = lineDStrem.flatMap(_.split(" "))

    //3 转换数据结构 word=>(word,1)
    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //4 进行累加
    val resultDSteam: DStream[(String, Int)] = word2oneDStream.updateStateByKey(updateFunc)

    //5 打印结果
    resultDSteam.print()


    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc = (current:Seq[Int], state:Option[Int]) => {
    //1.先对当前批次的数据求和
    val currentSum: Int = current.sum
    //2.取出历史状态数据的和
    val stateSum: Int = state.getOrElse(0)

    //3.将当前批次的和加上历史状态的和,返回
    Some(currentSum+stateSum)
  }

}
