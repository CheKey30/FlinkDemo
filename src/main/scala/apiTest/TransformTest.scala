package apiTest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    // 0.读取数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "/Users/shuchen/IdeaProjects/FlinkDemo/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    //1.转换样例类
    val dataStream = inputStream
      .map(data=>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    //2.分组聚合，输出每个传感器的当前最小值
    val aggStream = dataStream
      .keyBy("id")
      .min("temperature")
//    aggStream.print()

    //3. 输出当前最小的温度值和最近的时间戳，用reduce（相当于多个字段都进行聚合）
    val resultStream = dataStream
      .keyBy("id")
      .reduce((curState,newData) =>
        SensorReading(curState.id,newData.timeStamp,curState.temperature.min(newData.temperature))
      )
//    resultStream.print()

    //4.多流转换操作
    //4.1将传感器数据分成低温和高温2个（30c）
    val splitStream = dataStream
      .split(data=>{
        if(data.temperature>30.0) Seq("high") else Seq("low")
      })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    highTempStream.print("high")
    lowTempStream.print("low")
    env.execute("transform test")
  }
}
