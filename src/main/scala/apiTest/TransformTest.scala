package apiTest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "/Users/shuchen/IdeaProjects/FlinkDemo/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream
      .map(data=>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    val aggStream = dataStream
      .keyBy("id")
      .min("temperature")

    aggStream.print()

    env.execute("transform test")
  }
}
