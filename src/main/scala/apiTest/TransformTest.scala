package apiTest

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.concurrent.Future.never.value

object TransformTest {
  def main(args: Array[String]): Unit = {
    // 0.读取数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = "D:\\FlinkDemo\\src\\main\\resources\\sensor.txt"
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
      .min("temperature").filter(new MyFilter)
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

//    highTempStream.print("high")
//    lowTempStream.print("low")

    // 4.2 合流 connect
    val warningStream = highTempStream.map(data=>(data.id,data.temperature))
    val connectedStrems = warningStream.connect(lowTempStream);

    // 用comap对数据分别进行处理
    val coMapResultStream: DataStream[Any] = connectedStrems
      .map(warningData=>(warningData._1,warningData._2,"warning"),lowTempData=>(lowTempData.id,"healthy"))


    // 4.3 union 合流
    val unionStream = highTempStream.union(lowTempStream)

    coMapResultStream.print("co")
    env.execute("transform test")
  }


}
//自定义函数类
class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = t.id.startsWith("s1")
}

// 富函数,可以获取到运行时上下文
class MyRichFilter extends RichFilterFunction[SensorReading]{
  override def open(parameters: Configuration): Unit = {
    // 初始化操作，比如连接数据库
  }
  override def filter(t: SensorReading): Boolean = t.id.startsWith("s1")

  override def close(): Unit = {
    //收尾操作
  }

}
