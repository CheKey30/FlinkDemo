import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit={
    //init
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val paramTool: ParameterTool  = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")
    //set the number of threads
    streamEnv.setParallelism(9)
    // receive a socket txt stream
    // use "nc -lk 7777" to start a terminal to get input
    val inputDataStream = streamEnv.socketTextStream(host,port)
    // process data
    val resultDataStream = inputDataStream.flatMap(x=>x.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)
    //print result
    resultDataStream.print().setParallelism(1)
    //start stream program
    streamEnv.execute("Stream wordCount")

    /*输出的前面数字为并行子任务的编号，默认的最大数字为机器核心数。相同的统计字符key会被同一个子任务处理。
    因为flink会根据keyby的关键字进行hash来分配子任务。可以给每一步算子设置不同的并行度(在写入文件时并行取1)
    */
  }

}
