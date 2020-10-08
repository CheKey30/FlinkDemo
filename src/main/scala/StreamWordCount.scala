import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit={
    //init
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // receive a socket txt stream
    // use "nc -lk 7777" to start a terminal to get input
    val inputDataStream = streamEnv.socketTextStream("localhost",7777)
    // process data
    val resultDataStream = inputDataStream.flatMap(x=>x.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)
    //print result
    resultDataStream.print()
    //start stream program
    streamEnv.execute("Stream wordCount")

  }

}
