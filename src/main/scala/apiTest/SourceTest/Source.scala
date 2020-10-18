package apiTest

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

//定义样例类，温度传感器
case class SensorReading(id: String, timeStamp: Long, temperature: Double)

class SensorSource() extends SourceFunction[SensorReading]{

  // define a flag to stop flow
  var running: Boolean = true
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    val rand = new Random()
    // randomly generate 10 temperatures for 10 sensors (i,temp)
    var currTemp = 1.to(10).map(i=>("sensor_"+i,rand.nextDouble()*100))
    while(running){
      // update temp of each sensor, call ctx.collect to send data
      currTemp = currTemp.map(data=>(data._1,data._2+rand.nextGaussian()))
      // get the timestamp for sensor
      val curTime  = System.currentTimeMillis();
      currTemp.foreach(data => ctx.collect(SensorReading(data._1,curTime,data._2)))
      Thread.sleep(500)
    }

  }

  override def cancel(): Unit = running = false
}

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // read data from set
    val datalist = List(
      SensorReading("s1",1000000,35.8),
      SensorReading("s2",10000001,36),
      SensorReading("s3",10000002,37),
      SensorReading("s4",10000004,38),
      SensorReading("s5",10000005,39)
    )

    val stream1 = env.fromCollection(datalist)
    stream1.print()

    // read from file
    val inputPath = "/Users/shuchen/IdeaProjects/FlinkDemo/src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)
    stream2.print()


    //read from kafka
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","localhost:9092")
    prop.setProperty("group.id","consumer-group")
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),prop))
    stream3.print()

    //define source
    val stream4 = env.addSource(new SensorSource())
    stream4.print()

    env.execute("source")
  }
}
