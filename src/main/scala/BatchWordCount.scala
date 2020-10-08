import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._



object BatchWordCount{
  def main(args: Array[String]): Unit = {
    // define batch environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // read data
    val inputPath: String = "D:\\com.flinkTest.scala\\src\\main\\resources\\chat.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // divide words and do count
    val wordCountDataSet: DataSet[(String,Int)] = inputDataSet.flatMap(_.split(" ")) //divide by " "
      .map((_,1))  // for each word _ count as 1
      .groupBy(0) // group by the first value of the two value set
      .sum(1) // sum all two value set's second value

    //print result
    wordCountDataSet.print()
  }
}
