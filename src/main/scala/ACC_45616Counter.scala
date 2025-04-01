import org.apache.spark.{SparkConf, SparkContext}

object ACC_45616Counter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    val result = logs
      .flatMap { case (filename, content) =>
        val lines = content.split("\n")
        lines.zipWithIndex
          .filter { case (line, _) => line.startsWith("CARD_SEARCH_END") || line.startsWith("QS")}
          .map { case (_, index) => lines(index + 1) }
          .filter { line => line.contains("ACC_45616") }
          .map { _ => 1 }
      }
      .sum
    println(result)

    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}