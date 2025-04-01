import org.apache.spark.{SparkConf, SparkContext}

object ACC_45616Counter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // замеряем время
    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    val result = logs
      .map { case (filename, content) =>
        var lines = content.split("\n")
        val searchResultIndices = lines.zipWithIndex
          .filter { case (line, _) => line.contains("CARD_SEARCH_END") }
          .map { case (_, index) => index + 1 }
        // результаты поиска
        searchResultIndices
          .map { i => lines(i) }
          .filter { line => line.contains("ACC_45616") }
          .map { _ => 1 }.sum
      }
      .sum
    println(result)

    // выведем, сколько времени занял анализ
    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}