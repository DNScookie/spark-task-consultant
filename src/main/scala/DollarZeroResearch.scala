import org.apache.spark.{SparkConf, SparkContext}

object DollarZeroResearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    // выведем все документы, содержащие строки с "$0"
    val dollarZeroLines = logs
      .flatMap { case (filename, content) =>
        val lines = content.split("\n")
        val dollarZeroLines = lines.filter(line => line.contains("$0"))
        if (dollarZeroLines.nonEmpty) {
          dollarZeroLines.map(line => (filename, line))
        } else {
          Array.empty[(String, String)]
        }
      }
      .collect()
    // распечатаем результат
    dollarZeroLines.foreach { case (filename, line) =>
      println(s"Файл: $filename, Строка: $line")
    }

    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}