import org.apache.spark.{SparkConf, SparkContext}

object DollarZeroResearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    // посчитаем, сколько строк начинается с "$0" и содержит "ACC_45616"
    val result = logs
      .flatMap { case (filename, content) =>
        val lines = content.split("\n")
        lines.zipWithIndex
          .filter { case (line, _) => line.startsWith("$0") && line.contains("ACC_45616") }
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