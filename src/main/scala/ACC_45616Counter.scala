import org.apache.spark.{SparkConf, SparkContext}

object ACC_45616Counter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // замеряем время
    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    val result = logs.flatMap { case (_, content) =>
      content.split("\n").sliding(2).collect {
        case Array(prev, next) if prev.contains("CARD_SEARCH_END") && next.contains("ACC_45616") => 1
      }
    }.sum()
    println(result)

    // выведем, сколько времени занял анализ
    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}