import org.apache.spark.{SparkConf, SparkContext}

object ACC_45616Counter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()
    // загрузим все строки из файлов в директории "Сессии"
    val logs = sc.textFile("Сессии/*")

    // посчитаем все строки, начинающиеся с "$0" и содержащие "ACC_45616"
    val result = logs
      .filter(line => line.startsWith("$0") && line.contains("ACC_45616"))
      .map(_ => 1)
      .reduce(_ + _)
    println(result)

    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}