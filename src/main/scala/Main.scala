import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Загрузка всех логов из директории
    val logs = sc.textFile("Сессии\\*")

    // выведем количество строк в файлах
    val totalLines = logs.count()
    println(s"Общее количество строк в файлах: $totalLines")
    scala.io.StdIn.readLine()

    // посчитаем количество строк, которые начинаются с DOC_OPEN и содержат ACC_45616
    val filteredLines = logs.filter(line => line.startsWith("DOC_OPEN") && line.contains("ACC_45616"))
    val count = filteredLines.count()
    println(s"Количество строк, которые начинаются с DOC_OPEN и содержат ACC_45616: $count")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}