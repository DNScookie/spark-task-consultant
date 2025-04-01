import org.apache.spark.{SparkConf, SparkContext}

object Anomalies {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // замеряем время
    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    // посчитаем количество символов { и } в каждом файле
    val filesWithMismatchedBraces = logs
      .map { case (filename, content) =>
        val counts = content.foldLeft((0, 0)) { (acc, char) =>
          char match {
            case '{' => (acc._1 + 1, acc._2)
            case '}' => (acc._1, acc._2 + 1)
            case _ => acc
          }
        }
        println(s"Файл: $filename, Открывающиеся скобки: ${counts._1}, Закрывающиеся скобки: ${counts._2}")
        (filename, counts)
      }
      .filter { case (_, (openBraces, closeBraces)) => openBraces != closeBraces }
      .map { case (filename, _) => filename }
      .collect()

    // выведем имена файлов с несоответствием количества скобок
    if (filesWithMismatchedBraces.isEmpty) {
      println("Нет файлов с несоответствием количества скобок.")
    } else {
      filesWithMismatchedBraces.foreach(println)
    }

    // выведем, сколько времени занял анализ
    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}