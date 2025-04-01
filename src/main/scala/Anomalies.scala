import org.apache.spark.{SparkConf, SparkContext}

object Anomalies {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // замеряем время
    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    // выведем названия файлов, в которых невозможно распарсить дату на строчке с "DOC_OPEN"
    val filesWithInvalidDate = logs
      .map { case (filename, content) =>
        val lines = content.split("\n")
        val invalidLines = lines.filter(line => line.startsWith("DOC_OPEN") && !line.split(" ")(1).matches("\\d{2}\\.\\d{2}\\.\\d{4}_\\d{2}:\\d{2}:\\d{2}"))
        if (invalidLines.nonEmpty) {
          println(s"Файл: $filename, Неверные строки: ${invalidLines.mkString(", ")}")
          Some(filename)
        } else {
          None
        }
      }
      .filter(_.isDefined)
      .map(_.get)
      .collect()

    println("Нажмите Enter, чтобы продолжить...")
    scala.io.StdIn.readLine()

    // выведем названия файлов, в которых невозможно распарсить дату на строчке с "QS"
    val filesWithInvalidQSDate = logs
      .map { case (filename, content) =>
        val lines = content.split("\n")
        val invalidLines = lines.filter(line => line.startsWith("QS") && !line.split(" ")(1).matches("\\d{2}\\.\\d{2}\\.\\d{4}_\\d{2}:\\d{2}:\\d{2}"))
        if (invalidLines.nonEmpty) {
          println(s"Файл: $filename, Неверные строки: ${invalidLines.mkString(", ")}")
          Some(filename)
        } else {
          None
        }
      }
      .filter(_.isDefined)
      .map(_.get)
      .collect()

    println("Нажмите Enter, чтобы продолжить...")
    scala.io.StdIn.readLine()

    // найдём все строки с QUEST_197336 и выведем их с именами файлов
    val questLines = logs
      .flatMap { case (filename, content) =>
        val lines = content.split("\n")
        val questLines = lines.filter(line => line.contains("QUEST_197336"))
        if (questLines.nonEmpty) {
          questLines.map(line => (filename, line))
        } else {
          Array.empty[(String, String)]
        }
      }
      .collect()

    questLines.foreach { case (filename, line) =>
      println(s"Файл: $filename, Строка: $line")
    }

    // выведем, сколько времени занял анализ
    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}