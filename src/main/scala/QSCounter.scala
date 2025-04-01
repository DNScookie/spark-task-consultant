import org.apache.spark.{SparkConf, SparkContext}

object QSCounter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogFileAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // замеряем время
    val startTime = System.currentTimeMillis()
    val logs = sc.wholeTextFiles("Сессии\\*")

    val result = logs
      .flatMap { case (filename, content) =>
        var lines = content.split("\n")
        val searchResults = lines.zipWithIndex
          .filter { case (line, _) => line.contains("QS") }
          .map { case (_, index) => index + 1 }
          .map { i => lines(i) }
          .map(_.split(" ")(0))
        var occurrences = lines
          .filter(_.startsWith("DOC_OPEN"))
          .filter(line => searchResults.exists(line.contains))
          .map(_.split(" "))
        occurrences.map(line => ((line(1).split("_")(0), line(3)), 1))
      }
      .reduceByKey(_ + _) // ((date, docId), count)

    val dateFormat = new java.text.SimpleDateFormat("dd.MM.yyyy")
    // теперь нужно для каждой даты по возрастанию вывести количество открытий каждого документа
    val sortedResult = result
      .map { case ((date, docId), count) => (dateFormat.parse(date), (docId, count)) }
      .groupByKey()
      .map { case (date, docCounts) =>
        val sortedDocs = docCounts.toList.sortBy { case (_, count) => -count }
        (date, sortedDocs)
      }
      .sortBy { case (date, _) => date }
      .collect()

    // выводим результат
    sortedResult.foreach { case (date, docCounts) =>
      println(s"Дата: $date")
      docCounts.foreach { case (docId, count) =>
        println(s"  Документ: $docId, Количество открытий: $count")
      }
    }

    // выведем, сколько времени занял анализ
    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}