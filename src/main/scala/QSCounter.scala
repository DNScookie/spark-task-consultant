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
        val lines = content.split("\n")
        // найдём все строки с "QS" и запомним дату поиска и ID
        val searchResults = lines.zipWithIndex
          .filter { case (line, _) => line.startsWith("QS") }
          .map { case (line, index) => (line.split(" ")(1), lines(index + 1).split(" ")(0)) } // (date, searchId)
        // составим пары с ключом (дата, ID документа) и значением 1
        lines
          .filter(_.startsWith("DOC_OPEN"))
          .flatMap { line =>
            searchResults.collect {
              // связываем открытие документа с датой поиска
              case (date, searchId) if line.contains(searchId) => (date, line)
            }
          }
          .map { case (date, line) => (date, line.split(" "))}
          // если дата открытия не записалась из-за сбоя, берём дату поиска
          .map { case (date, line) => ((if (line(1) == "") date else line(1), line(3)), 1) }
      }
      .reduceByKey(_ + _) // ((date, docId), count)

    val dateFormat = new java.text.SimpleDateFormat("dd.MM.yyyy") // формат даты менялся, однако диапазон сбоя это не затронуло
    val sortedResult = result
      .map { case ((date, docId), count) => (dateFormat.parse(date), (docId, count)) }
      .groupByKey()
      .map { case (date, docCounts) =>
        val sortedDocs = docCounts.toList.sortBy { case (_, count) => -count }
        (date, sortedDocs)
      }
      .sortBy { case (date, _) => date }
      .collect()

    // для каждой даты выведем результаты
    sortedResult.foreach { case (date, docCounts) =>
      val formattedDate = dateFormat.format(date)
      println(s"Дата: $formattedDate")
      docCounts.foreach { case (docId, count) =>
        println(s"  Документ: $docId, Количество открытий: $count")
      }
    }

    // сохраним результат в csv файл
    val outputPath = "output/QSCounterResult.csv"
    val header = "Дата,Документ,Количество открытий\n"
    val csvData = sortedResult.flatMap { case (date, docCounts) =>
      docCounts.map { case (docId, count) => s"${dateFormat.format(date)},$docId,$count" }
    }.mkString("\n")
    val csvContent = header + csvData
    val outputFile = new java.io.File(outputPath)
    val writer = new java.io.PrintWriter(outputFile)
    writer.write(csvContent)
    writer.close()
    println(s"Результат сохранён в файл: $outputPath")

    // выведем, сколько времени занял анализ
    println(s"Время выполнения: ${(System.currentTimeMillis() - startTime) / 1000.0} секунд")

    println("Нажмите Enter, чтобы завершить программу...")
    scala.io.StdIn.readLine()
    sc.stop()
  }
}