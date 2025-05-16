val lines = sc.textFile("hdfs:///user/root/inspections.tsv")

val data = lines.filter(!_.startsWith("INSPECTION ID")).map(_.split("\t"))
val valid = data.filter { cols => cols.length > 11 && cols(11).matches("\\d{1,2}/\\d{1,2}/\\d{4}")}
val ymCounter = valid.map { cols =>val Array(m, d, y) = cols(11).split("/"); (s"$y-$m", 1)}.reduceByKey(_ + _)

ymCounter.saveAsTextFile("inspections-by-yearmonth")

import java.time.LocalDate
import java.time.format.DateTimeFormatter
val fmt = DateTimeFormatter.ofPattern("M/d/yyyy")

val dowCounts = valid
  .map { arr =>
    val Array(mStr, dStr, yStr) = arr(11).split("/")
    val year  = yStr.toInt
    val month = mStr.toInt
    val day   = dStr.toInt
    val date  = java.time.LocalDate.of(year, month, day)
    (date.getDayOfWeek.toString, 1)
  }
  .reduceByKey(_ + _)
  //val dowCounts = valid.map { arr => val Array(mStr, dStr, yStr) = arr(11).split("/"); val year  = yStr.toInt; val month = mStr.toInt; val day   = dStr.toInt; val date  = java.time.LocalDate.of(year, month, day); (date.getDayOfWeek.toString, 1)}.reduceByKey(_ + _)
dowCounter.saveAsTextFile("inspections-by-dow")
