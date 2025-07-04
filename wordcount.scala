/* wordcount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val logFile = "input.txt" 
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 1).cache()
    val wordCounts = logData.flatMap(line=>line.split(" ")).filter(_.trim.nonEmpty).map(x => (x, 1)).reduceByKey((x, y) => x + y).map { case (value, count) =>
      s"$value $count"
    }
    
    wordCounts.saveAsTextFile("output")

    sc.stop()
  }
}

