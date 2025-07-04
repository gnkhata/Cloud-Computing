/* wordcount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

object PageRank {
  def main(args: Array[String]) {
    val logFile = "input.txt" 
    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    
    val links = logData.map(x => (x.split(" ")(0), x.split(" ")(1))).groupByKey()
    val nodes = logData.flatMap(line=>line.split(" ")).filter(_.trim.nonEmpty).distinct()
    
    var node_count = nodes.count()
    var ranks = nodes.map(x => (x, 1/node_count.toDouble))
    
    var iter = 0
    if (args.length > 0){
        iter = args(0).toInt
    }else{
        iter = 10
    }
    
    for (i <- 0 until iter){
        var danglingMass = ranks.subtractByKey(links).values.reduce((x,y)=>x+y)
        /*
        println("\n"+danglingMass+"\n")
        println("\n")
        */
        val contri = links.join(ranks).flatMap{
            case(pageId, (pageLinks, rank)) =>
             pageLinks.map(dest => (dest, rank/pageLinks.size))
        }
        /*
        contri.reduceByKey((x,y)=>x+y).collect.foreach(println(_))
        println("\n")
        */
        ranks = contri.reduceByKey((x,y)=>x+y).mapValues(v => (0.1*(1/node_count.toDouble)).toDouble + (0.9* ((danglingMass / node_count.toDouble) + v )).toDouble)
    }
    ranks.sortByKey().saveAsTextFile("output")

    sc.stop()
  }
}

