import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import java.io.Serializable


class MetricsCalculatorReuseObjects(
                                     var totalWords : Int,
                                     var longestWord: Int,
                                     var happyMentions : Int,
                                     var numberReportCards: Int)  extends Serializable {

  def sequenceOp(reportCardContent : String) = {
    val words = reportCardContent.split(" ")
    totalWords += words.length
    longestWord = Math.max(longestWord, words.map( w => w.length).max)
    happyMentions += words.count(w => w.toLowerCase.equals("happy"))
    numberReportCards +=1
    this
  }

  def compOp(other : MetricsCalculatorReuseObjects)  = {
    totalWords += other.totalWords
    longestWord = Math.max(this.longestWord, other.longestWord)
    happyMentions += other.happyMentions
    numberReportCards += other.numberReportCards
    this
  }

  def toReportCardMetrics =
    ReportCardMetrics2(
      longestWord,
      happyMentions,
      totalWords.toDouble/numberReportCards)
}


object MetricsCalculator2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AggregateSample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textRDD: RDD[String] = sc.textFile("src\\main\\resources\\InstructorReport")
    val keyValue: RDD[(String, String)] = textRDD.map(x => {
      val list = x.split(",")
      Tuple2(list(0), list(1))
    })

    calculateReportCardStatistics(keyValue)
  }


  def calculateReportCardStatistics(rdd: RDD[(String, String)]
                                   ) = {
    val result = rdd.aggregateByKey(new MetricsCalculatorReuseObjects(0,0,0,0))(
      seqOp = ((reportCardMetrics, reportCardText) =>
        reportCardMetrics.sequenceOp(reportCardText)),
      combOp = ((x,y) => x.compOp(y)))
      .mapValues(_.toReportCardMetrics)

    result.foreach(k => println(k._1 + " : " + k._2))

  }
}

case class ReportCardMetrics2 (longestWord : Int, happyMentions : Int ,avgWordsPerReport : Double ) {
  @Override
  override def toString: String = {
    longestWord + " " + happyMentions + " " + avgWordsPerReport
  }

}
