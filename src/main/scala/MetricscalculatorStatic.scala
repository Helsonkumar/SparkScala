import MetricsCalculator2.calculateReportCardStatistics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.io.Serializable


//** In this example we have abstracted the away the seqp and combOp into a static singleton object
//** Here since we have mentioned our functions into a static singleton object which is not serialized during runtime
//** we avoid the risk of serialization
//** This would be effective in serializing the function during runtime. Here we have limited usuage of serializable interface
//** We may get NotSerializable error if the nested objects are not serializable
//** Note  : Case classes are serializable by default

case class Accum(var totalWords  : Int  , var longestWord  : Int , var  happyCount  : Int ,  var recordCount  : Int)

case class ReportCardMetrics (longestWord : Int, happyMentions : Int ,avgWordsPerReport : Double ) {
  @Override
  override def toString: String = {
    longestWord + " " + happyMentions + " " + avgWordsPerReport
  }

}


object MetricsFunc {

  def seqOp(accum : Accum ,  reportText  : String) : Accum = {
       val words =  reportText.split(" ")
       accum.happyCount += words.count(w => w.toLowerCase.equals("happy"))
       accum.longestWord = Math.max(accum.longestWord , words.map(w => w.length).max)
       accum.totalWords += words.length
       accum.recordCount += 1
       accum
  }


  def combOp(accum1 : Accum , accum2 : Accum) : Accum  = {
    accum1.happyCount += accum2.happyCount
    accum1.longestWord = Math.max(accum1.longestWord, accum2.longestWord)
    accum1.totalWords += accum2.totalWords
    accum1.recordCount += accum2.recordCount
    accum1
  }


  def toReportCardMetrics(accum : Accum) = {
    ReportCardMetrics2(
      accum.longestWord,
      accum.happyCount,
      accum.totalWords.toDouble/accum.recordCount)
  }

}


object runner  {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MetricsFunc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textRDD: RDD[String] = sc.textFile("src\\main\\resources\\InstructorReport")
    val keyValue: RDD[(String, String)] = textRDD.map(x => {
      val list = x.split(",")
      Tuple2(list(0), list(1))
    })

    calculateStatistics(keyValue)
  }

  def calculateStatistics(rdd: RDD[(String, String)]
                                   ) = {
    val result = rdd.aggregateByKey(new Accum(0,0,0,0))(
      seqOp = ((reportCardMetrics, reportCardText) =>
        MetricsFunc.seqOp(reportCardMetrics,reportCardText)),
      combOp = ((accum1 : Accum , accum2 : Accum) => MetricsFunc.combOp(accum1,accum2)))
      .mapValues(v => MetricsFunc.toReportCardMetrics(v))

    result.foreach(k => println(k._1 + " : " + k._2))

  }

}



