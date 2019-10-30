package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.util.Try

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
//   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
  val date = opt[String](descr = "date of Select Query", required = true)
  val text = opt[Boolean](descr = "Use Text Data", required = false)
  val parquet = opt[Boolean](descr = "Use parquet Data", required = false)
  verify()
}

object Q2 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ2(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
     
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    val orders = sc.textFile(args.input() + "/orders.tbl")
     
     val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
     val date = args.date();
     
     val lineB = lineitem.filter(line => {
       line.split('|')(10) contains date
     })
     .map(line => {
       line.split('|')(0)
     })
     
     orders.map(line => {
       val tokens = line.split('|')
       (tokens(0), tokens(6))
     })
     .foreach(line => {
       if(Try(lineB.filter(p => p contains line._1).toBoolean).getOrElse(false)){
         (line._1,line._2)
       }
       else List()
     })
     .sortByKey()
     .take(20)
     .foreach(line => {
       println("(" + line._2 + "," + line._1 + ")")
     })
   }
}


//     val ordersB = orders.map(line=> {
//       val tokens = line.split('|')
//       (tokens(0),tokens(6))
//     })
//      .collectAsMap()
     
//     val ordersBroadcast = sc.broadcast(ordersB)
// //     val counts = 0
//     var queryOutput = scala.collection.mutable.ListBuffer[(String, String)]()
     
     
//      lineitem
//      .filter(line => {
//        line.split('|')(10) contains date
//      })
//      .map(line => {
//        val tokens = line.split('|')
//        (tokens(0),tokens(10))
//      })
//      .foreach(line => {
//        if(count.localValue < 20){
//          if(Try(ordersBroadcast.value(line._1).toBoolean).getOrElse(false)){
//            var output : (String, String) = (ordersBroadcast.value(line._1), line._1)
//            queryOutput += output
//            count += 1;
//            println("inside if")
//          }
// //          println("outside if")
//        }
//      })
     
//      println("ANSWER=");
//      for(output <- queryOutput){
//        println("(" + output._1 + "," + output._2 + ")")
//      }
