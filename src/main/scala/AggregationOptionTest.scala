import MySpark.testAggregations
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregationOptionTest {
  def main(args: Array[String]): Unit = {
    var sc =new  SparkContext(new SparkConf().setAppName("myTest").setMaster("local"))
    //var rdd = sc.textFile("file:///Users/zhipingwang/tmp/test.txt").flatMap(_.split(" "))
   // testAggregations(rdd)

    var rDD =  sc.parallelize(List(
      ("panda",0),
      ("pink",3),
      ("prate",3),
      ("panda",1),
      ("pink",4)));

    testReduceByKey(rDD)
  }


  private def testReduceByKey(rDD: RDD[(String, Int)]) = {
    var rdd = rDD.mapValues(((_, 1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2 + y._2));
    println("===test reduce by key ")
    rdd.foreach(x=>println(x))
  }
}
