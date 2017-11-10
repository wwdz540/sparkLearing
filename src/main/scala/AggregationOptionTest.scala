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

    testAggregation(rDD)
   // testGouptBy(rDD)

    //testReduceByKey(rDD)
  }


  private def testAggregation(rDD: RDD[(String, Int)]) = {
    var rdd = rDD.aggregateByKey(0)((x, y) => (x + y), (x, y) => x + y);
    rdd.foreach(println)
  }

  private def testGouptBy(rDD: RDD[(String, Int)]) = {
    var rdd = rDD.groupByKey();
    rdd.foreach(x => println(x))
  }

  private def testReduceByKey(rDD: RDD[(String, Int)]) = {
    var rdd = rDD.mapValues(((_, 1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2 + y._2));
    println("===test reduce by key ")
    rdd.foreach(x=>println(x))
  }
}
