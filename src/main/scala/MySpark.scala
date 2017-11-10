import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/***
  * 对spark的测试
  */

object MySpark {
  def main(args: Array[String]): Unit = {

    var sc =new  SparkContext(new SparkConf().setAppName("myTest").setMaster("local"))
    var rdd = sc.textFile("file:///Users/zhipingwang/tmp/test.txt").flatMap(_.split(" "))
    testAggregations(rdd)
  }

  def testAggregations(rDD: RDD[String]): Unit ={
     var rdd =   rDD.map(x => (x,1))
     rdd.foreach(x => (println(x)))


  }



  /**
    * 注意Any关键字。
    * */
  def printData(rDD: RDD[Any]): Unit ={
    rDD.foreach(x => (println(x)))
  }


}
