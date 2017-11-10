import org.apache.spark.{SparkConf, SparkContext}

object FoldAndAggregation {

  def main(args: Array[String]): Unit = {
    var sc =new  SparkContext(new SparkConf().setAppName("myTest").setMaster("local"))

    var rdd = sc.parallelize(List(1,2,3,4,5)).fold(0)((x,y)=>x+y);

    println(rdd)
  }
}
