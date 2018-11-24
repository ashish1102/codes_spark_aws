import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Joins {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Joins"))

    case class Customer (cust_id: Int, name: String)
    case class Txn (cust_id: Int, store_id: String, amount: Float)

    //val custs = sc.textFile("custs").map(_.split("\t")).map( r => (r(0), Customer(r(0), r(1))))

    val custs = sc.textFile("/home/user/spark_datasets/custs").map(_.split("\t"))
    val cust_recs = custs.map( r => (r(0).toInt, Customer(r(0).toInt, r(1))))

    val txns = sc.textFile("/home/user/spark_datasets/custs_txns").map(_.split("\t"))
    val txns_recs = txns.map( r => (r(0).toInt, Txn(r(0).toInt, r(1), r(2).toFloat)))

    val joind = cust_recs.join(txns_recs)

    val leftOuterjoind = cust_recs.leftOuterJoin(txns_recs)

    // joind.foreach(println)
    leftOuterjoind.foreach(println)

  }
}
