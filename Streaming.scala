import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(20))
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("/home/ashish/Desktop/ashish")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    //println("I am done")
    ssc.start()
    ssc.awaitTermination()
  }
}
