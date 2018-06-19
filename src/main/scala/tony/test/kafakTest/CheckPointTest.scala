package tony.test.kafakTest

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tony on 2018/4/27.
  * 将kafka消费信息做checkPoint，失败重启后能接着消费
  */
object CheckPointTest {

  def main(args: Array[String]) {

    //val c = createContext _

    val ssc = StreamingContext.getOrCreate("d:\\checkPoint",create)

    ssc.start()
    ssc.awaitTermination()
  }

  val create = () => { //失败重启后会多消费一个批次
    val conf = new SparkConf().setAppName("MyWordCount").setMaster("local[2]").set("spark.streaming.kafka.maxRatePerPartition", "1")
    val ssc = new StreamingContext(conf,Seconds(1))
    ssc.checkpoint("d:\\checkPoint")
    val topic = Set("test")
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "172.16.100.137:9092,172.16.100.138:9092,172.16.100.139:9092",
      "group.id" -> "test3",
      "auto.offset.reset" -> "smallest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topic)
    kafkaStream.checkpoint(Seconds(5))
    kafkaStream.print()
    ssc
  }


}
