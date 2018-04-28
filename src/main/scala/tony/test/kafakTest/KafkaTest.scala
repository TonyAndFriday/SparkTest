package tony.test.kafakTest

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tony on 2018/4/25.
  * 自定义kafkaManager，将Offset信息保存到zookeeper,失败重启后也能继续消费
  */
object KafkaTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MyWordCount").setMaster("local").set("spark.streaming.kafka.maxRatePerPartition", "1")
    val ssc = new StreamingContext(conf,Seconds(2))
    val topic = Set("test")
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "172.16.100.137:9092,172.16.100.138:9092,172.16.100.139:9092",
      "group.id" -> "test4",
      "auto.offset.reset" -> "smallest"
    )
    val km = new KafkaManager(kafkaParams)
    val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topic)
    //val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topic)
    kafkaStream.count().print()
    kafkaStream.print()
    kafkaStream.foreachRDD(x => {
      if(!x.isEmpty())
        km.updateZKOffsets(x)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
