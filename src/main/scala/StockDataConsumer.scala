import java.time.Duration
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafkautils.Consumer.consumeFromKafka
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

object StockDataConsumer {

  def main(args: Array[String]): Unit = {

    val consumer: KafkaConsumer[String, JsonNode] = consumeFromKafka[String, JsonNode](consumerProperties(), args(0))

    while (true) {
      val recordList: List[ConsumerRecord[String, JsonNode]] = consumer.poll(Duration.ofMillis(10000)).asScala.toList

      val j: List[(Double, Date)] = recordList.map {
        record =>
          new ObjectMapper().treeToValue(record.value, classOf[StockData])
      }.map(sd => sd.getTradeDate -> sd.getTotalTradedVal.doubleValue)
        .groupBy(_._1)
        .toList
        .map(_._2)
        .map(lt => lt.map(dt => dt._2 -> dt._1))
        .map(_.max)


      j.foreach(y => println(f"Consumer Here : ${y._1}%15.2f     ${y._2}"))


    }
  }


  def consumerProperties(): Properties = {

    val props = new Properties()
    val kafkaConfigStream = classOf[ClassLoader].getResourceAsStream("/kafka.properties")
    props.load(kafkaConfigStream)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[JsonDeserializer].getName)
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group-Stock-Data")

    props
  }

}
