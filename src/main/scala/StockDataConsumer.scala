import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafkautils.Consumer.consumeFromKafka
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import kafkautils.Dispatcher._

import scala.jdk.CollectionConverters._

object StockDataConsumer {

  def main(args: Array[String]): Unit = {

    val readStockDataTopic = args.lift(0).getOrElse("stock-data-topic")
    val sendMaxTTVStockDataTopic = args.lift(1).getOrElse("max-TTV-stock-data-topic")

    val consumer: KafkaConsumer[String, JsonNode] = consumeFromKafka[String, JsonNode](consumerProperties(), readStockDataTopic)
    val stockDataUtil = new StockDataUtil()

    import scala.concurrent.duration._
    val deadline = 60.seconds.fromNow

    while (deadline.hasTimeLeft) {
      val recordList: List[ConsumerRecord[String, JsonNode]] = consumer.poll(java.time.Duration.ofMillis(10)).asScala.toList
      val batchMaxRecord: List[(Date, StockData)] = recordList.map {
        record =>
          new ObjectMapper().treeToValue(record.value, classOf[StockData])
      }.map(stockData => stockData.getTradeDate -> stockData)
        .groupBy(_._1)
        .toList
        .map(_._2)
        .map(stockDataUtil.maxTotTrdVal)

      stockDataUtil.dateMaxRecord(batchMaxRecord)
    }

    val dayMaxTtvRecord = stockDataUtil.dateMaxRecord

    dayMaxTtvRecord
      .flatMap(dayStockData => List[JsonNode](new ObjectMapper().valueToTree(dayStockData._2)))
      .foreach {
        recValue =>
          val producerRecord = new ProducerRecord[String, JsonNode](sendMaxTTVStockDataTopic, recValue.hashCode().toString, recValue)
          writeToKafka[String, JsonNode](StockDataProducer.producerProperties(), producerRecord)
      }
  }


  def consumerProperties(): Properties = {

    val props = new Properties()
    val kafkaConfigStream = classOf[ClassLoader].getResourceAsStream("/kafka.properties")
    props.load(kafkaConfigStream)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[JsonDeserializer].getName)
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "consumer-group-Stock-Data")

    props
  }

}
