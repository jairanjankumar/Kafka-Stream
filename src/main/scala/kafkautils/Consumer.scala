package kafkautils

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

object Consumer {

  def consumeFromKafka[K, V](prop: Properties, topic: String): KafkaConsumer[K, V] = {

    val consumer: KafkaConsumer[K, V] = new KafkaConsumer[K, V](prop)
    consumer.subscribe(util.Arrays.asList(topic))
    consumer
  }
}
