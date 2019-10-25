package kafkautils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Dispatcher {

  def writeToKafka[K, V](producerProperties: Properties, record: ProducerRecord[K, V]): Unit = {

    val producer = new KafkaProducer[K, V](producerProperties)
    producer.send(record)
    producer.close()
  }
}
