import java.io.File
import java.util.Properties
import java.util.concurrent.Executors

import com.fasterxml.jackson.databind.{JsonNode, MappingIterator, ObjectMapper}
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import org.apache.kafka.clients.producer.{ProducerConfig, _}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import fileutils.FileUtil._
import kafkautils.Dispatcher._

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object StockDataProducer {

  def main(args: Array[String]): Unit = {

    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(args.lift(0).getOrElse("3").toInt))
    implicit val kafkaTopic: String = args.lift(1).getOrElse("stock-data-topic")
    val inputFilesDirectory: String = args.lift(2).getOrElse("./inputfilesdirectory")
    val archiveDirectory: String = args.lift(3).getOrElse("./archive")

    val files: List[File] = getListOfFiles(inputFilesDirectory)

    files foreach { file =>

      val sendToKafka = Future {
        readAndSendToKafka(file)
      }

      sendToKafka.onComplete {
        case Success(_) => {
          //println("Success")
          moveFiles(file.getPath, archiveDirectory + "/" + file.getName)
        }
        case Failure(exception) => println(s"Failure ~ ${exception.printStackTrace()}")
      }

      Await.result(sendToKafka, 60 second);
    }
  }

  def readAndSendToKafka(file: File)(implicit kafkaTopic: String): Unit = {

    val dayStockData: List[StockData] = getStockData(file)

    dayStockData
      .flatMap(stockData => List[JsonNode](new ObjectMapper().valueToTree(stockData)))
      .foreach { recValue =>
        val producerRecord = new ProducerRecord[String, JsonNode](kafkaTopic, recValue.hashCode().toString, recValue)
        writeToKafka[String, JsonNode](producerProperties(), producerRecord)
      }
  }

  def getStockData(dataFile: File): List[StockData] = {

    val d: MappingIterator[StockData] = new CsvMapper().readerWithTypedSchemaFor(classOf[StockData]).readValues(dataFile)
    d.readAll().asScala.toList
  }


  def producerProperties(): Properties = {

    val props = new Properties()
    val kafkaConfigStream = classOf[ClassLoader].getResourceAsStream("/kafka.properties")
    props.load(kafkaConfigStream)

    // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[JsonSerializer].getName)

    props
  }
}