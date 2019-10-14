import java.io.File
import java.util.Properties
import java.util.concurrent.Executors

import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import org.apache.kafka.clients.producer._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

object Producer {

  def main(args: Array[String] = Array("3", "stock-data-topic")): Unit = {

    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(args(0).toInt))
    implicit val kafkaTopic: String = args(1)

    val files: List[File] = getListOfFiles("./inputfilesdirectory")

    files.map { file =>
      Future {
        readAndSendToKafka(file)
      }
    }

    /*    files.foreach(k => getStocks(k).foreach {
          g =>
            Future {
              writeToKafka(g)
            }
        })*/

    /*    val f: List[Future[Unit]] = files.map { f =>

        }

        f.map {
          _.onComplete {
            case Success(a) => println(s"done  $a")
            case Failure(b) => println(b.getMessage);
          }
        }

        f.map { k =>
          Await.result(k, 2000 second);
        }*/

    Thread.sleep(100000)


  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def readAndSendToKafka(file: File)(implicit kafkaTopic: String) = {
    val dayStockData = getStocks(file)
    dayStockData.map(writeToKafka)
  }

  def getStocks(dataFile: File): List[StockData] = {

    import scala.jdk.CollectionConverters._

    val d: MappingIterator[StockData] = new CsvMapper().readerWithTypedSchemaFor(classOf[StockData]).readValues(dataFile)
    val i = d.readAll()
    i.asScala.toList
  }

  def writeToKafka(stockData: StockData)(implicit kafkaTopic: String): Unit = {

    val props = new Properties()

    import java.io.InputStream
    val kafkaConfigStream = classOf[ClassLoader].getResourceAsStream("/kafka.properties")
    props.load(kafkaConfigStream)

    // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[JsonSerializer[StockData]].getName)
    val producer = new KafkaProducer[String, StockData](props)

    val record: ProducerRecord[String, StockData] = new ProducerRecord[String, StockData](kafkaTopic, stockData.hashCode.toString, stockData)


    println("hahaha : " + stockData)

    Thread.sleep(2000)

    producer.send(record)
    // }
    producer.close()
  }
}