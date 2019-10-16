# Kafka-Stream
**Kafka Stream - Stock Trade**


How to Run this Application

1) Start the Kafka, for testing I have used local Kafka setup.

Start the broker 

**$ sh kafka-server-start /usr/local/etc/kafka/server.properties**


2) Create two Kafka Topics

`stock-data-topic` - to send all stock data

**sh kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stock-data-topic**


`max-TTV-stock-data-topic` - to send record for the maximum traded value (TOTTRDVAL in the sample data file) for each day

**sh kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic max-TTV-stock-data-topic**


3) Set the `kafka.properties` file according to you Kafka setup


4) Run `StockDataConsumer.scala`'s main method
You may provide the optional parameters in case you want to change the topic names

Example: `stock-data-topic max-TTV-stock-data-topic`


Code snippet

` val readStockDataTopic = args.lift(0).getOrElse("stock-data-topic")
 val sendMaxTTVStockDataTopic = args.lift(1).getOrElse("")`
    
    

_Please note StockDataConsumer.scala code will only be active for 60 seconds, and then send output to max-TTV-stock-data-topic kafka topic._
    
`val deadline = 60.seconds.fromNow
while (deadline.hasTimeLeft) { ...`



5) Run `StockDataProducer.scala`'s main method, there are 4 optional parameters.
a. ThreadPool size
b. Kafka-topic
c. inputFilesDirectory
d. archive directory

Example: `4 stock-data-topic ./inputfilesdirectory ./archive`



-------
For testing in local, you may also run kafka consumer 

**$ sh kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-data-topic --from-beginning**


**$ sh kafka-console-consumer --bootstrap-server localhost:9092 --topic max-TTV-stock-data-topic --from-beginning**
`{"symbol":"ICICIBANK","series":"EQ","open":"367.8","high":381.8,"low":366.6,"close":380.15,"last":379.3,"previousClose":367.7,"totalTradedQty":2.1501136E7,"totalTradedVal":8.0807185982E9,"tradeDate":"08-Jan-2019","totalTrades":180003.0,"isinCode":"INE090A01021"}
{"symbol":"INFY","series":"EQ","open":"686.2","high":689.8,"low":664.0,"close":676.1,"last":675.6,"previousClose":670.05,"totalTradedQty":1.8130292E7,"totalTradedVal":1.22997380748E10,"tradeDate":"09-Jan-2019","totalTrades":314702.0,"isinCode":"INE009A01021"}
{"symbol":"YESBANK","series":"EQ","open":"193.9","high":194.4,"low":185.8,"close":187.15,"last":186.55,"previousClose":189.65,"totalTradedQty":4.0515242E7,"totalTradedVal":7.71776961175E9,"tradeDate":"07-Jan-2019","totalTrades":215186.0,"isinCode":"INE528G01027"}
`
