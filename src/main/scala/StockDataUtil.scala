import java.util.Date

class StockDataUtil {

  var dateMaxRecord: List[(Date, StockData)] = Nil

  def dateMaxRecord(batchMaxRecord: List[(Date, StockData)]): Unit = {
    dateMaxRecord = (batchMaxRecord ::: dateMaxRecord)
      .groupBy(_._1)
      .toList
      .map(_._2)
      .map(maxTotTrdVal)
  }

  def maxTotTrdVal(listStockData: List[(Date, StockData)]): (Date, StockData) = {

    listStockData.reduceLeft { (s1, s2) =>
      if (s1._2.getTotalTradedVal > s2._2.getTotalTradedVal) s1
      else s2
    }
  }

}
