package com.spark.assignment

import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object ProcessOrderData extends Serializable {
  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OrderMatching")
      .getOrCreate()

    val inputParams = CommandHelper.readArgs(args, logger)
    val filePath = inputParams("inputPath")

    case class Order(orderId: Int, userName: String, orderTime: Long, orderType: String, quantity: Int, price: Int)

    // Define the schema for the order data
    val orderSchema = StructType(Seq(
      StructField("orderId", IntegerType, nullable = true),
      StructField("userName", StringType, nullable = true),
      StructField("orderTime", LongType, nullable = true),
      StructField("orderType", StringType, nullable = true),
      StructField("quantity", IntegerType, nullable = true),
      StructField("price", IntegerType, nullable = true)
    ))
    // input path with filename
    val ordersDF = spark.read.schema(orderSchema).csv(filePath).as[Order]

    // Step 3: Create order books
    val buyOrderBook = scala.collection.mutable.Map.empty[Int, Order]
    val sellOrderBook = scala.collection.mutable.Map.empty[Int, Order]

    // Step 4: Process each order
    val matchedOrders = ordersDF.flatMap { order =>
      if (order.orderType == "BUY") {
        val matchingSellOrder = sellOrderBook.values
          .filter(_.quantity == order.quantity).headOption

        if (matchingSellOrder.isDefined) {
          val matchingOrderId = matchingSellOrder.get.orderId
          sellOrderBook.remove(matchingOrderId)

          // Create the match record
          Some((order.orderId, matchingOrderId, order.orderTime, matchingSellOrder.get.quantity, matchingSellOrder.get.price))
        } else {
          // Add the BUY order to the order book
          buyOrderBook += (order.orderId -> order)
          None
        }
      } else if (order.orderType == "SELL") {
        val matchingBuyOrder = buyOrderBook.values
          .filter(_.quantity == order.quantity).headOption

        if (matchingBuyOrder.isDefined) {
          val matchingOrderId = matchingBuyOrder.get.orderId
          buyOrderBook.remove(matchingOrderId)

          // Create the match record
          Some((order.orderId, matchingOrderId, order.orderTime, matchingBuyOrder.get.quantity, matchingBuyOrder.get.price))
        } else {
          // Add the SELL order to the order book
          sellOrderBook += (order.orderId -> order)
          None
        }
      } else {
        None
      }
    }

    val finalOrder = matchedOrders.toDF("BuyerOrderId", "SellerOrderId", "OrderTime",  "Quantity", "Price" )
    finalOrder.show(10, false)

    // Step 7: Stop the SparkSession
    spark.stop()
  }
}
