package com.dataman.omega.service.utils

import scala.util.Random

/**
 * Created by mymac on 15/10/19.
 */
object RandomLogGenerator {
  def main(args: Array[String]) = {
    val random = new Random()


    case class ImpressionLog(timestamp: Long, website: String, bid: Double, cookie: String)

    println("Sending messages...")
    //var i = 0
    // infinite loop
    while (true) {
      val timestamp = System.currentTimeMillis()
      val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
      val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
      val bid = math.abs(random.nextDouble()) % 1
      val log = ImpressionLog(timestamp, website, bid, cookie)
    //  i = i + 1
      Thread sleep 1000
    //  if (i % 10000 == 0) {
        println(s"Sent $i messages: " + log.toString)
    //  }
    }
  }
  object Constants {
    val NumPublishers = 5
    val NumAdvertisers = 3

    val Publishers = (0 to NumPublishers).map("publisher_" +)
    val Advertisers = (0 to NumAdvertisers).map("advertiser_" +)
    val UnknownGeo = "unknown"
    val Geos = Seq("NY", "CA", "FL", "MI", "HI", UnknownGeo)
    val NumWebsites = 10000
    val NumCookies = 10000

  }
}
