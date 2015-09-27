//package com.dataman.omega.service.server
//
//import akka.actor._
//import com.dataman.omega.service.actor.PostgresActor
//import com.dataman.omega.service.models.ClusterDAO
//import com.thenewmotion.akka.rabbitmq._
//
///**
//* Created by fchen on 15-8-4.
//*/
//class RabbitMQService extends Actor{
//
////  implicit val system = context.system
////
////  val postgresWorker = context.actorOf(Props[PostgresActor], "postgres-worker")
////
////  def postgresCall(message: Any) =
////    (postgresWorker ? message).mapTo[String]
////
////  def receive = {
////
////  }
////  val factory = new ConnectionFactory()
////  val connection = system.actorOf(ConnectionActor.props(factory), "rabbitmq")
//  val exchange = "amq.fanout"
//
//  def setupSubscriber(channel: Channel, self: ActorRef) {
//    val queue = channel.queueDeclare().getQueue
//    channel.queueBind(queue, exchange, "")
//    val consumer = new DefaultConsumer(channel) {
//      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) {
//        println("received: " + fromBytes(body))
//        ClusterDAO.updateName(1, "abc")
//      }
//    }
//    channel.basicConsume(queue, true, consumer)
//  }
//
//  def start = {
//    connection ! CreateChannel(ChannelActor.props(setupSubscriber), Some("subscriber"))
//  }
//
//  def fromBytes(x: Array[Byte]) = new String(x, "UTF-8")
//}
