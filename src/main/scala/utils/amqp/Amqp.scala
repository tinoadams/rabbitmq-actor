package utils.amqp

import scala.collection.JavaConversions.mapAsJavaMap

import com.rabbitmq.client.Channel

trait AmqpDeclaration {
  def setupChannel(channel: Channel)
}

case class AmqpExchange(
  exchangeName: String,
  exchangeType: String,
  durable: Boolean = false,
  autoDelete: Boolean = false,
  internal: Boolean = false,
  arguments: Option[Map[String, Object]] = None)
  extends AmqpDeclaration {

  def setupChannel(channel: Channel) =
    channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, internal, arguments.map(mapAsJavaMap(_)).getOrElse(null))
}

case class AmqpQueue(
  queueName: String,
  durable: Boolean = false,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  arguments: Option[Map[String, Object]] = None)
  extends AmqpDeclaration {

  def setupChannel(channel: Channel) =
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments.map(mapAsJavaMap(_)).getOrElse(null))
}

case class AmqpBinding(
  exchange: AmqpExchange,
  queue: AmqpQueue,
  routingKey: String,
  arguments: Option[Map[String, Object]] = None)
  extends AmqpDeclaration {

  def setupChannel(channel: Channel) = {
    exchange.setupChannel(channel)
    queue.setupChannel(channel)
    channel.queueBind(queue.queueName, exchange.exchangeName, routingKey, arguments.map(mapAsJavaMap(_)).getOrElse(null))
  }
}
