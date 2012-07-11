package utils.amqp

import org.slf4j.LoggerFactory

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Connection
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException

import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.duration.intToDurationInt
import akka.util.Timeout

trait RabbitMQSender extends Actor {
  def connection: Connection
  def binding: Either[AmqpExchange, AmqpBinding]

  private val logger = LoggerFactory.getLogger(classOf[RabbitMQSender])

  logger.debug("Creating new RabbitMQSender channel: {}", self.path.toString)
  private val channel = connection.createChannel

  // her we either declare an exchange OR an exchange, queue and binding
  private val (exchange, declaration) = binding.fold(exchange => (exchange, exchange), binding => (binding.exchange, binding))
  logger.debug("Event sender declaring: {}", declaration)
  declaration.setupChannel(channel)

  protected def sendMessage(
    body: Array[Byte],
    routingKey: String,
    mandatory: Boolean = false,
    immediate: Boolean = false,
    props: Option[AMQP.BasicProperties] = None) =
    {
      logger.trace("Publishing message with routing key '{}' on exchange '{}'", routingKey, exchange)
      channel.basicPublish(exchange.exchangeName, routingKey, mandatory, immediate, props.getOrElse(null), body)
    }

  override def postStop {
    logger.trace("Closing RabbitMQSender channel: {}", self.path.toString)
    try channel.close() catch { case _ => }
    super.postStop
  }
}

trait RabbitMQReceiver extends Actor {
  def connection: Connection
  def binding: Either[AmqpQueue, AmqpBinding]

  // how long do we wait for incoming messages to be confirmed (ack,reject,accept) by the receiving actor (self type actor)
  def messageHandlingTimeout = Timeout(1 seconds)

  // list of consumer control message
  trait ControlMessage
  case class ConsumeOk(consumerTag: String) extends ControlMessage
  case class CancelOk(consumerTag: String) extends ControlMessage
  case class Cancel(consumerTag: String) extends ControlMessage
  case class RecoverOk(consumerTag: String) extends ControlMessage

  // possible messages to signal channel shutdown
  trait ShutdownMessage
  case class ShutdownSignal(consumerTag: String, sig: ShutdownSignalException) extends ShutdownMessage
  case object UnexpectedShutdown extends ShutdownMessage

  // received message envelope
  case class IncomingMessage(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte])

  // confirmation messages to let the MQ know what to do with the message
  trait ConfirmationResponse
  case class Ack(multiple: Boolean = false) extends ConfirmationResponse
  case class Reject(requeue: Boolean = false) extends ConfirmationResponse
  case object Accept extends ConfirmationResponse

  private val logger = LoggerFactory.getLogger(classOf[RabbitMQReceiver])

  logger.debug("Creating new RabbitMQReceiver channel: {}", self.path.toString)
  private val channel = connection.createChannel

  // her we either declare a queue OR an exchange, queue and binding
  private val (queue, declaration) = binding.fold(queue => (queue, queue), binding => (binding.queue, binding))
  logger.debug("Event receiver declaring: {}", declaration)
  declaration.setupChannel(channel)

  // create a consumer which synchronously sends incoming messages to the actor 
  private val actorConsumer = new DefaultConsumer(channel) {

    override def handleDelivery(
      consumerTag: String,
      envelope: Envelope,
      properties: AMQP.BasicProperties,
      body: Array[Byte]) {
      logger.trace("Received message from queue: {}", queue.queueName)

      // send all msgs to the event handler and wait for a confirmation, blocking
      val result = {
        try {
          val message = new IncomingMessage(consumerTag, envelope, properties, body)
          // actor can die whilst we are waiting which means the channel will be closed...
          val reply = ask(self, message)(messageHandlingTimeout)
          Await.result(reply, messageHandlingTimeout.duration)
        }
        catch {
          case e =>
            logger.error("Error occured during message delivery, requeueing message", e)
            new Reject(true)
        }
      }
      // since we were waiting for the result we need to check if this actor has been terminated and the channel is closed
      if (!channel.isOpen) {
        logger.error("Unable to confirm message '{}' - channel is already closed, actor must be dead", envelope.getDeliveryTag(), result)
        return
      }
      // evaluate the result returned by the handler
      try {
        result match {
          case msg @ Ack(multiple) =>
            logger.trace("Acknowleging '{}' message: {}", msg, envelope.getDeliveryTag())
            channel.basicAck(envelope.getDeliveryTag(), multiple)
          case msg @ Reject(requeue) =>
            logger.trace("Rejecting '{}' message: {}", msg, envelope.getDeliveryTag())
            channel.basicReject(envelope.getDeliveryTag(), requeue)
          case Accept =>
            logger.trace("Message has been accepted by recipient: {}", envelope.getDeliveryTag())
          case msg =>
            logger.trace("Actor response {} for message {} is unknown, dropping message", msg, envelope.getDeliveryTag())
            channel.basicReject(envelope.getDeliveryTag(), false)
        }
      }
      // connection must be gone
      catch {
        case e =>
          logger.error("Unable to confirm message, informing actor", e)
          self ! UnexpectedShutdown
      }
    }

    override def handleConsumeOk(consumerTag: String) {
      logger.trace("Consume ok received, informing actor")
      self ! new ConsumeOk(consumerTag)
    }

    override def handleCancelOk(consumerTag: String) {
      logger.trace("Cancel ok received, informing actor")
      self ! new CancelOk(consumerTag)
    }

    override def handleCancel(consumerTag: String) {
      logger.trace("Cancel received, informing actor")
      self ! new Cancel(consumerTag)
    }

    override def handleRecoverOk(consumerTag: String) {
      logger.trace("Recover ok received, informing actor")
      self ! RecoverOk(consumerTag)
    }

    override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException) {
      logger.trace("Connection shutdown, informing actor")
      self ! new ShutdownSignal(consumerTag, sig)
    }
  }

  // register the consumer at the channel
  channel.basicConsume(queue.queueName, false, self.path.toString, actorConsumer)

  override def postStop {
    logger.trace("Closing RabbitMQReceiver channel: {}", self.path.toString)
    try channel.close() catch { case _ => }
    super.postStop
  }
}
