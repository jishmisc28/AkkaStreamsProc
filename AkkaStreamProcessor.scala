import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

implicit val system = ActorSystem
implicit val materializer = ActorMaterializer

// Set up the Kafka consumer settings
val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
  .withBootstrapServers("localhost:9092")
  .withGroupId("group1")
  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

// Set up the Kafka producer settings
val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
  .withBootstrapServers("localhost:9092")

// Create the Akka streams graph
val graph = GraphDSL.create() { implicit builder =>
  import GraphDSL.Implicits._

  // Set up the Kafka source and sink
  val source = Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
  val sink = Producer.commitableSink(producerSettings)

  // Set up the flow to perform the real-time analytics
  val flow = Flow[ConsumerMessage.CommittableMessage[String, String]]
    .map(msg => {
      // Perform the analytics on the message value
      val result = analyze(msg.record.value)
      // Create a new producer message with the result as the value
      ProducerMessage.Message(new ProducerRecord[String, String]("results", result), msg.committableOffset)
    })

  // Connect the source, flow, and sink
  source ~> flow ~> sink

  // Return the sink as the closed shape
  SinkShape(sink.in)
}

// Run the graph
val runnableGraph = RunnableGraph.fromGraph(graph)
val future = runnableGraph.run()

