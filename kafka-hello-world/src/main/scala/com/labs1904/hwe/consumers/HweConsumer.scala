package com.labs1904.hwe.consumers

import com.labs1904.hwe.producers.SimpleProducer
import com.labs1904.hwe.util.Util
import com.labs1904.hwe.util.Util.getScramAuthString
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Arrays, Properties, UUID}

object HweConsumer {
  val BootstrapServer : String = "CHANGE_ME"
  val consumerTopic: String = "question-1-updated"
  val producerTopic: String = "question-1-output"
  val username: String = "CHANGE_ME"
  val password: String = "CHANGE_ME"
  val trustStore: String = "CHANGE_ME"
  case class RawUser(id: String, username: String, name: String, sex: String, email: String, birthday: String)
  case class EnrichedUser(id: String, username: String, name: String, sex: String, email: String, birthday: String, numberAsWord: String, hweDeveloper: String = "Brandon Chapple")


  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    val consumerProperties = SimpleConsumer.getProperties(BootstrapServer)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)

    // Create the KafkaProducer
    val producerProperties = SimpleProducer.getProperties(BootstrapServer)
    val producer = new KafkaProducer[String, String](producerProperties)

    // Subscribe to the topic
    consumer.subscribe(Arrays.asList(consumerTopic))

    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.forEach((record: ConsumerRecord[String, String]) => {
        // Retrieve the message from each record
        val message = record.value()
        println(s"Message Received: $message")
        val split = message.split("\t").map(_.trim)
        var rawUser = RawUser(split(0), split(1), split(2), split(3), split(4), split(5))
        val idAsInt = Integer.parseInt(rawUser.id) % 100
        var enrichedUser = EnrichedUser(split(0), split(1), split(2), split(3), split(4), split(5), Integer.toString(idAsInt))


        val userCSV = enrichedUser.id + "," + enrichedUser.username + "," + enrichedUser.name + "," + enrichedUser.sex + "," + enrichedUser.email + "," + enrichedUser.birthday + "," + enrichedUser.numberAsWord + "," + enrichedUser.hweDeveloper
        val producerRecord = new ProducerRecord[String, String](producerTopic, "Enriched User: ", userCSV)
        producer.send(producerRecord)

      })
    }
  }
}