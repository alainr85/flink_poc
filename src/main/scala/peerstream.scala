package com.rs.peersource

import java.util.Properties
import _root_.kafka.consumer.ConsumerConfig
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.util.Collector
import scala.collection.mutable
import java.nio.{ByteBuffer, ByteOrder}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object PeerStream {

  def main(args: Array[String]): Unit = {

    /*
     *  Instead of ouputting to Kafka (which is done in the live system)
     *  We're simply going to output to a socket. If you want to see what's being
     *  written netcat the socket and our output will be written to stdout
     * */
    val outputHost = "localhost"
    val outputPort = 9999

    /*
     * Setup Flink
     *
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.enableCheckpointing(1000)

    /* Setup Kafka
     *
     */
    val props = new Properties()
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "peersource")
    props.put("auto.commit.enable", "false")

    val stream = env.addSource(new PersistentKafkaSource[Event]("peersource",
      new EventDeSerializer(),
      new ConsumerConfig(props)))

    /*
    * Configure the stream
    * */

    stream
      .map(JDecoder())
      .filter(LegalBlacklistFilter())
      .filter(GeoCoder())
      .timeWindow(Time.seconds(5))

    stream.writeToSocket(outputHost, outputPort, new SimpleStringSchema())

    // trigger program execution
    env.execute()
  }
}


class EventDeSerializer extends DeserializationSchema[Event] with SerializationSchema[Event, Array[Byte]] {

  override def deserialize(bytes: Array[Byte]): Event = {
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val address: Int = buffer.getInt(0)
    val eventType: Int = buffer.getInt(4)
    Event(address, eventType)
  }

  override def serialize(t: Event): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer.putInt(0, t.sourceAddress)
    byteBuffer.putInt(4, t.event)
    byteBuffer.array()
  }

  override def isEndOfStream(t: Event): Boolean = false

  override def getProducedType: TypeInformation[Event] = {
    createTypeInformation[Event]
  }
}



case class Event(sourceAddress: Int, event: Event.EventType) {

  override def toString: String = {
    s"Event ${Event.formatAddress(sourceAddress)} : ${Event.eventTypeName(event)}"
  }
}


case class Alert(address: Int, state: State, transition: Event.EventType) {

  override def toString: String = {
    s"ALERT ${Event.formatAddress(address)} : ${state.name} -> ${Event.eventTypeName(transition)}"
  }
}


object Event {

  type EventType = Int

  val a : EventType = 1
  val b : EventType = 2
  val c : EventType = 3
  val d : EventType = 4
  val e : EventType = 5
  val f : EventType = 6
  val g : EventType = 7

  def eventTypeName(evt: EventType): String = {
    String.valueOf(('a' + evt - 1).asInstanceOf[Char])
  }

  def formatAddress(address: Int): String = {
    val b1 = (address >>> 24) & 0xff
    val b2 = (address >>> 16) & 0xff
    val b3 = (address >>>  8) & 0xff
    val b4 =  address         & 0xff

    s"$b1.$b2.$b3.$b4"
  }
}
