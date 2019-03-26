package com.coffeebeans.cblabs

import java.nio.ByteBuffer

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

class KinesisSerializer[T](serializer: T => Array[Byte], streamName: String) extends KinesisSerializationSchema[T] {

  override def serialize(t: T): ByteBuffer = {

    ByteBuffer.wrap(serializer(t))
  }

  override def getTargetStream(t: T): String = {
    streamName
  }
}

object KinesisSerializer {

  def toJson[T <: AnyRef](streamName: String, env: StreamExecutionEnvironment)(implicit typeInfo: TypeInformation[T]): KinesisSerializer[T] = {
    def serializer  = (event:T) => {
      implicit val formats = DefaultFormats.preservingEmptyValues
      write[T](event).getBytes
    }

    new KinesisSerializer[T](serializer, streamName)
  }
}
