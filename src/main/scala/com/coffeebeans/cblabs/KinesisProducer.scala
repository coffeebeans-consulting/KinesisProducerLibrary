package com.coffeebeans.cblabs

import java.util
import java.util.Properties
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class KinesisProducer[T: ClassTag](streamName: String,
                                   config: Properties,
                                   kinesisQueueLimit: Int = 250,
                                   batchSize: Int = 60,
                                   numberOfConcurrentRequests: Int = 5,
                                   threadPoolSize: Int = 5) extends RichSinkFunction[T] with CheckpointedFunction {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var KINESIS_STREAM_NAME = streamName
  //This can be overridden in the config
  private var kinesisRecords: BlockingQueue[T] = new ArrayBlockingQueue[T](kinesisQueueLimit)
  private var partitioner: String = _
  private var serializer: KinesisSerializer[T] = _
  private var atomicCounter = new AtomicLong()
  private val requestHandlerObjects: BlockingQueue[KinesisRequestHandler] = new ArrayBlockingQueue[KinesisRequestHandler](numberOfConcurrentRequests)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    /*
      Initialize credentials
     */
    for(i <- 0 to numberOfConcurrentRequests-1){
      val requestHandler = new KinesisRequestHandler(config)
      requestHandler.init(KINESIS_STREAM_NAME)
      requestHandlerObjects.add(requestHandler)
    }
    //Start the child threads to batch.
    val threadPool = Executors.newFixedThreadPool(threadPoolSize)
    threadPool.submit(new RequestHandler())
  }

  private class RequestHandler extends Thread {
    val LOGGER = LoggerFactory.getLogger(this.getClass)
    private var recordArray = new util.ArrayList[T]()
    private var rateLimitBackOff: Boolean = false

    override def run(): Unit = {
      while (true) {
        for (i <- 0 to batchSize) {
          recordArray.add(kinesisRecords.take())
        }
        try {
          val requestHandler = requestHandlerObjects.take()
          requestHandler.flushRecordsToKinesis(recordArray, KINESIS_STREAM_NAME, serializer)
          this.LOGGER.debug("Finished flushing records to kinesis")
          rateLimitBackOff = false
          /*
            Flush my list
          */
          recordArray.clear()
          requestHandlerObjects.put(requestHandler)
        } catch {
          case x =>
            this.LOGGER.error(x.getStackTrace.toString)
        }
      }
    }
  }

  override def close(): Unit = {
    super.close()
    LOGGER.info("Closing producer")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    /*
      For now do nothing
     */
  }

  def setPartitioner(partitioner: String): Unit = {
    this.partitioner = partitioner
  }

  def setSerializer(serializer: KinesisSerializer[T]): Unit = {
    this.serializer = serializer
  }

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    /*
      This is called for every event that is passed through the stream. The current implementation keeps adding to a
      queue which waits on a flush method that basically waits for responses from kinesis. This is to avoid throttling
      from the kinesis end.
     */
    kinesisRecords.put(value)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    /*
      For now Do nothing
     */
  }
}
