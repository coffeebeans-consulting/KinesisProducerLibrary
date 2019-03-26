package com.coffeebeans.cblabs

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.amazonaws.services.kinesis.model._
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

class KinesisRequestHandler(config: Properties) {

  private val reentrantLock = new ReentrantLock()
  private var kinesisProducer: AmazonKinesis = _
  val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var rateLimitBackOff: Boolean = false
  private var rateLimitBackoffTime: Long = 800
  private var partitionCounter: AtomicLong = new AtomicLong()
  private var partitioner: String = _

  def init(kinesisStreamName: String) {
    val credentialsProvider = new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = {
        return new BasicAWSCredentials(
          config.getProperty("awsAccessKey"),
          config.getProperty("awsSecret"))
      }

      override def refresh(): Unit = {
        //DO nothing
      }
    }
    try {
      credentialsProvider.getCredentials
    } catch {
      case x: Exception =>
        throw new AmazonClientException(
          "Cannot load the credentials from the credential profiles file. " +
            "Please make sure that your credentials file is at the correct " +
            "location (~/.aws/credentials), and is in valid format.", x)
    }
    /*
      Build kinesis client
     */
    kinesisProducer = AmazonKinesisClientBuilder.standard()
      .withCredentials(credentialsProvider)
      .withRegion(config.getProperty("awsRegion"))
      .build()
    /*
      Check the status of the stream. Apparently it might not be active due to failures sometimes
    */
    val describeStreamRequest = new DescribeStreamRequest().withStreamName(kinesisStreamName)

    try {
      val streamDescription = kinesisProducer.describeStream(describeStreamRequest).getStreamDescription
      this.LOGGER.info("Stream " + kinesisStreamName + " has a status of " + streamDescription.getStreamStatus)

      if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
        waitForStreamToBecomeAvailable(kinesisStreamName)
      }
    } catch {
      case r: ResourceNotFoundException =>
        this.LOGGER.info("Stream " + kinesisStreamName + " not found")
    }
  }

  private def waitForStreamToBecomeAvailable(streamName: String) {
    LOGGER.info("Waiting for " + streamName + " to become ACTIVE")
    val startTime = System.currentTimeMillis
    val endTime = startTime + TimeUnit.MINUTES.toMillis(10)
    while ( {
      System.currentTimeMillis < endTime
    }) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(20))
      try {
        val describeStreamRequest = new DescribeStreamRequest()
        describeStreamRequest.setStreamName(streamName)
        // ask for no more than 10 shards at a time -- this is an optional parameter
        describeStreamRequest.setLimit(10)
        val describeStreamResponse = kinesisProducer.describeStream(describeStreamRequest)
        val streamStatus = describeStreamResponse.getStreamDescription.getStreamStatus
        LOGGER.info("\t- current state: " + streamStatus + "\n")
        if ("ACTIVE" == streamStatus) return
      } catch {
        case ex: ResourceNotFoundException =>
        // ResourceNotFound means the stream doesn't exist yet,
        // so ignore this error and just keep polling.
        case ase: AmazonServiceException =>
          throw ase
      }
    }
    throw new RuntimeException(String.format("Stream %s never became active", streamName))
  }

  private def getDefaultPartitionId: String ={
    if(partitionCounter.get() == 120)
      partitionCounter.set(0)
    val partitionKey = partitionCounter.getAndAdd(1).toString
    partitionKey
  }

  def flushRecordsToKinesis[T: ClassTag](recordArray: util.ArrayList[T],
                                         kinesisStreamName: String,
                                         serializer: KinesisSerializer[T]): Unit = {
    //Lock the producing of data to kinesis
    reentrantLock.lock()
    try {
      var failedRecords: util.ArrayList[PutRecordsRequestEntry] = new util.ArrayList[PutRecordsRequestEntry]()
      this.LOGGER.debug("execute by thread: ", Thread.currentThread().getId)
      val putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(kinesisStreamName);
      val putRecordsRequestEntryList = new util.ArrayList[PutRecordsRequestEntry]()
      for (kinesisRecord <- recordArray.asScala) {
        val partitionId = getDefaultPartitionId
        val putRecordsRequestEntry = new PutRecordsRequestEntry()
        val serializedData = serializer.serialize(kinesisRecord)
        LOGGER.debug("Setting data %s with partition key %s", kinesisRecord, partitionId)
        putRecordsRequestEntry.setData(serializedData)
        putRecordsRequestEntry.setPartitionKey(partitionId)
        putRecordsRequestEntryList.add(putRecordsRequestEntry)
      }
      putRecordsRequest.setRecords(putRecordsRequestEntryList)
      if (putRecordsRequestEntryList.size() > 0) {
        val putRecordsResult = kinesisProducer.putRecords(putRecordsRequest)
        checkRateLimitsAndRetryFailedRecords(putRecordsResult,
          putRecordsRequestEntryList,
          putRecordsRequest,
          failedRecords)
      }
      this.LOGGER.debug("done by thread: ", Thread.currentThread().getId)
    } catch {
      case e =>
        this.LOGGER.error("Failed while putting records with error : \n", e)
    } finally {
      reentrantLock.unlock()
    }
  }

  private def checkRateLimitsAndRetryFailedRecords(putRecordsResult: PutRecordsResult,
                                                   putRecordsRequestEntryList: util.ArrayList[PutRecordsRequestEntry],
                                                   putRecordsRequest: PutRecordsRequest,
                                                   failedRecords: util.ArrayList[PutRecordsRequestEntry]): Unit = {
    failedRecords.clear()
    val resultIterator = putRecordsResult.getRecords.iterator()
    var failedRecordIndex = 0
    while (resultIterator.hasNext) {
      val result = resultIterator.next()
      if (result.getErrorCode != null) {
        if (result.getErrorCode.equals("ProvisionedThroughputExceededException")) {
          rateLimitBackOff = true
        }
        failedRecords.add(putRecordsRequestEntryList.get(failedRecordIndex))
      }
      failedRecordIndex = failedRecordIndex + 1
    }
    /*
      TODO: make this more shard specific. This is fine for a simple test
     */
    if (rateLimitBackOff) {
      this.LOGGER.warn("Rate limit exceeded for shards. Backing of for " + rateLimitBackoffTime + " milliseconds")
      rateLimitBackOff = false
      Thread.sleep(rateLimitBackoffTime)
      retryFailedRecords(putRecordsRequest, failedRecords)
    }
  }

  private def retryFailedRecords(putRecordsRequest: PutRecordsRequest,
                                 failedRecords: util.ArrayList[PutRecordsRequestEntry]): Unit = {
    var attempt = 0L
    this.LOGGER.info("Retrying failed records")
    if (putRecordsRequest.getRecords.size() > 0) {
      while (attempt < 7 && failedRecords.size() > 0) {
        this.LOGGER.debug("Starting attempt " + attempt)
        putRecordsRequest.setRecords(failedRecords)
        val putRecordsResult = kinesisProducer.putRecords(putRecordsRequest)
        this.LOGGER.debug("sent records to kinesis")
        val resultIterator = putRecordsResult.getRecords.iterator()
        var failedRecordIndex = 0
        val recordsRequestList = new util.ArrayList[PutRecordsRequestEntry](failedRecords)
        failedRecords.clear()
        while (resultIterator.hasNext) {
          val result = resultIterator.next()
          this.LOGGER.debug("checking failed records retry state for object %s, for shard %s, for sequence %s", result, result.getShardId, result.getSequenceNumber)
          if (result.getErrorCode != null) {
            if (result.getErrorCode.equals("ProvisionedThroughputExceededException")) {
              rateLimitBackOff = true
            }
            failedRecords.add(recordsRequestList.get(failedRecordIndex))
          }
          failedRecordIndex = failedRecordIndex + 1
        }
        attempt = attempt + 1
        if (rateLimitBackOff) {
          this.LOGGER.warn("Rate limit exceeded for shards. Backing of for " + rateLimitBackoffTime + " milliseconds")
          rateLimitBackOff= false
          Thread.sleep(rateLimitBackoffTime)
        }
      }
    }
    if (failedRecords.size() > 0) {
      /*
        TODO: Maybe we want to throw an exception if the failed record count is too high??
       */
      this.LOGGER.warn("failed to add records. Number of failed records : " + failedRecords.size())
    }
    failedRecords.clear()
  }
}
