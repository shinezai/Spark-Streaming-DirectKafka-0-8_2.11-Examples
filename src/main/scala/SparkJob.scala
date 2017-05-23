
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.I0Itec.zkclient.ZkClient

import scala.util.{Failure, Success, Try}

/**
  * Created by Administrator on 2017/4/6.
  */
object SparkJob {
  def main(args: Array[String]): Unit = {
    val sparkJob = new SparkJob()
    sparkJob.runJob()
  }
}

class SparkJob extends Serializable {
  val logger = LogFactory.getLog(SparkJob.getClass)

  def runJob(): Unit = {
    val ssc:StreamingContext = createSparkStreamingContext()
    val kafkaStream = createKafakaDirectStreamWithZk(ssc)
    processKafkaStreamWithZk(kafkaStream)
    ssc.start()
    ssc.awaitTermination()
  }

  //zhuxian
  private def createSparkStreamingContext(): StreamingContext = {
    val sc = new SparkConf().setAppName(Param.JOB_NAME)
    //add gracefully stop
    sc.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //Setting the max receiving rate
    sc.set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc
  }

  def createKafakaDirectStreamWithZk(ssc: StreamingContext): InputDStream[(String, String)] = {
    val topics = Set[String](Param.TOPIC_INPUT)
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> Param.ZK_CONNECT,
      "group.id" -> Param.GROUP,
      "bootstrap.servers" -> Param.BROKERS
    )
    val topic = topics.last
    val storedOffsets = getFromOffsetsFromZk(topic)
    val kafkaStream = storedOffsets match {
      case None =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    kafkaStream
  }

  def getFromOffsetsFromZk(topic:String): Option[Map[TopicAndPartition, Long]] = {
    val zkHosts = Param.ZK_HOSTS
    val zkClient = new ZkClient(zkHosts, 30000, 30000, new ZkCustomSerializer())
    val zkPathOffset:String = "/kafka/consumers/"+Param.GROUP+"/offsets/"+Param.TOPIC_INPUT
    logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val numChild = zkClient.countChildren(zkPathOffset)
    numChild match {
      case 0 =>
        logger info(s"No child found in $zkPathOffset" + stopwatch)
        None
      case _ =>
        val zkChildren = zkClient.getChildren(zkPathOffset)
        logger.info(s"zkChildren is : $zkChildren")
        val zkChildrenArr = zkChildren.toArray
        logger.info(s"zkChildrenArr is : $zkChildrenArr")
        val offsets = zkChildrenArr.map(child => TopicAndPartition(topic, child.toString.toInt) -> zkClient.readData(zkPathOffset+"/"+child.toString).toString.toLong).toMap
        logger.info(s"readOffsets offsets are: $offsets")
        Some(offsets)
    }
  }

  def processKafkaStreamWithZk(kafkaStream:InputDStream[(String, String)]) = {
    kafkaStream.foreachRDD{ rdd =>
      // executed at the driver
      logger.info("1. execute in kafkaStream.foreachRDD")
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      printOffsets(offsetRanges)

      rdd.foreachPartition { iterator =>
        // executed at the worker
        val offset: OffsetRange = offsetRanges(TaskContext.get().partitionId())
        printOffsets(Array(offset))
        //map and filter
        val partitionIterator = transformData(iterator)
        //save data and offset
        saveData(partitionIterator, ()=>saveKafkaOffsetsToZk(offset))
      }
    }
  }

  protected def saveData(partitionIterator:Iterator[CreditMessage], saveKafkaOffsets:()=>Unit): Unit = {
    //save message to hbase and kafka
    logger.info(s"2.3  execute in saveData(). is partitionIterator empty ?: ${partitionIterator.isEmpty}")
    if(partitionIterator.isEmpty){
      logger.info("2.4 execute in saveData(). partitionIterator is empty. return")
      return
    }

    logger.info("2.4 execute in get connection from hbase connection pool and kafka producer connection pool ")
    var producer: KafkaProducer[String, String] = null
    var table: HTable = null
    try {
      table = HBaseConnectionPool.getTable(Param.HBASE_TABLE_NAME)
      producer = KafkaProducerPool.getKafkaProducer()
      partitionIterator.foreach {
        message =>
          logger.info("3. execute in partitionIterator.foreach ")
          sendToHBase(message, table)
          sendToKafka(message, producer)
      }
      saveKafkaOffsets()
    } catch {
      case e: Exception =>
        logger.info("2.7 execute in process Exception")
        logger.error("error when send message", e)
    } finally {
      logger.info("2.8 execute in begin to execute finally")
      HBaseConnectionPool.returnTable(table)
      KafkaProducerPool.returnKafkaProducer(producer)
    }
  }
  protected def saveKafkaOffsetsToZk(offset : OffsetRange): Unit = {
    logger.info(s"2.5 execute in saveKafkaOffsetsToZk(). before update zookeeper ")
    val zkPathOffset:String = "/kafka/consumers/"+Param.GROUP+"/offsets/"+Param.TOPIC_INPUT+"/"+offset.partition.toString
    var ZkConnect:ZkClient = null
    val fromOffsetStr = offset.fromOffset.toString
    try {
      ZkConnect = ZkConnectionPool.getZkConnection()
      ZkUtils.updatePersistentPath(ZkConnect, zkPathOffset, fromOffsetStr)
      logger.info(s"2.6 execute in saveKafkaOffsetsToZk(). after update zookeeper")
    }catch {
      case e: Exception =>
        logger.info("data save to zookeeper failed")
    }finally {
      ZkConnectionPool.returnZkConnection(ZkConnect)
    }
  }
  protected def transformData(iterator: Iterator[(String, String)]): Iterator[CreditMessage] = {
    //don't call iterator.size() otherwise the data will lose
    logger.info(s"2. execute in rdd.foreachPartition. is iterator empty?  ${iterator.isEmpty}")
    iterator.map(_._2).map { record =>
      logger.info("2.1 execute in iterator.map")
      println(record)
      Try(JSON.parseObject(record, classOf[CreditMessage])) match {
        case Success(creditMessage: CreditMessage) => creditMessage
        case Failure(e: Throwable) =>
          logger.error(s"error when parse json: $record", e)
          null
      }
    }.filter { obj =>
      logger.info("2.2 execute in iterator.filter")
      Try(obj != null && obj.getContent != null) match {
        case Success(_) => true
        case _ =>
          println("filter error")
          false
      }
    }
  }

  private def printOffsets(offsetRanges: Array[OffsetRange]): Unit = {
    for (o <- offsetRanges) {
      println(s"time:${System.currentTimeMillis()} topic:${o.topic} partition:${o.partition} fromOffset:${o.fromOffset} untilOffset:${o.untilOffset}")
    }
  }

  class Stopwatch {
    private val start = System.currentTimeMillis()
    override def toString() = (System.currentTimeMillis() - start) + "ms"
  }

  /**
    * send data to hbase
    *
    * @param data
    * @param table
    */
  def sendToHBase(data: CreditMessage, table: HTable): Unit = {
    val rowKey = data.getId_card + "|" + data.getType + "|" + data.getBusiness_number
    val put = new Put(Bytes.toBytes(MD5Utils.md5_16bit(rowKey)))
    put.addColumn(Param.HBASE_TABLE_FAMILY.getBytes,
      "report".getBytes(), //qualifier
      JSON.toJSONString(data.getContent().getEx_data,
        SerializerFeature.QuoteFieldNames,
        SerializerFeature.WriteMapNullValue,
        SerializerFeature.WriteNullStringAsEmpty)
        .getBytes())
    table.put(put)
  }

  /**
    * send data to kafka
    *
    * @param data
    * @param producer
    */
  def sendToKafka(data: CreditMessage, producer: KafkaProducer[String, String]): Unit = {
    val dataStatusObject = new DataStatusObject
    dataStatusObject.setBusinessNumber(data.getBusiness_number)
    dataStatusObject.setTenantId(data.getTenant_id)
    dataStatusObject.setType(data.getType)
    dataStatusObject.setStatus("1")
    val sendKey = data.getId_card + "|" + data.getType
    val record = new ProducerRecord[String, String](Param.TOPIC_OUTPUT, sendKey, JSON.toJSON(dataStatusObject).toString)
    producer.send(record)
  }

}

