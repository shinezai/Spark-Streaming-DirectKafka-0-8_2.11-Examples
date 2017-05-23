package utils

import java.util

import com.myhexin.micro.credit.etl.spark.streaming.Param
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

/**
  * Created by Administrator on 2017/5/3.
  */
object KafkaProducerPool {

  @volatile private var pool: GenericObjectPool[KafkaProducer[String, String]] = null

  private def getPool(): GenericObjectPool[KafkaProducer[String, String]] = {
    if (null == pool) {
      synchronized {
        if (null == pool) {
          val poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig()
          poolConfig.setMaxTotal(8)
          poolConfig.setMaxIdle(4)
          poolConfig.setMinIdle(2)

          pool = new GenericObjectPool[KafkaProducer[String, String]](new KafkaProducerPoolFactory[String, String](kafkaProducerConf()), poolConfig)
        }
      }
    }
    return pool
  }

  def getKafkaProducer(): KafkaProducer[String, String] = {
    getPool().borrowObject()
  }

  def returnKafkaProducer(producer: KafkaProducer[String, String]): Unit = {
    pool.returnObject(producer)
  }

  def close(): Unit = {
    pool.close()
  }

  /**
    * kafkaProducerConf
    *
    * @return
    */
  def kafkaProducerConf(): util.HashMap[String, Object] = {
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Param.BROKERS)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

}

class KafkaProducerPoolFactory[K, V](conf: util.Map[String, Object]) extends BasePooledObjectFactory[KafkaProducer[K, V]] {

  override def wrap(kafkaProducer: KafkaProducer[K, V]): PooledObject[KafkaProducer[K, V]] = {
    new DefaultPooledObject[KafkaProducer[K, V]](kafkaProducer)
  }

  override def create(): KafkaProducer[K, V] = {
    new KafkaProducer[K, V](conf)
  }

  override def validateObject(p: PooledObject[KafkaProducer[K, V]]): Boolean = {
    super.validateObject(p)
  }

  override def destroyObject(p: PooledObject[KafkaProducer[K, V]]): Unit = {
    p.getObject.close()
  }

}
