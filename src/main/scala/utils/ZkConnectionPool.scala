package utils

import com.myhexin.micro.credit.etl.spark.streaming.Param
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}

/**
  * Created by Administrator on 2017/5/22 0022.
  */
object ZkConnectionPool {
  @volatile private var pool: GenericObjectPool[ZkClient] = null

  private def getPool(): GenericObjectPool[ZkClient] = {
    if (null == pool) {
      synchronized {
        if (null == pool) {
          val poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig()
          poolConfig.setMaxTotal(8)
          poolConfig.setMaxIdle(4)
          poolConfig.setMinIdle(2)
          pool = new GenericObjectPool[ZkClient](new ZkConnectionPoolFactory(Param.ZK_HOSTS, 30000, 30000, new ZkCustomSerializer()), poolConfig)
        }
      }
    }
    return pool
  }

  def getZkConnection(): ZkClient = {
    getPool().borrowObject()
  }

  def returnZkConnection(connection: ZkClient): Unit = {
    pool.returnObject(connection)
  }

  def close(): Unit = {
    pool.close()
  }
}

class ZkConnectionPoolFactory(zkServers: String, sessionTimeout: Int, connectionTimeout: Int, zkSerializer: ZkSerializer) extends BasePooledObjectFactory[ZkClient] {

  override def wrap(ZkConnection: ZkClient): PooledObject[ZkClient] = {
    new DefaultPooledObject[ZkClient](ZkConnection)
  }

  override def create(): ZkClient = new ZkClient(zkServers: String, sessionTimeout: Int, connectionTimeout: Int, zkSerializer: ZkSerializer)

  override def validateObject(p: PooledObject[ZkClient]): Boolean = {
    super.validateObject(p)
  }

  override def destroyObject(p: PooledObject[ZkClient]): Unit = {
    p.getObject.close()
  }

}