package utils

import com.myhexin.micro.credit.etl.spark.streaming.Param
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}


/**
  * Created by Administrator on 2017/5/3.
  */
object HBaseConnectionPool {

  @volatile private var connection:Connection = null;

  private def getConnection(): Connection ={
    if(connection == null){
      synchronized{
        if(connection == null){
          // 创建HBase配置
          val conf = HBaseConfiguration.create()
          conf.set(HConstants.ZOOKEEPER_QUORUM, Param.HBASE_ZK)
          conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Param.HBASE_ZK_PORT)

          connection = ConnectionFactory.createConnection(conf)
        }
      }
    }

    return connection
  }

  def getTable(tableName:String): HTable ={
    return getConnection().getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
  }

  def returnTable(table:Table): Unit ={
    table.close()
  }

  def closeConnection(): Unit ={
    if(connection != null){
      connection.close()
    }
  }

}
