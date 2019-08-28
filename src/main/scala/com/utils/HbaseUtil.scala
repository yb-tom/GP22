package com.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object HbaseUtil {
//  val conf: Configuration = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.quorum","192.168.183.17")
//  conf.set("hbase.zookeeper.property.clientPort","2181")


  def getHBaseConf(quorum:String="192.168.183.17", port:String="2181") = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)

    conf
  }

  def getHBaseConnection(conf: Configuration) = {

    val connection: Connection = ConnectionFactory.createConnection(conf)
    connection
  }

  def isTableExits(admin:HBaseAdmin,table:String)={
    admin.tableExists(table)
  }

  def getHBaseAdmin(conn:Connection,tableName:String,columnFamily:String*) = {
    val admin: HBaseAdmin = (conn.getAdmin).asInstanceOf[HBaseAdmin]
    if(!isTableExits(admin,tableName)){
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      for(cf<- columnFamily){
        tableDesc.addFamily(new HColumnDescriptor(cf))
      }
      admin.createTable(tableDesc)
      println("表"+tableName+"创建成功！")
    }

    admin
  }

  def addRowData(conn:Connection,tableName:String,rowKey:String,columnFamily:String,column:String,value:String) = {
    val table: HTable = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
    val put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value))
//    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value))

    table.put(put)
    table.close()
    println("插入数据成功")
  }
}
