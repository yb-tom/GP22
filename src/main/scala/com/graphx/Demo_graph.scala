package com.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo_graph {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec","snappy")
      .appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //创建一个顶点的集合
    val users: RDD[(Long, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    //创建一个边的集合
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    //创建一个图
    val defaultUser = ("John Doe", "Missing")
    val graph = Graph(users, relationships, defaultUser)

    val verticesCount = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(verticesCount)

    val edgeCount = graph.edges.filter(e => e.srcId > e.dstId).count
    println(edgeCount)
  }
}
