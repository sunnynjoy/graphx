// Databricks notebook source
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.{Graph, VertexRDD}
val follow = GraphLoader.edgeListFile(sc, "/location/7sca2o801494929783152/followers.txt")
// count followers
val inDegrees: VertexRDD[Int] = follow.inDegrees

// count followers
val outDegrees: VertexRDD[Int] = follow.outDegrees

//counting ranking
val ranks = follow.pageRank(0.0001).vertices

//loading users
val users = sc.textFile("/location/7sca2o801494929783152/users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}

//ranks by username
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}

//followers by username
val followersAndFollowingByUsername = users.join(inDegrees).map {
  case (id, (username, inDegrees)) => (username, inDegrees)
}

//following by username
val followingByUsername = users.join(outDegrees).map {
  case (id, (username, outDegrees)) => (username, outDegrees)
}

val inAndOutBound = followersAndFollowingByUsername.join(followingByUsername)

//getting the followers information by its username

println(inAndOutBound.collect().mkString("\n"));

println("\n")

// Print the result
println(ranksByUsername.collect().sortBy(_._2).reverse.mkString("\n"))

println("\n")
